//! TCP transport: client connection pool and server accept loop.

use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};
use serde_bytes::ByteBuf;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
};
use tokio_util::{codec::LengthDelimitedCodec, sync::CancellationToken};

use crate::{
    dispatch::{DispatchTable, DynHandler, InboundKind},
    messaging::protocol::{self, Frame, RequestFrame, RequestKind, ResponseFrame, WireError},
};

/// An error which can occur when sending a request over the transport.
#[derive(Debug)]
pub(crate) enum TransportError {
    Connect(io::Error),
    ConnectionClosed,
    NodeShutdown,
    ReplyTimeout,
    Remote(WireError),
}

type PendingReplies = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<ByteBuf, WireError>>>>>;

/// One pooled connection slot per remote address. Dialing holds only this slot's lock,
/// so an unreachable peer never stalls sends to other peers.
type Slot = Arc<tokio::sync::Mutex<Option<Connection>>>;

/// A pool of lazily-established connections, one per remote messaging address.
#[derive(Clone)]
pub(crate) struct ConnectionPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    // Sync lock guarding the slot map only; never held across an await.
    slots: Mutex<HashMap<SocketAddr, Slot>>,
    closed: AtomicBool,
    connect_timeout: Duration,
    default_reply_timeout: Duration,
    max_frame_len: usize,
}

#[derive(Clone)]
struct Connection {
    outbound: mpsc::Sender<Frame>,
    pending: PendingReplies,
    next_request_id: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
}

impl ConnectionPool {
    pub(crate) fn new(
        connect_timeout: Duration,
        default_reply_timeout: Duration,
        max_frame_len: usize,
    ) -> Self {
        ConnectionPool {
            inner: Arc::new(PoolInner {
                slots: Mutex::new(HashMap::new()),
                closed: AtomicBool::new(false),
                connect_timeout,
                default_reply_timeout,
                max_frame_len,
            }),
        }
    }

    pub(crate) fn default_reply_timeout(&self) -> Duration {
        self.inner.default_reply_timeout
    }

    /// Closes the pool: connections are torn down and further requests fail.
    ///
    /// Dropping the pooled connections drops their outbound senders, which ends the
    /// writer and reader tasks and closes the sockets.
    pub(crate) fn shutdown(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);
        self.inner.slots.lock().unwrap().clear();
    }

    /// Sends a request and waits for the correlated response: the reply for asks, the
    /// delivery ack for tells.
    pub(crate) async fn request(
        &self,
        addr: SocketAddr,
        mut req: RequestFrame,
        timeout: Duration,
    ) -> Result<ByteBuf, TransportError> {
        let conn = self.get_or_connect(addr).await?;
        let request_id = conn.next_request_id.fetch_add(1, Ordering::Relaxed) + 1;
        req.request_id = Some(request_id);

        let (tx, rx) = oneshot::channel();
        conn.pending.lock().unwrap().insert(request_id, tx);

        if conn.outbound.send(Frame::Request(req)).await.is_err() {
            conn.pending.lock().unwrap().remove(&request_id);
            return Err(TransportError::ConnectionClosed);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(bytes))) => Ok(bytes),
            Ok(Ok(Err(err))) => Err(TransportError::Remote(err)),
            Ok(Err(_)) => Err(TransportError::ConnectionClosed),
            Err(_) => {
                conn.pending.lock().unwrap().remove(&request_id);
                Err(TransportError::ReplyTimeout)
            }
        }
    }

    /// Queues a request without expecting any response (fire-and-forget).
    pub(crate) async fn enqueue(
        &self,
        addr: SocketAddr,
        mut req: RequestFrame,
    ) -> Result<(), TransportError> {
        let conn = self.get_or_connect(addr).await?;
        req.request_id = None;
        conn.outbound
            .send(Frame::Request(req))
            .await
            .map_err(|_| TransportError::ConnectionClosed)
    }

    async fn get_or_connect(&self, addr: SocketAddr) -> Result<Connection, TransportError> {
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(TransportError::NodeShutdown);
        }
        let slot = {
            let mut slots = self.inner.slots.lock().unwrap();
            slots.entry(addr).or_default().clone()
        };
        // Only this address's slot is locked across the dial; concurrent requests to the
        // same address queue behind a single dial, other addresses are unaffected.
        let mut guard = slot.lock().await;
        if let Some(conn) = guard.as_ref()
            && !conn.closed.load(Ordering::Relaxed)
        {
            return Ok(conn.clone());
        }
        let conn = self.connect(addr).await?;
        *guard = Some(conn.clone());
        Ok(conn)
    }

    async fn connect(&self, addr: SocketAddr) -> Result<Connection, TransportError> {
        let stream = tokio::time::timeout(self.inner.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                TransportError::Connect(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "connect timed out",
                ))
            })?
            .map_err(TransportError::Connect)?;
        stream.set_nodelay(true).map_err(TransportError::Connect)?;

        let framed = LengthDelimitedCodec::builder()
            .max_frame_length(self.inner.max_frame_len)
            .new_framed(stream);
        let (mut sink, mut stream) = framed.split();

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(1024);
        let pending: PendingReplies = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));

        // Writer task: drain outbound frames into the socket.
        let writer_closed = closed.clone();
        tokio::spawn(async move {
            while let Some(frame) = outbound_rx.recv().await {
                let bytes = match protocol::encode(&frame) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        tracing::warn!("failed to encode frame: {err}");
                        continue;
                    }
                };
                if let Err(err) = sink.send(Bytes::from(bytes)).await {
                    tracing::debug!("connection write failed: {err}");
                    break;
                }
            }
            writer_closed.store(true, Ordering::Relaxed);
        });

        // Reader task: route responses to their pending oneshot by request id.
        let reader_pending = pending.clone();
        let reader_closed = closed.clone();
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                let bytes = match result {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        tracing::debug!("connection read failed: {err}");
                        break;
                    }
                };
                match protocol::decode(&bytes) {
                    Ok(Frame::Response(res)) => {
                        let tx = reader_pending.lock().unwrap().remove(&res.request_id);
                        if let Some(tx) = tx {
                            let _ = tx.send(res.result);
                        }
                    }
                    Ok(Frame::Request(_)) => {
                        tracing::warn!("unexpected request frame on client connection");
                    }
                    Err(err) => {
                        tracing::warn!("failed to decode frame: {err}");
                        break;
                    }
                }
            }
            reader_closed.store(true, Ordering::Relaxed);
            // Fail all in-flight asks by dropping their reply senders; new requests
            // will reconnect lazily.
            reader_pending.lock().unwrap().clear();
        });

        Ok(Connection {
            outbound: outbound_tx,
            pending,
            next_request_id: Arc::new(AtomicU64::new(0)),
            closed,
        })
    }
}

/// Accepts inbound connections and dispatches their requests until cancelled.
pub(crate) async fn run_server(
    listener: TcpListener,
    dispatch: Arc<DispatchTable>,
    cancel: CancellationToken,
    max_frame_len: usize,
    max_concurrent_requests: usize,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => match result {
                Ok((stream, _)) => {
                    tokio::spawn(handle_conn(
                        stream,
                        dispatch.clone(),
                        cancel.child_token(),
                        max_frame_len,
                        max_concurrent_requests,
                    ));
                }
                Err(err) => tracing::warn!("failed to accept connection: {err}"),
            }
        }
    }
}

async fn handle_conn(
    stream: TcpStream,
    dispatch: Arc<DispatchTable>,
    cancel: CancellationToken,
    max_frame_len: usize,
    max_concurrent_requests: usize,
) {
    if let Err(err) = stream.set_nodelay(true) {
        tracing::debug!("failed to set nodelay: {err}");
    }
    let framed = LengthDelimitedCodec::builder()
        .max_frame_length(max_frame_len)
        .new_framed(stream);
    let (mut sink, mut stream) = framed.split();

    // Replies come from concurrently spawned request tasks, so they are funnelled
    // through a channel to a single writer. Sized to the request cap so replies never
    // backpressure workers before the request semaphore does.
    let (reply_tx, mut reply_rx) = mpsc::channel::<ResponseFrame>(max_concurrent_requests);
    tokio::spawn(async move {
        while let Some(res) = reply_rx.recv().await {
            let bytes = match protocol::encode(&Frame::Response(res)) {
                Ok(bytes) => bytes,
                Err(err) => {
                    tracing::warn!("failed to encode response frame: {err}");
                    continue;
                }
            };
            if let Err(err) = sink.send(Bytes::from(bytes)).await {
                tracing::debug!("connection write failed: {err}");
                break;
            }
        }
    });

    // Caps concurrently processed requests for this connection. The read loop stops
    // pulling frames while no permits are available, so overload propagates as TCP
    // backpressure to the sender instead of unbounded task spawning.
    let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));

    // One FIFO worker per target actor, so messages from this connection to a given
    // actor are delivered in arrival order, while different target actors are
    // processed concurrently. Idle workers are swept periodically so a long-lived
    // connection does not accumulate tasks for actors it no longer messages.
    let mut workers: HashMap<u64, Worker> = HashMap::new();
    let mut idle_sweep = tokio::time::interval(WORKER_IDLE_SWEEP_INTERVAL);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = idle_sweep.tick() => {
                // Only fully drained workers are dropped: this loop is the sole
                // sender, so processed == sent means nothing is queued or in flight
                // and removal cannot reorder messages. The next request respawns one.
                workers.retain(|_, worker| {
                    worker.processed.load(Ordering::Relaxed) != worker.sent
                });
            }
            frame = stream.next() => {
                let bytes = match frame {
                    None => break,
                    Some(Err(err)) => {
                        tracing::warn!("connection read failed: {err}");
                        break;
                    }
                    Some(Ok(bytes)) => bytes,
                };
                match protocol::decode(&bytes) {
                    // Resolved inline so dispatch errors reply immediately and workers
                    // only ever exist for registered actors.
                    Ok(Frame::Request(req)) => match dispatch.resolve(&req) {
                        Ok(handler) => {
                            let permit = tokio::select! {
                                _ = cancel.cancelled() => break,
                                permit = semaphore.clone().acquire_owned() => {
                                    permit.expect("semaphore is never closed")
                                }
                            };
                            let sequence_id = req.target_sequence_id;
                            let worker = workers.entry(sequence_id).or_insert_with(|| {
                                spawn_worker(reply_tx.clone(), max_concurrent_requests)
                            });
                            // Never blocks: queued items hold permits, so a worker
                            // queue can never exceed its capacity.
                            if let Err(err) = worker.tx.send((handler, req, permit)).await {
                                // The worker died (e.g. a serialize impl panicked in a
                                // handler); replace it so the actor stays reachable.
                                tracing::warn!("request worker exited unexpectedly; restarting it");
                                let mut worker =
                                    spawn_worker(reply_tx.clone(), max_concurrent_requests);
                                if worker.tx.send(err.0).await.is_ok() {
                                    worker.sent += 1;
                                }
                                workers.insert(sequence_id, worker);
                            } else {
                                worker.sent += 1;
                            }
                        }
                        Err(err) => match req.request_id {
                            Some(request_id) => {
                                let _ = reply_tx
                                    .send(ResponseFrame {
                                        request_id,
                                        result: Err(err),
                                    })
                                    .await;
                            }
                            None => tracing::warn!("tell dispatch failed: {err:?}"),
                        },
                    },
                    Ok(Frame::Response(_)) => {
                        tracing::warn!("unexpected response frame on server connection");
                    }
                    Err(err) => {
                        tracing::warn!("failed to decode frame: {err}");
                        break;
                    }
                }
            }
        }
    }
    // Dropping the worker map ends the workers once their queues drain.
}

type WorkItem = (DynHandler, RequestFrame, OwnedSemaphorePermit);

const WORKER_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// A per-target-actor FIFO worker, with counters tracking whether it is drained.
struct Worker {
    tx: mpsc::Sender<WorkItem>,
    sent: u64,
    processed: Arc<AtomicU64>,
}

fn spawn_worker(reply_tx: mpsc::Sender<ResponseFrame>, capacity: usize) -> Worker {
    let (tx, mut rx) = mpsc::channel::<WorkItem>(capacity);
    let processed = Arc::new(AtomicU64::new(0));
    let counter = processed.clone();
    tokio::spawn(async move {
        while let Some((handler, req, permit)) = rx.recv().await {
            handle_request(handler, req, &reply_tx).await;
            drop(permit);
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
    Worker {
        tx,
        sent: 0,
        processed,
    }
}

async fn handle_request(
    handler: DynHandler,
    req: RequestFrame,
    reply_tx: &mpsc::Sender<ResponseFrame>,
) {
    let request_id = req.request_id;
    let kind = match req.kind {
        RequestKind::Ask => InboundKind::Ask {
            reply_timeout: req.reply_timeout_ms.map(Duration::from_millis),
        },
        RequestKind::Tell => InboundKind::Tell,
    };
    // Contained so a panicking handler (e.g. in a user serialize impl) cannot kill
    // the worker and drop the requests queued behind it.
    let result = match std::panic::AssertUnwindSafe(handler(req.payload.into_vec(), kind))
        .catch_unwind()
        .await
    {
        Ok(result) => result,
        Err(_) => {
            tracing::error!(
                "handler for {:?} on actor {:?} panicked",
                req.message_remote_id,
                req.actor_remote_id
            );
            // Panics are not part of the wire vocabulary; an asking caller times out.
            return;
        }
    };
    match request_id {
        Some(request_id) => {
            let result = result.map(|reply| ByteBuf::from(reply.unwrap_or_default()));
            let _ = reply_tx.send(ResponseFrame { request_id, result }).await;
        }
        None => {
            if let Err(err) = result {
                tracing::warn!("tell dispatch failed: {err:?}");
            }
        }
    }
}
