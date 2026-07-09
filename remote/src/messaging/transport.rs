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
use futures::{SinkExt, StreamExt};
use serde_bytes::ByteBuf;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::LengthDelimitedCodec, sync::CancellationToken};

use crate::{
    dispatch::{DispatchTable, InboundKind},
    messaging::protocol::{self, Frame, RequestFrame, ResponseFrame, WireError},
};

/// An error which can occur when sending a request over the transport.
#[derive(Debug)]
pub(crate) enum TransportError {
    Connect(io::Error),
    ConnectionClosed,
    ReplyTimeout,
    Remote(WireError),
}

type PendingReplies = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<ByteBuf, WireError>>>>>;

/// A pool of lazily-established connections, one per remote messaging address.
#[derive(Clone)]
pub(crate) struct ConnectionPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    connections: tokio::sync::Mutex<HashMap<SocketAddr, Connection>>,
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
                connections: tokio::sync::Mutex::new(HashMap::new()),
                connect_timeout,
                default_reply_timeout,
                max_frame_len,
            }),
        }
    }

    pub(crate) fn default_reply_timeout(&self) -> Duration {
        self.inner.default_reply_timeout
    }

    /// Sends an ask request and waits for the correlated response.
    pub(crate) async fn ask(
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

    /// Sends a tell request; returns once the frame is queued to the writer.
    pub(crate) async fn tell(
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
        // The pool lock is held across connect, serialising new dials; acceptable for v1.
        let mut connections = self.inner.connections.lock().await;
        if let Some(conn) = connections.get(&addr) {
            if !conn.closed.load(Ordering::Relaxed) {
                return Ok(conn.clone());
            }
            connections.remove(&addr);
        }

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

        let conn = Connection {
            outbound: outbound_tx,
            pending,
            next_request_id: Arc::new(AtomicU64::new(0)),
            closed,
        };
        connections.insert(addr, conn.clone());
        Ok(conn)
    }
}

/// Accepts inbound connections and dispatches their requests until cancelled.
pub(crate) async fn run_server(
    listener: TcpListener,
    dispatch: Arc<DispatchTable>,
    cancel: CancellationToken,
    max_frame_len: usize,
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
) {
    if let Err(err) = stream.set_nodelay(true) {
        tracing::debug!("failed to set nodelay: {err}");
    }
    let framed = LengthDelimitedCodec::builder()
        .max_frame_length(max_frame_len)
        .new_framed(stream);
    let (mut sink, mut stream) = framed.split();

    // Replies come from concurrently spawned request tasks, so they are funnelled
    // through a channel to a single writer.
    let (reply_tx, mut reply_rx) = mpsc::channel::<ResponseFrame>(1024);
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

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
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
                    Ok(Frame::Request(req)) => {
                        let dispatch = dispatch.clone();
                        let reply_tx = reply_tx.clone();
                        tokio::spawn(handle_request(req, dispatch, reply_tx));
                    }
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
}

async fn handle_request(
    req: RequestFrame,
    dispatch: Arc<DispatchTable>,
    reply_tx: mpsc::Sender<ResponseFrame>,
) {
    let request_id = req.request_id;
    let kind = match request_id {
        Some(_) => InboundKind::Ask {
            reply_timeout: req.reply_timeout_ms.map(Duration::from_millis),
        },
        None => InboundKind::Tell,
    };
    let result = match dispatch.resolve(&req) {
        Ok(handler) => handler(req.payload.into_vec(), kind).await,
        Err(err) => Err(err),
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
