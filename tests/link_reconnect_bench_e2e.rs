#![cfg(all(feature = "remote", feature = "remote_integration_tests"))]
//! End-to-end regression: remote link death triggers reconnect, and the bench continues.
//!
//! This mirrors the two-terminal "link" demos, but runs in a deterministic test:
//! - Client looks up a distributed actor on the server.
//! - Client links a local actor to the remote ref.
//! - We forcibly drop the TCP/TLS connection.
//! - `on_link_died(PeerDisconnected)` fires on the local actor, which performs a reconnect
//!   (refresh_force_new) and updates the shared remote ref.
//! - The ask benchmark continues and completes.

use std::{
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kameo::{Actor, Spawn, distributed_actor};
use kameo::remote::{DistributedActorRef, v2_bootstrap};
use tokio::sync::{Mutex, Notify, watch};
use tokio::time::{Duration, sleep, timeout};
use tracing_subscriber::EnvFilter;

#[derive(kameo::RemoteMessage, Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Ping {
    value: u64,
}

#[derive(kameo::Actor)]
struct PingActor {
    calls: Arc<AtomicUsize>,
}

impl kameo::message::Message<Ping> for PingActor {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: Ping,
        _ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.calls.fetch_add(1, Ordering::AcqRel);
        msg.value + 1
    }
}

distributed_actor! {
    PingActor {
        Ping,
    }
}

struct LinkReconnectClient {
    remote: Arc<Mutex<DistributedActorRef<PingActor>>>,
    refreshed: watch::Sender<u64>,
    link_died: Arc<Notify>,
    started: Arc<Notify>,
}

#[derive(Debug, Clone)]
struct Nop;

impl kameo::message::Message<Nop> for LinkReconnectClient {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: Nop,
        _ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> Self::Reply {
    }
}

impl Actor for LinkReconnectClient {
    type Args = Self;
    type Error = kameo::error::Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: kameo::actor::ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        tracing::warn!("test: LinkReconnectClient started");
        state.started.notify_waiters();
        Ok(state)
    }

    fn on_link_died(
        &mut self,
        actor_ref: kameo::actor::WeakActorRef<Self>,
        _id: kameo::actor::ActorId,
        reason: kameo::error::ActorStopReason,
    ) -> impl std::future::Future<
        Output = Result<ControlFlow<kameo::error::ActorStopReason>, Self::Error>,
    > + Send {
        let remote = self.remote.clone();
        let refreshed = self.refreshed.clone();
        let link_died = self.link_died.clone();

        async move {
            if !matches!(reason, kameo::error::ActorStopReason::PeerDisconnected) {
                return Ok::<_, kameo::error::Infallible>(ControlFlow::Break(reason));
            }

            tracing::warn!(reason = ?reason, "test: LinkReconnectClient on_link_died");
            link_died.notify_waiters();

            let Some(me) = actor_ref.upgrade() else {
                return Ok::<_, kameo::error::Infallible>(ControlFlow::Continue(()));
            };

            // Reconnect by forcing a new underlying socket on the existing ref.
            //
            // IMPORTANT: Do not call `DistributedActorRef::lookup()` here. That uses the global
            // transport singleton, which can be overwritten in integration tests that run multiple
            // transports (server + client) in a single process.
            for _ in 0..200 {
                let current = { remote.lock().await.clone() };
                // We are in a known-disconnect path. Always force a new socket/IO task so we
                // don't "succeed" by reusing a zombie handle that isn't marked closed yet.
                let refresh_res = current.refresh_force_new().await;
                match refresh_res {
                    Ok(Some(fresh)) => {
                        // Remote link entries are cleared on disconnect; relink after reconnect.
                        fresh.blocking_link(&me);
                        if let Some(conn) = fresh.__cached_connection_for_tests() {
                            tracing::warn!(
                                addr = %conn.addr,
                                closed = conn.is_closed(),
                                "test: lookup() produced a ref"
                            );
                        }
                        *remote.lock().await = fresh;
                        let next = *refreshed.borrow() + 1;
                        let _ = refreshed.send(next);
                        return Ok::<_, kameo::error::Infallible>(ControlFlow::Continue(()));
                    }
                    _ => sleep(Duration::from_millis(25)).await,
                };
            }

            // If we can't refresh after a bounded number of attempts, fail the actor.
            Ok::<_, kameo::error::Infallible>(ControlFlow::Break(
                kameo::error::ActorStopReason::PeerDisconnected,
            ))
        }
    }

    fn on_stop(
        &mut self,
        _actor_ref: kameo::actor::WeakActorRef<Self>,
        reason: kameo::error::ActorStopReason,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            tracing::warn!(reason = ?reason, "test: LinkReconnectClient stopped");
            Ok(())
        }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn link_died_reconnect_allows_bench_to_continue() {
    // Allow override via RUST_LOG, but default to warn to keep CI output minimal.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();

    let server_keypair = v2_bootstrap::test_keypair(9660);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = v2_bootstrap::test_keypair(9661);

    let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

    let server_transport = v2_bootstrap::bootstrap_on(server_addr, server_keypair.clone())
        .await
        .expect("server bootstrap");
    let client_transport = v2_bootstrap::bootstrap_on(client_addr, client_keypair)
        .await
        .expect("client bootstrap");
    eprintln!("test: bootstrapped server+client transports");

    let server_addr = server_transport
        .handle()
        .expect("server handle")
        .registry
        .bind_addr;
    eprintln!("test: server bind_addr={server_addr}");

    // Connect client to server.
    let client_handle = client_transport.handle().expect("client handle");
    let peer = client_handle.add_peer(&server_peer_id).await;
    eprintln!("test: connecting client->server peer_id={server_peer_id} addr={server_addr}");
    timeout(Duration::from_secs(5), peer.connect(&server_addr))
        .await
        .expect("connect timeout")
        .expect("connect");
    eprintln!("test: connected");
    sleep(Duration::from_millis(100)).await;

    // Start server actor.
    let calls = Arc::new(AtomicUsize::new(0));
    let actor_ref = <PingActor as Spawn>::spawn(PingActor { calls: calls.clone() });
    eprintln!("test: spawned server actor_id={:?}", actor_ref.id());
    server_transport
        .register_distributed_actor_sync(
            "link_reconnect_ping".to_string(),
            &actor_ref,
            Duration::from_secs(2),
        )
        .await
        .expect("register ping actor");
    eprintln!("test: registered distributed actor");

    // Lookup from the client and link our local watcher actor.
    let remote = timeout(
        Duration::from_secs(3),
        DistributedActorRef::<PingActor>::lookup("link_reconnect_ping"),
    )
    .await
    .expect("lookup timeout")
    .expect("lookup err")
    .expect("actor missing");
    eprintln!(
        "test: lookup returned actor_id={:?} peer_addr={} meta_len={}",
        remote.id(),
        remote.location().peer_addr,
        remote.location().metadata.len()
    );

    let remote_shared = Arc::new(Mutex::new(remote.clone()));
    let (refreshed_tx, mut refreshed_rx) = watch::channel::<u64>(0);
    let link_died = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());

    let watcher = <LinkReconnectClient as Spawn>::spawn(LinkReconnectClient {
        remote: remote_shared.clone(),
        refreshed: refreshed_tx.clone(),
        link_died: link_died.clone(),
        started: started.clone(),
    });

    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("watcher did not start");

    // Ensure the actor is actually running and can process mailbox signals.
    watcher
        .ask(Nop)
        .reply_timeout(Duration::from_secs(2))
        .send()
        .await
        .expect("watcher Nop");

    // This is the actual "remote link" the user-facing API exposes.
    remote.link(&watcher).await;
    eprintln!("test: linked watcher->remote");

    // Run a bounded ask bench in the background.
    let ok = Arc::new(AtomicUsize::new(0));
    let ok2 = ok.clone();
    let mut refreshed_rx2 = refreshed_rx.clone();
    const BENCH_TOTAL: u64 = 80;
    let remote_shared2 = remote_shared.clone();
    let bench = tokio::spawn(async move {
        for i in 0..BENCH_TOTAL {
            let mut logged_err = false;
            loop {
                let r = { remote_shared2.lock().await.clone() };
                match r
                    .ask(Ping { value: i })
                    .timeout(Duration::from_secs(1))
                    .send()
                    .await
                {
                    Ok(v) => {
                        if v != i + 1 {
                            return Err(format!("bad reply: got={v} want={}", i + 1));
                        }
                        ok2.fetch_add(1, Ordering::AcqRel);
                        break;
                    }
                    Err(e) => {
                        if !logged_err {
                            tracing::warn!(i, error = ?e, "test: bench ask error");
                            logged_err = true;
                        }
                        // Wait for a refresh event (watch does not drop notifications).
                        let before = *refreshed_rx2.borrow();
                        let _ = timeout(Duration::from_secs(1), async {
                            while *refreshed_rx2.borrow() == before {
                                refreshed_rx2
                                    .changed()
                                    .await
                                    .expect("refreshed watch closed");
                            }
                        })
                        .await;
                    }
                }
            }
        }
        Ok::<(), String>(())
    });

    // Let some messages succeed.
    timeout(Duration::from_secs(3), async {
        while ok.load(Ordering::Acquire) < 10 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bench did not start");
    eprintln!("test: bench started ok={}", ok.load(Ordering::Acquire));

    // Subscribe before triggering the event (Notify does not buffer).
    let link_died_wait = link_died.notified();
    let before_refresh = *refreshed_rx.borrow();

    // Simulate a link death by force-dropping the client's socket to the server. This exercises the
    // same `on_link_died(PeerDisconnected)` and refresh path as a real TCP/TLS termination, but
    // avoids the (much harder) semantic of "server process restarted but actor kept running".
    //
    // The server remains up and registered; the client must reconnect and continue the bench.
    client_handle
        .registry
        .handle_peer_connection_failure(server_addr)
        .await
        .expect("force drop client connection");
    eprintln!("test: forced client connection failure");

    // We must observe on_link_died.
    timeout(Duration::from_secs(3), link_died_wait)
        .await
        .expect("did not observe on_link_died in time");
    eprintln!("test: observed on_link_died");

    // And we must observe a refresh.
    timeout(Duration::from_secs(5), async {
        while *refreshed_rx.borrow() == before_refresh {
            refreshed_rx.changed().await.expect("refreshed watch closed");
        }
    })
    .await
    .expect("did not refresh in time");
    eprintln!("test: observed refresh event");

    // Probe a single ask from the updated ref to ensure the reconnect actually restored ask/response.
    let calls_before_probe = calls.load(Ordering::Acquire);
    let probe = {
        let r = remote_shared.lock().await.clone();
        r.ask(Ping { value: 999 })
            .timeout(Duration::from_secs(2))
            .send()
            .await
    };
    let calls_after_probe = calls.load(Ordering::Acquire);
    tracing::warn!(probe = ?probe, "test: post-refresh probe result");
    tracing::warn!(
        calls_before_probe,
        calls_after_probe,
        "test: server call count around probe"
    );
    assert_eq!(probe.unwrap(), 1000, "post-refresh probe failed");
    eprintln!("test: post-refresh probe ok");

    timeout(Duration::from_secs(30), bench)
        .await
        .expect("bench hung")
        .expect("bench panicked")
        .expect("bench failed");
    eprintln!("test: bench finished ok={}", ok.load(Ordering::Acquire));

    assert_eq!(
        ok.load(Ordering::Acquire),
        BENCH_TOTAL as usize,
        "bench did not complete"
    );
    assert!(
        calls.load(Ordering::Acquire) >= BENCH_TOTAL as usize,
        "server did not handle asks"
    );
}
