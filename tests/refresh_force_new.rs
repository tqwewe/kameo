#![cfg(all(feature = "remote", feature = "remote_integration_tests"))]
//! Regression tests for refresh behavior under connection instability.
//!
//! These tests focus on client-side robustness:
//! - refresh must not "commit suicide" when the cached connection is dead.
//! - refresh_force_new must force eviction/dial to escape potential zombie sockets.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use std::ops::ControlFlow;

use kameo::{Spawn, distributed_actor};
use kameo::remote::{DistributedActorRef, v2_bootstrap};
use tokio::time::{Duration, sleep, timeout};

#[derive(kameo::RemoteMessage, Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Ping {
    value: u64,
}

struct PingActor {
    calls: Arc<AtomicUsize>,
}

impl kameo::actor::Actor for PingActor {
    type Args = Self;
    type Error = kameo::error::Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: kameo::actor::ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(state)
    }

    fn on_link_died(
        &mut self,
        _actor_ref: kameo::actor::WeakActorRef<Self>,
        _id: kameo::actor::ActorId,
        reason: kameo::error::ActorStopReason,
    ) -> impl std::future::Future<
        Output = Result<ControlFlow<kameo::error::ActorStopReason>, Self::Error>,
    > + Send {
        async move {
            // For distributed actors we want to observe peer disconnects without stopping the actor.
            if matches!(reason, kameo::error::ActorStopReason::PeerDisconnected) {
                return Ok(ControlFlow::Continue(()));
            }
            Ok(ControlFlow::Break(reason))
        }
    }
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

async fn wait_until_closed(conn: &kameo_remote::connection_pool::ConnectionHandle) {
    for _ in 0..50 {
        if conn.is_closed() {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("connection did not become closed in time");
}

#[tokio::test]
#[serial_test::serial]
async fn refresh_force_new_eviction_closes_old_handle_and_redials() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    let server_keypair = v2_bootstrap::test_keypair(9560);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = v2_bootstrap::test_keypair(9561);

    let server_addr: std::net::SocketAddr = "127.0.0.1:9560".parse().unwrap();
    let client_addr: std::net::SocketAddr = "127.0.0.1:9561".parse().unwrap();

    let server_transport = v2_bootstrap::bootstrap_on(server_addr, server_keypair)
        .await
        .expect("server bootstrap");
    let client_transport = v2_bootstrap::bootstrap_on(client_addr, client_keypair)
        .await
        .expect("client bootstrap");

    // Connect client to server.
    let client_handle = client_transport.handle().expect("client handle");
    let peer = client_handle.add_peer(&server_peer_id).await;
    peer.connect(&server_addr).await.expect("connect");
    sleep(Duration::from_millis(100)).await;

    // Register server actor after the peer connection exists so sync propagation can ACK.
    let calls = Arc::new(AtomicUsize::new(0));
    let actor_ref = PingActor::spawn(PingActor { calls: calls.clone() });
    let server_actor_id = actor_ref.id();
    server_transport
        .register_distributed_actor_sync(
            "ping_actor".to_string(),
            &actor_ref,
            Duration::from_secs(2),
        )
        .await
        .expect("register");

    // Lookup by name using the global (client) transport.
    let remote = timeout(
        Duration::from_secs(3),
        DistributedActorRef::<PingActor>::lookup("ping_actor"),
    )
    .await
    .expect("lookup timeout")
    .expect("lookup err")
    .expect("actor missing");

    assert_eq!(remote.id(), server_actor_id, "lookup returned wrong actor_id");

    // Prove baseline ask works.
    let r1 = remote
        .ask(Ping { value: 1 })
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .expect("ask");
    assert_eq!(r1, 2);
    assert_eq!(calls.load(Ordering::Acquire), 1, "server should have handled 1 ask");

    let old_conn = remote.__cached_connection_for_tests().expect("cached conn");

    // Force a new connection.
    let fresh = remote
        .refresh_force_new()
        .await
        .expect("refresh_force_new err")
        .expect("refresh_force_new returned None");
    assert_eq!(fresh.id(), server_actor_id, "refresh returned wrong actor_id");

    // Ensure the server actor is still alive after the disconnect.
    let local_ok = actor_ref
        .ask(Ping { value: 1000 })
        .reply_timeout(Duration::from_secs(2))
        .send()
        .await
        .expect("local ask after refresh_force_new");
    assert_eq!(local_ok, 1001);

    // Old handle should become closed because we evicted/aborted the IO task.
    wait_until_closed(&old_conn).await;

    let new_conn = fresh.__cached_connection_for_tests().expect("fresh conn");
    assert!(!new_conn.is_closed(), "fresh connection should be active");

    // Instrument whether the ask actually hits the wire after refresh.
    let before_written = new_conn.bytes_written();
    let ask_task = {
        let fresh = fresh.clone();
        tokio::spawn(async move {
            fresh
                .ask(Ping { value: 10 })
                .timeout(Duration::from_secs(2))
                .send()
                .await
        })
    };
    sleep(Duration::from_millis(100)).await;
    let after_written = new_conn.bytes_written();
    assert!(
        after_written > before_written,
        "bytes_written did not increase after enqueue (before={before_written} after={after_written})"
    );

    let r2 = ask_task
        .await
        .expect("ask task panicked")
        .map_err(|e| {
            let handled = calls.load(Ordering::Acquire);
            panic!("ask after refresh_force_new: {e:?} (server_calls={handled})");
        })
        .unwrap();
    assert_eq!(r2, 11);
    // We performed:
    // - one remote ask before refresh_force_new
    // - one local ask (health check) after refresh_force_new
    // - one remote ask after refresh_force_new
    assert_eq!(calls.load(Ordering::Acquire), 3, "unexpected handler call count");
}

#[tokio::test]
#[serial_test::serial]
async fn refresh_recovers_after_connection_pool_disconnect() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    let server_keypair = v2_bootstrap::test_keypair(9570);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = v2_bootstrap::test_keypair(9571);

    let server_addr: std::net::SocketAddr = "127.0.0.1:9570".parse().unwrap();
    let client_addr: std::net::SocketAddr = "127.0.0.1:9571".parse().unwrap();

    let server_transport = v2_bootstrap::bootstrap_on(server_addr, server_keypair)
        .await
        .expect("server bootstrap");
    let client_transport = v2_bootstrap::bootstrap_on(client_addr, client_keypair)
        .await
        .expect("client bootstrap");

    let client_handle = client_transport.handle().expect("client handle");
    let peer = client_handle.add_peer(&server_peer_id).await;
    peer.connect(&server_addr).await.expect("connect");
    sleep(Duration::from_millis(100)).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let actor_ref = PingActor::spawn(PingActor { calls: calls.clone() });
    server_transport
        .register_distributed_actor_sync(
            "ping_actor2".to_string(),
            &actor_ref,
            Duration::from_secs(2),
        )
        .await
        .expect("register");

    let remote = timeout(
        Duration::from_secs(3),
        DistributedActorRef::<PingActor>::lookup("ping_actor2"),
    )
    .await
    .expect("lookup timeout")
    .expect("lookup err")
    .expect("actor missing");

    // Simulate abrupt local outage by dropping the client-side connection tasks.
    client_handle
        .registry
        .connection_pool
        .disconnect_connection_by_peer_id(&server_peer_id);

    // refresh() must not error or abort; it should recover by dialing again.
    let refreshed = remote
        .refresh()
        .await
        .expect("refresh err")
        .expect("refresh returned None");

    let r = refreshed
        .ask(Ping { value: 5 })
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .expect("ask after refresh");
    assert_eq!(r, 6);
    assert_eq!(calls.load(Ordering::Acquire), 1);
}
