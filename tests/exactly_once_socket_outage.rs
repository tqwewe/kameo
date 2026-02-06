#![cfg(all(feature = "remote", feature = "remote_integration_tests"))]
//! Regression test: an in-flight ask must not hang forever if the socket drops.
//!
//! This specifically covers a failure mode where the connection IO task exits and
//! cancels pending correlation slots, which previously could lead to an infinite
//! wait after timeout/cancellation.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use kameo::{Actor, distributed_actor};
use kameo::remote::{
    DistributedActorRef,
    exactly_once::{ExactlyOnceDedup, PayloadFingerprint, RequestId},
    transport::RemoteTransport,
};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

#[derive(kameo::RemoteMessage, Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct WorkMsg {
    value: u64,
}

// Integration tests are compiled as a separate crate, so they cannot implement
// kameo's HasTypeHash for kameo's ExactlyOnce<T> due to orphan rules.
// Use a local concrete wrapper to exercise the same behavior.
#[derive(kameo::RemoteMessage, Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TestExactlyOnceWorkMsg {
    request_id: RequestId,
    message: WorkMsg,
}

#[derive(kameo::Actor)]
struct ExactlyOnceOutageActor {
    dedup: ExactlyOnceDedup<u64>,
    exec_count: Arc<AtomicU64>,
    started: Arc<Notify>,
    allow_reply: Arc<Notify>,
    computed_and_cached: Arc<Notify>,
}

impl kameo::message::Message<TestExactlyOnceWorkMsg> for ExactlyOnceOutageActor {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: TestExactlyOnceWorkMsg,
        _ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Fingerprint is optional, but we use it to ensure request_id reuse can't "switch" payloads.
        let payload_bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&msg.message).expect("serialize for fingerprint");
        let fingerprint = PayloadFingerprint::from_bytes(payload_bytes.as_ref());

        let request_id = msg.request_id;
        let inner = msg.message;

        let did_compute = Arc::new(AtomicBool::new(false));
        let did_compute2 = did_compute.clone();

        let started = self.started.clone();
        let allow_reply = self.allow_reply.clone();
        let exec_count = self.exec_count.clone();

        let value = self
            .dedup
            .resolve_checked(request_id, fingerprint, || async move {
                did_compute2.store(true, Ordering::Release);
                exec_count.fetch_add(1, Ordering::AcqRel);
                started.notify_one();

                // Hold the handler open so the test can drop the socket before the reply is sent.
                allow_reply.notified().await;
                inner.value + 1
            })
            .await
            .expect("fingerprint match");

        // resolve_checked() only inserts after the compute future completes, so by the time we get
        // here the reply is cached (if it was a miss).
        if did_compute.load(Ordering::Acquire) {
            self.computed_and_cached.notify_one();
        }

        value
    }
}

distributed_actor! {
    ExactlyOnceOutageActor {
        TestExactlyOnceWorkMsg,
    }
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "Requires stable remote transport delivery; kept as a harness for socket-outage exactly-once retry behavior"]
async fn test_exactly_once_retry_does_not_hang_on_socket_drop() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn")
        .try_init();

    // Bootstrap server/client nodes with deterministic keypairs.
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9460);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9461);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9460".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9461".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Server actor with deterministic coordination hooks.
    let exec_count = Arc::new(AtomicU64::new(0));
    let started = Arc::new(Notify::new());
    let allow_reply = Arc::new(Notify::new());
    let computed_and_cached = Arc::new(Notify::new());

    let actor_ref = ExactlyOnceOutageActor::spawn(ExactlyOnceOutageActor {
        dedup: ExactlyOnceDedup::new(128),
        exec_count: exec_count.clone(),
        started: started.clone(),
        allow_reply: allow_reply.clone(),
        computed_and_cached: computed_and_cached.clone(),
    });

    server_transport
        .register_distributed_actor("exactly_once_outage_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");

    // Connect client to server (this triggers an immediate gossip round).
    let server_addr: std::net::SocketAddr = "127.0.0.1:9460".parse().unwrap();
    let client_handle = client_transport
        .handle()
        .expect("client transport missing handle");
    let peer = client_handle.add_peer(&server_peer_id).await;
    peer.connect(&server_addr).await.expect("Connection failed");

    // Actor discovery via gossip is currently flaky; construct a direct ref using the known ActorId.
    let location = kameo::remote::transport::RemoteActorLocation {
        peer_addr: server_addr,
        actor_id: actor_ref.id(),
        metadata: Vec::new(),
    };
    let peer_ref = client_handle
        .lookup_peer(&server_peer_id)
        .await
        .expect("lookup_peer failed");
    let conn = std::sync::Arc::as_ref(
        peer_ref
            .connection_ref()
            .expect("lookup_peer returned no connection"),
    )
    .clone();
    let remote_ref = DistributedActorRef::<ExactlyOnceOutageActor>::__new_for_tests(
        actor_ref.id(),
        location.clone(),
        client_transport.clone(),
        conn,
    );

    // Fire the first ask in a background task so we can cut the connection mid-flight.
    let request_id = RequestId::next();
    let first_msg = TestExactlyOnceWorkMsg {
        request_id,
        message: WorkMsg { value: 41 },
    };

    let first_ask = tokio::spawn(async move {
        // Use an explicit timeout so the test stays bounded.
        remote_ref
            .ask(first_msg)
            .timeout(Duration::from_secs(1))
            .send()
            .await
    });

    // Ensure the server actually started processing (handler entered) before killing the socket.
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("server did not start processing in time");

    // Simulate a socket outage/glitch by aborting the client-side connection IO task.
    // This forces correlation cancellation for pending asks.
    client_transport
        .handle()
        .expect("client transport missing handle")
        .registry
        .connection_pool
        .disconnect_connection_by_peer_id(&server_peer_id);

    // Let the server compute and cache the reply, but it should no longer be deliverable
    // over the aborted socket.
    allow_reply.notify_one();
    timeout(Duration::from_secs(2), computed_and_cached.notified())
        .await
        .expect("server did not compute/cache in time");

    // The first ask must return promptly (previously this could hang forever).
    let first_result = timeout(Duration::from_secs(2), first_ask)
        .await
        .expect("first ask task hung")
        .expect("first ask task panicked");
    assert!(first_result.is_err(), "expected first ask to fail after socket drop");

    // Reconnect, then retry with the same request id: server should hit the dedup cache.
    let peer = client_handle.add_peer(&server_peer_id).await;
    peer.connect(&server_addr).await.expect("Reconnect failed");
    let peer_ref2 = client_handle
        .lookup_peer(&server_peer_id)
        .await
        .expect("lookup_peer failed after reconnect");
    let conn2 = std::sync::Arc::as_ref(
        peer_ref2
            .connection_ref()
            .expect("lookup_peer returned no connection after reconnect"),
    )
    .clone();
    let remote_ref2 = DistributedActorRef::<ExactlyOnceOutageActor>::__new_for_tests(
        actor_ref.id(),
        location,
        client_transport.clone(),
        conn2,
    );

    let retry_msg = TestExactlyOnceWorkMsg {
        request_id,
        message: WorkMsg { value: 41 },
    };
    let reply: u64 = timeout(
        Duration::from_secs(5),
        remote_ref2.ask(retry_msg).timeout(Duration::from_secs(2)).send(),
    )
    .await
    .expect("retry ask hung")
    .expect("retry ask failed");
    assert_eq!(reply, 42);

    assert_eq!(
        exec_count.load(Ordering::Acquire),
        1,
        "handler should execute exactly once for the same RequestId"
    );

    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}
