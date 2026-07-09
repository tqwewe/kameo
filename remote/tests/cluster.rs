use std::time::{Duration, Instant};

use futures::{Future, StreamExt};
use kameo::prelude::*;
use kameo_remote::{
    FailureDetectorConfig, RegistryError, RemoteActor, RemoteMessage, RemoteMessages, RemoteNode,
    RemoteNodeConfig, RemoteSendError,
};
use serde::{Deserialize, Serialize};

#[derive(Actor)]
struct Counter {
    count: i64,
}

#[derive(Serialize, Deserialize)]
struct Inc {
    amount: i64,
}

impl Message<Inc> for Counter {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _: &mut Context<Self, Self::Reply>) -> i64 {
        self.count += msg.amount;
        self.count
    }
}

impl RemoteMessage for Inc {
    const REMOTE_ID: &'static str = "test::Inc";
}

#[derive(Serialize, Deserialize)]
struct Get;

impl Message<Get> for Counter {
    type Reply = i64;

    async fn handle(&mut self, _: Get, _: &mut Context<Self, Self::Reply>) -> i64 {
        self.count
    }
}

impl RemoteMessage for Get {
    const REMOTE_ID: &'static str = "test::Get";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestError(String);

#[derive(Serialize, Deserialize)]
struct Fail;

impl Message<Fail> for Counter {
    type Reply = Result<i64, TestError>;

    async fn handle(
        &mut self,
        _: Fail,
        _: &mut Context<Self, Self::Reply>,
    ) -> Result<i64, TestError> {
        Err(TestError("boom".to_string()))
    }
}

impl RemoteMessage for Fail {
    const REMOTE_ID: &'static str = "test::Fail";
}

/// Handled by `Counter`, but deliberately not declared in `remote_messages`.
#[derive(Serialize, Deserialize)]
struct Undeclared;

impl Message<Undeclared> for Counter {
    type Reply = ();

    async fn handle(&mut self, _: Undeclared, _: &mut Context<Self, Self::Reply>) {}
}

impl RemoteMessage for Undeclared {
    const REMOTE_ID: &'static str = "test::Undeclared";
}

impl RemoteActor for Counter {
    const REMOTE_ID: &'static str = "test::Counter";

    fn remote_messages(handlers: &mut RemoteMessages<Self>) {
        handlers.add::<Inc>();
        handlers.add::<Get>();
        handlers.add::<Fail>();
    }
}

#[derive(Actor)]
struct OtherActor;

impl RemoteActor for OtherActor {
    const REMOTE_ID: &'static str = "test::OtherActor";

    fn remote_messages(_: &mut RemoteMessages<Self>) {}
}

#[derive(Actor)]
struct Sequencer {
    values: Vec<u32>,
}

#[derive(Serialize, Deserialize)]
struct Push(u32);

impl Message<Push> for Sequencer {
    type Reply = ();

    async fn handle(&mut self, msg: Push, _: &mut Context<Self, Self::Reply>) {
        self.values.push(msg.0);
    }
}

impl RemoteMessage for Push {
    const REMOTE_ID: &'static str = "test::Push";
}

#[derive(Serialize, Deserialize)]
struct GetAll;

impl Message<GetAll> for Sequencer {
    type Reply = Vec<u32>;

    async fn handle(&mut self, _: GetAll, _: &mut Context<Self, Self::Reply>) -> Vec<u32> {
        self.values.clone()
    }
}

impl RemoteMessage for GetAll {
    const REMOTE_ID: &'static str = "test::GetAll";
}

impl RemoteActor for Sequencer {
    const REMOTE_ID: &'static str = "test::Sequencer";

    fn remote_messages(handlers: &mut RemoteMessages<Self>) {
        handlers.add::<Push>();
        handlers.add::<GetAll>();
    }
}

fn test_config(seed_nodes: Vec<String>) -> RemoteNodeConfig {
    RemoteNodeConfig {
        cluster_id: "kameo-test".to_string(),
        gossip_listen_addr: "127.0.0.1:0".parse().unwrap(),
        messaging_listen_addr: "127.0.0.1:0".parse().unwrap(),
        seed_nodes,
        gossip_interval: Duration::from_millis(50),
        failure_detector_config: FailureDetectorConfig {
            phi_threshold: 8.0,
            sampling_window_size: 1000,
            max_interval: Duration::from_millis(500),
            initial_interval: Duration::from_millis(100),
            dead_node_grace_period: Duration::from_secs(20),
        },
        ..Default::default()
    }
}

async fn spawn_test_node(seed_nodes: Vec<String>) -> RemoteNode {
    RemoteNode::bootstrap(test_config(seed_nodes))
        .await
        .unwrap()
}

/// Spawns two nodes, with the second seeded off the first.
async fn spawn_test_cluster() -> (RemoteNode, RemoteNode) {
    let node_a = spawn_test_node(vec![]).await;
    let node_b = spawn_test_node(vec![node_a.gossip_addr().to_string()]).await;
    (node_a, node_b)
}

async fn eventually<T, F, Fut>(deadline: Duration, mut condition: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    loop {
        if let Some(value) = condition().await {
            return value;
        }
        if start.elapsed() > deadline {
            panic!("condition not met within {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

const GOSSIP_DEADLINE: Duration = Duration::from_secs(10);
const DEATH_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
async fn register_and_lookup() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    assert_eq!(remote.node_id(), node_a.node_id());
    assert_eq!(remote.id().sequence_id, counter.id().sequence_id());
}

#[tokio::test(flavor = "multi_thread")]
async fn lookup_all_set_semantics() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter_1 = Counter::spawn(Counter { count: 0 });
    let counter_2 = Counter::spawn(Counter { count: 0 });
    let counter_3 = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter_1, "workers").await.unwrap();
    node_a.register(&counter_2, "workers").await.unwrap();
    node_b.register(&counter_3, "workers").await.unwrap();

    let refs = eventually(GOSSIP_DEADLINE, || async {
        let refs = node_b.lookup_all::<Counter>("workers").await.unwrap();
        (refs.len() == 3).then_some(refs)
    })
    .await;

    let mut ids: Vec<_> = refs.iter().map(|remote| remote.id().clone()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3, "provider ids should be distinct");
}

#[tokio::test(flavor = "multi_thread")]
async fn ask_round_trip() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    assert_eq!(remote.ask(&Inc { amount: 5 }).await.unwrap(), 5);
    assert_eq!(remote.ask(&Inc { amount: 2 }).await.unwrap(), 7);
}

#[tokio::test(flavor = "multi_thread")]
async fn ask_handler_error() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    let err = remote.ask(&Fail).await.unwrap_err();
    let RemoteSendError::HandlerError(err) = err else {
        panic!("expected HandlerError, got {err:?}");
    };
    assert_eq!(err, TestError("boom".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn tell_then_observe() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    remote.tell(&Inc { amount: 3 }).await.unwrap();

    let count = eventually(GOSSIP_DEADLINE, || async {
        let count = remote.ask(&Get).await.unwrap();
        (count == 3).then_some(count)
    })
    .await;
    assert_eq!(count, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_message() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    let err = remote.ask(&Undeclared).await.unwrap_err();
    let RemoteSendError::UnknownMessage {
        actor_remote_id,
        message_remote_id,
    } = err
    else {
        panic!("expected UnknownMessage, got {err:?}");
    };
    assert_eq!(actor_remote_id, "test::Counter");
    assert_eq!(message_remote_id, "test::Undeclared");
}

#[tokio::test(flavor = "multi_thread")]
async fn acked_tell_surfaces_errors() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    // The delivery ack carries dispatch errors back to the sender.
    let err = remote.tell(&Undeclared).await.unwrap_err();
    assert!(
        matches!(err, RemoteSendError::UnknownMessage { .. }),
        "expected UnknownMessage, got {err:?}"
    );

    // Fire-and-forget only reports local failures; the remote error is not observed.
    remote.tell(&Undeclared).send_unacked().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_node_fast_path() {
    // A bogus messaging advertise address: if any message touched TCP, it would fail
    // to connect. Local refs must work anyway, proving in-process dispatch.
    let mut config = test_config(vec![]);
    config.messaging_advertise_addr = Some("127.0.0.1:1".parse().unwrap());
    let node = RemoteNode::bootstrap(config).await.unwrap();

    let counter = Counter::spawn(Counter { count: 0 });
    node.register(&counter, "counter").await.unwrap();

    let local = eventually(GOSSIP_DEADLINE, || async {
        node.lookup::<Counter>("counter").await.unwrap()
    })
    .await;
    assert!(local.is_local());

    assert_eq!(local.ask(&Inc { amount: 4 }).await.unwrap(), 4);
    local.tell(&Inc { amount: 1 }).await.unwrap();
    assert_eq!(local.ask(&Get).await.unwrap(), 5);

    // Errors flow through the same dispatch path locally.
    let err = local.ask(&Fail).await.unwrap_err();
    assert!(matches!(err, RemoteSendError::HandlerError(_)));
    let err = local.tell(&Undeclared).await.unwrap_err();
    assert!(matches!(err, RemoteSendError::UnknownMessage { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn per_pair_ordering() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let sequencer = Sequencer::spawn(Sequencer { values: Vec::new() });
    node_a.register(&sequencer, "sequencer").await.unwrap();

    let remote = eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Sequencer>("sequencer").await.unwrap()
    })
    .await;

    // Pipelined unacked tells must be delivered in send order, and the final ask
    // must be ordered after all of them.
    for i in 0..200 {
        remote.tell(&Push(i)).send_unacked().await.unwrap();
    }
    let values = remote.ask(&GetAll).await.unwrap();
    assert_eq!(values, (0..200).collect::<Vec<u32>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_actor_type() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let other = OtherActor::spawn(OtherActor);
    node_a.register(&other, "conflict").await.unwrap();

    // Wait until node b sees the registration under its true type.
    eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<OtherActor>("conflict").await.unwrap()
    })
    .await;

    let err = node_b.lookup::<Counter>("conflict").await.unwrap_err();
    assert_eq!(
        err,
        RegistryError::BadActorType {
            name: "conflict".to_string(),
            expected_remote_id: "test::Counter",
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn deregister_removes() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    node_a.deregister(&counter, "counter").await.unwrap();

    eventually(GOSSIP_DEADLINE, || async {
        node_b
            .lookup::<Counter>("counter")
            .await
            .unwrap()
            .is_none()
            .then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn node_death_removes_registrations() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_b.register(&counter, "counter").await.unwrap();

    eventually(GOSSIP_DEADLINE, || async {
        node_a.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    // Dropping the node aborts its gossip and messaging tasks without a clean
    // departure, simulating a crash.
    drop(node_b);

    // The failure detector on node a must declare node b dead.
    eventually(DEATH_DEADLINE, || async {
        node_a
            .lookup::<Counter>("counter")
            .await
            .unwrap()
            .is_none()
            .then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_shutdown_deregisters_quickly() {
    // A slow failure detector, so fast removal can only come from the gossiped
    // deletions performed by shutdown, not from failure detection.
    let mut config_a = test_config(vec![]);
    config_a.failure_detector_config = FailureDetectorConfig::default();
    let node_a = RemoteNode::bootstrap(config_a).await.unwrap();
    let mut config_b = test_config(vec![node_a.gossip_addr().to_string()]);
    config_b.failure_detector_config = FailureDetectorConfig::default();
    let node_b = RemoteNode::bootstrap(config_b).await.unwrap();

    let counter = Counter::spawn(Counter { count: 0 });
    node_b.register(&counter, "counter").await.unwrap();

    eventually(GOSSIP_DEADLINE, || async {
        node_a.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    node_b.shutdown().await.unwrap();

    eventually(Duration::from_secs(3), || async {
        node_a
            .lookup::<Counter>("counter")
            .await
            .unwrap()
            .is_none()
            .then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_ref_rejected_after_restart() {
    let node_a = spawn_test_node(vec![]).await;

    // Node b has a fixed name so its restarted incarnation is the "same node".
    let mut config = test_config(vec![node_a.gossip_addr().to_string()]);
    config.node_name = Some("fixed-node".to_string());
    let node_b = RemoteNode::bootstrap(config).await.unwrap();
    let messaging_addr = node_b.messaging_addr();

    let counter = Counter::spawn(Counter { count: 0 });
    node_b.register(&counter, "counter").await.unwrap();

    let stale = eventually(GOSSIP_DEADLINE, || async {
        node_a.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    node_b.shutdown().await.unwrap();

    // Restart with the same name and messaging address, and re-register the same
    // actor so the stale reference's sequence id is valid on the new incarnation.
    let mut config = test_config(vec![node_a.gossip_addr().to_string()]);
    config.node_name = Some("fixed-node".to_string());
    config.messaging_listen_addr = messaging_addr;
    let node_b2 = RemoteNode::bootstrap(config).await.unwrap();
    assert_ne!(node_b2.generation_id(), stale.id().generation_id);
    node_b2.register(&counter, "counter").await.unwrap();

    // A fresh lookup targets the new incarnation and works.
    let fresh = eventually(GOSSIP_DEADLINE, || async {
        let remote = node_a.lookup::<Counter>("counter").await.unwrap()?;
        (remote.id().generation_id == node_b2.generation_id()).then_some(remote)
    })
    .await;
    assert_eq!(fresh.ask(&Get).await.unwrap(), 0);

    // The stale reference carries the old generation and must be rejected, even
    // though its sequence id matches a registered actor.
    assert_eq!(stale.id().sequence_id, fresh.id().sequence_id);
    let err = stale.ask(&Get).await.unwrap_err();
    assert!(
        matches!(err, RemoteSendError::ActorNotRunning),
        "expected ActorNotRunning, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_stream() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let mut watch = Box::pin(node_b.watch::<Counter>("watched").await);

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "watched").await.unwrap();

    // The stream yields provider sets on change; await one containing the registration.
    let providers = tokio::time::timeout(GOSSIP_DEADLINE, async {
        loop {
            let providers = watch.next().await.expect("watch stream ended");
            if !providers.is_empty() {
                break providers;
            }
        }
    })
    .await
    .expect("timed out waiting for provider");
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].node_id(), node_a.node_id());

    node_a.deregister(&counter, "watched").await.unwrap();

    tokio::time::timeout(GOSSIP_DEADLINE, async {
        loop {
            let providers = watch.next().await.expect("watch stream ended");
            if providers.is_empty() {
                break;
            }
        }
    })
    .await
    .expect("timed out waiting for deregistration");
}

#[tokio::test(flavor = "multi_thread")]
async fn actor_shutdown_auto_deregisters() {
    let (node_a, node_b) = spawn_test_cluster().await;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await.unwrap();

    eventually(GOSSIP_DEADLINE, || async {
        node_b.lookup::<Counter>("counter").await.unwrap()
    })
    .await;

    counter.stop_gracefully().await.unwrap();
    counter.wait_for_shutdown().await;

    eventually(GOSSIP_DEADLINE, || async {
        node_b
            .lookup::<Counter>("counter")
            .await
            .unwrap()
            .is_none()
            .then_some(())
    })
    .await;
}
