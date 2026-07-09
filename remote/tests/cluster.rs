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

async fn spawn_test_node(seed_nodes: Vec<String>) -> RemoteNode {
    let config = RemoteNodeConfig {
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
    };
    RemoteNode::bootstrap(config).await.unwrap()
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

    node_b.shutdown().await.unwrap();

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
