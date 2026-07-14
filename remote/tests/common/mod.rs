//! Helpers shared by the integration test crates.
#![allow(dead_code)] // Each test crate uses a different subset.

use std::time::{Duration, Instant};

use futures::Future;
use kameo::prelude::*;
use kameo_remote::{
    FailureDetectorConfig, RemoteActor, RemoteMessage, RemoteMessages, RemoteNode, RemoteNodeConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Actor)]
pub struct Counter {
    pub count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct Inc {
    pub amount: i64,
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
pub struct Get;

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
pub struct TestError(pub String);

#[derive(Serialize, Deserialize)]
pub struct Fail;

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

impl RemoteActor for Counter {
    const REMOTE_ID: &'static str = "test::Counter";

    fn remote_messages(handlers: &mut RemoteMessages<Self>) {
        handlers.add::<Inc>();
        handlers.add::<Get>();
        handlers.add::<Fail>();
    }
}

pub fn test_config(seed_nodes: Vec<String>) -> RemoteNodeConfig {
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

pub async fn spawn_test_node(seed_nodes: Vec<String>) -> RemoteNode {
    RemoteNode::bootstrap(test_config(seed_nodes))
        .await
        .unwrap()
}

/// Spawns two nodes, with the second seeded off the first.
pub async fn spawn_test_cluster() -> (RemoteNode, RemoteNode) {
    let node_a = spawn_test_node(vec![]).await;
    let node_b = spawn_test_node(vec![node_a.gossip_addr().to_string()]).await;
    (node_a, node_b)
}

pub async fn eventually<T, F, Fut>(deadline: Duration, mut condition: F) -> T
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

pub const GOSSIP_DEADLINE: Duration = Duration::from_secs(10);
pub const DEATH_DEADLINE: Duration = Duration::from_secs(30);
