#![cfg(all(feature = "remote", feature = "remote_integration_tests"))]
//! Tests for small message handling (<1MB) - should use regular protocol
//!
//! This test suite verifies that messages under the streaming threshold
//! use the regular protocol and maintain performance characteristics.

use kameo::remote::{
    DistributedActorRef, transport::RemoteTransport, type_hash::HasTypeHash, type_registry,
};
use kameo::{Actor, distributed_actor};
use kameo_remote::MAX_STREAM_SIZE;
use std::time::Instant;
use tokio::time::{Duration, timeout};

// Test message structure for small messages
#[derive(kameo::RemoteMessage, Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SmallTestMessage {
    pub data: Vec<u8>,
    pub id: String,
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SmallTestResponse {
    pub processed_bytes: usize,
    pub id: String,
    pub success: bool,
}

impl kameo::reply::Reply for SmallTestResponse {
    type Ok = Self;
    type Error = kameo::error::Infallible;
    type Value = Self;

    fn to_result(self) -> Result<Self, kameo::error::Infallible> {
        Ok(self)
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

// Test actor that handles small messages
#[derive(kameo::Actor)]
pub struct SmallMessageTestActor;

impl kameo::message::Message<SmallTestMessage> for SmallMessageTestActor {
    type Reply = SmallTestResponse;

    async fn handle(
        &mut self,
        message: SmallTestMessage,
        _ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SmallTestResponse {
            processed_bytes: message.data.len(),
            id: message.id,
            success: true,
        }
    }
}

distributed_actor! {
    SmallMessageTestActor {
        SmallTestMessage,
    }
}

fn assert_type_registered<M: HasTypeHash>(actor_id: kameo::actor::ActorId) {
    let type_hash = <M as HasTypeHash>::TYPE_HASH.as_u32();
    let entry = type_registry::lookup_handler(type_hash).expect("type hash not registered");
    assert_eq!(entry.actor_id, actor_id);
}

#[tokio::test]
#[serial_test::serial]
async fn test_small_tell_messages_100kb() {
    // Test 100KB messages use regular protocol
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn")
        .try_init();

    // Bootstrap test nodes with explicit keypairs
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9400);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9401);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9400".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9401".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Register test actor
    let actor_ref = SmallMessageTestActor::spawn(SmallMessageTestActor);
    server_transport
        .register_distributed_actor("small_test_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");
    assert_type_registered::<SmallTestMessage>(actor_ref.id());

    // Connect client to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9400".parse().unwrap())
            .await
            .expect("Connection failed");
    }

    // Wait for connection stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Look up remote actor
    let remote_ref = DistributedActorRef::<SmallMessageTestActor>::lookup("small_test_actor")
        .await
        .expect("Lookup failed")
        .expect("Actor not found");

    // Test 100KB message - should use regular protocol
    let data_100kb = vec![0u8; 100_000]; // 100KB
    assert!(data_100kb.len() < MAX_STREAM_SIZE);
    let message = SmallTestMessage {
        data: data_100kb.clone(),
        id: "test_100kb".to_string(),
    };

    let start = Instant::now();
    remote_ref.tell(message).send().await.expect("Tell failed");
    let duration = start.elapsed();

    println!("✅ 100KB tell message sent in {:?}", duration);

    // Verify it was processed (would need actor confirmation mechanism)
    assert!(
        duration < Duration::from_millis(100),
        "100KB message should be fast"
    );

    // Cleanup
    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}

#[tokio::test]
#[serial_test::serial]
async fn test_small_ask_messages_500kb() {
    // Test 500KB ask messages use regular protocol
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn")
        .try_init();

    // Bootstrap test nodes with different ports and explicit keypairs
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9410);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9411);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9410".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9411".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Register test actor
    let actor_ref = SmallMessageTestActor::spawn(SmallMessageTestActor);
    server_transport
        .register_distributed_actor("small_ask_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");
    assert_type_registered::<SmallTestMessage>(actor_ref.id());

    // Connect client to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9410".parse().unwrap())
            .await
            .expect("Connection failed");
    }

    // Wait for connection stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Look up remote actor
    let remote_ref = DistributedActorRef::<SmallMessageTestActor>::lookup("small_ask_actor")
        .await
        .expect("Lookup failed")
        .expect("Actor not found");

    // Test 500KB ask message - should use regular protocol
    let data_500kb = vec![42u8; 500_000]; // 500KB
    assert!(data_500kb.len() < MAX_STREAM_SIZE);
    let message = SmallTestMessage {
        data: data_500kb.clone(),
        id: "test_500kb".to_string(),
    };

    let start = Instant::now();
    let response: SmallTestResponse =
        timeout(Duration::from_secs(5), remote_ref.ask(message).send())
            .await
            .expect("Ask timeout")
            .expect("Ask failed");
    let duration = start.elapsed();

    println!("✅ 500KB ask message completed in {:?}", duration);

    // Verify response
    assert_eq!(response.processed_bytes, 500_000);
    assert_eq!(response.id, "test_500kb");
    assert!(response.success);
    assert!(
        duration < Duration::from_millis(200),
        "500KB ask should be fast"
    );

    // Cleanup
    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}

#[tokio::test]
#[serial_test::serial]
async fn test_boundary_condition_999kb() {
    // Test message just under 1MB threshold - should use regular protocol
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn")
        .try_init();

    // Bootstrap test nodes with different ports and explicit keypairs
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9420);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9421);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9420".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9421".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Register test actor
    let actor_ref = SmallMessageTestActor::spawn(SmallMessageTestActor);
    server_transport
        .register_distributed_actor("boundary_test_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");
    assert_type_registered::<SmallTestMessage>(actor_ref.id());

    // Connect client to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9420".parse().unwrap())
            .await
            .expect("Connection failed");
    }

    // Wait for connection stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Look up remote actor
    let remote_ref = DistributedActorRef::<SmallMessageTestActor>::lookup("boundary_test_actor")
        .await
        .expect("Lookup failed")
        .expect("Actor not found");

    // Test 999KB message - should still use regular protocol
    let data_999kb = vec![99u8; 999_999]; // Just under 1MB
    let message = SmallTestMessage {
        data: data_999kb.clone(),
        id: "test_999kb".to_string(),
    };

    let start = Instant::now();
    let response: SmallTestResponse =
        timeout(Duration::from_secs(5), remote_ref.ask(message).send())
            .await
            .expect("Ask timeout")
            .expect("Ask failed");
    let duration = start.elapsed();

    println!("✅ 999KB boundary ask message completed in {:?}", duration);

    // Verify response
    assert_eq!(response.processed_bytes, 999_999);
    assert_eq!(response.id, "test_999kb");
    assert!(response.success);

    // Should still be reasonably fast for regular protocol
    assert!(
        duration < Duration::from_millis(500),
        "999KB ask should be reasonably fast"
    );

    // Cleanup
    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}

#[tokio::test]
#[serial_test::serial]
async fn test_performance_baseline_small_messages() {
    // Establish performance baseline for small messages
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn")
        .try_init();

    // Bootstrap test nodes with different ports and explicit keypairs
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9430);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9431);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9430".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9431".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Register test actor
    let actor_ref = SmallMessageTestActor::spawn(SmallMessageTestActor);
    server_transport
        .register_distributed_actor("perf_test_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");
    assert_type_registered::<SmallTestMessage>(actor_ref.id());

    fn assert_type_registered<M: HasTypeHash>(actor_id: kameo::actor::ActorId) {
        let type_hash = <M as HasTypeHash>::TYPE_HASH.as_u32();
        let entry = type_registry::lookup_handler(type_hash).expect("type hash not registered");
        assert_eq!(entry.actor_id, actor_id);
    }

    // Connect client to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9430".parse().unwrap())
            .await
            .expect("Connection failed");
    }

    // Wait for connection stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Look up remote actor
    let remote_ref = DistributedActorRef::<SmallMessageTestActor>::lookup("perf_test_actor")
        .await
        .expect("Lookup failed")
        .expect("Actor not found");

    // Test multiple sizes to establish baseline
    let test_sizes = [1_000, 10_000, 100_000, 500_000]; // 1KB, 10KB, 100KB, 500KB

    for size in &test_sizes {
        let data = vec![(*size % 256) as u8; *size];
        let message = SmallTestMessage {
            data,
            id: format!("perf_test_{}", size),
        };

        let start = Instant::now();
        let response: SmallTestResponse =
            timeout(Duration::from_secs(5), remote_ref.ask(message).send())
                .await
                .expect("Ask timeout")
                .expect("Ask failed");
        let duration = start.elapsed();

        println!(
            "📊 {}KB message: {:?} ({:.2} MB/s)",
            size / 1000,
            duration,
            (*size as f64 / 1024.0 / 1024.0) / duration.as_secs_f64()
        );

        assert_eq!(response.processed_bytes, *size);
        assert!(response.success);

        // Performance assertions - regular protocol should be quite fast
        match *size {
            1_000 => assert!(
                duration < Duration::from_millis(10),
                "1KB should be very fast"
            ),
            10_000 => assert!(duration < Duration::from_millis(20), "10KB should be fast"),
            100_000 => assert!(
                duration < Duration::from_millis(50),
                "100KB should be reasonably fast"
            ),
            500_000 => assert!(
                duration < Duration::from_millis(100),
                "500KB should be acceptable"
            ),
            _ => {}
        }
    }

    // Cleanup
    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}
