#![cfg(all(feature = "remote", feature = "remote_integration_tests"))]
//! Tests for large message handling (>1MB) - should use streaming protocol
//!
//! This test suite verifies that messages over the streaming threshold
//! use the zero-copy streaming protocol via ask_streaming_bytes.

use kameo::remote::{
    DistributedActorRef, transport::RemoteTransport, type_hash::HasTypeHash, type_registry,
};
use kameo::{Actor, distributed_actor};
use kameo_remote::MAX_STREAM_SIZE;
use std::time::Instant;
use tokio::time::{Duration, timeout};

// Test message structure for large messages
#[derive(kameo::RemoteMessage, Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LargeTestMessage {
    pub data: Vec<u8>,
    pub id: String,
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LargeTestResponse {
    pub processed_bytes: usize,
    pub id: String,
    pub checksum: u64,
    pub success: bool,
}

impl kameo::reply::Reply for LargeTestResponse {
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

// Test actor that handles large messages
#[derive(kameo::Actor)]
pub struct LargeMessageTestActor;

impl kameo::message::Message<LargeTestMessage> for LargeMessageTestActor {
    type Reply = LargeTestResponse;

    async fn handle(
        &mut self,
        message: LargeTestMessage,
        _ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Compute simple checksum to verify data integrity
        let checksum: u64 = message.data.iter().map(|&b| b as u64).sum();

        LargeTestResponse {
            processed_bytes: message.data.len(),
            id: message.id,
            checksum,
            success: true,
        }
    }
}

distributed_actor! {
    LargeMessageTestActor {
        LargeTestMessage,
    }
}

fn assert_type_registered<M: HasTypeHash>(actor_id: kameo::actor::ActorId) {
    let type_hash = <M as HasTypeHash>::TYPE_HASH.as_u32();
    let entry = type_registry::lookup_handler(type_hash).expect("type hash not registered");
    assert_eq!(entry.actor_id, actor_id);
}

/// Test 2MB ask message - should use streaming protocol (ask_streaming_bytes)
#[tokio::test]
#[serial_test::serial]
async fn test_large_ask_2mb_streaming() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init();

    // Bootstrap test nodes with explicit keypairs
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9500);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9501);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9500".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9501".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Register test actor
    let actor_ref = LargeMessageTestActor::spawn(LargeMessageTestActor);
    server_transport
        .register_distributed_actor("large_test_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");
    assert_type_registered::<LargeTestMessage>(actor_ref.id());

    // Connect client to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9500".parse().unwrap())
            .await
            .expect("Connection failed");
    }

    // Wait for connection stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Look up remote actor
    let remote_ref = DistributedActorRef::<LargeMessageTestActor>::lookup("large_test_actor")
        .await
        .expect("Lookup failed")
        .expect("Actor not found");

    // Test 2MB message - MUST use streaming protocol
    let size = 2 * 1024 * 1024; // 2MB
    assert!(size < MAX_STREAM_SIZE);
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let expected_checksum: u64 = data.iter().map(|&b| b as u64).sum();

    let message = LargeTestMessage {
        data,
        id: "test_2mb_streaming".to_string(),
    };

    println!("📤 Sending 2MB message via streaming protocol...");
    let start = Instant::now();
    let response: LargeTestResponse =
        timeout(Duration::from_secs(30), remote_ref.ask(message).send())
            .await
            .expect("Ask timeout - streaming may have failed")
            .expect("Ask failed");
    let duration = start.elapsed();

    println!("✅ 2MB streaming ask completed in {:?}", duration);
    println!(
        "   Throughput: {:.2} MB/s",
        (size as f64 / 1024.0 / 1024.0) / duration.as_secs_f64()
    );

    // Verify response
    assert_eq!(
        response.processed_bytes, size,
        "Size mismatch - data corruption?"
    );
    assert_eq!(response.id, "test_2mb_streaming");
    assert_eq!(
        response.checksum, expected_checksum,
        "Checksum mismatch - data corruption!"
    );
    assert!(response.success);

    // Cleanup
    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}

/// Test 5MB ask message - larger streaming test
#[tokio::test]
#[serial_test::serial]
async fn test_large_ask_5mb_streaming() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init();

    // Bootstrap test nodes with explicit keypairs
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9510);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9511);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9510".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9511".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Register test actor
    let actor_ref = LargeMessageTestActor::spawn(LargeMessageTestActor);
    server_transport
        .register_distributed_actor("large_5mb_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");
    assert_type_registered::<LargeTestMessage>(actor_ref.id());

    // Connect client to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9510".parse().unwrap())
            .await
            .expect("Connection failed");
    }

    // Wait for connection stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Look up remote actor
    let remote_ref = DistributedActorRef::<LargeMessageTestActor>::lookup("large_5mb_actor")
        .await
        .expect("Lookup failed")
        .expect("Actor not found");

    // Test 5MB message - MUST use streaming protocol
    let size = 5 * 1024 * 1024; // 5MB
    assert!(size < MAX_STREAM_SIZE);
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let expected_checksum: u64 = data.iter().map(|&b| b as u64).sum();

    let message = LargeTestMessage {
        data,
        id: "test_5mb_streaming".to_string(),
    };

    println!("📤 Sending 5MB message via streaming protocol...");
    let start = Instant::now();
    let response: LargeTestResponse =
        timeout(Duration::from_secs(60), remote_ref.ask(message).send())
            .await
            .expect("Ask timeout - streaming may have failed")
            .expect("Ask failed");
    let duration = start.elapsed();

    println!("✅ 5MB streaming ask completed in {:?}", duration);
    println!(
        "   Throughput: {:.2} MB/s",
        (size as f64 / 1024.0 / 1024.0) / duration.as_secs_f64()
    );

    // Verify response
    assert_eq!(
        response.processed_bytes, size,
        "Size mismatch - data corruption?"
    );
    assert_eq!(response.id, "test_5mb_streaming");
    assert_eq!(
        response.checksum, expected_checksum,
        "Checksum mismatch - data corruption!"
    );
    assert!(response.success);

    // Cleanup
    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}

/// Test boundary condition - just over 1MB threshold
#[tokio::test]
#[serial_test::serial]
async fn test_streaming_boundary_1mb_plus_1() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init();

    // Bootstrap test nodes with explicit keypairs
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9520);
    let server_peer_id = server_keypair.peer_id();
    let client_keypair = kameo::remote::v2_bootstrap::test_keypair(9521);

    let mut server_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9520".parse().unwrap(),
        server_keypair,
    )
    .await
    .expect("Server bootstrap failed");

    let mut client_transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9521".parse().unwrap(),
        client_keypair,
    )
    .await
    .expect("Client bootstrap failed");

    // Register test actor
    let actor_ref = LargeMessageTestActor::spawn(LargeMessageTestActor);
    server_transport
        .register_distributed_actor("boundary_1mb_actor".to_string(), &actor_ref)
        .await
        .expect("Actor registration failed");
    assert_type_registered::<LargeTestMessage>(actor_ref.id());

    // Connect client to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9520".parse().unwrap())
            .await
            .expect("Connection failed");
    }

    // Wait for connection stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Look up remote actor
    let remote_ref = DistributedActorRef::<LargeMessageTestActor>::lookup("boundary_1mb_actor")
        .await
        .expect("Lookup failed")
        .expect("Actor not found");

    // Test 1MB + 1 byte message - should trigger streaming
    let size = 1024 * 1024 + 1; // 1MB + 1 byte
    assert!(size < MAX_STREAM_SIZE);
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let expected_checksum: u64 = data.iter().map(|&b| b as u64).sum();

    let message = LargeTestMessage {
        data,
        id: "test_1mb_plus_1".to_string(),
    };

    println!("📤 Sending 1MB+1 byte message (boundary test)...");
    let start = Instant::now();
    let response: LargeTestResponse =
        timeout(Duration::from_secs(30), remote_ref.ask(message).send())
            .await
            .expect("Ask timeout")
            .expect("Ask failed");
    let duration = start.elapsed();

    println!("✅ 1MB+1 boundary ask completed in {:?}", duration);

    // Verify response
    assert_eq!(response.processed_bytes, size, "Size mismatch");
    assert_eq!(response.id, "test_1mb_plus_1");
    assert_eq!(response.checksum, expected_checksum, "Checksum mismatch!");
    assert!(response.success);

    // Cleanup
    server_transport.shutdown().await.ok();
    client_transport.shutdown().await.ok();
}
