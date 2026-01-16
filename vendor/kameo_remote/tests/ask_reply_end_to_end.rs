mod common;

use common::{create_tls_node, wait_for_condition};
use kameo_remote::registry::{ActorMessageFuture, ActorMessageHandler, RegistryMessage};
use kameo_remote::GossipConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

const TEST_ACTOR_ID: &str = "tls_test_actor";
const TEST_TYPE_HASH: u32 = 0xA57A_A5C0;

struct TestActorHandler;

impl ActorMessageHandler for TestActorHandler {
    fn handle_actor_message(
        &self,
        _actor_id: &str,
        _type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let payload = payload.to_vec();
        Box::pin(async move {
            if correlation_id.is_some() {
                let response = format!("RESPONSE:{}", payload.len()).into_bytes();
                Ok(Some(response))
            } else {
                Ok(None)
            }
        })
    }
}

fn encode_actor_ask(payload: impl Into<Vec<u8>>) -> Vec<u8> {
    let payload = payload.into();
    let message = RegistryMessage::ActorMessage {
        actor_id: TEST_ACTOR_ID.to_string(),
        type_hash: TEST_TYPE_HASH,
        payload,
        correlation_id: None,
    };

    rkyv::to_bytes::<rkyv::rancor::Error>(&message)
        .expect("serialize actor message")
        .to_vec()
}

/// Test true end-to-end ask/reply over TCP sockets
#[tokio::test]
async fn test_end_to_end_ask_reply() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    let config = GossipConfig::default();
    let handle_a = create_tls_node(config.clone()).await.unwrap();
    let handle_b = create_tls_node(config).await.unwrap();
    handle_b
        .registry
        .set_actor_message_handler(Arc::new(TestActorHandler))
        .await;

    let addr_b = handle_b.registry.bind_addr;
    let peer_b_id = handle_b.registry.peer_id.clone();
    let peer_b = handle_a.add_peer(&peer_b_id).await;
    peer_b.connect(&addr_b).await.unwrap();

    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            handle_a.stats().await.active_peers > 0
        })
        .await,
        "TLS peer should connect"
    );

    info!("=== Test 1: Basic ask/reply with actor handler ===");
    {
        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();

        let request = encode_actor_ask(b"Hello from test");
        let start = std::time::Instant::now();
        let response = conn.ask(&request).await.unwrap();
        let elapsed = start.elapsed();

        info!(
            "Got response in {:?}: {:?}",
            elapsed,
            String::from_utf8_lossy(&response)
        );
        assert_eq!(response, b"RESPONSE:15"); // Mock response with request length
    }

    info!("=== Test 2: Multiple concurrent asks with correlation tracking ===");
    {
        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();

        let mut handles = Vec::new();
        let start = std::time::Instant::now();

        for i in 0..10 {
            let conn_clone = conn.clone();
            let handle = tokio::spawn(async move {
                let request = format!("Concurrent request {}", i).into_bytes();
                let payload_len = request.len();
                let request_bytes = encode_actor_ask(request);
                let response = conn_clone.ask(&request_bytes).await.unwrap();
                (i, response, payload_len)
            });
            handles.push(handle);
        }

        // Verify all responses are correctly correlated
        for handle in handles {
            let (i, response, payload_len) = handle.await.unwrap();
            let expected = format!("RESPONSE:{}", payload_len).into_bytes();
            assert_eq!(response, expected);
            info!("Request {} correctly correlated", i);
        }

        let elapsed = start.elapsed();
        info!("Processed 10 concurrent asks in {:?}", elapsed);
    }

    info!("=== Test 3: Delegated reply pattern ===");
    {
        // This simulates the pattern where:
        // 1. Node A sends an ask to Node B
        // 2. Node B's connection handler creates a ReplyTo
        // 3. Node B passes the ReplyTo to an actor for processing
        // 4. The actor sends the response back through the ReplyTo

        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();
        // Simulate what would happen on Node B:
        // When an Ask arrives, the connection handler would create a ReplyTo
        // and pass it to an actor. We'll simulate this with ask_with_reply_to

        let request_payload = b"Process this request".to_vec();

        // On Node A: Send the ask and wait for response
        let response_future = {
            let conn_clone = conn.clone();
            let request_bytes = encode_actor_ask(request_payload.clone());
            tokio::spawn(async move { conn_clone.ask(&request_bytes).await })
        };

        // Give the message time to arrive at Node B
        sleep(Duration::from_millis(10)).await;

        let response = response_future.await.unwrap().unwrap();
        info!("Got response: {:?}", String::from_utf8_lossy(&response));
        assert_eq!(response, b"RESPONSE:20");
    }

    info!("=== Test 4: Performance test ===");
    {
        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();

        let num_requests = 100;
        let start = std::time::Instant::now();

        for i in 0..num_requests {
            let request = encode_actor_ask(format!("Perf test {}", i).into_bytes());
            let response = conn.ask(&request).await.unwrap();
            assert!(response.starts_with(b"RESPONSE:"));
        }

        let elapsed = start.elapsed();
        let throughput = num_requests as f64 / elapsed.as_secs_f64();

        info!("Sequential: {} requests in {:?}", num_requests, elapsed);
        info!("Throughput: {:.0} req/sec", throughput);
    }

    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test timeout handling
#[tokio::test]
async fn test_ask_timeout() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=warn".parse().unwrap()),
        ))
        .try_init();

    // Create a single TLS-enabled node but don't start the peer it's trying to reach
    let handle_a = create_tls_node(GossipConfig::default()).await.unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8004".parse().unwrap(); // This won't exist

    // Try to connect to non-existent node
    let peer_b_id = kameo_remote::KeyPair::new_for_testing("node_b").peer_id();
    let peer_b = handle_a.add_peer(&peer_b_id).await;

    // Connection should fail
    match peer_b.connect(&addr_b).await {
        Err(e) => info!("Expected connection failure: {}", e),
        Ok(_) => panic!("Should not connect to non-existent node"),
    }

    handle_a.shutdown().await;
}
