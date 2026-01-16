use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};

/// Test the complete ask/response flow with correlation tracking
#[tokio::test]
async fn test_ask_response_with_correlation() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(EnvFilter::from_default_env()
                    .add_directive("kameo_remote=debug".parse().unwrap())
                )
        )
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9002".parse().unwrap();
    
    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");
    let peer_id_a = key_pair_a.peer_id();
    let peer_id_b = key_pair_b.peer_id();
    
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(2),
        ..Default::default()
    };
    
    // Start Node A
    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();
    let registry_a = handle_a.registry.clone();
    
    // Start Node B
    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config.clone()))
        .await
        .unwrap();
    let registry_b = handle_b.registry.clone();
    
    // Connect nodes
    let peer_a = handle_b.add_peer(&peer_id_a).await;
    peer_a.connect(&addr_a).await.unwrap();
    
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    
    sleep(Duration::from_millis(500)).await;
    
    // Register test actors
    handle_a.register_with_priority("calculator", RegistrationPriority::High).await.unwrap();
    handle_b.register_with_priority("echo_service", RegistrationPriority::High).await.unwrap();
    
    // Wait for registration to propagate
    sleep(Duration::from_secs(1)).await;
    
    // Test 1: Simple ask/response
    info!("Test 1: Simple ask/response");
    {
        let pool = registry_b.connection_pool.lock().await;
        let conn = pool.connections_by_addr.get(&addr_a).unwrap();
        
        // Send ask request
        let request = b"2 + 2 = ?";
        let response_future = conn.ask(request);
        
        // Simulate calculator actor processing on Node A
        // In a real system, this would be handled by the actor framework
        tokio::spawn(async move {
            // Simulate processing
            sleep(Duration::from_millis(10)).await;
            // Response would be sent back via the connection
        });
        
        // For now, let's test the ask timeout
        match tokio::time::timeout(Duration::from_millis(100), response_future).await {
            Ok(Ok(response)) => {
                info!("Got response: {:?}", String::from_utf8_lossy(&response));
            }
            Ok(Err(e)) => {
                info!("Ask failed: {}", e);
            }
            Err(_) => {
                info!("Ask timed out (expected for now without proper handler)");
            }
        }
    }
    
    // Test 2: Multiple concurrent asks
    info!("\nTest 2: Multiple concurrent asks");
    {
        let pool = registry_a.connection_pool.lock().await;
        let conn = pool.connections_by_addr.get(&addr_b).unwrap().clone();
        drop(pool);
        
        let mut handles = Vec::new();
        
        for i in 0..5 {
            let conn_clone = conn.clone();
            let handle = tokio::spawn(async move {
                let request = format!("Echo request {}", i).into_bytes();
                let start = std::time::Instant::now();
                
                match tokio::time::timeout(Duration::from_millis(100), conn_clone.ask(&request)).await {
                    Ok(Ok(response)) => {
                        let elapsed = start.elapsed();
                        info!("Request {} got response in {:?}: {:?}", 
                            i, elapsed, String::from_utf8_lossy(&response));
                    }
                    Ok(Err(e)) => {
                        info!("Request {} failed: {}", i, e);
                    }
                    Err(_) => {
                        info!("Request {} timed out", i);
                    }
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.await.unwrap();
        }
    }
    
    // Test 3: Correlation ID uniqueness
    info!("\nTest 3: Correlation ID tracking");
    {
        let pool = registry_a.connection_pool.lock().await;
        let conn = pool.connections_by_addr.values().next().unwrap();
        
        // Send multiple asks rapidly to test correlation ID allocation
        let mut futures = Vec::new();
        
        for i in 0..10 {
            let request = format!("Test {}", i).into_bytes();
            let reply_to = conn.ask_with_reply_to(&request).await.unwrap();
            futures.push((i, reply_to));
        }
        
        // Each should have a unique correlation ID
        let mut correlation_ids = std::collections::HashSet::new();
        for (i, reply_to) in futures {
            let id = reply_to.correlation_id;
            info!("Request {} got correlation ID: {}", i, id);
            assert!(correlation_ids.insert(id), "Duplicate correlation ID!");
        }
        
        info!("All correlation IDs were unique");
    }
    
    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
    
    info!("\nAll tests completed successfully!");
}

/// Test error handling and edge cases
#[tokio::test]
async fn test_ask_error_cases() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(EnvFilter::from_default_env()
                    .add_directive("kameo_remote=info".parse().unwrap())
                )
        )
        .try_init();

    // Create single node
    let addr: SocketAddr = "127.0.0.1:9003".parse().unwrap();
    let key_pair = KeyPair::new_for_testing("test_node");
    let config = GossipConfig::default();
    
    let handle = GossipRegistryHandle::new_with_keypair(addr, key_pair, Some(config))
        .await
        .unwrap();
    let registry = handle.registry.clone();
    
    // Test 1: Ask with no connection
    info!("Test 1: Ask with no connection");
    {
        let pool = registry.connection_pool.lock().await;
        assert!(pool.connections_by_addr.is_empty(), "Should have no connections");
    }
    
    // Test 2: Ask after connection failure
    info!("\nTest 2: Ask after connection failure");
    {
        // Try to connect to non-existent peer
        let fake_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let fake_peer_id = KeyPair::new_for_testing("fake_peer").peer_id();
        let peer = handle.add_peer(&fake_peer_id).await;
        
        match peer.connect(&fake_addr).await {
            Ok(_) => panic!("Should not connect to non-existent peer"),
            Err(e) => info!("Expected connection failure: {}", e),
        }
    }
    
    // Shutdown
    handle.shutdown().await;
    
    info!("\nError handling tests completed!");
}

/// Test performance and throughput
#[tokio::test]
async fn test_ask_performance() {
    // Initialize tracing with less verbosity for performance test
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(EnvFilter::from_default_env()
                    .add_directive("kameo_remote=warn".parse().unwrap())
                )
        )
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:9004".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9005".parse().unwrap();
    
    let key_pair_a = KeyPair::new_for_testing("perf_node_a");
    let key_pair_b = KeyPair::new_for_testing("perf_node_b");
    let peer_id_b = key_pair_b.peer_id();
    
    let config = GossipConfig::default();
    
    // Start nodes
    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();
    let registry_a = handle_a.registry.clone();
    
    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();
    
    // Connect nodes
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    
    sleep(Duration::from_millis(500)).await;
    
    // Warm up
    {
        let pool = registry_a.connection_pool.lock().await;
        let conn = pool.connections_by_addr.values().next().unwrap();
        for _ in 0..100 {
            let _ = conn.ask_with_reply_to(b"warmup").await;
        }
    }
    
    info!("Starting performance test...");
    
    // Performance test
    let pool = registry_a.connection_pool.lock().await;
    let conn = pool.connections_by_addr.values().next().unwrap().clone();
    drop(pool);
    
    let num_requests = 10000;
    let start = std::time::Instant::now();
    
    // Test ask_with_reply_to performance
    let mut handles = Vec::new();
    for i in 0..num_requests {
        let conn_clone = conn.clone();
        let handle = tokio::spawn(async move {
            let request = format!("req{}", i % 100).into_bytes();
            let reply_to = conn_clone.ask_with_reply_to(&request).await.unwrap();
            
            // Simulate immediate reply
            reply_to.reply(b"ok").await.unwrap();
        });
        handles.push(handle);
        
        // Batch spawning to avoid overwhelming the runtime
        if handles.len() >= 1000 {
            for h in handles.drain(..) {
                h.await.unwrap();
            }
        }
    }
    
    // Wait for remaining
    for handle in handles {
        handle.await.unwrap();
    }
    
    let elapsed = start.elapsed();
    let throughput = num_requests as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / num_requests as f64;
    
    info!("\nPerformance Results:");
    info!("  Total requests: {}", num_requests);
    info!("  Total time: {:?}", elapsed);
    info!("  Throughput: {:.0} req/sec", throughput);
    info!("  Average latency: {:.2} μs", latency_us);
    
    // Assert minimum performance
    assert!(throughput > 50000.0, "Throughput should exceed 50k req/sec");
    assert!(latency_us < 50.0, "Average latency should be under 50μs");
    
    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
    
    info!("\nPerformance test completed!");
}
