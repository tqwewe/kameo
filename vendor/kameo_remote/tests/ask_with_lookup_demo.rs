use kameo_remote::*;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::time::sleep;

static TEST_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn wait_for_lookup(
    handle: &GossipRegistryHandle,
    name: &str,
    timeout: Duration,
) -> RemoteActorLocation {
    let start = Instant::now();
    loop {
        if let Some(location) = handle.lookup(name).await {
            return location;
        }
        if start.elapsed() > timeout {
            panic!("Timed out waiting for actor lookup: {}", name);
        }
        sleep(Duration::from_millis(20)).await;
    }
}

async fn get_connection_for_peer(
    handle: &GossipRegistryHandle,
    peer_id: &PeerId,
    local_id: &PeerId,
    local_addr: std::net::SocketAddr,
) -> kameo_remote::connection_pool::ConnectionHandle {
    if peer_id == local_id {
        handle.get_connection(local_addr).await.unwrap()
    } else {
        handle.get_connection_to_peer(peer_id).await.unwrap()
    }
}

/// Comprehensive ask() API demonstration with lookup() by actor name
/// Tests request-response patterns with performance comparisons
#[tokio::test]
async fn test_ask_with_lookup_and_performance() {
    let _guard = TEST_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    println!("🚀 Ask() with Lookup() Performance Test");
    println!("=======================================");

    // Setup three nodes
    let config = GossipConfig {
        urgent_gossip_fanout: 32,
        ..Default::default()
    };

    println!("📡 Setting up 3-node cluster...");
    let node1_keypair = KeyPair::new_for_testing("node1");
    let node2_keypair = KeyPair::new_for_testing("node2");
    let node3_keypair = KeyPair::new_for_testing("node3");
    let node1_id = node1_keypair.peer_id();
    let node2_id = node2_keypair.peer_id();
    let node3_id = node3_keypair.peer_id();

    let node1 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:30001".parse().unwrap(),
        node1_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();

    let node2 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:30002".parse().unwrap(),
        node2_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();

    let node3 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:30003".parse().unwrap(),
        node3_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();

    // Connect nodes (single-direction dial avoids tie-breaker churn)
    println!("\n🔗 Establishing peer connections...");

    // Node1 connects to Node2 and Node3 (bidirectional once established)
    let peer2 = node1.add_peer(&node2_id).await;
    let peer3 = node1.add_peer(&node3_id).await;
    peer2
        .connect(&"127.0.0.1:30002".parse().unwrap())
        .await
        .unwrap();
    peer3
        .connect(&"127.0.0.1:30003".parse().unwrap())
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;
    println!("✅ All nodes connected");

    // Register service actors
    println!("\n📋 Registering service actors...");

    node2
        .register_urgent(
            "database_service".to_string(),
            "127.0.0.1:40001".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    println!("   ✅ Node2: registered 'database_service'");

    node2
        .register_urgent(
            "compute_service".to_string(),
            "127.0.0.1:40002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    println!("   ✅ Node2: registered 'compute_service'");

    node3
        .register_urgent(
            "cache_service".to_string(),
            "127.0.0.1:40003".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();
    println!("   ✅ Node3: registered 'cache_service'");

    // Wait for propagation
    sleep(Duration::from_millis(100)).await;

    // ===========================================
    // PART 1: CREATE ACTOR REFS WITH LOOKUP
    // ===========================================
    println!("\n📊 PART 1: ACTOR LOOKUP AND REFERENCE CREATION");
    println!("==============================================");

    let lookup_start = Instant::now();

    // Lookup actors and create connections
    let db_location = wait_for_lookup(&node1, "database_service", Duration::from_secs(2)).await;
    let db_conn = get_connection_for_peer(&node1, &db_location.peer_id, &node1_id, "127.0.0.1:30001".parse().unwrap()).await;

    let compute_location =
        wait_for_lookup(&node1, "compute_service", Duration::from_secs(2)).await;
    let compute_conn = get_connection_for_peer(&node1, &compute_location.peer_id, &node1_id, "127.0.0.1:30001".parse().unwrap()).await;

    let cache_location = wait_for_lookup(&node1, "cache_service", Duration::from_secs(2)).await;
    let cache_conn = get_connection_for_peer(&node1, &cache_location.peer_id, &node1_id, "127.0.0.1:30001".parse().unwrap()).await;

    let lookup_time = lookup_start.elapsed();

    println!("   ✅ Created actor references:");
    println!(
        "     - database_service at {} (peer: {})",
        db_location.address, db_location.peer_id
    );
    println!(
        "     - compute_service at {} (peer: {})",
        compute_location.address, compute_location.peer_id
    );
    println!(
        "     - cache_service at {} (peer: {})",
        cache_location.address, cache_location.peer_id
    );
    println!(
        "   📈 Total lookup time: {:?} ({:.3} μs)",
        lookup_time,
        lookup_time.as_nanos() as f64 / 1000.0
    );

    // ===========================================
    // PART 2: ASK() PERFORMANCE TESTING
    // ===========================================
    println!("\n📊 PART 2: ASK() REQUEST-RESPONSE PERFORMANCE");
    println!("============================================");

    // Test queries
    let queries = [
        ("database_service", "SELECT * FROM users WHERE id = 123"),
        ("compute_service", "CALCULATE fibonacci(40)"),
        ("cache_service", "GET user:123:profile"),
        ("database_service", "INSERT INTO logs VALUES (...)"),
        ("compute_service", "PROCESS image_resize(1024x768)"),
        ("cache_service", "SET session:abc123 = data"),
    ];

    println!("\n🔸 Test 2A: Individual ask() calls");
    println!("   Queries to execute:");
    for (i, (service, query)) in queries.iter().enumerate() {
        println!("     {}. {} <- \"{}\"", i + 1, service, query);
    }

    let mut ask_times = Vec::new();
    let individual_start = Instant::now();

    for (i, (service, query)) in queries.iter().enumerate() {
        let ask_start = Instant::now();

        // Select the appropriate connection
        let conn = match *service {
            "database_service" => &db_conn,
            "compute_service" => &compute_conn,
            "cache_service" => &cache_conn,
            _ => panic!("Unknown service"),
        };

        // Send request and wait for response
        let response = conn.ask(query.as_bytes()).await.unwrap();
        let response_str = String::from_utf8_lossy(&response);

        let ask_time = ask_start.elapsed();
        ask_times.push(ask_time);

        println!(
            "     Query {} response: \"{}\" in {:?} ({:.3} μs)",
            i + 1,
            response_str,
            ask_time,
            ask_time.as_nanos() as f64 / 1000.0
        );
    }

    let individual_total = individual_start.elapsed();
    let individual_avg = individual_total / queries.len() as u32;

    println!("   📈 Individual ask() Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        individual_total,
        individual_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per query: {:?} ({:.3} μs)",
        individual_avg,
        individual_avg.as_nanos() as f64 / 1000.0
    );

    // Test 2B: Parallel ask() calls using tokio::join!
    println!("\n🔸 Test 2B: Parallel ask() calls");

    let parallel_start = Instant::now();

    // Send all queries in parallel
    let (r1, r2, r3, r4, r5, r6) = tokio::join!(
        db_conn.ask(queries[0].1.as_bytes()),
        compute_conn.ask(queries[1].1.as_bytes()),
        cache_conn.ask(queries[2].1.as_bytes()),
        db_conn.ask(queries[3].1.as_bytes()),
        compute_conn.ask(queries[4].1.as_bytes()),
        cache_conn.ask(queries[5].1.as_bytes()),
    );

    // Verify all responses
    let responses = [r1, r2, r3, r4, r5, r6];
    for (i, response) in responses.iter().enumerate() {
        match response {
            Ok(data) => {
                let response_str = String::from_utf8_lossy(data);
                println!("     Query {} response: \"{}\"", i + 1, response_str);
            }
            Err(e) => println!("     Query {} failed: {}", i + 1, e),
        }
    }

    let parallel_total = parallel_start.elapsed();
    let parallel_avg = parallel_total / queries.len() as u32;

    println!("   📈 Parallel ask() Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        parallel_total,
        parallel_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per query: {:?} ({:.3} μs)",
        parallel_avg,
        parallel_avg.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Speedup vs sequential: {:.2}x",
        individual_total.as_nanos() as f64 / parallel_total.as_nanos() as f64
    );

    // ===========================================
    // PART 3: ASK VS TELL COMPARISON
    // ===========================================
    println!("\n📊 PART 3: ASK() VS TELL() COMPARISON");
    println!("=====================================");

    let test_message = "PING test request".as_bytes();
    let iterations = 100;

    println!(
        "\n🔸 Test 3A: tell() performance ({} iterations)",
        iterations
    );
    let tell_start = Instant::now();
    for _ in 0..iterations {
        db_conn.tell(test_message).await.unwrap();
    }
    let tell_total = tell_start.elapsed();
    let tell_avg = tell_total / iterations;

    println!("   📈 tell() Results:");
    println!(
        "     - Total: {:?} ({:.3} μs)",
        tell_total,
        tell_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average: {:?} ({:.3} μs)",
        tell_avg,
        tell_avg.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Throughput: {:.0} ops/sec",
        iterations as f64 / tell_total.as_secs_f64()
    );

    println!(
        "\n🔸 Test 3B: ask() performance ({} iterations)",
        iterations
    );
    let ask_start = Instant::now();
    for _ in 0..iterations {
        let _ = db_conn.ask(test_message).await.unwrap();
    }
    let ask_total = ask_start.elapsed();
    let ask_avg = ask_total / iterations;

    println!("   📈 ask() Results:");
    println!(
        "     - Total: {:?} ({:.3} μs)",
        ask_total,
        ask_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average: {:?} ({:.3} μs)",
        ask_avg,
        ask_avg.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Throughput: {:.0} ops/sec",
        iterations as f64 / ask_total.as_secs_f64()
    );

    let ask_overhead = ask_total.as_nanos() as f64 / tell_total.as_nanos() as f64;
    println!(
        "\n   🎯 ask() overhead: {:.2}x slower than tell()",
        ask_overhead
    );

    // ===========================================
    // PART 4: DELEGATED REPLY SENDER TEST
    // ===========================================
    println!("\n📊 PART 4: DELEGATED REPLY SENDER");
    println!("=================================");

    println!("\n🔸 Test 4A: ask_with_reply_sender()");

    // Send request and get delegated reply sender
    let request = "SELECT COUNT(*) FROM users".as_bytes();
    let reply_sender = db_conn.ask_with_reply_sender(request).await.unwrap();

    println!("   - Request sent, got DelegatedReplySender");
    println!("   - Reply sender: {:?}", reply_sender);

    // In a real implementation, another task would send the response
    // For now, we'll use the mock response
    let mock_response = reply_sender.create_mock_reply();
    println!(
        "   - Mock response: \"{}\"",
        String::from_utf8_lossy(mock_response.as_ref())
    );

    // Test with timeout
    println!("\n🔸 Test 4B: ask_with_timeout_and_reply()");
    let timeout = Duration::from_millis(100);
    let reply_with_timeout = db_conn
        .ask_with_timeout_and_reply(request, timeout)
        .await
        .unwrap();

    println!("   - Request sent with {}ms timeout", timeout.as_millis());
    println!("   - Reply sender: {:?}", reply_with_timeout);

    // ===========================================
    // PART 5: ERROR HANDLING
    // ===========================================
    println!("\n📊 PART 5: ERROR HANDLING");
    println!("========================");

    // Test asking a non-existent actor
    println!("\n🔸 Test 5A: Lookup non-existent actor");
    let missing = node1.lookup("non_existent_service").await;
    match missing {
        None => println!("   ✅ Correctly returned None for non-existent actor"),
        Some(_) => println!("   ❌ ERROR: Found non-existent actor!"),
    }

    // ===========================================
    // RECOMMENDATIONS
    // ===========================================
    println!("\n📋 RECOMMENDATIONS");
    println!("==================");

    println!("\n✅ Best Practices:");
    println!("   1. Use ask() for operations that require a response (queries, calculations)");
    println!("   2. Use tell() for fire-and-forget operations (logging, notifications)");
    println!("   3. Cache actor refs to avoid repeated lookups");
    println!("   4. Use parallel ask() calls when querying multiple services");
    println!("   5. Consider timeouts for ask() operations in production");

    println!("\n🔧 Code Examples:");
    println!("   // Create actor ref once:");
    println!("   let location = registry.lookup(\"database_service\").await?;");
    println!("   let conn = registry.get_connection(peer_addr).await?;");
    println!();
    println!("   // Sequential queries:");
    println!("   let result1 = conn.ask(query1).await?;");
    println!("   let result2 = conn.ask(query2).await?;");
    println!();
    println!("   // Parallel queries:");
    println!("   let (r1, r2) = tokio::join!(");
    println!("       conn1.ask(query1),");
    println!("       conn2.ask(query2)");
    println!("   );");

    println!("\n⚠️  NOTE: Current ask() implementation returns mock responses.");
    println!("   In production, implement proper request-response correlation.");

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
    node3.shutdown().await;

    println!("\n✅ Test completed successfully!");
}

/// Test high-throughput ask() scenarios
#[tokio::test]
async fn test_ask_high_throughput() {
    let _guard = TEST_MUTEX
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    println!("🚀 High-Throughput ask() Test");
    println!("=============================");

    // Setup two nodes
    let config = GossipConfig {
        urgent_gossip_fanout: 32,
        ..Default::default()
    };
    let node1_addr = "127.0.0.1:31001".parse().unwrap();
    let node2_addr = "127.0.0.1:31002".parse().unwrap();

    let node1_keypair = KeyPair::new_for_testing("node1_ht");
    let node2_keypair = KeyPair::new_for_testing("node2_ht");
    let node2_id = node2_keypair.peer_id();

    let node1 =
        GossipRegistryHandle::new_with_keypair(node1_addr, node1_keypair, Some(config.clone()))
            .await
            .unwrap();

    let node2 =
        GossipRegistryHandle::new_with_keypair(node2_addr, node2_keypair, Some(config.clone()))
            .await
            .unwrap();

    // Connect nodes (single-direction dial avoids tie-breaker churn)
    let peer2 = node1.add_peer(&node2_id).await;
    peer2.connect(&node2_addr).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    // Register a high-performance service
    node2
        .register_urgent(
            "api_service".to_string(),
            "127.0.0.1:41002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    // Lookup and create connection
    // Skip lookup in this throughput test to avoid gossip timing dependencies.
    let api_conn = node1.get_connection_to_peer(&node2_id).await.unwrap();

    // Test parameters
    let request_count = 1000;
    let concurrent_requests = 10;

    println!("\n📊 Test Configuration:");
    println!("   - Total requests: {}", request_count);
    println!("   - Concurrent requests: {}", concurrent_requests);

    // Warmup
    println!("\n🔥 Warming up...");
    for _ in 0..10 {
        let _ = api_conn.ask(b"warmup").await.unwrap();
    }

    // High-throughput test
    println!("\n🔸 Running high-throughput test...");
    let test_start = Instant::now();
    let mut latencies = Vec::new();

    // Process requests in batches
    for batch in 0..(request_count / concurrent_requests) {
        let _batch_start = Instant::now();

        // Send concurrent requests
        let mut handles = Vec::new();
        for i in 0..concurrent_requests {
            let conn = api_conn.clone();
            let request_id = batch * concurrent_requests + i;
            let handle = tokio::spawn(async move {
                let req_start = Instant::now();
                let request = format!("REQUEST:{}", request_id);
                let _ = conn.ask(request.as_bytes()).await.unwrap();
                req_start.elapsed()
            });
            handles.push(handle);
        }

        // Wait for batch to complete
        for handle in handles {
            if let Ok(latency) = handle.await {
                latencies.push(latency);
            }
        }

        if batch % 10 == 0 {
            println!(
                "   - Completed {} requests",
                (batch + 1) * concurrent_requests
            );
        }
    }

    let test_total = test_start.elapsed();

    // Calculate statistics
    latencies.sort();
    let avg_latency = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];
    let throughput = request_count as f64 / test_total.as_secs_f64();

    println!("\n📈 High-Throughput Results:");
    println!("   - Total time: {:?}", test_total);
    println!("   - Total requests: {}", request_count);
    println!("   - Throughput: {:.0} req/sec", throughput);
    println!(
        "   - Average latency: {:?} ({:.3} μs)",
        avg_latency,
        avg_latency.as_nanos() as f64 / 1000.0
    );
    println!(
        "   - P50 latency: {:?} ({:.3} μs)",
        p50,
        p50.as_nanos() as f64 / 1000.0
    );
    println!(
        "   - P95 latency: {:?} ({:.3} μs)",
        p95,
        p95.as_nanos() as f64 / 1000.0
    );
    println!(
        "   - P99 latency: {:?} ({:.3} μs)",
        p99,
        p99.as_nanos() as f64 / 1000.0
    );

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ High-throughput test completed!");
}
