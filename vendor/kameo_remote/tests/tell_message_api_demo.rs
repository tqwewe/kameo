use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Comprehensive TellMessage API demonstration
/// Tests both gossip and non-gossip data with clear performance comparisons
#[tokio::test]
async fn test_tell_message_api_comprehensive() {
    println!("🚀 TellMessage API Comprehensive Test");
    println!("=====================================");

    // Setup two nodes with basic gossip (TLS-only)
    let node1_addr = "127.0.0.1:27001".parse().unwrap();
    let node2_addr = "127.0.0.1:27002".parse().unwrap();

    let node1_keypair = KeyPair::new_for_testing("node1");
    let node2_keypair = KeyPair::new_for_testing("node2");

    let node1 = GossipRegistryHandle::new_with_keypair(
        node1_addr,
        node1_keypair,
        Some(GossipConfig::default()),
    )
    .await
    .unwrap();
    let node2 = GossipRegistryHandle::new_with_keypair(
        node2_addr,
        node2_keypair,
        Some(GossipConfig::default()),
    )
    .await
    .unwrap();

    // Wait for connection establishment
    sleep(Duration::from_millis(100)).await;

    // Get the raw connection handle (this is what applications would use)
    let conn = node1.get_connection(node2_addr).await.unwrap();
    println!("✅ Connection established between nodes");

    // ===========================================
    // PART 1: NON-GOSSIP APPLICATION DATA
    // ===========================================
    println!("\n📊 PART 1: NON-GOSSIP APPLICATION DATA");
    println!("======================================");

    // Test 1: Single application messages (like chat messages, API requests, etc.)
    println!("\n🔸 Test 1A: Single Application Messages");
    let chat_messages = [
        "Hello, how are you?",
        "I'm working on a distributed system",
        "The TellMessage API is really cool!",
        "It supports both single and batch messages",
        "Performance is excellent on localhost",
    ];

    println!("   Messages to send:");
    for (i, msg) in chat_messages.iter().enumerate() {
        println!("     {}. {:?} ({} bytes)", i + 1, msg, msg.len());
    }

    // Send each message individually
    println!("\n   Sending messages individually (5 separate tell() calls):");
    let single_start = Instant::now();
    for (i, msg) in chat_messages.iter().enumerate() {
        let start = Instant::now();
        conn.tell(msg.as_bytes()).await.unwrap();
        let elapsed = start.elapsed();
        println!(
            "     Message {}: {:?} μs",
            i + 1,
            elapsed.as_nanos() as f64 / 1000.0
        );
    }
    let single_total = single_start.elapsed();

    println!("   📈 Single Message Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        single_total,
        single_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per message: {:?} ({:.3} μs)",
        single_total / 5,
        single_total.as_nanos() as f64 / 5000.0
    );
    println!("     - Method: 5 separate conn.tell() calls");

    // Test 1B: Same messages sent as a batch
    println!("\n🔸 Test 1B: Same Messages as Single Batch");
    let batch_data: Vec<&[u8]> = chat_messages.iter().map(|msg| msg.as_bytes()).collect();

    println!("   Batch configuration:");
    println!("     - {} messages in one batch", batch_data.len());
    println!(
        "     - Total payload: {} bytes",
        batch_data.iter().map(|msg| msg.len()).sum::<usize>()
    );
    println!("     - Method: Single conn.tell() call with batch");

    let batch_start = Instant::now();
    conn.tell(kameo_remote::connection_pool::TellMessage::batch(
        batch_data,
    ))
    .await
    .unwrap();
    let batch_total = batch_start.elapsed();

    println!("   📈 Batch Message Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        batch_total,
        batch_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per message: {:?} ({:.3} μs)",
        batch_total / 5,
        batch_total.as_nanos() as f64 / 5000.0
    );
    println!("     - Method: 1 batched conn.tell() call");

    let improvement = single_total.as_nanos() as f64 / batch_total.as_nanos() as f64;
    println!(
        "   🎯 Performance Improvement: {:.2}x faster with batching",
        improvement
    );

    // ===========================================
    // PART 2: LARGER DATA PAYLOADS
    // ===========================================
    println!("\n📊 PART 2: LARGER DATA PAYLOADS (File Transfer Simulation)");
    println!("=========================================================");

    // Simulate transferring multiple files
    let file_data = vec![
        ("config.json".to_string(), vec![b'{'; 1024]), // 1KB file
        ("data.csv".to_string(), vec![b','; 2048]),    // 2KB file
        ("image.jpg".to_string(), vec![b'x'; 4096]),   // 4KB file
        ("document.pdf".to_string(), vec![b'%'; 8192]), // 8KB file
    ];

    println!("\n🔸 Test 2A: File Transfer - Individual Sends");
    println!("   Files to transfer:");
    for (name, data) in &file_data {
        println!("     - {}: {} bytes", name, data.len());
    }

    let file_single_start = Instant::now();
    for (i, (name, data)) in file_data.iter().enumerate() {
        let start = Instant::now();
        conn.tell(data.as_slice()).await.unwrap();
        let elapsed = start.elapsed();
        println!(
            "     File {}: {} transferred in {:?} ({:.3} μs)",
            i + 1,
            name,
            elapsed,
            elapsed.as_nanos() as f64 / 1000.0
        );
    }
    let file_single_total = file_single_start.elapsed();

    let total_bytes: usize = file_data.iter().map(|(_, data)| data.len()).sum();
    let single_throughput =
        total_bytes as f64 / file_single_total.as_secs_f64() / (1024.0 * 1024.0);

    println!("   📈 Individual File Transfer Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        file_single_total,
        file_single_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Total data: {} bytes ({:.2} KB)",
        total_bytes,
        total_bytes as f64 / 1024.0
    );
    println!("     - Throughput: {:.2} MB/s", single_throughput);

    // Test 2B: Same files sent as a batch
    println!("\n🔸 Test 2B: File Transfer - Batch Send");
    let file_batch_data: Vec<&[u8]> = file_data.iter().map(|(_, data)| data.as_slice()).collect();

    println!("   Batch transfer configuration:");
    println!("     - {} files in one batch", file_batch_data.len());
    println!(
        "     - Total payload: {} bytes ({:.2} KB)",
        total_bytes,
        total_bytes as f64 / 1024.0
    );

    let file_batch_start = Instant::now();
    conn.tell(kameo_remote::connection_pool::TellMessage::batch(
        file_batch_data,
    ))
    .await
    .unwrap();
    let file_batch_total = file_batch_start.elapsed();

    let batch_throughput = total_bytes as f64 / file_batch_total.as_secs_f64() / (1024.0 * 1024.0);

    println!("   📈 Batch File Transfer Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        file_batch_total,
        file_batch_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Total data: {} bytes ({:.2} KB)",
        total_bytes,
        total_bytes as f64 / 1024.0
    );
    println!("     - Throughput: {:.2} MB/s", batch_throughput);

    let file_improvement = file_single_total.as_nanos() as f64 / file_batch_total.as_nanos() as f64;
    let throughput_improvement = batch_throughput / single_throughput;
    println!(
        "   🎯 Performance Improvement: {:.2}x faster, {:.2}x better throughput",
        file_improvement, throughput_improvement
    );

    // ===========================================
    // PART 3: MIXED GOSSIP AND APPLICATION DATA
    // ===========================================
    println!("\n📊 PART 3: MIXED GOSSIP AND APPLICATION DATA");
    println!("============================================");

    // Register an actor (this generates gossip traffic)
    println!("\n🔸 Test 3A: Gossip Activity (Actor Registration)");
    let actor_addr = "127.0.0.1:27100".parse().unwrap();
    let gossip_start = Instant::now();

    node1
        .register_urgent(
            "test_actor".to_string(),
            actor_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    // Wait for gossip propagation
    let mut propagated = false;
    while !propagated && gossip_start.elapsed() < Duration::from_secs(1) {
        if node2.lookup("test_actor").await.is_some() {
            propagated = true;
        } else {
            sleep(Duration::from_millis(1)).await;
        }
    }

    let gossip_time = gossip_start.elapsed();
    println!("   📈 Gossip Propagation:");
    println!(
        "     - Actor registered and propagated in: {:?} ({:.3} μs)",
        gossip_time,
        gossip_time.as_nanos() as f64 / 1000.0
    );
    println!("     - This used the same connection pool as our application data");

    // Send some application data right after gossip
    println!("\n🔸 Test 3B: Application Data After Gossip");
    let post_gossip_messages = vec![
        "Actor registration complete",
        "System is ready for requests",
        "Connection pool is shared between gossip and application data",
    ];

    let mixed_start = Instant::now();
    for msg in &post_gossip_messages {
        conn.tell(msg.as_bytes()).await.unwrap();
    }
    let mixed_time = mixed_start.elapsed();

    println!("   📈 Application Data After Gossip:");
    println!(
        "     - {} messages sent in: {:?} ({:.3} μs)",
        post_gossip_messages.len(),
        mixed_time,
        mixed_time.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per message: {:?} ({:.3} μs)",
        mixed_time / 3,
        mixed_time.as_nanos() as f64 / 3000.0
    );

    // ===========================================
    // PART 4: ASK() REQUEST-RESPONSE PATTERNS
    // ===========================================
    println!("\n📊 PART 4: ASK() REQUEST-RESPONSE PATTERNS");
    println!("==========================================");

    // Test 4A: Simple ask() requests
    println!("\n🔸 Test 4A: Simple ask() Requests");
    let api_requests = [
        "GET /api/users",
        "POST /api/messages",
        "PUT /api/settings",
        "DELETE /api/cache",
    ];

    println!("   API requests to test:");
    for (i, req) in api_requests.iter().enumerate() {
        println!("     {}. {:?} ({} bytes)", i + 1, req, req.len());
    }

    let mut ask_latencies = Vec::new();
    let ask_start = Instant::now();

    for (i, request) in api_requests.iter().enumerate() {
        let req_start = Instant::now();
        let response = conn.ask(request.as_bytes()).await.unwrap();
        let req_time = req_start.elapsed();
        ask_latencies.push(req_time);

        let response_str = String::from_utf8_lossy(&response);
        println!(
            "     Request {}: {:?} -> {:?} in {:?} ({:.3} μs)",
            i + 1,
            request,
            response_str,
            req_time,
            req_time.as_nanos() as f64 / 1000.0
        );
    }

    let ask_total = ask_start.elapsed();
    let ask_avg = ask_total / api_requests.len() as u32;

    println!("   📈 ask() Request-Response Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        ask_total,
        ask_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average per request: {:?} ({:.3} μs)",
        ask_avg,
        ask_avg.as_nanos() as f64 / 1000.0
    );
    println!("     - Note: Currently using mock responses (placeholder implementation)");

    // Test 4B: High-volume ask() requests
    println!("\n🔸 Test 4B: High-Volume ask() Requests");
    let volume_request = "GET /api/status".as_bytes();
    let volume_count = 100;

    println!("   High-volume test configuration:");
    println!(
        "     - Request: {:?} ({} bytes)",
        std::str::from_utf8(volume_request).unwrap(),
        volume_request.len()
    );
    println!("     - Count: {} requests", volume_count);

    let volume_start = Instant::now();
    let mut volume_latencies = Vec::new();

    for i in 0..volume_count {
        let req_start = Instant::now();
        let _response = conn.ask(volume_request).await.unwrap();
        let req_time = req_start.elapsed();
        volume_latencies.push(req_time);

        if i % 20 == 0 {
            println!("     - Completed {}/{} requests", i, volume_count);
        }
    }

    let volume_total = volume_start.elapsed();
    let volume_avg = volume_total / volume_count as u32;
    let volume_rps = volume_count as f64 / volume_total.as_secs_f64();

    // Calculate latency percentiles
    volume_latencies.sort();
    let p50 = volume_latencies[volume_latencies.len() / 2];
    let p95 = volume_latencies[volume_latencies.len() * 95 / 100];
    let p99 = volume_latencies[volume_latencies.len() * 99 / 100];

    println!("   📈 High-Volume ask() Results:");
    println!(
        "     - Total time: {:?} ({:.3} μs)",
        volume_total,
        volume_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - Average latency: {:?} ({:.3} μs)",
        volume_avg,
        volume_avg.as_nanos() as f64 / 1000.0
    );
    println!("     - Requests/sec: {:.0}", volume_rps);
    println!(
        "     - P50 latency: {:?} ({:.3} μs)",
        p50,
        p50.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - P95 latency: {:?} ({:.3} μs)",
        p95,
        p95.as_nanos() as f64 / 1000.0
    );
    println!(
        "     - P99 latency: {:?} ({:.3} μs)",
        p99,
        p99.as_nanos() as f64 / 1000.0
    );

    // Test 4C: ask() vs tell() comparison
    println!("\n🔸 Test 4C: ask() vs tell() Performance Comparison");
    let comparison_request = "PING test message".as_bytes();
    let comparison_count = 50;

    println!("   Comparison test configuration:");
    println!(
        "     - Message: {:?} ({} bytes)",
        std::str::from_utf8(comparison_request).unwrap(),
        comparison_request.len()
    );
    println!("     - Count: {} operations each", comparison_count);

    // Test tell() performance
    println!("\n   Testing tell() (fire-and-forget):");
    let tell_start = Instant::now();
    for i in 0..comparison_count {
        conn.tell(comparison_request).await.unwrap();
        if i % 10 == 0 {
            println!("     - tell() {}/{} sent", i, comparison_count);
        }
    }
    let tell_total = tell_start.elapsed();
    let tell_avg = tell_total / comparison_count as u32;
    let tell_rps = comparison_count as f64 / tell_total.as_secs_f64();

    // Test ask() performance
    println!("\n   Testing ask() (request-response):");
    let ask_comp_start = Instant::now();
    for i in 0..comparison_count {
        let _response = conn.ask(comparison_request).await.unwrap();
        if i % 10 == 0 {
            println!("     - ask() {}/{} completed", i, comparison_count);
        }
    }
    let ask_comp_total = ask_comp_start.elapsed();
    let ask_comp_avg = ask_comp_total / comparison_count as u32;
    let ask_comp_rps = comparison_count as f64 / ask_comp_total.as_secs_f64();

    println!("   📊 tell() vs ask() Comparison:");
    println!("     tell() (fire-and-forget):");
    println!(
        "       - Total time: {:?} ({:.3} μs)",
        tell_total,
        tell_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "       - Average latency: {:?} ({:.3} μs)",
        tell_avg,
        tell_avg.as_nanos() as f64 / 1000.0
    );
    println!("       - Operations/sec: {:.0}", tell_rps);
    println!("     ask() (request-response):");
    println!(
        "       - Total time: {:?} ({:.3} μs)",
        ask_comp_total,
        ask_comp_total.as_nanos() as f64 / 1000.0
    );
    println!(
        "       - Average latency: {:?} ({:.3} μs)",
        ask_comp_avg,
        ask_comp_avg.as_nanos() as f64 / 1000.0
    );
    println!("       - Operations/sec: {:.0}", ask_comp_rps);

    let ask_overhead = ask_comp_total.as_nanos() as f64 / tell_total.as_nanos() as f64;
    println!(
        "     🎯 ask() overhead: {:.2}x slower than tell() (expected due to response handling)",
        ask_overhead
    );

    // ===========================================
    // SUMMARY AND RECOMMENDATIONS
    // ===========================================
    println!("\n🎯 SUMMARY AND RECOMMENDATIONS");
    println!("==============================");

    println!("\n✅ Connection Pool API Benefits:");
    println!("   1. **tell() - Fire-and-forget**: Automatic detection works with single messages, arrays, and batches");
    println!(
        "   2. **ask() - Request-response**: Simple API for synchronous communication patterns"
    );
    println!(
        "   3. **Performance**: Batching provides {:.1}x-{:.1}x speedup for multiple messages",
        improvement.min(file_improvement),
        improvement.max(file_improvement)
    );
    println!("   4. **Shared Infrastructure**: Same connection pool handles gossip, application data, and request-response");
    println!("   5. **Type Safety**: Compile-time guarantees with ergonomic API");

    println!("\n📋 When to Use tell() (Fire-and-forget):");
    println!("   - ✅ Notifications and events");
    println!("   - ✅ Logging and metrics");
    println!("   - ✅ Multiple small messages (use batching)");
    println!("   - ✅ High-throughput data streaming");

    println!("\n📋 When to Use ask() (Request-response):");
    println!("   - ✅ API calls requiring responses");
    println!("   - ✅ Database queries");
    println!("   - ✅ Configuration requests");
    println!("   - ✅ Health checks and status queries");

    println!("\n📋 When to Use Batching:");
    println!("   - ✅ Multiple small messages (chat, notifications, logs)");
    println!("   - ✅ File transfers with multiple files");
    println!("   - ✅ Bulk data operations");
    println!("   - ✅ High-frequency updates");

    println!("\n🔧 API Usage Examples:");
    println!("   // Fire-and-forget (tell):");
    println!("   conn.tell(data).await?;                    // Single message");
    println!("   conn.tell(&[&data1, &data2]).await?;      // Batch (auto-detected)");
    println!("   conn.tell(TellMessage::batch(vec![&data1, &data2])).await?;");
    println!("   ");
    println!("   // Request-response (ask):");
    println!("   let response = conn.ask(request).await?;   // Returns Vec<u8>");
    println!("   ");
    println!("   // Mixed patterns:");
    println!("   conn.tell(notification).await?;           // Send notification");
    println!("   let status = conn.ask(health_check).await?; // Get status");

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ Test completed successfully!");
}

/// High-volume performance test to demonstrate batching benefits at scale
#[tokio::test]
async fn test_tell_message_high_volume_performance() {
    println!("🚀 High-Volume Performance Test");
    println!("===============================");

    // Setup
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:28001".parse().unwrap();
    let node2_addr = "127.0.0.1:28002".parse().unwrap();

    let node1_keypair = KeyPair::new_for_testing("perf_node1");
    let node2_keypair = KeyPair::new_for_testing("perf_node2");

    let node1 =
        GossipRegistryHandle::new_with_keypair(node1_addr, node1_keypair, Some(config.clone()))
            .await
            .unwrap();
    let node2 = GossipRegistryHandle::new_with_keypair(node2_addr, node2_keypair, Some(config))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    // Test parameters
    let message_count = 1000;
    let message_size = 512; // 512 bytes each
    let test_data = vec![b'A'; message_size];

    println!("📊 Test Configuration:");
    println!("   - Messages: {}", message_count);
    println!("   - Message size: {} bytes", message_size);
    println!(
        "   - Total data: {:.2} MB",
        (message_count * message_size) as f64 / (1024.0 * 1024.0)
    );

    // Warmup
    println!("\n🔥 Warming up connection...");
    for _ in 0..10 {
        conn.tell(&test_data).await.unwrap();
    }

    // Test 1: Individual messages
    println!("\n🔸 Test 1: {} Individual Messages", message_count);
    let individual_start = Instant::now();
    for i in 0..message_count {
        conn.tell(&test_data).await.unwrap();
        if i % 200 == 0 {
            println!("   - Sent {}/{} messages", i, message_count);
        }
    }
    let individual_time = individual_start.elapsed();

    let individual_throughput =
        (message_count * message_size) as f64 / individual_time.as_secs_f64() / (1024.0 * 1024.0);
    let individual_msg_per_sec = message_count as f64 / individual_time.as_secs_f64();

    println!("   📈 Individual Message Results:");
    println!("     - Total time: {:?}", individual_time);
    println!("     - Messages/sec: {:.0}", individual_msg_per_sec);
    println!("     - Throughput: {:.2} MB/s", individual_throughput);
    println!(
        "     - Avg latency: {:.3} μs",
        individual_time.as_nanos() as f64 / message_count as f64 / 1000.0
    );

    // Test 2: Batch messages (batches of 50)
    println!("\n🔸 Test 2: Batched Messages (50 messages per batch)");
    let batch_size = 50;
    let batch_count = message_count / batch_size;

    println!("   - Batch size: {} messages", batch_size);
    println!("   - Number of batches: {}", batch_count);

    let batch_start = Instant::now();
    for i in 0..batch_count {
        let batch_data: Vec<&[u8]> = (0..batch_size).map(|_| test_data.as_slice()).collect();
        conn.tell(kameo_remote::connection_pool::TellMessage::batch(
            batch_data,
        ))
        .await
        .unwrap();
        if i % 4 == 0 {
            println!(
                "   - Sent batch {}/{} ({} messages)",
                i,
                batch_count,
                i * batch_size
            );
        }
    }
    let batch_time = batch_start.elapsed();

    let batch_throughput =
        (message_count * message_size) as f64 / batch_time.as_secs_f64() / (1024.0 * 1024.0);
    let batch_msg_per_sec = message_count as f64 / batch_time.as_secs_f64();

    println!("   📈 Batch Message Results:");
    println!("     - Total time: {:?}", batch_time);
    println!("     - Messages/sec: {:.0}", batch_msg_per_sec);
    println!("     - Throughput: {:.2} MB/s", batch_throughput);
    println!(
        "     - Avg latency: {:.3} μs",
        batch_time.as_nanos() as f64 / message_count as f64 / 1000.0
    );

    // Comparison
    let speedup = individual_time.as_nanos() as f64 / batch_time.as_nanos() as f64;
    let throughput_improvement = batch_throughput / individual_throughput;

    println!("\n🎯 PERFORMANCE COMPARISON:");
    println!(
        "   - Batch is {:.2}x faster than individual messages",
        speedup
    );
    println!(
        "   - Throughput improvement: {:.2}x",
        throughput_improvement
    );
    println!(
        "   - Time saved: {:?} ({:.1}% reduction)",
        individual_time - batch_time,
        (1.0 - batch_time.as_secs_f64() / individual_time.as_secs_f64()) * 100.0
    );

    println!("\n💡 Key Insights:");
    println!("   - Batching reduces per-message overhead (TCP headers, system calls)");
    println!("   - Larger batches = better performance (up to a point)");
    println!("   - Network latency is amortized across multiple messages");
    println!("   - CPU usage is more efficient with batching");

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    // Performance assertions
    assert!(
        speedup > 5.0,
        "Batching should provide at least 5x speedup for high-volume data"
    );
    assert!(
        throughput_improvement > 3.0,
        "Batching should provide at least 3x throughput improvement"
    );

    println!("\n✅ High-volume performance test completed successfully!");
}
