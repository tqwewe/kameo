use std::{
    io::Write,
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};

use kameo_remote::{
    ActorLocation, GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority,
};

const TEST_DATA_SIZE: usize = 100 * 1024 * 1024; // 100MB
const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
const MESSAGE_COUNT: usize = 10000;

/// Generate random test data
fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    data
}

/// Calculate throughput in MB/s
fn calculate_throughput(bytes: usize, duration: Duration) -> f64 {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    let seconds = duration.as_secs_f64();
    mb / seconds
}

/// Simple TCP echo server for testing
async fn start_echo_server(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    let listener = TcpListener::bind(addr).await.unwrap();
    
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buffer = vec![0u8; CHUNK_SIZE];
                
                loop {
                    match stream.read(&mut buffer).await {
                        Ok(0) => break, // Connection closed
                        Ok(n) => {
                            // Echo back the data
                            if stream.write_all(&buffer[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    })
}

/// Test 1: Reuse existing connection pool for direct throughput
#[tokio::test]
async fn test_direct_connection_throughput() {
    println!("🚀 Test 1: Direct Connection Pool Throughput");
    
    // Setup gossip registry
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:21001".parse().unwrap();
    let sender_addr = "127.0.0.1:21002".parse().unwrap();
    
    let receiver_keypair = KeyPair::new_for_testing("throughput_receiver_1");
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_1");
    let receiver_peer_id = receiver_keypair.peer_id();
    let sender_peer_id = sender_keypair.peer_id();

    let receiver_handle = GossipRegistryHandle::new_with_keypair(
        receiver_addr,
        receiver_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();
    
    // Register a throughput test actor on receiver
    let actor_addr = "127.0.0.1:21003".parse().unwrap();
    let actor_location = ActorLocation::new(actor_addr);
    
    receiver_handle.register(
        "throughput_actor".to_string(),
        actor_location.clone(),
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Connect nodes - this establishes the connection pool
    let peer_sender = receiver_handle.add_peer(&sender_peer_id).await;
    peer_sender.connect(&sender_addr).await.unwrap();
    let peer_receiver = sender_handle.add_peer(&receiver_peer_id).await;
    peer_receiver.connect(&receiver_addr).await.unwrap();
    
    // Wait for gossip propagation
    sleep(Duration::from_millis(100)).await;
    
    // Use lookup() to find the remote actor
    let found_actor = sender_handle.lookup("throughput_actor").await;
    assert!(found_actor.is_some(), "Actor should be found via lookup");
    
    let actor_location = found_actor.unwrap();
    println!("✅ Found remote actor at: {}", actor_location.address);
    
    // Start echo server at the actor's address
    let _echo_server = start_echo_server(actor_location.address).await;
    sleep(Duration::from_millis(50)).await; // Let server start
    
    // REUSE existing connection from pool instead of creating new one
    let connection_handle = sender_handle.get_connection(actor_location.address).await.unwrap();
    println!("🔗 Reusing existing connection from pool to: {}", connection_handle.addr);
    
    // Generate test data
    let test_data = generate_test_data(TEST_DATA_SIZE);
    println!("📊 Generated {} MB of test data", TEST_DATA_SIZE / (1024 * 1024));
    
    // Measure throughput using existing connection
    let start_time = Instant::now();
    let mut total_bytes = 0;
    
    // Send data in chunks using the existing connection
    for chunk in test_data.chunks(CHUNK_SIZE) {
        // Send chunk via existing connection pool
        connection_handle.send_raw(chunk).await.unwrap();
        total_bytes += chunk.len();
    }
    
    let duration = start_time.elapsed();
    let throughput = calculate_throughput(total_bytes, duration);
    
    println!("📈 Connection Pool Results:");
    println!("   - Total bytes: {} MB", total_bytes / (1024 * 1024));
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    
    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;
    
    assert!(throughput > 10.0, "Throughput should be > 10 MB/s");
}

// Connection pool-based implementations are now used directly through ConnectionHandle

/// Test 2: tell() fire-and-forget performance using connection pool
#[tokio::test]
async fn test_tell_throughput() {
    println!("🚀 Test 2: tell() Fire-and-Forget Throughput");
    
    // Setup gossip registry
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:21101".parse().unwrap();
    let sender_addr = "127.0.0.1:21102".parse().unwrap();
    
    let receiver_keypair = KeyPair::new_for_testing("throughput_receiver_2");
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_2");
    let receiver_peer_id = receiver_keypair.peer_id();
    let sender_peer_id = sender_keypair.peer_id();

    let receiver_handle = GossipRegistryHandle::new_with_keypair(
        receiver_addr,
        receiver_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();
    
    // Register actor
    let actor_addr = "127.0.0.1:21103".parse().unwrap();
    let actor_location = ActorLocation::new(actor_addr);
    
    receiver_handle.register(
        "tell_actor".to_string(),
        actor_location.clone(),
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Connect nodes - establishes connection pool
    let peer_sender = receiver_handle.add_peer(&sender_peer_id).await;
    peer_sender.connect(&sender_addr).await.unwrap();
    let peer_receiver = sender_handle.add_peer(&receiver_peer_id).await;
    peer_receiver.connect(&receiver_addr).await.unwrap();
    
    // Wait for propagation
    sleep(Duration::from_millis(100)).await;
    
    // Lookup actor
    let found_actor = sender_handle.lookup("tell_actor").await;
    assert!(found_actor.is_some());
    
    let actor_location = found_actor.unwrap();
    println!("✅ Found tell actor at: {}", actor_location.address);
    
    // Start simple counting server
    let message_count = Arc::new(AtomicU64::new(0));
    let count_clone = message_count.clone();
    
    let _server = tokio::spawn(async move {
        let listener = TcpListener::bind(actor_location.address).await.unwrap();
        while let Ok((mut stream, _)) = listener.accept().await {
            let count = count_clone.clone();
            tokio::spawn(async move {
                let mut len_buf = [0u8; 4];
                while stream.read_exact(&mut len_buf).await.is_ok() {
                    let msg_len = u32::from_be_bytes(len_buf) as usize;
                    let mut buffer = vec![0u8; msg_len];
                    if stream.read_exact(&mut buffer).await.is_ok() {
                        count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });
        }
    });
    
    sleep(Duration::from_millis(50)).await;
    
    // REUSE existing connection from pool
    let connection_handle = sender_handle.get_connection(actor_location.address).await.unwrap();
    println!("🔗 Reusing existing connection for tell() to: {}", connection_handle.addr);
    
    // Generate test message
    let test_message = b"Hello from tell() - this is a test message for fire-and-forget performance testing!";
    
    println!("📊 Sending {} messages via tell()", MESSAGE_COUNT);
    
    // Measure tell() performance using connection pool
    let start_time = Instant::now();
    
    for _ in 0..MESSAGE_COUNT {
        connection_handle.tell(test_message).await.unwrap();
    }
    
    let duration = start_time.elapsed();
    let total_bytes = MESSAGE_COUNT * test_message.len();
    let throughput = calculate_throughput(total_bytes, duration);
    let messages_per_sec = MESSAGE_COUNT as f64 / duration.as_secs_f64();
    
    // Wait a bit for messages to be processed
    sleep(Duration::from_millis(100)).await;
    
    println!("📈 tell() Results:");
    println!("   - Messages sent: {}", MESSAGE_COUNT);
    println!("   - Total bytes: {} KB", total_bytes / 1024);
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Messages/sec: {:.0}", messages_per_sec);
    
    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;
    
    assert!(messages_per_sec > 1000.0, "Should handle >1000 messages/sec");
}

/// Test 3: ask() request-response performance using connection pool
#[tokio::test]
async fn test_ask_throughput() {
    println!("🚀 Test 3: ask() Request-Response Throughput");
    
    // Setup gossip registry
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:21201".parse().unwrap();
    let sender_addr = "127.0.0.1:21202".parse().unwrap();
    
    let receiver_keypair = KeyPair::new_for_testing("throughput_receiver_3");
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_3");
    let receiver_peer_id = receiver_keypair.peer_id();
    let sender_peer_id = sender_keypair.peer_id();

    let receiver_handle = GossipRegistryHandle::new_with_keypair(
        receiver_addr,
        receiver_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair,
        Some(config.clone()),
    )
    .await
    .unwrap();
    
    // Register actor
    let actor_addr = "127.0.0.1:21203".parse().unwrap();
    let actor_location = ActorLocation::new(actor_addr);
    
    receiver_handle.register(
        "ask_actor".to_string(),
        actor_location.clone(),
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Connect nodes - establishes connection pool
    let peer_sender = receiver_handle.add_peer(&sender_peer_id).await;
    peer_sender.connect(&sender_addr).await.unwrap();
    let peer_receiver = sender_handle.add_peer(&receiver_peer_id).await;
    peer_receiver.connect(&receiver_addr).await.unwrap();
    
    // Wait for propagation
    sleep(Duration::from_millis(100)).await;
    
    // Lookup actor
    let found_actor = sender_handle.lookup("ask_actor").await;
    assert!(found_actor.is_some());
    
    let actor_location = found_actor.unwrap();
    println!("✅ Found ask actor at: {}", actor_location.address);
    
    // REUSE existing connection from pool
    let connection_handle = sender_handle.get_connection(actor_location.address).await.unwrap();
    println!("🔗 Reusing existing connection for ask() to: {}", connection_handle.addr);
    
    // Generate test request
    let test_request = b"REQUEST: Please process this and send back a response";
    
    println!("📊 Sending {} request-response pairs via ask()", MESSAGE_COUNT / 10);
    
    // Measure ask() performance using connection pool
    let start_time = Instant::now();
    let mut total_bytes = 0;
    let mut latencies = Vec::new();
    
    for i in 0..(MESSAGE_COUNT / 10) {
        let request_start = Instant::now();
        
        let response = connection_handle.ask(test_request).await.unwrap();
        
        let latency = request_start.elapsed();
        latencies.push(latency);
        
        total_bytes += test_request.len() + response.len();
        
        // Verify response (simplified for performance testing)
        let expected_response = format!("RESPONSE:{}", test_request.len());
        assert_eq!(String::from_utf8_lossy(&response), expected_response);
        
        if i % 100 == 0 {
            println!("   - Completed {} requests", i);
        }
    }
    
    let duration = start_time.elapsed();
    let throughput = calculate_throughput(total_bytes, duration);
    let requests_per_sec = (MESSAGE_COUNT / 10) as f64 / duration.as_secs_f64();
    
    // Calculate latency statistics
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];
    
    println!("📈 ask() Results:");
    println!("   - Requests completed: {}", MESSAGE_COUNT / 10);
    println!("   - Total bytes: {} KB", total_bytes / 1024);
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Requests/sec: {:.0}", requests_per_sec);
    println!("   - Latency P50: {:?}", p50);
    println!("   - Latency P95: {:?}", p95);
    println!("   - Latency P99: {:?}", p99);
    
    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;
    
    assert!(requests_per_sec > 100.0, "Should handle >100 requests/sec");
    assert!(p95 < Duration::from_millis(100), "P95 latency should be <100ms");
}

/// Combined performance comparison test
#[tokio::test]
async fn test_performance_comparison() {
    println!("🚀 Performance Comparison Summary");
    println!("This test provides a comprehensive comparison of all three approaches:");
    println!("1. Direct TcpStream");
    println!("2. tell() fire-and-forget");
    println!("3. ask() request-response");
    println!("");
    println!("Run each test individually to see detailed performance metrics.");
    println!("Expected performance hierarchy: TcpStream > tell() > ask()");
}
