//! Streaming API client example
//!
//! This demonstrates the new high-performance streaming API for large messages.
//! Messages are streamed directly to the socket, bypassing the ring buffer.
//!
//! Run after starting the server:
//! cargo run --example streaming_api_server --features remote
//! cargo run --example streaming_api_client --features remote

use kameo::remote::{transport::RemoteTransport, DistributedActorRef, streaming::StreamFactory};
use std::time::Instant;

// Import shared message definitions for streaming
mod streaming_messages;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    println!("\n🚀 === STREAMING API CLIENT ===");

    // Bootstrap on port 9311
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9311".parse()?).await?;
    println!("✅ Client listening on {}", transport.local_addr());

    // Connect to server
    println!("\n📡 Connecting to server at 127.0.0.1:9310...");
    if let Some(handle) = transport.handle() {
        let peer = handle
            .add_peer(&kameo_remote::PeerId::new("kameo_node_9310"))
            .await;
        peer.connect(&"127.0.0.1:9310".parse()?).await?;
        println!("✅ Connected to server");
    }

    // Wait for connection to stabilize and actor registration to propagate
    println!("\n⏳ Waiting for actor registration to propagate...");
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Look up remote actor
    println!("\n🔍 Looking up remote BacktestActor...");
    let backtest_ref = match DistributedActorRef::lookup("backtest_executor").await? {
        Some(ref_) => {
            println!("✅ Found BacktestActor on server");
            println!("📍 Actor ID: {:?}", ref_.id());
            ref_
        }
        None => {
            println!("❌ BacktestActor not found on server");
            return Err("Actor not found".into());
        }
    };

    println!("\n📤 Starting streaming tests...");
    let all_tests_start = Instant::now();

    // Test 1: Send a small BacktestRunConfig using regular tell (for comparison)
    println!("\n🧪 Test 1: Sending BacktestRunConfig (small message, normal tell)");
    let config = BacktestRunConfig::create_mock();
    let start = Instant::now();
    backtest_ref.tell(config).send().await?;
    println!("✅ Sent BacktestRunConfig in {:?}", start.elapsed());

    // Test 2: Create a stream and send a 5MB PreBacktest message
    println!("\n🧪 Test 2: Streaming 5MB PreBacktest message");
    let prebacktest_5mb = PreBacktestMessage::create_mock(5);
    println!("   Created mock PreBacktest with ~{} candles", prebacktest_5mb.candles.values().map(|v| v.len()).sum::<usize>());
    
    // Create a stream for PreBacktest messages
    let mut stream = backtest_ref.create_stream::<PreBacktestMessage>("prebacktest_data").await?;
    println!("   Created stream with ID: {}", stream.id());
    
    let start = Instant::now();
    stream.send(prebacktest_5mb).await?;
    println!("✅ Streamed 5MB PreBacktest in {:?}", start.elapsed());
    
    // Test 3: Send multiple messages through the same stream
    println!("\n🧪 Test 3: Streaming multiple messages through same stream");
    let multi_start = Instant::now();
    
    for i in 0..3 {
        let mut msg = PreBacktestMessage::create_mock(2);
        msg.backtest_id = format!("multi_test_{}", i);
        stream.send(msg).await?;
        println!("   Sent message {} through stream", i);
    }
    
    println!("✅ Streamed 3 messages in {:?}", multi_start.elapsed());
    
    // Close the stream
    stream.close().await?;
    println!("   Stream closed");

    // Test 4: Create a new stream for a 35MB message
    println!("\n🧪 Test 4: Streaming 35MB PreBacktest message");
    let prebacktest_35mb = PreBacktestMessage::create_mock(35);
    println!("   Created mock PreBacktest with ~{} candles", prebacktest_35mb.candles.values().map(|v| v.len()).sum::<usize>());
    
    let mut large_stream = backtest_ref.create_stream::<PreBacktestMessage>("large_prebacktest").await?;
    println!("   Created stream with ID: {}", large_stream.id());
    
    let start = Instant::now();
    large_stream.send(prebacktest_35mb).await?;
    println!("✅ Streamed 35MB PreBacktest in {:?}", start.elapsed());
    
    large_stream.close().await?;

    // Test 5: Multiple concurrent streams
    println!("\n🧪 Test 5: Concurrent streams - 3 x 10MB PreBacktest messages");
    let concurrent_start = Instant::now();
    
    let mut handles = vec![];
    for i in 0..3 {
        let backtest_ref = backtest_ref.clone();
        let handle = tokio::spawn(async move {
            let mut msg = PreBacktestMessage::create_mock(10);
            msg.backtest_id = format!("concurrent_stream_{}", i);
            
            let mut stream = backtest_ref.create_stream::<PreBacktestMessage>(&format!("concurrent_{}", i)).await?;
            let start = Instant::now();
            stream.send(msg).await?;
            stream.close().await?;
            
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(start.elapsed())
        });
        handles.push(handle);
    }
    
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await? {
            Ok(duration) => println!("   Stream {} completed in {:?}", i, duration),
            Err(e) => println!("   Stream {} failed: {}", i, e),
        }
    }
    println!("✅ Concurrent streaming completed in {:?}", concurrent_start.elapsed());

    // Final wait
    println!("\n⏳ Waiting for all messages to be processed...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let total_duration = all_tests_start.elapsed();
    println!("\n🎉 All streaming tests completed!");
    println!("⏱️  Total time: {:?}", total_duration);
    println!("\n📊 Summary:");
    println!("   - Small messages (BacktestRunConfig) use normal tell");
    println!("   - Large messages (PreBacktest >1MB) use the streaming API");
    println!("   - Streaming writes directly to socket for maximum performance");
    println!("   - Multiple messages can be sent through the same stream");
    println!("   - Multiple concurrent streams are supported");

    Ok(())
}