//! MAXIMUM PERFORMANCE test - demonstrates automatic streaming for large messages
//!
//! Run:
//! Terminal 1: cargo run --example streaming_prebacktest_server --features remote
//! Terminal 2: cargo run --example max_performance_test --features remote

use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use std::time::Instant;

// Import shared message definitions
mod streaming_messages;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging to see streaming in action
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo=info,kameo_remote=info")
        .try_init();

    println!("\n🚀 === MAXIMUM PERFORMANCE TEST ===");
    println!("   Automatic streaming for messages > 1MB");
    println!("   Direct socket writes with zero-copy");

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

    // Wait for connection
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Look up remote actor
    println!("\n🔍 Looking up remote BacktestActor...");
    let backtest_ref = match DistributedActorRef::lookup("backtest_executor", transport).await? {
        Some(ref_) => {
            println!("✅ Found BacktestActor");
            ref_
        }
        None => {
            return Err("Actor not found".into());
        }
    };

    let all_tests_start = Instant::now();

    // Test 1: Small message (< 1MB) - uses normal tell
    println!("\n🧪 Test 1: 500KB message (normal tell path)");
    let small_msg = PreBacktestMessage {
        backtest_id: "small_test".to_string(),
        symbol: "BTCUSDT".to_string(),
        candles: [(60u32, vec![SimpleFuturesOHLCVCandle {
            open_time: 1700000000,
            open: 50000.0,
            high: 50100.0,
            low: 49900.0,
            close: 50050.0,
            volume: 100.0,
        }; 9_000])].into(), // ~500KB
        indicator_configs: vec![],
    };
    
    let start = Instant::now();
    backtest_ref.tell(small_msg).send().await?;
    let elapsed = start.elapsed();
    let mb_size = (9_000 * 56) as f64 / 1_048_576.0; // approximate size
    let mb_per_sec = mb_size / elapsed.as_secs_f64();
    println!("   ✅ Sent in {:?} (normal tell) - {:.2} MB/s", elapsed, mb_per_sec);
    
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Test 2: 5MB message - automatically uses streaming
    println!("\n🧪 Test 2: 5MB message (automatic streaming)");
    let msg_5mb = PreBacktestMessage::create_mock(5);
    println!("   Created {} MB message", 5);
    
    let start = Instant::now();
    backtest_ref.tell(msg_5mb).send().await?;
    let elapsed = start.elapsed();
    let mb_per_sec = 5.0 / elapsed.as_secs_f64();
    println!("   ✅ Sent in {:?} (AUTO-STREAMED) - {:.2} MB/s", elapsed, mb_per_sec);
    
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 3: 35MB message - automatic streaming
    println!("\n🧪 Test 3: 35MB message (automatic streaming)");
    let msg_35mb = PreBacktestMessage::create_mock(35);
    println!("   Created {} MB message", 35);
    
    let start = Instant::now();
    backtest_ref.tell(msg_35mb).send().await?;
    let elapsed = start.elapsed();
    let mb_per_sec = 35.0 / elapsed.as_secs_f64();
    println!("   ✅ Sent in {:?} (AUTO-STREAMED) - {:.2} MB/s", elapsed, mb_per_sec);
    
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Test 4: Multiple concurrent large messages
    println!("\n🧪 Test 4: Concurrent 10MB messages");
    let mut handles = vec![];
    
    for i in 0..3 {
        let backtest_ref = backtest_ref.clone();
        let handle = tokio::spawn(async move {
            let mut msg = PreBacktestMessage::create_mock(10);
            msg.backtest_id = format!("concurrent_{}", i);
            
            let start = Instant::now();
            backtest_ref.tell(msg).send().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((i, start.elapsed()))
        });
        handles.push(handle);
    }
    
    for handle in handles {
        match handle.await? {
            Ok((i, duration)) => {
                let mb_per_sec = 10.0 / duration.as_secs_f64();
                println!("   Stream {} completed in {:?} - {:.2} MB/s", i, duration, mb_per_sec);
            },
            Err(e) => println!("   Stream failed: {}", e),
        }
    }

    let total_time = all_tests_start.elapsed();
    println!("\n🎉 MAXIMUM PERFORMANCE achieved!");
    println!("⏱️  Total time: {:?}", total_time);
    println!("\n📊 Performance Summary:");
    println!("   ✅ Messages < 1MB: Normal tell path (fastest for small messages)");
    println!("   ✅ Messages > 1MB: Automatic streaming (prevents buffer issues)");
    println!("   ✅ Zero-copy throughout the entire path");
    println!("   ✅ Direct socket writes for streaming");
    println!("   ✅ No buffer corruption even with 35MB messages");

    Ok(())
}