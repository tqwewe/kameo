//! Simple test to verify streaming works correctly
//!
//! Run after starting the server:
//! cargo run --example streaming_prebacktest_server --features remote
//! cargo run --example test_streaming_simple --features remote

use kameo::remote::{transport::RemoteTransport, DistributedActorRef, streaming::StreamFactory};
use std::time::Instant;

// Import shared message definitions for streaming
mod streaming_messages;
use streaming_messages::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,kameo=debug")
        .try_init();

    println!("\n🚀 === SIMPLE STREAMING TEST CLIENT ===");

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

    // Wait for connection to stabilize
    println!("\n⏳ Waiting for actor registration to propagate...");
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Look up remote actor
    println!("\n🔍 Looking up remote BacktestActor...");
    let backtest_ref = match DistributedActorRef::lookup("backtest_executor", transport).await? {
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

    // Test 1: Send a small message via normal tell (should work)
    println!("\n🧪 Test 1: Sending small BacktestRunConfig via normal tell");
    let config = BacktestRunConfig::create_mock();
    let start = Instant::now();
    backtest_ref.tell(config).send().await?;
    println!("✅ Sent BacktestRunConfig in {:?}", start.elapsed());
    
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 2: Send a medium message (100KB) via normal tell
    println!("\n🧪 Test 2: Sending 100KB PreBacktest via normal tell");
    let small_prebacktest = PreBacktestMessage {
        backtest_id: "small_test".to_string(),
        symbol: "BTCUSDT".to_string(),
        candles: [(60u32, vec![SimpleFuturesOHLCVCandle {
            open_time: 1700000000,
            open: 50000.0,
            high: 50100.0,
            low: 49900.0,
            close: 50050.0,
            volume: 100.0,
        }; 1800])].into(), // ~100KB
        indicator_configs: vec![],
    };
    let start = Instant::now();
    backtest_ref.tell(small_prebacktest).send().await?;
    println!("✅ Sent 100KB PreBacktest in {:?}", start.elapsed());
    
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 3: Send a 1MB message via normal tell (should still work but be on the edge)
    println!("\n🧪 Test 3: Sending 1MB PreBacktest via normal tell");
    let medium_prebacktest = PreBacktestMessage::create_mock(1);
    println!("   Created mock PreBacktest with ~{} candles", medium_prebacktest.candles.values().map(|v| v.len()).sum::<usize>());
    let start = Instant::now();
    backtest_ref.tell(medium_prebacktest).send().await?;
    println!("✅ Sent 1MB PreBacktest in {:?}", start.elapsed());
    
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Test 4: Send a 5MB message via streaming API
    println!("\n🧪 Test 4: Sending 5MB PreBacktest via streaming API");
    let large_prebacktest = PreBacktestMessage::create_mock(5);
    println!("   Created mock PreBacktest with ~{} candles", large_prebacktest.candles.values().map(|v| v.len()).sum::<usize>());
    
    // Create a stream and send the large message
    let mut stream = backtest_ref.create_stream::<PreBacktestMessage>("test_5mb").await?;
    println!("   Created stream with ID: {}", stream.id());
    
    let start = Instant::now();
    stream.send(large_prebacktest).await?;
    stream.close().await?;
    println!("✅ Sent 5MB PreBacktest via streaming in {:?}", start.elapsed());
    
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    println!("\n🎉 All tests completed!");

    Ok(())
}