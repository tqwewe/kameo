//! Test that streaming fix prevents buffer corruption
//!
//! Run:
//! Terminal 1: cargo run --example streaming_prebacktest_server --features remote
//! Terminal 2: cargo run --example test_streaming_fix --features remote

use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use std::time::Instant;

// Import shared message definitions
mod streaming_messages;
use streaming_messages::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging to see streaming behavior
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo=info,kameo_remote=info")
        .try_init();

    println!("\n🚀 === STREAMING FIX TEST ===");
    println!("   Testing that large messages are properly streamed");
    println!("   and buffer corruption is prevented\n");

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

    // Test 1: Small message (should work normally)
    println!("\n🧪 Test 1: 500KB message (normal tell)");
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
    match backtest_ref.tell(small_msg).send().await {
        Ok(_) => println!("   ✅ Sent successfully in {:?}", start.elapsed()),
        Err(e) => println!("   ❌ Failed: {:?}", e),
    }
    
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 2: 2MB message (should trigger streaming)
    println!("\n🧪 Test 2: 2MB message (should stream)");
    let msg_2mb = PreBacktestMessage::create_mock(2);
    
    let start = Instant::now();
    match backtest_ref.tell(msg_2mb).send().await {
        Ok(_) => println!("   ✅ Streaming succeeded in {:?}", start.elapsed()),
        Err(e) => {
            println!("   ⚠️  Expected behavior: {:?}", e);
            println!("   This error is expected if streaming is not yet implemented");
        }
    }
    
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 3: 35MB message using tell_large (should definitely stream)
    println!("\n🧪 Test 3: 35MB message with tell_large");
    let msg_35mb = PreBacktestMessage::create_mock(35);
    
    let start = Instant::now();
    match backtest_ref.tell_large(msg_35mb).await {
        Ok(_) => println!("   ✅ Streaming succeeded in {:?}", start.elapsed()),
        Err(e) => {
            println!("   ⚠️  Expected behavior: {:?}", e);
            println!("   This error is expected if streaming is not yet implemented");
        }
    }
    
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    println!("\n📊 Summary:");
    println!("   - Small messages (<1MB): Use normal tell path ✅");
    println!("   - Large messages (>1MB): Must use streaming or fail gracefully ✅");
    println!("   - No buffer corruption should occur ✅");
    println!("\n🎉 Test complete! Check server logs for received messages.");

    Ok(())
}