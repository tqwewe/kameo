//! Test ONLY streaming API without any large tell messages
//!
//! Run:
//! cargo run --example streaming_prebacktest_server --features remote
//! cargo run --example streaming_only_test --features remote

use kameo::remote::{transport::RemoteTransport, DistributedActorRef, streaming::StreamFactory};
use std::time::Instant;

// Import shared message definitions
mod streaming_messages;
use streaming_messages::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,kameo=info")
        .try_init();

    println!("\n🚀 === STREAMING ONLY TEST ===");

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
            println!("❌ BacktestActor not found");
            return Err("Actor not found".into());
        }
    };

    // Test 1: Send a tiny config message via normal tell (should work)
    println!("\n🧪 Test 1: Sending tiny message via tell");
    let config = BacktestRunConfig::create_mock();
    backtest_ref.tell(config).send().await?;
    println!("✅ Sent tiny message");
    
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 2: Send a 300KB message via streaming (should be single frame)
    println!("\n🧪 Test 2: Sending 300KB message via streaming");
    let msg = PreBacktestMessage {
        backtest_id: "stream_test_300kb".to_string(),
        symbol: "BTCUSDT".to_string(),
        candles: [(60u32, vec![SimpleFuturesOHLCVCandle {
            open_time: 1700000000,
            open: 50000.0,
            high: 50100.0,
            low: 49900.0,
            close: 50050.0,
            volume: 100.0,
        }; 5400])].into(), // ~300KB
        indicator_configs: vec![],
    };
    
    let mut stream = backtest_ref.create_stream::<PreBacktestMessage>("test_300kb").await?;
    println!("   Stream ID: {}", stream.id());
    stream.send(msg).await?;
    stream.close().await?;
    println!("✅ Sent 300KB via streaming");
    
    // Wait and observe
    println!("\n⏳ Waiting 3 seconds to observe results...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    println!("\n🎉 Test completed!");

    Ok(())
}