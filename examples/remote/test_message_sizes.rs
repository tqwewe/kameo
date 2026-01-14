//! Test different message sizes to find the exact point where things break
//!
//! Run after starting the server:
//! cargo run --example streaming_prebacktest_server --features remote
//! cargo run --example test_message_sizes --features remote

use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use std::time::Instant;

// Import shared message definitions
mod streaming_messages;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    println!("\n🚀 === MESSAGE SIZE TEST CLIENT ===");

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
            println!("✅ Found BacktestActor on server");
            ref_
        }
        None => {
            println!("❌ BacktestActor not found on server");
            return Err("Actor not found".into());
        }
    };

    // Test different message sizes
    let test_sizes = vec![
        ("100KB", 100),
        ("500KB", 500),
        ("900KB", 900),
        ("1MB", 1024),
        ("1.1MB", 1126), 
        ("1.5MB", 1536),
        ("2MB", 2048),
    ];

    for (name, size_kb) in test_sizes {
        println!("\n🧪 Test: Sending {} message", name);
        
        // Create message of specific size
        let candles_count = (size_kb * 1024) / 56; // Each candle is ~56 bytes
        let msg = PreBacktestMessage {
            backtest_id: format!("test_{}", name),
            symbol: "BTCUSDT".to_string(),
            candles: [(60u32, vec![SimpleFuturesOHLCVCandle {
                open_time: 1700000000,
                open: 50000.0,
                high: 50100.0,
                low: 49900.0,
                close: 50050.0,
                volume: 100.0,
            }; candles_count])].into(),
            indicator_configs: vec![],
        };
        
        // Serialize to get exact size
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();
        println!("   Actual serialized size: {} bytes ({:.2} MB)", 
            serialized.len(), 
            serialized.len() as f64 / 1_048_576.0);
        
        let start = Instant::now();
        match backtest_ref.tell(msg).send().await {
            Ok(_) => {
                println!("   ✅ Successfully sent in {:?}", start.elapsed());
                // Wait for processing
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => {
                println!("   ❌ Failed to send: {:?}", e);
                println!("   ⚠️  Messages larger than this size will fail!");
                break;
            }
        }
    }

    println!("\n🎉 Size test completed!");
    
    // Give server time to process
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
}