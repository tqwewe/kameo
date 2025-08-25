//! PreBacktestMessage streaming client - generates large message to trigger streaming bug
//!
//! This reproduces the exact issue from trading-backend-poc where large PreBacktestMessage  
//! fails with "Expected Gossip message but got StreamStart" error.
//!
//! Run after starting the server:
//! cargo run --example prebacktest_stream_client --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use rustc_hash::FxHashMap;
use std::time::Duration;

// Import the same message types from server
#[derive(kameo::RemoteMessage, Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PreBacktestMessage {
    pub symbol: CryptoFuturesSymbol,
    pub candles: FxHashMap<i64, Vec<FuturesOHLCVCandle>>,
    pub price_events: Vec<TimeSeriesEvent>,
    pub oi_snapshots: Vec<TimeSeriesEvent>,
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct CryptoFuturesSymbol {
    pub base: String,
    pub quote: String,
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct FuturesOHLCVCandle {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TimeSeriesEvent {
    pub timestamp: i64,
    pub value: f64,
    pub event_type: String,
}

// Generate realistic large PreBacktestMessage that triggers streaming (>1MB)
fn generate_large_prebacktest_message() -> PreBacktestMessage {
    println!("📊 Generating large PreBacktestMessage to trigger streaming...");
    
    let symbol = CryptoFuturesSymbol {
        base: "SOL".to_string(),
        quote: "USDT".to_string(),
    };
    
    let mut candles = FxHashMap::default();
    
    // Generate realistic candle data for multiple timeframes (like the real system)
    let timeframes = vec![
        (60, 260640),    // 1m candles
        (300, 52128),    // 5m candles  
        (900, 17376),    // 15m candles
        (1800, 8688),    // 30m candles
        (3600, 4344),    // 1h candles
        (14400, 1086),   // 4h candles
        (86400, 181),    // 1d candles
    ];
    
    let mut total_candles = 0;
    for (timeframe, count) in timeframes {
        let mut tf_candles = Vec::with_capacity(count);
        for i in 0..count {
            tf_candles.push(FuturesOHLCVCandle {
                timestamp: 1735689600000 + (i as i64 * timeframe * 1000), // Start from 2025-01-01
                open: 186.0 + (i as f64 * 0.01) % 10.0,
                high: 187.0 + (i as f64 * 0.01) % 10.0,
                low: 185.0 + (i as f64 * 0.01) % 10.0,
                close: 186.5 + (i as f64 * 0.01) % 10.0,
                volume: 1000.0 + (i as f64 * 10.0) % 5000.0,
            });
        }
        total_candles += count;
        candles.insert(timeframe, tf_candles);
    }
    
    // Generate price events (largest data set)
    let mut price_events = Vec::with_capacity(260640);
    for i in 0..260640 {
        price_events.push(TimeSeriesEvent {
            timestamp: 1735689600000 + (i as i64 * 60 * 1000), // Every minute
            value: 186.0 + (i as f64 * 0.001) % 20.0,
            event_type: "price".to_string(),
        });
    }
    
    // Generate OI snapshots
    let mut oi_snapshots = Vec::with_capacity(52114);
    for i in 0..52114 {
        oi_snapshots.push(TimeSeriesEvent {
            timestamp: 1735689600000 + (i as i64 * 300 * 1000), // Every 5 minutes
            value: 50000000.0 + (i as f64 * 1000.0) % 10000000.0,
            event_type: "oi_snapshot".to_string(),
        });
    }
    
    let message = PreBacktestMessage {
        symbol,
        candles,
        price_events,
        oi_snapshots,
    };
    
    // Calculate estimated size
    let estimated_size = {
        let candles_size = total_candles * std::mem::size_of::<FuturesOHLCVCandle>();
        let price_events_size = message.price_events.len() * 64;
        let oi_snapshots_size = message.oi_snapshots.len() * 64;
        (candles_size + price_events_size + oi_snapshots_size) as f64 / (1024.0 * 1024.0)
    };
    
    println!("📏 Generated PreBacktestMessage:");
    println!("  📊 Total candles: {} across {} timeframes", total_candles, message.candles.len());
    println!("  🎯 Price events: {}", message.price_events.len());  
    println!("  📦 OI snapshots: {}", message.oi_snapshots.len());
    println!("  💾 Estimated size: ~{:.2} MB", estimated_size);
    println!("  ⚡ This should trigger automatic streaming (>1MB threshold)");
    
    message
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Ensure the rustls CryptoProvider is installed (required for TLS)
    kameo_remote::tls::ensure_crypto_provider();

    // Enable detailed logging to see the streaming vs gossip issue
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,kameo=debug")
        .try_init();

    println!("\n🚀 === PREBACKTEST STREAMING CLIENT (TA MANAGER SIMULATION) ===");

    let client_keypair = kameo_remote::KeyPair::new_for_testing("prebacktest_client_key");
    println!("🔐 Client using Ed25519 keypair for TLS encryption");

    // Bootstrap on port 9001 (like the real TA manager/optimizer)
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9001".parse()?,
        client_keypair,
    ).await?;
    println!("✅ TA Manager listening on {} with TLS encryption", transport.local_addr());

    // Connect to server with TLS encryption
    println!("\n📡 Connecting to ExecutionRouter at 127.0.0.1:9002 with TLS...");
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("prebacktest_server_key");
        let peer = handle.add_peer(&server_peer_id).await;
        
        match peer.connect(&"127.0.0.1:9002".parse().unwrap()).await {
            Ok(_) => println!("✅ TLS connection established with ExecutionRouter!"),
            Err(e) => {
                println!("❌ Failed to connect to ExecutionRouter: {:?}", e);
                println!("   Make sure the server is running first:");
                println!("   cargo run --example prebacktest_stream_server --features remote");
                return Ok(());
            }
        }
    }

    // Wait for connection to fully establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Look up the ExecutionRouter via distributed actor discovery
    println!("🔍 Looking up ExecutionRouter via distributed actor discovery...");
    let execution_router = loop {
        match DistributedActorRef::lookup("crypto_futures_execution_router").await {
            Ok(Some(actor_ref)) => {
                println!("✅ Found ExecutionRouter: {:?}", actor_ref.id());
                break actor_ref;
            }
            Ok(None) => {
                println!("⏳ ExecutionRouter not found, retrying...");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(e) => {
                println!("❌ Error looking up ExecutionRouter: {:?}", e);
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        }
    };

    println!("\n🚀 Starting PreBacktestMessage streaming test...");

    // Generate the large message that will trigger streaming
    let large_message = generate_large_prebacktest_message();

    println!("\n📤 Sending PreBacktestMessage via distributed actor...");
    println!("   This should trigger automatic streaming due to size >1MB");
    println!("   Watch for 'Expected Gossip message but got StreamStart' error!\n");

    // Send the message - this should trigger the streaming bug
    match execution_router.tell(large_message).send().await {
        Ok(_) => {
            println!("✅ PreBacktestMessage sent successfully!");
            println!("   If you see this, the streaming issue is fixed!");
        }
        Err(e) => {
            println!("❌ Failed to send PreBacktestMessage: {:?}", e);
            println!("   This is likely the 'Expected Gossip message but got StreamStart' bug!");
        }
    }

    // Wait a moment to see any additional error logs
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    println!("\n🔍 Check server logs for streaming protocol errors!");
    println!("Expected error: 'Expected Gossip message but got StreamStart'");

    Ok(())
}