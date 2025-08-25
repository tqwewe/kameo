//! PreBacktestMessage streaming server - reproduces the "Expected Gossip message but got StreamStart" issue
//!
//! This reproduces the exact issue from trading-backend-poc where large PreBacktestMessage
//! fails due to streaming protocol vs gossip protocol conflict.
//!
//! Run this first:
//! cargo run --example prebacktest_stream_server --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use rustc_hash::FxHashMap;

// Simulate the trading system's PreBacktestMessage with realistic large data
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

// Mock execution router actor
struct ExecutionRouterActor {
    messages_received: u32,
}

impl Actor for ExecutionRouterActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🚀 ExecutionRouter started - ready to receive large PreBacktestMessages");
        Ok(Self {
            messages_received: 0,
        })
    }
}

impl Message<PreBacktestMessage> for ExecutionRouterActor {
    type Reply = ();
    
    async fn handle(&mut self, msg: PreBacktestMessage, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.messages_received += 1;
        
        // Calculate message statistics (like the real system)
        let total_candles: usize = msg.candles.values().map(|v| v.len()).sum();
        let estimated_size_mb = {
            let candles_size = total_candles * std::mem::size_of::<FuturesOHLCVCandle>();
            let price_events_size = msg.price_events.len() * 64; // Rough estimate
            let oi_snapshots_size = msg.oi_snapshots.len() * 64; // Rough estimate
            (candles_size + price_events_size + oi_snapshots_size) as f64 / (1024.0 * 1024.0)
        };
        
        println!("✅ EXECUTOR: PreBacktestMessage received successfully!");
        println!("  📈 Symbol: {}/{}", msg.symbol.base, msg.symbol.quote);
        println!("  📊 Total candles: {} across {} timeframes", total_candles, msg.candles.len());
        for (tf, candles) in &msg.candles {
            println!("    - Timeframe {}s: {} candles", tf, candles.len());
        }
        println!("  🎯 Price events: {}", msg.price_events.len());
        println!("  📦 OI snapshots: {}", msg.oi_snapshots.len());
        println!("  💾 Estimated message size: ~{:.2} MB", estimated_size_mb);
        println!("  📨 Total messages received: {}", self.messages_received);
    }
}

// Register with distributed actor macro
distributed_actor! {
    ExecutionRouterActor {
        PreBacktestMessage,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Ensure the rustls CryptoProvider is installed (required for TLS)
    kameo_remote::tls::ensure_crypto_provider();
    
    // Enable detailed logging to see the streaming vs gossip issue
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,kameo=debug")
        .try_init();

    println!("\n🚀 === PREBACKTEST STREAMING SERVER ===");
    println!("This reproduces the 'Expected Gossip message but got StreamStart' issue");

    let server_keypair = kameo_remote::KeyPair::new_for_testing("prebacktest_server_key");
    println!("🔐 Server using Ed25519 keypair for TLS encryption");
    
    // Bootstrap on port 9002 (like the real execution service)
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9002".parse()?,
        server_keypair
    ).await?;
    println!("✅ ExecutionRouter listening on {} with TLS encryption", transport.local_addr());

    // Create and register ExecutionRouter
    let actor_ref = ExecutionRouterActor::spawn(());
    let actor_id = actor_ref.id();

    transport
        .register_distributed_actor("crypto_futures_execution_router".to_string(), &actor_ref)
        .await?;

    println!("✅ ExecutionRouter registered with ID {:?}", actor_id);

    // Debug: Print the type hash being used
    use kameo::remote::type_hash::HasTypeHash;
    println!(
        "🔍 PreBacktestMessage type hash: {:08x}",
        <PreBacktestMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );

    // Add the client (TA Manager) as a trusted peer
    if let Some(handle) = transport.handle() {
        let client_peer_id = kameo_remote::PeerId::new("prebacktest_client_key");
        let _peer = handle.add_peer(&client_peer_id).await;
        println!("✅ Added TA Manager as trusted peer for TLS");
    }
    
    println!("📡 Server ready for large PreBacktestMessage streaming");
    println!("\n📡 Run client with:");
    println!("   cargo run --example prebacktest_stream_client --features remote");
    println!("\n💤 Server will run until you press Ctrl+C...\n");

    // Keep server running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}