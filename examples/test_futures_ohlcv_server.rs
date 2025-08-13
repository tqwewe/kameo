//! Test server for FuturesOHLCVCandle with mapping syntax and zero-copy deserialization
//! This demonstrates the proper way to handle trading-poc messages with rkyv

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo_remote;
use rkyv::Deserialize;

// Import FuturesOHLCVCandle from trading-poc
use trading_exchange_core::types::FuturesOHLCVCandle;

// Test message containing FuturesOHLCVCandle
#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CandleMessage {
    pub candle: FuturesOHLCVCandle,
    pub symbol: String,
}

// Test actor
#[derive(Debug)]
struct TradingActor {
    candles_received: u32,
}

impl Actor for TradingActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    
    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🚀 TradingActor started");
        Ok(Self { candles_received: 0 })
    }
}

// Archived handler for zero-copy deserialization
impl TradingActor {
    async fn handle_candle_message(&mut self, msg: &rkyv::Archived<CandleMessage>) {
        println!("🎯 ZERO-COPY HANDLER CALLED!");
        self.candles_received += 1;
        
        // Access fields directly from archived type - no deserialization!
        let symbol = &msg.symbol;
        let candle = &msg.candle;
        
        println!("  Symbol: {}", symbol);
        println!("  Open: {}", candle.open);
        println!("  High: {}", candle.high);
        println!("  Low: {}", candle.low);
        println!("  Close: {}", candle.close);
        println!("  Volume: {}", candle.volume);
        println!("  Timestamp: {}", candle.open_timestamp);
        println!("  Total candles received: {}", self.candles_received);
        
        // If we need to deserialize for processing:
        if self.candles_received == 1 {
            println!("📊 Deserializing first candle for detailed processing...");
            let mut failure = rkyv::rancor::Failure;
            let mut deserializer = rkyv::rancor::Strategy::<rkyv::rancor::Failure, rkyv::rancor::Panic>::wrap(&mut failure);
            let deserialized: CandleMessage = msg.deserialize(deserializer).unwrap();
            println!("  Deserialized symbol: {}", deserialized.symbol);
        }
    }
}

// MAPPING syntax - specify custom handler name
distributed_actor! {
    TradingActor {
        CandleMessage => handle_candle_message,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🧪 TEST FUTURES OHLCV SERVER WITH MAPPING SYNTAX");
    
    // Use fixed keypair for testing
    let server_keypair = kameo_remote::KeyPair::new_for_testing("test_futures_server");
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9350".parse()?,
        server_keypair
    ).await?;
    println!("✅ Server on 127.0.0.1:9350 with TLS");
    
    let actor_ref = TradingActor::spawn(());
    let actor_id = actor_ref.id();
    
    transport.register_actor("trading_actor".to_string(), actor_id).await?;
    println!("✅ TradingActor registered with ID {:?}", actor_id);
    
    // Add client as trusted peer for TLS
    if let Some(handle) = transport.handle() {
        let client_peer_id = kameo_remote::PeerId::new("test_futures_client");
        let _peer = handle.add_peer(&client_peer_id).await;
        println!("✅ Added client as trusted peer for TLS");
    }
    
    println!("📡 Ready. Run: cargo run --example test_futures_ohlcv_client --features remote");
    tokio::signal::ctrl_c().await?;
    Ok(())
}