//! Server that receives PreBacktestMessage messages to test serialization
//! Helps debug server crashes by testing complex nested data structures

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo::distributed_actor;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::time::Instant;

// Types from trading-poc
pub type TimestampMS = i64;

// Simplified types to match trading-poc structures
#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct CryptoFuturesSymbol {
    #[rkyv(with = rkyv::with::AsOwned)]
    pub base: Cow<'static, str>,
    #[rkyv(with = rkyv::with::AsOwned)]
    pub quote: Cow<'static, str>,
}

#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct FuturesOHLCVCandle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub open_time: TimestampMS,
    pub close_time: TimestampMS,
}

#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct TimeSeriesEvent<T> {
    pub t: TimestampMS,
    pub v: T,
}

#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
pub enum TimeSeriesEventType {
    Price(f64),
    Volume(f64),
    Indicator(String, f64),
}

/// Pre-backtest message with candles and events
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct PreBacktestMessage {
    pub symbol: CryptoFuturesSymbol,
    pub candles: FxHashMap<i64, Vec<FuturesOHLCVCandle>>,
    pub price_events: Vec<TimeSeriesEvent<TimeSeriesEventType>>,
}

/// Actor that processes PreBacktestMessage data
#[derive(Debug)]
struct BacktestMessageActor {
    messages_received: u64,
    total_candles: u64,
    total_events: u64,
    symbols_seen: Vec<String>,
    start_time: Instant,
    last_report_time: Instant,
    last_report_count: u64,
}

impl Actor for BacktestMessageActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    
    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("📊 BacktestMessageActor started - ready to receive PreBacktestMessages");
        Ok(Self {
            messages_received: 0,
            total_candles: 0,
            total_events: 0,
            symbols_seen: Vec::new(),
            start_time: Instant::now(),
            last_report_time: Instant::now(),
            last_report_count: 0,
        })
    }
}

// Message handler for PreBacktestMessage using Message trait
impl Message<PreBacktestMessage> for BacktestMessageActor {
    type Reply = ();
    
    async fn handle(
        &mut self,
        msg: PreBacktestMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Process the message
        self.messages_received += 1;
        
        // Track symbol
        let symbol_str = format!("{}:{}", msg.symbol.base, msg.symbol.quote);
        if !self.symbols_seen.contains(&symbol_str) {
            self.symbols_seen.push(symbol_str.clone());
            println!("🆕 New symbol seen: {}", symbol_str);
        }
        
        // Count candles
        let candle_count: usize = msg.candles.values().map(|v| v.len()).sum();
        self.total_candles += candle_count as u64;
        
        // Count events
        self.total_events += msg.price_events.len() as u64;
        
        // Print detailed deserialized contents for first message
        if self.messages_received == 1 {
            println!("\n📝 === FIRST MESSAGE DESERIALIZED CONTENTS ===");
            println!("🏷️ Symbol: {} / {}", msg.symbol.base, msg.symbol.quote);
            
            println!("\n📊 Candles by timeframe:");
            for (timeframe, candles) in &msg.candles {
                println!("   - Timeframe {} ms: {} candles", timeframe, candles.len());
                if let Some(first_candle) = candles.first() {
                    println!("     First candle: O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.2}",
                        first_candle.open, first_candle.high, first_candle.low, 
                        first_candle.close, first_candle.volume);
                    println!("     Time: {} -> {}", first_candle.open_time, first_candle.close_time);
                }
            }
            
            println!("\n📈 Price Events ({} total):", msg.price_events.len());
            for (i, event) in msg.price_events.iter().take(5).enumerate() {
                match &event.v {
                    TimeSeriesEventType::Price(p) => {
                        println!("   [{}] Price @ {}: {:.2}", i, event.t, p);
                    }
                    TimeSeriesEventType::Volume(v) => {
                        println!("   [{}] Volume @ {}: {:.2}", i, event.t, v);
                    }
                    TimeSeriesEventType::Indicator(name, val) => {
                        println!("   [{}] {} @ {}: {:.2}", i, name, event.t, val);
                    }
                }
            }
            if msg.price_events.len() > 5 {
                println!("   ... and {} more events", msg.price_events.len() - 5);
            }
            
            println!("\n✅ Message successfully deserialized and processed!");
            println!("   - Total candles across all timeframes: {}", candle_count);
            println!("   - Total events: {}", msg.price_events.len());
            println!("==========================================\n");
        }
        
        // Report progress every 100 messages
        if self.messages_received % 100 == 0 {
            let now = Instant::now();
            let interval_duration = now - self.last_report_time;
            let interval_count = self.messages_received - self.last_report_count;
            let interval_rate = interval_count as f64 / interval_duration.as_secs_f64();
            
            let total_duration = now - self.start_time;
            let overall_rate = self.messages_received as f64 / total_duration.as_secs_f64();
            
            println!(
                "📊 [{:6}] Rate: {:.0} msg/s (avg: {:.0} msg/s), Candles: {}, Events: {}, Symbols: {}",
                self.messages_received,
                interval_rate,
                overall_rate,
                self.total_candles,
                self.total_events,
                self.symbols_seen.len()
            );
            
            self.last_report_time = now;
            self.last_report_count = self.messages_received;
        }
    }
}

// Use distributed_actor! with list syntax (uses Message trait)
distributed_actor! {
    BacktestMessageActor {
        PreBacktestMessage,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing if enabled
    #[cfg(feature = "tracing")]
    tracing_subscriber::fmt()
        .with_env_filter("info,kameo=debug,kameo_remote=debug")
        .init();

    println!("\n🚀 === TELL PREBACKTEST SERVER ===");
    
    // Use deterministic keypair for testing
    let server_keypair = kameo_remote::KeyPair::new_for_testing("candle_server_test_key");
    
    // Initialize transport with TLS
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9342".parse()?,
        server_keypair,
    ).await?;
    
    println!("✅ Server listening on {}", transport.local_addr());
    
    // Spawn the backtest message actor
    let backtest_actor = BacktestMessageActor::spawn(());
    
    // Register with transport - this automatically registers handlers too!
    transport.register_distributed_actor("backtest_actor".to_string(), &backtest_actor).await?;
    let actor_id = backtest_actor.id();
    
    println!("✅ BacktestMessageActor registered:");
    println!("   - Actor ID: {:?}", actor_id);
    println!("   - Name: 'backtest_actor'");
    println!("   - Message type: PreBacktestMessage");
    println!("   - Base message size: ~{} bytes (plus dynamic data)", std::mem::size_of::<PreBacktestMessage>());
    
    println!("\n📡 Server ready. Waiting for PreBacktestMessage data...");
    println!("   Run the client with: cargo run --example tell_candle_client --features remote");
    
    // Keep server running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}