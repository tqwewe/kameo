//! Server that processes FuturesOHLCVCandle ask requests and returns analysis results
//! Demonstrates ask/reply with real-world trading data structures and zero-copy potential

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo::distributed_actor;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use std::time::Instant;

// Types from trading-poc (simplified for this example)
pub type TimestampMS = i64;

/// Taker flow tracking for buy/sell volume
#[derive(Debug, Clone, Default, Archive, RSerialize, RDeserialize)]
pub struct TakerFlow {
    pub taker_buy_volume: Option<f64>,
    pub taker_sell_volume: Option<f64>,
}

/// Futures OHLCV candle with taker flow data
#[derive(RemoteMessage, Default, Clone, Archive, RSerialize, RDeserialize)]
pub struct FuturesOHLCVCandle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    open_time: TimestampMS,
    close_time: TimestampMS,
    event_time: TimestampMS,
    taker_flow: TakerFlow,
    oi: Option<f64>,
    closed: bool,
}

/// Analysis result for a candle
#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct CandleAnalysis {
    pub price_change: f64,
    pub price_change_percent: f64,
    pub volume_weighted_price: f64,
    pub taker_buy_ratio: f64,
    pub volatility: f64,
    pub processing_time_us: u64,
}

// Implement Reply trait for the response type
impl kameo::reply::Reply for CandleAnalysis {
    type Ok = Self;
    type Error = kameo::error::Infallible;
    type Value = Self;

    fn to_result(self) -> Result<Self, kameo::error::Infallible> {
        Ok(self)
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

/// Actor that analyzes candle data with ask/reply support
#[derive(Debug)]
struct CandleAnalyzerActor {
    candles_analyzed: u64,
    total_volume: f64,
    high_watermark: f64,
    low_watermark: f64,
}

impl Actor for CandleAnalyzerActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    
    async fn on_start(
        _args: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        println!("📊 CandleAnalyzerActor started - ready to analyze candles");
        Ok(Self {
            candles_analyzed: 0,
            total_volume: 0.0,
            high_watermark: 0.0,
            low_watermark: f64::MAX,
        })
    }
}

// Message handler for FuturesOHLCVCandle with ask/reply support
impl Message<FuturesOHLCVCandle> for CandleAnalyzerActor {
    type Reply = CandleAnalysis;
    
    async fn handle(
        &mut self,
        candle: FuturesOHLCVCandle,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let start = Instant::now();
        
        // Update statistics
        self.candles_analyzed += 1;
        self.total_volume += candle.volume;
        
        if candle.high > self.high_watermark {
            self.high_watermark = candle.high;
        }
        if candle.low < self.low_watermark && candle.low > 0.0 {
            self.low_watermark = candle.low;
        }
        
        // Calculate analysis
        let price_change = candle.close - candle.open;
        let price_change_percent = if candle.open != 0.0 {
            (price_change / candle.open) * 100.0
        } else {
            0.0
        };
        
        // Volume weighted average price (simplified)
        let vwap = (candle.high + candle.low + candle.close) / 3.0;
        
        // Taker buy ratio
        let taker_buy_ratio = if let Some(buy_vol) = candle.taker_flow.taker_buy_volume {
            if let Some(sell_vol) = candle.taker_flow.taker_sell_volume {
                let total = buy_vol + sell_vol;
                if total > 0.0 {
                    buy_vol / total
                } else {
                    0.5
                }
            } else {
                1.0
            }
        } else {
            0.5
        };
        
        // Simple volatility calculation
        let volatility = ((candle.high - candle.low) / candle.close) * 100.0;
        
        let processing_time_us = start.elapsed().as_micros() as u64;
        
        // Log progress every 1000 candles
        if self.candles_analyzed % 1000 == 0 {
            println!(
                "📈 [{:7}] Analyzed - Volume: {:.2}, High: {:.2}, Low: {:.2}",
                self.candles_analyzed,
                self.total_volume,
                self.high_watermark,
                self.low_watermark
            );
        }
        
        CandleAnalysis {
            price_change,
            price_change_percent,
            volume_weighted_price: vwap,
            taker_buy_ratio,
            volatility,
            processing_time_us,
        }
    }
}

// Use distributed_actor! with list syntax for ask/reply support
distributed_actor! {
    CandleAnalyzerActor {
        FuturesOHLCVCandle,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing if enabled
    #[cfg(feature = "tracing")]
    tracing_subscriber::fmt()
        .with_env_filter("info,kameo=debug,kameo_remote=debug")
        .init();

    println!("\n🚀 === ASK CANDLE SERVER (ZERO-COPY) ===");
    
    // Use deterministic keypair for testing
    let server_keypair = kameo_remote::KeyPair::new_for_testing("ask_candle_server_test_key");
    println!("🔐 Server using Ed25519 keypair for TLS encryption");
    
    // Initialize transport with TLS
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9344".parse()?,
        server_keypair,
    ).await?;
    
    println!("✅ Server listening on {} with TLS encryption", transport.local_addr());
    
    // Spawn the candle analyzer actor
    let candle_analyzer = CandleAnalyzerActor::spawn(());
    
    // Register with transport - automatically handles distributed ask/reply
    transport.register_distributed_actor("candle_analyzer".to_string(), &candle_analyzer).await?;
    let actor_id = candle_analyzer.id();
    
    println!("✅ CandleAnalyzerActor registered:");
    println!("   - Actor ID: {:?}", actor_id);
    println!("   - Name: 'candle_analyzer'");
    println!("   - Ask/Reply: Enabled");
    println!("   - Zero-copy: Potential (using rkyv)");
    println!("   - Message size: ~{} bytes", std::mem::size_of::<FuturesOHLCVCandle>());
    println!("   - Reply size: ~{} bytes", std::mem::size_of::<CandleAnalysis>());
    
    println!("\n📡 Server ready. Waiting for candle analysis requests...");
    println!("   Run the client with: cargo run --example ask_candle_client --features remote");
    
    // Keep server running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}