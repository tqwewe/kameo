//! Client that sends PreBacktestMessage messages to test serialization
//! Helps debug server crashes by testing complex nested data structures

use kameo::remote::transport::RemoteTransport;
use kameo::remote::distributed_actor_ref::DistributedActorRef;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

impl PreBacktestMessage {
    pub fn new_test(symbol: CryptoFuturesSymbol, base_timestamp: TimestampMS, timeframe_count: usize) -> Self {
        let mut candles = FxHashMap::default();
        let mut price_events = Vec::new();
        
        // Create candles for different timeframes (1m, 5m, 15m, 1h)
        let timeframes = vec![60000, 300000, 900000, 3600000]; // in milliseconds
        
        for (tf_idx, &timeframe) in timeframes.iter().enumerate().take(timeframe_count) {
            let mut timeframe_candles = Vec::new();
            let candle_count = 100 / (tf_idx + 1); // Fewer candles for longer timeframes
            
            for i in 0..candle_count {
                let timestamp = base_timestamp + (i as i64 * timeframe);
                let price_base = 50000.0 + (i as f64 * 10.0);
                let volatility = (rand::random::<f64>() * 0.02) - 0.01; // ±1% volatility
                
                timeframe_candles.push(FuturesOHLCVCandle {
                    open: price_base,
                    high: price_base * (1.0 + volatility.abs()),
                    low: price_base * (1.0 - volatility.abs()),
                    close: price_base * (1.0 + volatility),
                    volume: 1000.0 + (rand::random::<f64>() * 5000.0),
                    open_time: timestamp,
                    close_time: timestamp + timeframe,
                });
                
                // Add some price events
                if i % 5 == 0 {
                    price_events.push(TimeSeriesEvent {
                        t: timestamp,
                        v: TimeSeriesEventType::Price(price_base * (1.0 + volatility)),
                    });
                }
                
                // Add some indicator events
                if i % 10 == 0 {
                    price_events.push(TimeSeriesEvent {
                        t: timestamp,
                        v: TimeSeriesEventType::Indicator(
                            format!("RSI_{}", timeframe / 60000),
                            30.0 + rand::random::<f64>() * 40.0, // RSI between 30-70
                        ),
                    });
                }
            }
            
            candles.insert(timeframe, timeframe_candles);
        }
        
        // Add some volume events
        for i in 0..20 {
            let timestamp = base_timestamp + (i as i64 * 300000); // Every 5 minutes
            price_events.push(TimeSeriesEvent {
                t: timestamp,
                v: TimeSeriesEventType::Volume(1000.0 + rand::random::<f64>() * 10000.0),
            });
        }
        
        Self {
            symbol,
            candles,
            price_events,
        }
    }
}

// Simple random float generator (avoiding external deps)
mod rand {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(12345);
    
    pub fn random<T>() -> f64 {
        let mut x = SEED.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        SEED.store(x, Ordering::Relaxed);
        (x as f64) / (u64::MAX as f64)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing if enabled
    #[cfg(feature = "tracing")]
    tracing_subscriber::fmt()
        .with_env_filter("info,kameo=debug,kameo_remote=debug")
        .init();

    println!("\n🚀 === TELL PREBACKTEST CLIENT (ZERO-COPY) ===");
    
    // Use deterministic keypairs for testing
    let client_keypair = kameo_remote::KeyPair::new_for_testing("candle_client_test_key");
    println!("🔐 Client using Ed25519 keypair for TLS encryption");
    
    // Bootstrap client transport with TLS
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9343".parse()?,
        client_keypair,
    ).await?;
    
    println!("✅ Client listening on {} with TLS encryption", transport.local_addr());
    
    // Connect to server as peer with TLS
    println!("\n📡 Connecting to server at 127.0.0.1:9342 with TLS...");
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("candle_server_test_key");
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9342".parse()?).await?;
        println!("✅ Connected to server with TLS encryption and mutual authentication");
    }
    
    // Wait for gossip to propagate (gossip interval is 5 seconds)
    println!("⏳ Waiting for gossip to propagate actor registration...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
    // Look up the remote backtest message actor
    println!("\n🔍 Looking up remote BacktestMessageActor...");
    let backtest_actor = match DistributedActorRef::lookup("backtest_actor").await? {
        Some(ref_) => {
            println!("✅ Found BacktestMessageActor on server");
            ref_
        }
        None => {
            println!("❌ BacktestMessageActor not found on server");
            return Err("Actor not found".into());
        }
    };
    
    // Test with different symbols and message sizes
    let symbols = vec![
        ("BTC", "USDT"),
        ("ETH", "USDT"),
        ("SOL", "USDT"),
    ];
    
    println!("\n📊 Starting to send PreBacktestMessages:");
    println!("   - Message type: PreBacktestMessage");
    println!("   - Testing {} symbols", symbols.len());
    
    let base_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    
    // Send just ONE message to test deserialization
    println!("\n📤 Sending ONE PreBacktestMessage to test deserialization...");
    
    let symbol = CryptoFuturesSymbol {
        base: Cow::Borrowed("BTC"),
        quote: Cow::Borrowed("USDT"),
    };
    
    let msg = PreBacktestMessage::new_test(
        symbol,
        base_timestamp,
        4, // 4 timeframes (1m, 5m, 15m, 1h)
    );
    
    // Calculate message details
    let candle_count: usize = msg.candles.values().map(|v| v.len()).sum();
    let event_count = msg.price_events.len();
    
    println!("📊 Message details:");
    println!("   - Symbol: BTC/USDT");
    println!("   - Timeframes: {}", msg.candles.len());
    println!("   - Total candles: {}", candle_count);
    println!("   - Total events: {}", event_count);
    
    // Send the message
    let send_start = Instant::now();
    backtest_actor.tell(msg).send().await?;
    let send_elapsed = send_start.elapsed();
    
    println!("\n✅ Message sent successfully!");
    println!("   - Send time: {:?}", send_elapsed);
    
    // Give server time to process remaining messages
    println!("\n⏳ Waiting for server to process remaining messages...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    
    println!("\n✅ Client completed successfully!");
    println!("   Check server output for processing statistics");
    
    Ok(())
}