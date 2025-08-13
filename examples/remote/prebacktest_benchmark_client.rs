//! High-performance benchmark client for PreBacktestMessage
//! Tests actual throughput without artificial delays

use kameo::remote::transport::RemoteTransport;
use kameo::remote::distributed_actor_ref::DistributedActorRef;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Types from trading-poc
pub type TimestampMS = i64;

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
        
        let timeframes = vec![60000, 300000, 900000, 3600000];
        
        for (tf_idx, &timeframe) in timeframes.iter().enumerate().take(timeframe_count) {
            let mut timeframe_candles = Vec::new();
            let candle_count = 100 / (tf_idx + 1);
            
            for i in 0..candle_count {
                let timestamp = base_timestamp + (i as i64 * timeframe);
                let price_base = 50000.0 + (i as f64 * 10.0);
                
                timeframe_candles.push(FuturesOHLCVCandle {
                    open: price_base,
                    high: price_base * 1.01,
                    low: price_base * 0.99,
                    close: price_base * 1.005,
                    volume: 1000.0 + (i as f64 * 100.0),
                    open_time: timestamp,
                    close_time: timestamp + timeframe,
                });
                
                if i % 5 == 0 {
                    price_events.push(TimeSeriesEvent {
                        t: timestamp,
                        v: TimeSeriesEventType::Price(price_base * 1.005),
                    });
                }
                
                if i % 10 == 0 {
                    price_events.push(TimeSeriesEvent {
                        t: timestamp,
                        v: TimeSeriesEventType::Indicator(
                            format!("RSI_{}", timeframe / 60000),
                            50.0 + (i as f64 % 30.0),
                        ),
                    });
                }
            }
            
            candles.insert(timeframe, timeframe_candles);
        }
        
        for i in 0..20 {
            let timestamp = base_timestamp + (i as i64 * 300000);
            price_events.push(TimeSeriesEvent {
                t: timestamp,
                v: TimeSeriesEventType::Volume(5000.0 + (i as f64 * 500.0)),
            });
        }
        
        Self { symbol, candles, price_events }
    }
    
    pub fn calculate_size(&self) -> usize {
        // Calculate actual serialized size
        let mut size = 0;
        
        // Symbol overhead
        size += self.symbol.base.len() + self.symbol.quote.len() + 16;
        
        // Candles
        for (_, candles) in &self.candles {
            size += 8; // timeframe key
            size += candles.len() * std::mem::size_of::<FuturesOHLCVCandle>();
        }
        
        // Events
        size += self.price_events.len() * 24; // Approximate event size
        for event in &self.price_events {
            if let TimeSeriesEventType::Indicator(name, _) = &event.v {
                size += name.len();
            }
        }
        
        // HashMap and Vec overhead
        size += 100; // Approximate structural overhead
        
        size
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "tracing")]
    tracing_subscriber::fmt()
        .with_env_filter("info,kameo=warn,kameo_remote=warn")
        .init();

    println!("\n🚀 === PREBACKTEST HIGH-PERFORMANCE BENCHMARK ===");
    
    let client_keypair = kameo_remote::KeyPair::new_for_testing("benchmark_client_test_key");
    
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9343".parse()?,
        client_keypair,
    ).await?;
    
    println!("✅ Client listening on {}", transport.local_addr());
    
    // Connect to server
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("candle_server_test_key");
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9342".parse()?).await?;
        println!("✅ Connected to server");
    }
    
    // Wait for gossip
    println!("⏳ Waiting for gossip propagation...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
    let backtest_actor = match DistributedActorRef::lookup("backtest_actor").await? {
        Some(ref_) => {
            println!("✅ Found BacktestMessageActor");
            ref_
        }
        None => {
            return Err("Actor not found".into());
        }
    };
    
    let base_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    
    // Create a sample message to measure size
    let sample_msg = PreBacktestMessage::new_test(
        CryptoFuturesSymbol {
            base: Cow::Borrowed("BTC"),
            quote: Cow::Borrowed("USDT"),
        },
        base_timestamp,
        4,
    );
    
    let actual_msg_size = sample_msg.calculate_size();
    let candle_count: usize = sample_msg.candles.values().map(|v| v.len()).sum();
    let event_count = sample_msg.price_events.len();
    
    println!("\n📊 Message characteristics:");
    println!("   - Calculated size: {} bytes", actual_msg_size);
    println!("   - Candles: {}", candle_count);
    println!("   - Events: {}", event_count);
    println!("   - Timeframes: {}", sample_msg.candles.len());
    
    // SINGLE MESSAGE TEST - Send just one message to verify deserialization
    println!("\n📤 === SENDING SINGLE MESSAGE FOR DESERIALIZATION TEST ===");
    
    let test_msg = PreBacktestMessage::new_test(
        CryptoFuturesSymbol {
            base: Cow::Borrowed("BTC"),
            quote: Cow::Borrowed("USDT"),
        },
        base_timestamp,
        4,  // 4 timeframes
    );
    
    println!("📊 Sending message with:");
    println!("   - Symbol: BTC/USDT");
    println!("   - Timeframes: {}", test_msg.candles.len());
    println!("   - Total candles: {}", candle_count);
    println!("   - Total events: {}", test_msg.price_events.len());
    println!("   - Message size: {} bytes", actual_msg_size);
    
    let send_start = Instant::now();
    
    // Send the tell message and await to ensure it's actually sent
    println!("📨 Calling tell() on actor...");
    backtest_actor.tell(test_msg).send().await.map_err(|e| format!("Failed to send: {:?}", e))?;
    
    let send_time = send_start.elapsed();
    
    println!("\n✅ Message sent and confirmed in {:?}", send_time);
    println!("📡 Check server output for deserialized contents...");
    
    // Wait longer for server to process and display
    println!("⏳ Waiting for server to process message...");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    
    /* COMMENTED OUT - Full benchmark code preserved below
    
    // Warm-up with 100 messages
    println!("\n🔥 Warming up with 100 messages...");
    for i in 0..100 {
        let msg = PreBacktestMessage::new_test(
            CryptoFuturesSymbol {
                base: Cow::Borrowed("BTC"),
                quote: Cow::Borrowed("USDT"),
            },
            base_timestamp + (i * 60000),
            4,
        );
        backtest_actor.tell(msg).send();
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Benchmark configurations
    let test_configs = vec![
        (10_000, 1000, "10K messages, batch 1000"),
        (50_000, 5000, "50K messages, batch 5000"),
        (100_000, 10000, "100K messages, batch 10000"),
    ];
    
    for (total_messages, batch_size, description) in test_configs {
        println!("\n📊 Test: {}", description);
        
        let mut messages_sent = 0u64;
        let benchmark_start = Instant::now();
        let mut last_report = Instant::now();
        
        for batch_num in 0..(total_messages / batch_size) {
            let batch_start = Instant::now();
            
            // Send batch without any delays
            for i in 0..batch_size {
                let msg_idx = batch_num * batch_size + i;
                let symbol_idx = msg_idx % 3;
                let (base, quote) = match symbol_idx {
                    0 => ("BTC", "USDT"),
                    1 => ("ETH", "USDT"),
                    _ => ("SOL", "USDT"),
                };
                
                let msg = PreBacktestMessage::new_test(
                    CryptoFuturesSymbol {
                        base: Cow::Borrowed(base),
                        quote: Cow::Borrowed(quote),
                    },
                    base_timestamp + (msg_idx as i64 * 60000),
                    4,
                );
                
                backtest_actor.tell(msg).send();
                messages_sent += 1;
            }
            
            // Only report every second
            if last_report.elapsed() > std::time::Duration::from_secs(1) {
                let elapsed = benchmark_start.elapsed();
                let rate = messages_sent as f64 / elapsed.as_secs_f64();
                let throughput = (messages_sent as usize * actual_msg_size) as f64 / 1_000_000.0 / elapsed.as_secs_f64();
                
                println!("   [{:6}/{:6}] Rate: {:.0} msg/s, Throughput: {:.2} MB/s",
                    messages_sent, total_messages, rate, throughput);
                last_report = Instant::now();
            }
        }
        
        let total_elapsed = benchmark_start.elapsed();
        let final_rate = messages_sent as f64 / total_elapsed.as_secs_f64();
        let total_bytes = messages_sent as usize * actual_msg_size;
        let final_throughput = (total_bytes as f64 / 1_000_000.0) / total_elapsed.as_secs_f64();
        
        println!("\n   ✅ Results:");
        println!("      - Messages: {}", messages_sent);
        println!("      - Time: {:?}", total_elapsed);
        println!("      - Rate: {:.0} msg/s", final_rate);
        println!("      - Throughput: {:.2} MB/s", final_throughput);
        println!("      - Total data: {:.2} MB", total_bytes as f64 / 1_000_000.0);
        println!("      - Avg latency: {:.2} μs/msg", total_elapsed.as_micros() as f64 / messages_sent as f64);
        
        // Give server time to catch up before next test
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    
    */
    
    println!("\n✅ Benchmark complete!");
    Ok(())
}