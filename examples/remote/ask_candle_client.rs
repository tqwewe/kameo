//! Client that sends FuturesOHLCVCandle ask requests and receives analysis results
//! Tests ask/reply performance with real-world trading data structures

use kameo::actor::ActorRef;
use kameo::remote::transport::RemoteTransport;
use kameo::remote::distributed_actor_ref::DistributedActorRef;
use kameo::distributed_actor;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

impl FuturesOHLCVCandle {
    pub fn new_test(timestamp: TimestampMS, price: f64, volume: f64) -> Self {
        let volatility = (rand::random::<f64>() * 0.02) - 0.01; // ±1% volatility
        Self {
            open: price,
            high: price * (1.0 + volatility.abs()),
            low: price * (1.0 - volatility.abs()),
            close: price * (1.0 + volatility),
            volume,
            open_time: timestamp,
            close_time: timestamp + 60000, // 1 minute candle
            event_time: timestamp,
            taker_flow: TakerFlow {
                taker_buy_volume: Some(volume * (0.4 + rand::random::<f64>() * 0.2)),
                taker_sell_volume: Some(volume * (0.4 + rand::random::<f64>() * 0.2)),
            },
            oi: Some(volume * 100.0),
            closed: true,
        }
    }
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

    println!("\n🚀 === ASK CANDLE CLIENT (ZERO-COPY) ===");
    
    // Use deterministic keypairs for testing
    let client_keypair = kameo_remote::KeyPair::new_for_testing("ask_candle_client_test_key");
    println!("🔐 Client using Ed25519 keypair for TLS encryption");
    
    // Bootstrap client transport with TLS
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9345".parse()?,  // Client port
        client_keypair,
    ).await?;
    
    println!("✅ Client listening on {} with TLS encryption", transport.local_addr());
    
    // Connect to server as peer with TLS
    println!("\n📡 Connecting to server at 127.0.0.1:9344 with TLS...");
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("ask_candle_server_test_key");
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9344".parse()?).await?;
        println!("✅ Connected to server with TLS encryption and mutual authentication");
    }
    
    // Wait for gossip to propagate (gossip interval is 5 seconds)
    println!("⏳ Waiting for gossip to propagate actor registration...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
    // Look up the remote candle analyzer actor
    println!("\n🔍 Looking up remote CandleAnalyzerActor...");
    let candle_analyzer = match DistributedActorRef::lookup("candle_analyzer").await? {
        Some(ref_) => {
            println!("✅ Found CandleAnalyzerActor on server");
            ref_
        }
        None => {
            println!("❌ CandleAnalyzerActor not found on server");
            return Err("Actor not found".into());
        }
    };
    
    // Warm-up requests
    println!("\n🔥 Warming up with 10 requests...");
    let base_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    
    for i in 0..10 {
        let candle = FuturesOHLCVCandle::new_test(
            base_timestamp + (i * 60000),
            50000.0 + (i as f64 * 10.0),
            1000.0 + (i as f64 * 100.0),
        );
        
        let _analysis: CandleAnalysis = candle_analyzer.ask(candle).send().await?;
    }
    
    // Benchmark configuration
    let num_requests = 1000;
    let batch_size = 100;
    
    println!("\n📊 Starting ask/reply benchmark:");
    println!("   - Total requests: {}", num_requests);
    println!("   - Batch size: {}", batch_size);
    println!("   - Message size: ~{} bytes", std::mem::size_of::<FuturesOHLCVCandle>());
    println!("   - Reply size: ~{} bytes", std::mem::size_of::<CandleAnalysis>());
    
    let mut total_latency_us = 0u64;
    let mut min_latency_us = u64::MAX;
    let mut max_latency_us = 0u64;
    let mut successful_requests = 0u64;
    
    let benchmark_start = Instant::now();
    
    for batch in 0..(num_requests / batch_size) {
        let batch_start = Instant::now();
        
        for i in 0..batch_size {
            let candle = FuturesOHLCVCandle::new_test(
                base_timestamp + ((batch * batch_size + i) as i64 * 60000),
                50000.0 + rand::random::<f64>() * 10000.0,
                1000.0 + rand::random::<f64>() * 5000.0,
            );
            
            let request_start = Instant::now();
            
            // Send ask request and wait for reply
            let analysis: CandleAnalysis = candle_analyzer.ask(candle).send().await?;
            
            let request_latency_us = request_start.elapsed().as_micros() as u64;
            
            // Track statistics
            total_latency_us += request_latency_us;
            min_latency_us = min_latency_us.min(request_latency_us);
            max_latency_us = max_latency_us.max(request_latency_us);
            successful_requests += 1;
            
            // Verify we got a valid response
            if analysis.processing_time_us == 0 {
                println!("⚠️ Warning: Received invalid analysis result");
            }
        }
        
        let batch_elapsed = batch_start.elapsed();
        
        // Progress update every batch
        if batch % 2 == 0 {
            let total_elapsed = benchmark_start.elapsed();
            let rate = successful_requests as f64 / total_elapsed.as_secs_f64();
            println!(
                "   [{:4}/{:4}] Rate: {:.0} req/s, Batch: {:?}, Avg latency: {:.0} μs",
                successful_requests,
                num_requests,
                rate,
                batch_elapsed,
                total_latency_us / successful_requests.max(1)
            );
        }
    }
    
    let total_elapsed = benchmark_start.elapsed();
    let avg_latency_us = total_latency_us / successful_requests.max(1);
    let req_per_sec = successful_requests as f64 / total_elapsed.as_secs_f64();
    
    // Calculate throughput
    let message_size = std::mem::size_of::<FuturesOHLCVCandle>() + std::mem::size_of::<CandleAnalysis>();
    let total_bytes = successful_requests as usize * message_size;
    let throughput_mbps = (total_bytes as f64 / 1_000_000.0) / total_elapsed.as_secs_f64();
    
    println!("\n✅ === ASK/REPLY BENCHMARK RESULTS ===");
    println!("   Successful requests: {}/{}", successful_requests, num_requests);
    println!("   Total time: {:?}", total_elapsed);
    println!("   Request rate: {:.0} req/s", req_per_sec);
    println!("   Throughput: {:.2} MB/s", throughput_mbps);
    println!("   Latency (avg/min/max): {:.0}/{:.0}/{:.0} μs", 
             avg_latency_us, min_latency_us, max_latency_us);
    println!("   Total data exchanged: {:.2} MB", total_bytes as f64 / 1_000_000.0);
    
    println!("\n✅ Client completed successfully!");
    Ok(())
}