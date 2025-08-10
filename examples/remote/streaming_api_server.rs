//! Streaming API server example
//!
//! This demonstrates receiving messages through the high-performance streaming API.
//! The server processes both regular tell messages and streamed messages.
//!
//! Run before starting the client:
//! cargo run --example streaming_api_server --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};

// Import shared message definitions for streaming
mod streaming_messages;
use streaming_messages::*;

// Track statistics globally
static TOTAL_BYTES_RECEIVED: AtomicUsize = AtomicUsize::new(0);
static MESSAGES_RECEIVED: AtomicUsize = AtomicUsize::new(0);

/// Actor that processes backtest-related messages
#[derive(Debug)]
struct BacktestActor {
    prebacktest_count: usize,
    config_count: usize,
    bytes_received: usize,
}

impl Actor for BacktestActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    
    async fn on_start(
        _args: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        println!("\n🎬 BacktestActor started and ready to receive messages!");
        println!("   Supports both regular tell and streaming API");
        Ok(Self {
            prebacktest_count: 0,
            config_count: 0,
            bytes_received: 0,
        })
    }
}

// Handle PreBacktestMessage - these will be streamed due to size
impl Message<PreBacktestMessage> for BacktestActor {
    type Reply = ();

    async fn handle(&mut self, msg: PreBacktestMessage, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let start = Instant::now();
        self.prebacktest_count += 1;
        
        // Calculate message size
        let size = std::mem::size_of_val(&msg) + 
                   msg.candles.values().map(|v| v.len() * 56).sum::<usize>() + 
                   msg.indicator_configs.len() * 100;
        
        self.bytes_received += size;
        TOTAL_BYTES_RECEIVED.fetch_add(size, Ordering::Relaxed);
        MESSAGES_RECEIVED.fetch_add(1, Ordering::Relaxed);
        
        let total_candles: usize = msg.candles.values().map(|v| v.len()).sum();
        
        println!("\n📥 Received PreBacktest message #{}", self.prebacktest_count);
        println!("   Backtest ID: {}", msg.backtest_id);
        println!("   Symbol: {}", msg.symbol);
        println!("   Total candles: {}", total_candles);
        println!("   Indicators: {}", msg.indicator_configs.len());
        println!("   Message size: {:.2} MB", size as f64 / 1_048_576.0);
        println!("   Processing time: {:?}", start.elapsed());
        
        // Check if this was likely streamed
        if size > 1_048_576 {
            println!("   🌊 This message was likely received via streaming API");
        }
    }
}

// Handle BacktestRunConfig - smaller messages but still important
impl Message<BacktestRunConfig> for BacktestActor {
    type Reply = ();

    async fn handle(&mut self, msg: BacktestRunConfig, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.config_count += 1;
        MESSAGES_RECEIVED.fetch_add(1, Ordering::Relaxed);
        
        println!("\n⚙️  Received BacktestRunConfig #{}", self.config_count);
        println!("   Symbol: {}", msg.symbol);
        println!("   Time range: {} - {}", msg.start_time, msg.end_time);
        println!("   Initial balance: ${}", msg.initial_balance);
        println!("   Leverage: {}x", msg.leverage);
        println!("   Strategy params: {:?}", msg.strategy_params);
    }
}

// ZERO-COPY handlers for distributed messages
impl BacktestActor {
    async fn handle_prebacktest(&mut self, msg: &rkyv::Archived<PreBacktestMessage>) {
        let start = Instant::now();
        self.prebacktest_count += 1;
        
        // Note: Size calculation is approximate for archived data
        let total_candles: usize = msg.candles.iter().map(|(_, v)| v.len()).sum();
        let approx_size = total_candles * 56 + 1024;
        
        self.bytes_received += approx_size;
        TOTAL_BYTES_RECEIVED.fetch_add(approx_size, Ordering::Relaxed);
        MESSAGES_RECEIVED.fetch_add(1, Ordering::Relaxed);
        
        println!("\n📥 [ZERO-COPY] Received PreBacktest via remote_tell");
        println!("   Processing time: {:?}", start.elapsed());
        println!("   Total candles: {}", total_candles);
        println!("   Approx size: {:.2} MB", approx_size as f64 / 1_048_576.0);
        
        if approx_size > 1_048_576 {
            println!("   🌊 This large message was received via streaming API!");
        }
    }
    
    async fn handle_config(&mut self, msg: &rkyv::Archived<BacktestRunConfig>) {
        self.config_count += 1;
        MESSAGES_RECEIVED.fetch_add(1, Ordering::Relaxed);
        println!("\n⚙️  [ZERO-COPY] Received BacktestRunConfig via remote_tell");
        println!("   Symbol: {}", msg.symbol.as_str());
    }
}

// Register with distributed actor macro
kameo::distributed_actor! {
    BacktestActor {
        PreBacktestMessage => handle_prebacktest,
        BacktestRunConfig => handle_config,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=info")
        .try_init();

    println!("\n🚀 === STREAMING API SERVER ===");

    // Bootstrap on default server port
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9310".parse()?).await?;
    println!("✅ Server listening on {}", transport.local_addr());

    // Spawn the BacktestActor
    let backtest_actor = BacktestActor::spawn(());
    let actor_id = backtest_actor.id();

    // Register it as a distributed actor with the name "backtest_executor"
    transport.register_actor("backtest_executor".to_string(), actor_id).await?;
    println!("✅ Registered BacktestActor as 'backtest_executor' with ID: {:?}", actor_id);
    
    // Add client as peer
    if let Some(handle) = transport.handle() {
        let _peer = handle.add_peer(&kameo_remote::PeerId::new("kameo_node_9311")).await;
        println!("✅ Added client node as peer");
    }

    println!("\n📊 Status:");
    println!("   Waiting for messages...");
    println!("   - Regular tell messages for small payloads");
    println!("   - Streaming API for large messages (>1MB)");
    println!("   - Zero-copy deserialization for all remote messages");

    // Spawn a task to periodically print statistics
    tokio::spawn(async {
        let mut last_messages = 0;
        let mut last_bytes = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            let messages = MESSAGES_RECEIVED.load(Ordering::Relaxed);
            let bytes = TOTAL_BYTES_RECEIVED.load(Ordering::Relaxed);
            
            if messages > last_messages {
                let new_messages = messages - last_messages;
                let new_bytes = bytes - last_bytes;
                println!("\n📈 Stats update: +{} messages, +{:.2} MB", 
                    new_messages, 
                    new_bytes as f64 / 1_048_576.0);
                last_messages = messages;
                last_bytes = bytes;
            }
        }
    });

    // Keep the server running
    println!("\n⏳ Server is running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;

    // Final statistics
    let total_messages = MESSAGES_RECEIVED.load(Ordering::Relaxed);
    let total_bytes = TOTAL_BYTES_RECEIVED.load(Ordering::Relaxed);
    
    println!("\n🛑 Shutting down server...");
    println!("\n📊 Final Statistics:");
    println!("   Total messages received: {}", total_messages);
    println!("   Total data received: {:.2} MB", total_bytes as f64 / 1_048_576.0);
    if total_messages > 0 {
        println!("   Average message size: {:.2} MB", 
            (total_bytes as f64 / total_messages as f64) / 1_048_576.0);
    }
    
    Ok(())
}