//! TAManager (client) for bugfix test
//!
//! This simulates the real TAManager that sends PreBacktest and BacktestIteration messages
//! to Executor and should receive BacktestSummary messages back.
//!
//! Run after starting the server:
//! cargo run --example tell_bugfix_client --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import SHARED bugfix message definitions - SINGLE SOURCE OF TRUTH
mod bugfix_messages;
use bugfix_messages::*;

// TAManager actor that can receive responses from Executor
struct TAManagerActor {
    responses_received: u32,
}

impl Actor for TAManagerActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🚀 TAManagerActor started - simulating real TAManager");
        println!("📤 Ready to send: PreBacktest → BacktestIteration, then receive BacktestSummary");
        Ok(Self {
            responses_received: 0,
        })
    }
}

// Handler for TAManagerResponse messages from TAManager
use kameo::message::{Context, Message};

impl Message<TAManagerResponse> for TAManagerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: TAManagerResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.responses_received += 1;

        println!(
            "🚨📨 [TAMANAGER] RECEIVED TAManagerResponse #{}: {}",
            msg.message_id, msg.response_data
        );
        println!("🔥 [TAMANAGER] TAManagerResponse handler working! Bidirectional messaging SUCCESS!");
        
        println!(
            "📨 [TAMANAGER] Total responses received from TAManager: {}",
            self.responses_received
        );
    }
}

// Handler for BacktestSummaryMessage sent from Executor - THE CRITICAL TEST!
impl Message<BacktestSummaryMessage> for TAManagerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: BacktestSummaryMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.responses_received += 1;

        println!(
            "🚨🎯📡 *** [TAMANAGER] BacktestSummary RECEIVED from Executor! *** 🚨🎯📡"
        );
        println!(
            "🎉 SUCCESS: BacktestSummary handler IS working! ID={}, PnL={}, trades={}, win_rate={}%, data_size={} bytes",
            msg.backtest_id, msg.total_pnl, msg.total_trades, msg.win_rate, msg.summary_data.len()
        );
        println!("🔥 [TAMANAGER] BacktestSummary bidirectional messaging SUCCESS!");
        println!("🔬 [TAMANAGER] Message details: backtest_id={}, summary_data first 10 bytes: {:?}", 
                msg.backtest_id, &msg.summary_data[..std::cmp::min(10, msg.summary_data.len())]);
        
        println!(
            "📨 [TAMANAGER] Total responses received from Executor: {}",
            self.responses_received
        );
    }
}

// Register with distributed actor macro for bidirectional communication
distributed_actor! {
    TAManagerActor {
        TAManagerResponse,
        BacktestSummaryMessage, // The key message we need to receive from Executor!
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Ensure the rustls CryptoProvider is installed (required for TLS)
    kameo_remote::tls::ensure_crypto_provider();

    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    println!("\n🚀 === BUGFIX TAMANAGER CLIENT (WITH TLS) ===");

    // For production, you would load keypair from secure storage or generate and save it
    // Here we use a deterministic keypair for the example (using seed-based generation)
    let client_keypair = {
        // In production: load from file or use proper key management
        // For this example, we'll use a fixed seed for reproducibility
        kameo_remote::KeyPair::new_for_testing("tls_client_production_key")
    };
    println!("🔐 TAManager using Ed25519 keypair for TLS encryption");
    println!("⚠️  Note: In production, use properly generated and stored keypairs");

    // Bootstrap on port 9311 with TLS enabled using keypair
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9311".parse()?,
        client_keypair,
    )
    .await?;
    println!(
        "✅ TAManager listening on {} with TLS encryption",
        transport.local_addr()
    );

    // Connect to Executor server with TLS encryption
    println!("\n📡 Connecting to Executor server at 127.0.0.1:9310 with TLS...");
    if let Some(handle) = transport.handle() {
        // Add the server as a trusted peer using its keypair-based PeerId
        let server_peer_id = kameo_remote::PeerId::new("tls_server_production_key");

        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9310".parse()?).await?;
        println!("✅ Connected to Executor server with TLS encryption and mutual authentication");
        
        // Give time for connection to stabilize
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Create and register TAManagerActor for bidirectional communication
    println!("\n🎬 Creating TAManagerActor to receive Executor responses...");
    let ta_manager_actor_ref = TAManagerActor::spawn(());
    let ta_manager_actor_id = ta_manager_actor_ref.id();

    // Use sync registration to wait for peer confirmation (eliminates need for sleep delays)
    transport
        .register_distributed_actor_sync(
            "ta_manager".to_string(),
            &ta_manager_actor_ref,
            std::time::Duration::from_secs(2),
        )
        .await?;
    println!(
        "✅ TAManagerActor registered as 'ta_manager' with ID {:?} and gossip confirmed",
        ta_manager_actor_id
    );

    // Look up remote Executor actor (with connection caching)
    println!("\n🔍 Looking up remote ExecutorActor...");
    
    // Give gossip protocol time to propagate actor registration
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    
    let executor_ref = match DistributedActorRef::lookup("executor").await? {
        Some(ref_) => {
            println!("✅ Found ExecutorActor on server with cached connection");
            println!("📍 Actor ID: {:?}", ref_.id());
            ref_
        }
        None => {
            println!("❌ ExecutorActor not found on server");
            return Err("Actor not found".into());
        }
    };

    // === BACKTEST MESSAGE FLOW TEST ===
    println!("\n📤 Testing BacktestSummary message flow simulation...");
    println!("🔄 Sending: PreBacktest → BacktestIteration, then Executor sends BacktestSummary back");
    let all_tests_start = std::time::Instant::now();

    // Debug: Print the type hashes being used
    use kameo::remote::type_hash::HasTypeHash;
    println!("\n🔍 ExecutionRouter type hashes:");
    println!(
        "   PreBacktestMessage: {:08x}",
        <PreBacktestMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!(
        "   BacktestIterationMessage: {:08x}",
        <BacktestIterationMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!(
        "   BacktestSummaryMessage: {:08x} 🚨",
        <BacktestSummaryMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );

    // Step 1: Send PreBacktest message
    println!("\n🧪 Step 1: Sending PreBacktest message (TAManager → Executor)");
    let start = std::time::Instant::now();
    executor_ref
        .tell(PreBacktestMessage {
            strategy: "important_points".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1h".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("✅ Sent PreBacktest message in {:?}", duration);

    // Small delay to simulate processing time
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Step 2: Send BacktestIteration message
    println!("\n🧪 Step 2: Sending BacktestIteration message (TAManager → Executor)");
    let start = std::time::Instant::now();
    executor_ref
        .tell(BacktestIterationMessage {
            iteration: 1,
            progress_pct: 50.0,
            current_pnl: 125.75,
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("✅ Sent BacktestIteration message in {:?}", duration);

    // Small delay to simulate processing time
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Step 3: Wait for Executor to send BacktestSummary back to us
    println!("\n🧪 Step 3: 🚨 Waiting for BacktestSummary from Executor 🚨");
    println!("🚨 This is the message that fails to be received in the real system!");
    println!("⏳ [TAMANAGER] The Executor should now send BacktestSummary back...");
    
    // Give enough time for the BacktestSummary to be sent and received
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let all_tests_duration = all_tests_start.elapsed();
    println!("\n🎉 Message flow complete! Check output above for BacktestSummary receipt.");
    println!("⏱️  Total time: {:?}", all_tests_duration);
    
    println!("\n🚨 KEY TEST RESULT:");
    println!("🔍 Look above for '🚨🎯📡 *** [TAMANAGER] BacktestSummary RECEIVED from Executor! ***'");
    println!("   ✅ If you see it: Bidirectional BacktestSummary messaging works!");
    println!("   ❌ If you don't: We've reproduced the real system issue!");

    Ok(())
}