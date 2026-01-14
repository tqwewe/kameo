//! Concrete actor tell client (using newtDistributedActorRef API)
//!
//! This demonstrates the zero-cost abstraction approach where each message type
//! gets its own monomorphized code path for maximum performance.
//!
//! Run after starting the server:
//! cargo run --example tell_concrete_client --features remote

#![allow(dead_code, unused_variables)]

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import shared message definitions - defined once at compile time!
mod tell_messages;
use tell_messages::*;

// Client actor that can receive messages from server
struct ClientActor {
    responses_received: u32,
    benchmarks_received: u32,
    benchmark_start: Option<std::time::Instant>,
}

impl Actor for ClientActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🎬 ClientActor started and ready to receive server responses!");
        Ok(Self {
            responses_received: 0,
            benchmarks_received: 0,
            benchmark_start: None,
        })
    }
}

// Handler for ServerResponse messages from server
use kameo::message::{Context, Message};

impl Message<ServerResponse> for ClientActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ServerResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.responses_received += 1;

        // ADD VERY VISIBLE DEBUG OUTPUT
        println!(
            "🚨🚨🚨 [CLIENT] RECEIVED ServerResponse #{}: {} 🚨🚨🚨",
            msg.message_id, msg.response_data
        );
        println!("🔥 [CLIENT] ServerResponse handler is working! Bidirectional messaging SUCCESS!");

        if self.responses_received % 1 == 0 {
            // Log every response for debugging
            println!(
                "📨 [CLIENT] Total responses received from server: {}",
                self.responses_received
            );
        }
    }
}

// Handler for ServerBenchmark messages from server
impl Message<ServerBenchmark> for ClientActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: ServerBenchmark,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.benchmarks_received == 0 {
            self.benchmark_start = Some(std::time::Instant::now());
            println!("📊 [CLIENT] Started receiving server benchmark messages...");
        }
        self.benchmarks_received += 1;

        if self.benchmarks_received % 100 == 0 {
            println!(
                "📊 [CLIENT] Received {} server benchmarks",
                self.benchmarks_received
            );
        }

        if self.benchmarks_received == 1000 {
            let duration = self.benchmark_start.unwrap().elapsed();
            println!("✅ [CLIENT] Received all 1000 benchmarks in {:?}", duration);
            println!(
                "   Throughput: {:.2} messages/second",
                1000.0 / duration.as_secs_f64()
            );
            println!(
                "   Bandwidth: {:.2} MB/s",
                (1000.0 * 1024.0) / 1_048_576.0 / duration.as_secs_f64()
            );
        }
    }
}

// Register with distributed actor macro for bidirectional communication
distributed_actor! {
    ClientActor {
        ServerResponse,
        ServerBenchmark,
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

    println!("\n🚀 === CONCRETE ACTOR TELL CLIENT (WITH TLS) ===");

    // For production, you would load keypair from secure storage or generate and save it
    // Here we use a deterministic keypair for the example (using seed-based generation)
    let client_keypair = {
        // In production: load from file or use proper key management
        // For this example, we'll use a fixed seed for reproducibility
        kameo_remote::KeyPair::new_for_testing("tls_client_production_key")
    };
    println!("🔐 Client using Ed25519 keypair for TLS encryption");
    println!("⚠️  Note: In production, use properly generated and stored keypairs");

    // Bootstrap on port 9311 with TLS enabled using keypair
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9311".parse()?,
        client_keypair,
    )
    .await?;
    println!(
        "✅ Client listening on {} with TLS encryption",
        transport.local_addr()
    );

    // Connect to server with TLS encryption
    println!("\n📡 Connecting to server at 127.0.0.1:9310 with TLS...");
    if let Some(handle) = transport.handle() {
        // Add the server as a trusted peer using its keypair-based PeerId
        let server_peer_id = kameo_remote::PeerId::new("tls_server_production_key");

        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9310".parse()?).await?;
        println!("✅ Connected to server with TLS encryption and mutual authentication");
    }

    // Create and register ClientActor for bidirectional communication
    println!("\n🎬 Creating ClientActor to receive server responses...");
    let client_actor_ref = ClientActor::spawn(());
    let client_actor_id = client_actor_ref.id();

    // Use sync registration to wait for peer confirmation (eliminates need for sleep delays)
    transport
        .register_distributed_actor_sync(
            "client".to_string(),
            &client_actor_ref,
            std::time::Duration::from_secs(2),
        )
        .await?;
    println!(
        "✅ ClientActor registered as 'client' with ID {:?} and gossip confirmed",
        client_actor_id
    );

    // Look up remote actor (with connection caching) - zero-cost abstraction!
    println!("\n🔍 Looking up remote LoggerActor...");
    let logger_ref = match DistributedActorRef::lookup("logger").await? {
        Some(ref_) => {
            println!("✅ Found LoggerActor on server with cached connection");
            println!("📍 Actor ID: {:?}", ref_.id());
            ref_
        }
        None => {
            println!("❌ LoggerActor not found on server");
            return Err("Actor not found".into());
        }
    };

    // === MINIMAL BIDIRECTIONAL TEST ===
    println!("\n📤 Testing minimal bidirectional messaging...");
    let all_tests_start = std::time::Instant::now();

    // Single test: Send one INFO message from client → server
    println!("\n🧪 Sending single INFO message (client → server)");

    // Debug: Print the type hash being used
    use kameo::remote::type_hash::HasTypeHash;
    println!(
        "🔍 LogMessage type hash: {:08x}",
        <LogMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );

    let start = std::time::Instant::now();
    logger_ref
        .tell(LogMessage {
            level: "INFO".to_string(),
            content: "Minimal bidirectional test started".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("✅ Sent INFO message in {:?}", duration);

    // === COMMENTED OUT - Other test messages ===
    // Test 2: Send WARNING message
    // println!("\n🧪 Test 2: Sending WARNING log");
    // let start = std::time::Instant::now();
    // logger_ref
    //     .tell(LogMessage {
    //         level: "WARNING".to_string(),
    //         content: "High memory usage detected".to_string(),
    //     })
    //     .send()
    //     .await?;
    // let duration = start.elapsed();
    // println!("✅ Sent WARNING message in {:?}", duration);

    // Test 3: Send ERROR message
    // println!("\n🧪 Test 3: Sending ERROR log");
    // let start = std::time::Instant::now();
    // logger_ref
    //     .tell(LogMessage {
    //         level: "ERROR".to_string(),
    //         content: "Failed to connect to database".to_string(),
    //     })
    //     .send()
    //     .await?;
    // let duration = start.elapsed();
    // println!("✅ Sent ERROR message in {:?}", duration);

    // Test 4: Send multiple messages quickly
    // println!("\n🧪 Test 4: Sending 5 messages quickly");
    // let batch_start = std::time::Instant::now();
    // let mut individual_times = Vec::new();
    // for i in 1..=5 {
    //     let start = std::time::Instant::now();
    //     logger_ref
    //         .tell(LogMessage {
    //             level: "DEBUG".to_string(),
    //             content: format!("Debug message #{}", i),
    //         })
    //         .send()
    //         .await?;
    //     let duration = start.elapsed();
    //     individual_times.push(duration);
    //     println!("   Message {} sent in {:?}", i, duration);
    // }
    // let batch_duration = batch_start.elapsed();
    // let avg_duration =
    //     individual_times.iter().sum::<std::time::Duration>() / individual_times.len() as u32;
    // println!(
    //     "✅ Sent 5 DEBUG messages in {:?} (avg: {:?}/message)",
    //     batch_duration, avg_duration
    // );

    // Test 5: Send 100 messages to measure throughput
    // println!("\n🧪 Test 5: Sending 100 messages for throughput test");
    // let throughput_start = std::time::Instant::now();
    // for i in 1..=100 {
    //     logger_ref
    //         .tell(LogMessage {
    //             level: "TRACE".to_string(),
    //             content: format!("Throughput test message #{}", i),
    //         })
    //         .send()
    //         .await?;
    // }
    // let throughput_duration = throughput_start.elapsed();
    // let messages_per_second = 100.0 / throughput_duration.as_secs_f64();
    // println!(
    //     "✅ Sent 100 messages in {:?} ({:.2} messages/second)",
    //     throughput_duration, messages_per_second
    // );

    let all_tests_duration = all_tests_start.elapsed();
    println!("\n🎉 Client→Server message sent! Check the server output for the logged message.");
    println!("⏱️  Total time: {:?}", all_tests_duration);

    // === COMMENTED OUT - Benchmarks ===
    // === REALISTIC INDICATOR MESSAGE BENCHMARK ===
    // println!("\n🚀 === REALISTIC INDICATOR MESSAGE BENCHMARK ===");
    // println!("Testing zero-copy performance with real indicator data from trading-ta\n");
    //
    // if let Err(e) = run_indicator_benchmark(&logger_ref).await {
    //     println!("❌ Indicator benchmark failed: {:?}", e);
    // }

    println!("\n🏁 [CLIENT] Sending completion marker to server...");

    // Calculate total messages sent (just 1 for minimal test)
    let total_messages = 1; // Just the INFO message
    let test_complete = TestComplete {
        total_messages_sent: total_messages as u32,
        test_duration_ms: all_tests_duration.as_millis() as u64,
    };

    logger_ref.tell(test_complete).send().await?;
    println!("🏁 [CLIENT] Sent completion marker to server");

    // Wait for server→client response (reduced from 15s since messages are fast)
    // TODO remove this wait, it should be automatically handled via distributed_actor handler??
    println!("\n⏳ [CLIENT] Waiting for server to send bidirectional response...");
    println!("   (This tests server → client messaging)");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    println!("\n🔍 [CLIENT] Check above for any received messages from server");

    Ok(())
}

// Real indicator output types from trading-ta (simplified for benchmark)
#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IndicatorData {
    pub symbol: String,
    pub iteration: u64,
    pub ema_outputs: Vec<EmaIndicator>,
    pub delta_vix_outputs: Vec<DeltaVixIndicator>,
    pub supertrend1_outputs: Vec<SupertrendIndicator>,
    pub supertrend2_outputs: Vec<SupertrendIndicator>,
}

#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct EmaIndicator {
    pub value: f64,
}

#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct DeltaVixIndicator {
    pub dvix: f64,
    pub dvixema: f64,
    pub top_signal: bool,
    pub bottom_signal: bool,
}

#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct SupertrendIndicator {
    pub up: f64,
    pub dn: f64,
    pub trend: i8,
    pub value: f64,
    pub buy_signal: bool,
    pub sell_signal: bool,
}

// === COMMENTED OUT - Benchmark function ===
// async fn run_indicator_benchmark(
//     logger_ref: &DistributedActorRef,
// ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//     ... (all benchmark code commented out for minimal test)
// }
