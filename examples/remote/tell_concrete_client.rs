//! Concrete actor tell client (using newtDistributedActorRef API)
//!
//! This demonstrates the zero-cost abstraction approach where each message type
//! gets its own monomorphized code path for maximum performance.
//!
//! Run after starting the server:
//! cargo run --example tell_concrete_client --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import shared message definitions - defined once at compile time!
mod tell_messages;
use tell_messages::*;

// Client actor that can receive messages from server
struct ClientActor {
    responses_received: u32,
}

impl Actor for ClientActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🎬 ClientActor started and ready to receive server responses!");
        Ok(Self {
            responses_received: 0,
        })
    }
}

// Handler for ServerResponse messages from server
use kameo::message::{Message, Context};

impl Message<ServerResponse> for ClientActor {
    type Reply = ();
    
    async fn handle(&mut self, _msg: ServerResponse, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.responses_received += 1;
        // Silent response handling - only log significant milestones
        if self.responses_received == 1
            || (self.responses_received > 0 && self.responses_received % 50 == 0)
        {
            println!(
                "📨 [CLIENT] Received {} responses from server",
                self.responses_received
            );
        }
    }
}

// Register with distributed actor macro for bidirectional communication
distributed_actor! {
    ClientActor {
        ServerResponse,
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

    // Wait for connection to stabilize
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Create and register ClientActor for bidirectional communication
    println!("\n🎬 Creating ClientActor to receive server responses...");
    let client_actor_ref = ClientActor::spawn(());
    let client_actor_id = client_actor_ref.id();

    // Register the message types this client actor handles
    // ClientActor::__register_message_types(client_actor_ref.clone());

    transport
        .register_actor("client".to_string(), client_actor_id)
        .await?;
    println!(
        "✅ ClientActor registered as 'client' with ID {:?}",
        client_actor_id
    );

    // Wait for gossip to propagate the actor registration from server
    println!("⏳ Waiting for gossip to propagate actor registration...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

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

    // Send tell messages
    println!("\n📤 Sending tell messages to remote actor...");
    let all_tests_start = std::time::Instant::now();

    // Test 1: Send INFO message
    println!("\n🧪 Test 1: Sending INFO log");

    // Debug: Print the type hash being used
    use kameo::remote::type_hash::HasTypeHash;
    println!(
        "🔍 LogMessage type hash: {:08x}",
        <LogMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!(
        "🔍 TellConcrete type hash: {:08x}",
        <TellConcrete as HasTypeHash>::TYPE_HASH.as_u32()
    );

    let start = std::time::Instant::now();
    logger_ref
        .tell(LogMessage {
            level: "INFO".to_string(),
            content: "Application started".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("✅ Sent INFO message in {:?}", duration);

    // Test 2: Send WARNING message
    println!("\n🧪 Test 2: Sending WARNING log");
    let start = std::time::Instant::now();
    logger_ref
        .tell(LogMessage {
            level: "WARNING".to_string(),
            content: "High memory usage detected".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("✅ Sent WARNING message in {:?}", duration);

    // Test 3: Send ERROR message
    println!("\n🧪 Test 3: Sending ERROR log");
    let start = std::time::Instant::now();
    logger_ref
        .tell(LogMessage {
            level: "ERROR".to_string(),
            content: "Failed to connect to database".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("✅ Sent ERROR message in {:?}", duration);

    // Test 4: Send multiple messages quickly
    println!("\n🧪 Test 4: Sending 5 messages quickly");
    let batch_start = std::time::Instant::now();
    let mut individual_times = Vec::new();
    for i in 1..=5 {
        let start = std::time::Instant::now();
        logger_ref
            .tell(LogMessage {
                level: "DEBUG".to_string(),
                content: format!("Debug message #{}", i),
            })
            .send()
            .await?;
        let duration = start.elapsed();
        individual_times.push(duration);
        println!("   Message {} sent in {:?}", i, duration);
    }
    let batch_duration = batch_start.elapsed();
    let avg_duration =
        individual_times.iter().sum::<std::time::Duration>() / individual_times.len() as u32;
    println!(
        "✅ Sent 5 DEBUG messages in {:?} (avg: {:?}/message)",
        batch_duration, avg_duration
    );

    // Test 5: Send 100 messages to measure throughput
    println!("\n🧪 Test 5: Sending 100 messages for throughput test");
    let throughput_start = std::time::Instant::now();
    for i in 1..=100 {
        logger_ref
            .tell(LogMessage {
                level: "TRACE".to_string(),
                content: format!("Throughput test message #{}", i),
            })
            .send()
            .await?;
    }
    let throughput_duration = throughput_start.elapsed();
    let messages_per_second = 100.0 / throughput_duration.as_secs_f64();
    println!(
        "✅ Sent 100 messages in {:?} ({:.2} messages/second)",
        throughput_duration, messages_per_second
    );

    // CRITICAL: Give background writer time to actually send messages!
    // The send() method returns immediately but messages are in a queue
    println!("\n⏳ Waiting for background writer to flush messages...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let all_tests_duration = all_tests_start.elapsed();
    println!("\n🎉 All tests passed! Check the server output for the logged messages.");
    println!("⏱️  Total time for all tests: {:?}", all_tests_duration);

    // === REALISTIC INDICATOR MESSAGE BENCHMARK ===
    println!("\n🚀 === REALISTIC INDICATOR MESSAGE BENCHMARK ===");
    println!("Testing zero-copy performance with real indicator data from trading-ta\n");

    if let Err(e) = run_indicator_benchmark(&logger_ref).await {
        println!("❌ Indicator benchmark failed: {:?}", e);
    }

    Ok(())
}

use std::time::Instant;

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

async fn run_indicator_benchmark(
    logger_ref: &DistributedActorRef,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_configs = vec![
        (100, 5, "Small indicators (100 candles)"), // ~10KB per message
        (500, 3, "Medium indicators (500 candles)"), // ~50KB per message
        (1000, 2, "Large indicators (1000 candles)"), // ~100KB per message
    ];

    for (num_candles, num_messages, test_name) in test_configs {
        println!(
            "\n📊 {} - {} candles/message, {} messages",
            test_name, num_candles, num_messages
        );

        // Generate realistic indicator data
        let mut ema_outputs = Vec::with_capacity(num_candles);
        let mut delta_vix_outputs = Vec::with_capacity(num_candles);
        let mut supertrend1_outputs = Vec::with_capacity(num_candles);
        let mut supertrend2_outputs = Vec::with_capacity(num_candles);

        for i in 0..num_candles {
            // EMA (typical moving average)
            ema_outputs.push(EmaIndicator {
                value: 50000.0 + (i as f64 * 0.1),
            });

            // Delta VIX (volatility-based signals)
            delta_vix_outputs.push(DeltaVixIndicator {
                dvix: 0.001 * (i as f64).sin(),
                dvixema: 20.0 + (i as f64 * 0.01).cos(),
                top_signal: i % 30 == 0,
                bottom_signal: i % 25 == 0,
            });

            // Supertrend 1 (trend following)
            supertrend1_outputs.push(SupertrendIndicator {
                up: 50500.0 + (i as f64 * 0.15),
                dn: 49500.0 + (i as f64 * 0.15),
                trend: if i % 20 < 10 { 1 } else { -1 },
                value: 49500.0 + (i as f64 * 0.15),
                buy_signal: i % 40 == 0,
                sell_signal: i % 35 == 0,
            });

            // Supertrend 2 (different parameters)
            supertrend2_outputs.push(SupertrendIndicator {
                up: 51000.0 + (i as f64 * 0.2),
                dn: 49000.0 + (i as f64 * 0.2),
                trend: if i % 30 < 15 { 1 } else { -1 },
                value: 49000.0 + (i as f64 * 0.2),
                buy_signal: i % 50 == 0,
                sell_signal: i % 45 == 0,
            });
        }

        // Create sample message to measure size
        let sample_msg = IndicatorData {
            symbol: "BTCUSDT".to_string(),
            iteration: 0,
            ema_outputs: ema_outputs.clone(),
            delta_vix_outputs: delta_vix_outputs.clone(),
            supertrend1_outputs: supertrend1_outputs.clone(),
            supertrend2_outputs: supertrend2_outputs.clone(),
        };

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&sample_msg)?;
        println!(
            "  Message size: {} bytes ({:.2} KB)",
            serialized.len(),
            serialized.len() as f64 / 1024.0
        );
        println!(
            "  Total data: {:.2} MB",
            (serialized.len() * num_messages) as f64 / 1_048_576.0
        );

        // Benchmark sending
        println!("  Running benchmark...");
        let bench_start = Instant::now();
        let mut send_times = Vec::with_capacity(num_messages);

        for i in 0..num_messages {
            let msg = IndicatorData {
                symbol: "BTCUSDT".to_string(),
                iteration: i as u64,
                ema_outputs: ema_outputs.clone(),
                delta_vix_outputs: delta_vix_outputs.clone(),
                supertrend1_outputs: supertrend1_outputs.clone(),
                supertrend2_outputs: supertrend2_outputs.clone(),
            };

            let send_start = Instant::now();
            logger_ref.tell(msg).send().await?;
            send_times.push(send_start.elapsed());
        }

        let total_duration = bench_start.elapsed();

        // Calculate statistics
        send_times.sort();
        let avg = send_times.iter().sum::<std::time::Duration>() / send_times.len() as u32;
        let p95 = send_times[send_times.len() * 95 / 100];

        let messages_per_second = num_messages as f64 / total_duration.as_secs_f64();
        let mbps =
            (serialized.len() * num_messages) as f64 / 1_048_576.0 / total_duration.as_secs_f64();

        println!("  ✅ Results:");
        println!("     Total time: {:?}", total_duration);
        println!(
            "     Throughput: {:.2} messages/second",
            messages_per_second
        );
        println!("     Bandwidth: {:.2} MB/s", mbps);
        println!("     Average latency: {:?}", avg);
        println!("     P95 latency: {:?}", p95);
    }

    println!("\n🏁 Indicator benchmark complete!");
    println!("\nThese results show zero-copy performance with realistic indicator data sizes.");

    // WAIT FOR BIDIRECTIONAL MESSAGING - Server needs time to lookup client and send responses
    println!("\n⏳ [CLIENT] Waiting for server to find client and send bidirectional responses...");
    println!("   (Server does lookup every 3 seconds, so we'll wait 15 seconds)");
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    // First test with a small TellConcrete message
    println!("\n🧪 Testing TellConcrete messages...");

    // Small message first
    let small_msg = TellConcrete {
        id: 123,
        data: vec![1, 2, 3, 4, 5],
    };
    println!("   Sending small TellConcrete message (5 bytes)...");
    logger_ref.tell(small_msg).send().await?;
    println!("   ✅ Sent small message");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Test large message handling (larger than buffer size)
    println!("\n🧪 Testing large message handling (>4KB buffer)...");

    // Create a message larger than the 4KB buffer
    let large_data_size = 1024 * 1024 * 5; // 5MB
    let large_data = vec![42u8; large_data_size];
    let large_msg = TellConcrete {
        id: 999999,
        data: large_data,
    };

    println!("   Sending 5MB TellConcrete message (id: 999999)...");
    println!("   Using actor ref: {:?}", logger_ref.id());
    let send_start = Instant::now();
    logger_ref.tell(large_msg).send().await?;
    let send_time = send_start.elapsed();
    println!("   ✅ Successfully sent 5MB message in {:?}", send_time);

    // Test even larger message (35MB like the PreBacktest)
    println!("\n   Testing 35MB message (like PreBacktest)...");
    let huge_data_size = 1024 * 1024 * 35; // 35MB
    let huge_data = vec![99u8; huge_data_size];
    let huge_msg = TellConcrete {
        id: 999998,
        data: huge_data,
    };

    println!("   Sending 35MB message...");
    let send_start = Instant::now();
    logger_ref.tell(huge_msg).send().await?;
    let send_time = send_start.elapsed();
    println!("   ✅ Successfully sent 35MB message in {:?}", send_time);

    // Give server time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(())
}
