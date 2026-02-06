//! Concrete actor tell server
//!
//! Run this first:
//! cargo run --example tell_concrete_server --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::{DistributedActorRef, transport::RemoteTransport};

// Import shared message definitions - same file as client!
mod tell_messages;
use tell_messages::*;

#[derive(Debug)]
pub struct ClientActor;
impl Actor for ClientActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        unimplemented!()
    }
}

// Concrete actor - logger that receives log messages
struct LoggerActor {
    message_count: u32,
    client_ref: Option<DistributedActorRef<ClientActor>>,
    test_completed: bool,
    _server_batch_sent: bool,
}

// Separate actor for TellConcrete messages
#[derive(Debug)]
#[allow(dead_code)]
struct ConcreteActor {
    count: u32,
}

impl Actor for LoggerActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            message_count: 0,
            client_ref: None,
            test_completed: false,
            _server_batch_sent: false,
        })
    }
}

// Message for setting client reference
#[derive(Clone)]
pub struct SetClientRef {
    pub client_ref: DistributedActorRef<ClientActor>,
}

impl Message<SetClientRef> for LoggerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SetClientRef,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("🔗 [SERVER] Client reference set for bidirectional messaging!");
        self.client_ref = Some(msg.client_ref);
    }
}

impl Message<LogMessage> for LoggerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: LogMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.message_count += 1;
        // LOG MESSAGE FOR TEST - ADD VERY VISIBLE DEBUG OUTPUT
        println!(
            "🚨🚨🚨 [SERVER] RECEIVED LogMessage #{}: {} - {} 🚨🚨🚨",
            self.message_count, msg.level, msg.content
        );
        println!("🔥 [SERVER] Message handler is working! Sending response back to client...");

        // BIDIRECTIONAL: Send single response back to client!
        if let Err(e) = self.send_single_response_to_client().await {
            println!("❌ [SERVER] Failed to send response to client: {:?}", e);
        } else {
            println!("✅ [SERVER] Successfully sent response back to client!");
        }
    }
}

impl Message<GetCountMessage> for LoggerActor {
    type Reply = CountResponse;

    async fn handle(
        &mut self,
        _msg: GetCountMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        CountResponse {
            count: self.message_count,
        }
    }
}

// Handler method for distributed actor - now accepts archived types for zero-copy
impl LoggerActor {
    #[allow(dead_code)]
    async fn handle_log(&mut self, msg: &rkyv::Archived<LogMessage>) {
        self.message_count += 1;
        // Access fields directly from archived type - no deserialization!
        let level = &msg.level;
        let content = &msg.content;

        // LOG MESSAGE FOR MINIMAL TEST
        println!(
            "📝 [SERVER] Received LogMessage #{}: {} - {}",
            self.message_count, level, content
        );

        // BIDIRECTIONAL: Send single response back to client!
        if let Err(e) = self.send_single_response_to_client().await {
            println!("❌ [SERVER] Failed to send response to client: {:?}", e);
        }
    }

    #[allow(dead_code)]
    async fn handle_tell_concrete(&mut self, msg: &rkyv::Archived<TellConcrete>) {
        self.message_count += 1;
        let data_len = msg.data.len();

        // Silent processing for TellConcrete messages

        // Special handling for our test messages
        if msg.id == 999999 {
            println!(
                "   ✅ Successfully received 5MB test message! (actual size: {} bytes)",
                data_len
            );
        } else if msg.id == 999998 {
            println!(
                "   ✅ Successfully received 35MB test message! (actual size: {} bytes)",
                data_len
            );
        } else if data_len > 1024 {
            println!(
                "📦 Received large message {} with {} bytes",
                msg.id, data_len
            );
        }

        // === COMMENTED OUT - TellConcrete responses ===
        // BIDIRECTIONAL: Send response back to client for TellConcrete messages too!
        // if let Err(e) = self.send_response_to_client().await {
        //     println!(
        //         "❌ [SERVER] Failed to send TellConcrete response to client: {:?}",
        //         e
        //     );
        // }
    }

    // === COMMENTED OUT - Indicator handling ===
    // Handle IndicatorData messages (from benchmark)
    // async fn handle_indicator(&mut self, _archived: &rkyv::Archived<IndicatorData>) {
    //     // Just count them, don't print each one
    //     self.message_count += 1;
    //     if self.message_count % 100 == 0 {
    //         println!("📊 Received {} indicator messages", self.message_count);
    //     }
    // }

    // BIDIRECTIONAL: Send single response back to client
    async fn send_single_response_to_client(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Only do expensive lookup if we don't have a client ref yet
        if self.client_ref.is_none() {
            match DistributedActorRef::<ClientActor>::lookup("client").await {
                Ok(Some(client_ref)) => {
                    println!(
                        "✅ [SERVER] Found client on-demand! Setting up bidirectional messaging..."
                    );
                    self.client_ref = Some(client_ref);
                }
                Ok(None) => {
                    // Client not registered yet - avoid logging to prevent spam
                    println!("⚠️  [SERVER] Client actor not found yet - cannot send response");
                    return Ok(());
                }
                Err(e) => {
                    println!("❌ [SERVER] Error looking up client: {:?}", e);
                    return Ok(());
                }
            }
        }

        if let Some(client_ref) = &self.client_ref {
            let response = ServerResponse {
                message_id: self.message_count,
                response_data: format!(
                    "Server processed message #{} - bidirectional test working!",
                    self.message_count
                ),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .to_string(),
            };

            client_ref.tell(response).send().await?;
            println!(
                "📤 [SERVER] Sent response #{} to client (server → client test)",
                self.message_count
            );
        } else {
            println!("⚠️  [SERVER] Cannot send response - no client reference available");
        }

        Ok(())
    }

    // Handle TestComplete message - signals end of client→server phase
    #[allow(dead_code)]
    async fn handle_test_complete(&mut self, msg: &rkyv::Archived<TestComplete>) {
        println!(
            "🏁 [SERVER] Client completed! Sent {} messages in {}ms",
            msg.total_messages_sent, msg.test_duration_ms
        );

        self.test_completed = true;

        // === COMMENTED OUT - Server batch ===
        // Trigger server→client batch
        // if let Err(e) = self.send_server_batch().await {
        //     println!("❌ [SERVER] Failed to send server batch: {:?}", e);
        // }

        println!("✅ [SERVER] Minimal bidirectional test complete!");
    }
}

// Implement Message trait for TestComplete - required for distributed_actor! macro
impl Message<TestComplete> for LoggerActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: TestComplete,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!(
            "🚨🚨🚨 [SERVER] RECEIVED TestComplete! Client sent {} messages in {}ms 🚨🚨🚨",
            msg.total_messages_sent, msg.test_duration_ms
        );
        println!("🔥 [SERVER] TestComplete handler is working!");

        self.test_completed = true;

        // === COMMENTED OUT - Server batch ===
        // Trigger server→client batch
        // if let Err(e) = self.send_server_batch().await {
        //     println!("❌ [SERVER] Failed to send server batch: {:?}", e);
        // }

        println!("✅ [SERVER] Minimal bidirectional test complete!");
    }
}

impl LoggerActor {
    // === COMMENTED OUT - Server batch messages ===
    // Send server→client batch messages
    // async fn send_server_batch(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //     if self.server_batch_sent {
    //         return Ok(()); // Already sent, don't send again
    //     }
    //
    //     if let Some(client_ref) = &self.client_ref {
    //         println!("📤 [SERVER] Starting server→client batch (1000 benchmark messages)...");
    //
    //         // Send 1000 benchmark messages
    //         for i in 0..1000 {
    //             let msg = ServerBenchmark {
    //                 sequence: i,
    //                 payload: vec![42u8; 1024], // 1KB payload
    //                 timestamp: std::time::SystemTime::now()
    //                     .duration_since(std::time::UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_millis() as u64,
    //             };
    //
    //             client_ref.tell(msg).send().await?;
    //
    //             // Log progress every 100 messages
    //             if (i + 1) % 100 == 0 {
    //                 println!("📤 [SERVER] Sent {} benchmark messages", i + 1);
    //             }
    //         }
    //
    //         self.server_batch_sent = true;
    //         println!("✅ [SERVER] Sent all 1000 benchmark messages to client");
    //     } else {
    //         println!("⚠️  [SERVER] Cannot send server batch - no client reference");
    //     }
    //
    //     Ok(())
    // }
}

// Import IndicatorData type from client
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

// Register with distributed actor macro for LoggerActor
distributed_actor! {
    LoggerActor {
        LogMessage,
        GetCountMessage,
        TestComplete,
    }
}

// Note: TellConcrete and IndicatorData are handled by ConcreteActor, not LoggerActor

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Ensure the rustls CryptoProvider is installed (required for TLS)
    kameo_remote::tls::ensure_crypto_provider();

    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    println!("\n🚀 === CONCRETE ACTOR TELL SERVER (WITH TLS) ===");

    // For production, you would load keypair from secure storage or generate and save it
    // Here we use a deterministic keypair for the example (using seed-based generation)
    let server_keypair = {
        // In production: load from file or use proper key management
        // For this example, we'll use a fixed seed for reproducibility
        kameo_remote::KeyPair::new_for_testing("tls_server_production_key")
    };
    println!("🔐 Server using Ed25519 keypair for TLS encryption");
    println!("⚠️  Note: In production, use properly generated and stored keypairs");

    // Bootstrap on port 9310 with TLS enabled using keypair
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9310".parse()?,
        server_keypair,
    )
    .await?;
    println!(
        "✅ Server listening on {} with TLS encryption",
        transport.local_addr()
    );

    // Create and register LoggerActor
    let actor_ref = <LoggerActor as Actor>::spawn(());
    let actor_id = actor_ref.id();

    // Use sync registration to wait for peer confirmation (eliminates need for sleep delays)
    transport
        .register_distributed_actor_sync(
            "logger".to_string(),
            &actor_ref,
            std::time::Duration::from_secs(1),
        )
        .await?;

    println!("✅ LoggerActor registered with ID {:?}", actor_id);

    // Debug: Print the type hash being used
    use kameo::remote::type_hash::HasTypeHash;
    println!(
        "🔍 Server LogMessage type hash: {:08x}",
        <LogMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!(
        "🔍 Server TellConcrete type hash: {:08x}",
        <TellConcrete as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!(
        "🔍 Server IndicatorData type hash: {:08x}",
        <IndicatorData as HasTypeHash>::TYPE_HASH.as_u32()
    );

    // Add the client as a trusted peer for TLS authentication
    if let Some(handle) = transport.handle() {
        // The client will use this same keypair name, so we add it as a trusted peer
        let client_peer_id =
            kameo_remote::KeyPair::new_for_testing("tls_client_production_key").peer_id();
        let _peer = handle.add_peer(&client_peer_id).await;
        println!(
            "✅ Added client as trusted peer for TLS: {}",
            client_peer_id
        );
    }

    println!("📡 Server ready for TLS-encrypted connections from trusted clients");

    println!("\n📡 Server ready. Run client with:");
    println!("   cargo run --example tell_concrete_client --features remote");
    println!("\n💤 Server will run until you press Ctrl+C...\n");

    // Clone actor_ref for client lookup task
    let lookup_actor_ref = actor_ref.clone();

    // Continuous client connection task - attempt to connect to client every 1 second
    let transport_cloned = transport.clone();
    tokio::spawn(async move {
        let mut retry_count = 0;
        loop {
            retry_count += 1;

            // Try to connect to client
            if let Some(handle) = transport_cloned.handle() {
                let client_peer_id =
                    kameo_remote::KeyPair::new_for_testing("tls_client_production_key").peer_id();
                let peer = handle.add_peer(&client_peer_id).await;
                match peer.connect(&"127.0.0.1:9311".parse().unwrap()).await {
                    Ok(_) => {
                        println!("✅ [SERVER] Connected to client!");
                        // Connection established, now try to lookup client actor
                        match DistributedActorRef::<ClientActor>::lookup("client").await {
                            Ok(Some(client_ref)) => {
                                println!(
                                    "✅ [SERVER] Found client! Setting up bidirectional messaging..."
                                );
                                if let Err(e) = lookup_actor_ref
                                    .tell(SetClientRef { client_ref })
                                    .send()
                                    .await
                                {
                                    println!("❌ [SERVER] Failed to set client reference: {:?}", e);
                                } else {
                                    println!("🎉 [SERVER] Bidirectional messaging ready!");
                                    break; // Stop looking once we find the client
                                }
                            }
                            Ok(None) => {
                                println!(
                                    "⏳ [SERVER] Client connected but actor not registered yet, will retry..."
                                );
                            }
                            Err(e) => {
                                println!("❌ [SERVER] Error looking up client: {:?}", e);
                            }
                        }
                    }
                    Err(_) => {
                        // Only log periodically to avoid spam
                        if retry_count == 1 || retry_count % 30 == 0 {
                            println!(
                                "🔍 [SERVER] Attempting to lookup client for bidirectional messaging (attempt #{})...",
                                retry_count
                            );
                            println!(
                                "🔐 [SERVER] Attempting to establish TLS connection to client at 127.0.0.1:9311..."
                            );
                            println!(
                                "⚠️  [SERVER] Could not connect to client yet: Connection refused"
                            );
                            println!("⏳ [SERVER] Client not found yet, will retry...");
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    // Clone actor_ref for signal handler
    let signal_actor_ref = actor_ref.clone();

    // Set up Ctrl+C handler
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("\n\n📊 === SERVER SUMMARY ===");

        // Send final message count request using TELL (fire-and-forget)
        let _ = signal_actor_ref.tell(GetCountMessage).send().await;
        println!("📩 Final message count request sent via TELL");

        std::process::exit(0);
    });

    // Keep server running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
