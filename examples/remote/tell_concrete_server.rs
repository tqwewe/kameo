//! Concrete actor tell server
//!
//! Run this first:
//! cargo run --example tell_concrete_server --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import shared message definitions - same file as client!
mod tell_messages;
use tell_messages::*;

// Concrete actor - logger that receives log messages
struct LoggerActor {
    message_count: u32,
    client_ref: Option<DistributedActorRef>,
}

// Separate actor for TellConcrete messages
#[derive(Debug)]
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
        })
    }
}

// Message for setting client reference
#[derive(Clone)]
pub struct SetClientRef {
    pub client_ref: DistributedActorRef,
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
        // This is only used for local messages, not remote
        self.message_count += 1;
        println!("📨 Received local message: {} - {}", msg.level, msg.content);
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
    async fn handle_log(&mut self, msg: &rkyv::Archived<LogMessage>) {
        self.message_count += 1;
        // Access fields directly from archived type - no deserialization!
        let level = &msg.level;
        let content = &msg.content;

        // LOG EVERY MESSAGE TO DEBUG
        println!("📝 [SERVER] Received LogMessage #{}: {} - {}", 
                 self.message_count, level, content);

        // BIDIRECTIONAL: Send response back to client!
        if let Err(e) = self.send_response_to_client().await {
            println!("❌ [SERVER] Failed to send response to client: {:?}", e);
        }
    }

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

        // BIDIRECTIONAL: Send response back to client for TellConcrete messages too!
        if let Err(e) = self.send_response_to_client().await {
            println!(
                "❌ [SERVER] Failed to send TellConcrete response to client: {:?}",
                e
            );
        }
    }

    // Handle IndicatorData messages (from benchmark)
    async fn handle_indicator(&mut self, _archived: &rkyv::Archived<IndicatorData>) {
        // Just count them, don't print each one
        self.message_count += 1;
        if self.message_count % 100 == 0 {
            println!("📊 Received {} indicator messages", self.message_count);
        }
    }

    // BIDIRECTIONAL: Send response back to client using stored DistributedActorRef
    async fn send_response_to_client(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Only do expensive lookup if we don't have a client ref yet
        if self.client_ref.is_none() {
            match DistributedActorRef::lookup("client").await {
                Ok(Some(client_ref)) => {
                    println!("✅ [SERVER] Found client on-demand! Setting up bidirectional messaging...");
                    self.client_ref = Some(client_ref);
                }
                Ok(None) => {
                    // Client not registered yet - avoid logging to prevent spam
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
                response_data: format!("Server processed message #{}", self.message_count),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .to_string(),
            };

            client_ref.tell(response).send().await?;
            // Silent response sending - only log significant milestones
            if self.message_count == 1 || (self.message_count > 0 && self.message_count % 50 == 0) {
                println!("📤 [SERVER] Sent {} responses to client", self.message_count);
            }
        } else {
            // Client not found - this is normal during startup, no need to log
        }

        Ok(())
    }
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
        server_keypair
    ).await?;
    println!("✅ Server listening on {} with TLS encryption", transport.local_addr());

    // Create and register LoggerActor
    let actor_ref = LoggerActor::spawn(());
    let actor_id = actor_ref.id();

    // COMMENTED OUT: Auto-register message types when registering with transport
    // LoggerActor::__register_message_types(actor_ref.clone());

    transport
        .register_actor("logger".to_string(), actor_id)
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
        let client_peer_id = kameo_remote::PeerId::new("tls_client_production_key");
        let peer = handle
            .add_peer(&client_peer_id)
            .await;
        println!("✅ Added client as trusted peer for TLS: {}", client_peer_id);
    }
    
    println!("📡 Server ready for TLS-encrypted connections from trusted clients");

    println!("\n📡 Server ready. Run client with:");
    println!("   cargo run --example tell_concrete_client --features remote");
    println!("\n💤 Server will run until you press Ctrl+C...\n");

    // Clone actor_ref for client lookup task
    let lookup_actor_ref = actor_ref.clone();

    // Spawn task to periodically try to lookup client for bidirectional messaging
    let transport_cloned = transport.clone();
    tokio::spawn(async move {
        // Start looking immediately - the client will register with Immediate priority
        // which triggers instant gossip sync
        
        let mut retry_count = 0;
        let mut client_connected = false;
        loop {
            retry_count += 1;
            println!("🔍 [SERVER] Attempting to lookup client for bidirectional messaging (attempt #{})...", retry_count);

            // First, try to establish TLS connection to client if we haven't already
            if !client_connected {
                if let Some(handle) = transport_cloned.handle() {
                    // Try to connect to the client's listening port
                    println!("🔐 [SERVER] Attempting to establish TLS connection to client at 127.0.0.1:9311...");
                    let client_peer_id = kameo_remote::PeerId::new("tls_client_production_key");
                    let peer = handle.add_peer(&client_peer_id).await;
                    match peer.connect(&"127.0.0.1:9311".parse().unwrap()).await {
                        Ok(_) => {
                            println!("✅ [SERVER] TLS connection established with client!");
                            client_connected = true;
                        }
                        Err(e) => {
                            println!("⚠️  [SERVER] Could not connect to client yet: {:?}", e);
                            // Continue anyway, client might not be up yet
                        }
                    }
                }
            }

            match DistributedActorRef::lookup("client").await {
                Ok(Some(client_ref)) => {
                    println!("✅ [SERVER] Found client! Setting up bidirectional messaging...");
                    println!("   Client actor ID: {:?}", client_ref.id());

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
                    println!("⏳ [SERVER] Client not found yet, will retry...");
                    
                    // After a few attempts, suggest checking if client is running
                    if retry_count == 5 {
                        println!("💡 [SERVER] Still can't find client after {} attempts.", retry_count);
                        println!("   Make sure the client is running with:");
                        println!("   cargo run --example tell_concrete_client --features remote");
                    }
                }
                Err(e) => {
                    println!("❌ [SERVER] Error looking up client: {:?}", e);
                }
            }
            
            // Wait before next retry
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
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
