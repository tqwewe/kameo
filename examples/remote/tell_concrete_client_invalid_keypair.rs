//! IDENTICAL to tell_concrete_client but with INVALID keypair (seed 99 instead of 43)
//!
//! This test uses the EXACT same code as tell_concrete_client.rs but with
//! a different keypair seed (99 instead of 43). This demonstrates what happens 
//! when an unauthorized client with wrong cryptographic identity tries to connect.
//!
//! ## Test Scenario:
//! - Server expects client with seed 43 (PeerId: 1ba66c6751506850ae0787244c69476b6d45fb857a914a5a0445a24253f7b810)
//! - This client uses seed 99 (PeerId: d523845a249f6994b019cbb33057d352237858ff79a98cb2359d805ee45044d6)
//! - Everything else is IDENTICAL to the real client
//!
//! ## Expected Behavior:
//! The system should detect the wrong cryptographic identity and reject the client.
//!
//! Run after starting the server:
//! cargo run --example tell_concrete_client_invalid_keypair --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::{transport::RemoteTransport, DynamicDistributedActorRef};

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
impl ClientActor {
    async fn handle_server_response(&mut self, msg: &rkyv::Archived<ServerResponse>) {
        self.responses_received += 1;
        // Silent response handling - only log significant milestones
        if self.responses_received == 1 || (self.responses_received > 0 && self.responses_received % 50 == 0) {
            println!("📨 [CLIENT] Received {} responses from server", self.responses_received);
        }
    }
}

// Register with distributed actor macro for bidirectional communication
distributed_actor! {
    ClientActor {
        ServerResponse => handle_server_response,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    println!("\n🚀 === CONCRETE ACTOR TELL CLIENT WITH INVALID KEYPAIR ===");
    println!("⚠️  WARNING: This client uses WRONG cryptographic identity!");
    println!("    Server expects seed 43, but this client uses seed 99.\n");

    // Bootstrap on port 9311 with WRONG cryptographic keypair
    println!("🔑 Setting up client with WRONG cryptographic identity...");
    
    // DIFFERENCE: Use seed 99 instead of 43 (what server expects)
    let client_keypair = kameo_remote::KeyPair::new_for_testing("99"); // WRONG seed
    let client_peer_id = client_keypair.peer_id();
    println!("   Client PeerId: {}", client_peer_id);
    println!("   Expected PeerId: 1ba66c6751506850ae0787244c69476b6d45fb857a914a5a0445a24253f7b810");
    println!("   🚨 MISMATCH: Client identity does not match server expectation");
    
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair("127.0.0.1:9311".parse()?, client_keypair).await?;
    println!("✅ Client listening on {} with WRONG crypto identity", transport.local_addr());

    // Connect to server using server's expected PeerId (IDENTICAL to real client code)
    println!("\n📡 Connecting to server at 127.0.0.1:9310...");
    let server_keypair = kameo_remote::KeyPair::new_for_testing("42"); // Must match server
    let server_peer_id = server_keypair.peer_id();
    println!("   Connecting to server PeerId: {}", server_peer_id);
    
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("⚠️  Connected to server with WRONG cryptographic identity!");
                println!("    This may indicate a security vulnerability.");
            }
            Err(e) => {
                println!("✅ Connection properly rejected: {}", e);
                if e.to_string().contains("Invalid peer public key") {
                    println!("   🔒 Cryptographic validation rejected the connection");
                } else if e.to_string().contains("AuthenticationFailed") {
                    println!("   🚫 Authentication failed - unauthorized client");
                } else {
                    println!("   ℹ️  Connection error: {}", e);
                }
                return Err(e.into());
            }
        }
    }

    // Wait for connection to stabilize (IDENTICAL to real client code)
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Create and register ClientActor for bidirectional communication (IDENTICAL to real client code)
    println!("\n🎬 Creating ClientActor to receive server responses...");
    let client_actor_ref = <ClientActor as Actor>::spawn(());
    let client_actor_id = client_actor_ref.id();

    transport
        .register_actor("client".to_string(), client_actor_id)
        .await?;
    println!(
        "✅ ClientActor registered as 'client' with ID {:?}",
        client_actor_id
    );

    // Look up remote actor (IDENTICAL to real client code)
    println!("\n🔍 Looking up remote LoggerActor...");
    let logger_ref = match DynamicDistributedActorRef::lookup("logger", transport.clone()).await? {
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

    // Send tell messages (IDENTICAL to real client code)
    println!("\n📤 Sending tell messages to remote actor...");
    let all_tests_start = std::time::Instant::now();

    // Test 1: Send INFO message (IDENTICAL to real client code)
    println!("\n🧪 Test 1: Sending INFO log");

    // Debug: Print the type hash being used (IDENTICAL to real client code)
    use kameo::remote::type_hash::HasTypeHash;
    println!(
        "🔍 LogMessage type hash: {:08x}",
        <LogMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );

    let start = std::time::Instant::now();
    logger_ref
        .tell(LogMessage {
            level: "INFO".to_string(),
            content: "SECURITY TEST: Application started with WRONG keypair".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("⚠️  Sent INFO message in {:?} - message accepted!", duration);

    // Test 2: Send WARNING message (IDENTICAL to real client code)
    println!("\n🧪 Test 2: Sending WARNING log");
    let start = std::time::Instant::now();
    logger_ref
        .tell(LogMessage {
            level: "WARNING".to_string(),
            content: "SECURITY TEST: High memory usage detected with WRONG keypair".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("⚠️  Sent WARNING message in {:?} - message accepted!", duration);

    // Test 3: Send ERROR message (IDENTICAL to real client code)
    println!("\n🧪 Test 3: Sending ERROR log");
    let start = std::time::Instant::now();
    logger_ref
        .tell(LogMessage {
            level: "ERROR".to_string(),
            content: "SECURITY TEST: Failed to connect to database with WRONG keypair".to_string(),
        })
        .send()
        .await?;
    let duration = start.elapsed();
    println!("⚠️  Sent ERROR message in {:?} - message accepted!", duration);

    // Test 4: Send multiple messages quickly (IDENTICAL to real client code)
    println!("\n🧪 Test 4: Sending 5 messages quickly");
    let batch_start = std::time::Instant::now();
    let mut individual_times = Vec::new();
    for i in 1..=5 {
        let start = std::time::Instant::now();
        logger_ref
            .tell(LogMessage {
                level: "DEBUG".to_string(),
                content: format!("SECURITY TEST: Debug message #{} with WRONG keypair", i),
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
        "⚠️  Sent 5 DEBUG messages in {:?} (avg: {:?}/message) - all accepted!",
        batch_duration, avg_duration
    );

    // Wait for messages to be processed (IDENTICAL to real client code)
    println!("\n⏳ Waiting for messages to be processed...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let all_tests_duration = all_tests_start.elapsed();
    println!("\n🚨 SECURITY ALERT: WRONG keypair completed all message sending!");
    println!("⚠️  This indicates the security system did not detect the keypair mismatch.");
    println!("⏱️  Total time for all tests with WRONG keypair: {:?}", all_tests_duration);
    println!("\n🔍 Check server logs to see if messages were received and processed.");
    println!("    If messages were processed, this shows a potential security gap.");

    Ok(())
}
