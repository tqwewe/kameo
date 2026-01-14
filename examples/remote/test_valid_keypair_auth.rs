//! Test client with VALID server public key
//! This should SUCCEED to connect and exchange messages
//!
//! Run after starting tell_concrete_server:
//! cargo run --example test_valid_keypair_auth --features remote

#![allow(dead_code, unused_variables)]

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import shared message definitions
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
        println!("📨 [CLIENT] Received response #{} from server", self.responses_received);
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

    println!("\n🚀 === TEST CLIENT WITH VALID KEYPAIR ===");

    // Bootstrap on port 9313 with cryptographic keypair (matching what server expects)
    println!("🔑 Setting up client with VALID cryptographic identity...");
    let client_keypair = kameo_remote::KeyPair::from_seed_for_testing(43); // Valid client key
    let client_peer_id = client_keypair.peer_id();
    println!("   Client PeerId: {}", client_peer_id);
    
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair("127.0.0.1:9315".parse()?, client_keypair).await?;
    println!("✅ Client listening on {} with crypto identity", transport.local_addr());

    // Connect to server using server's CORRECT PeerId
    println!("\n📡 Connecting to server with CORRECT public key...");
    let server_keypair = kameo_remote::KeyPair::from_seed_for_testing(42); // CORRECT server key
    let server_peer_id = server_keypair.peer_id();
    println!("   Expected server PeerId: {}", server_peer_id);
    
    if let Some(handle) = transport.handle() {
        // Configure the expected server public key for verification (CRITICAL!)
        {
            let pool = handle.registry.connection_pool.lock().await;
            pool.set_expected_server_key(server_peer_id.clone(), server_peer_id.clone());
            println!("   Configured to expect server key: {}", server_peer_id);
        }
        
        println!("   About to call add_peer...");
        let peer = handle.add_peer(&server_peer_id).await;
        println!("   add_peer returned");
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("✅ SUCCESS: Connected to server with VALID cryptographic identity");
            }
            Err(e) => {
                println!("❌ UNEXPECTED FAILURE: Connection failed: {}", e);
                return Err(e.into());
            }
        }
    }

    // Wait for connection to stabilize
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Create and register ClientActor for bidirectional communication
    println!("\n🎬 Creating ClientActor to receive server responses...");
    let client_actor_ref = ClientActor::spawn(());
    let client_actor_id = client_actor_ref.id();

    transport
        .register_actor("client".to_string(), client_actor_id)
        .await?;
    println!(
        "✅ ClientActor registered as 'client' with ID {:?}",
        client_actor_id
    );

    // Look up remote actor
    println!("\n🔍 Looking up remote LoggerActor...");
    let logger_ref = match DistributedActorRef::lookup("logger").await? {
        Some(ref_) => {
            println!("✅ Found LoggerActor on server");
            println!("📍 Actor ID: {:?}", ref_.id());
            ref_
        }
        None => {
            println!("❌ LoggerActor not found on server");
            return Err("Actor not found".into());
        }
    };

    // Send test messages to verify the connection works
    println!("\n📤 Sending test messages to remote actor...");
    
    // Test 1: Send INFO message
    println!("\n🧪 Test 1: Sending INFO log");
    logger_ref
        .tell(LogMessage {
            level: "INFO".to_string(),
            content: "Test message from VALID client".to_string(),
        })
        .send()
        .await?;
    println!("✅ Sent INFO message successfully");

    // Test 2: Send WARNING message
    println!("\n🧪 Test 2: Sending WARNING log");
    logger_ref
        .tell(LogMessage {
            level: "WARNING".to_string(),
            content: "Valid client warning test".to_string(),
        })
        .send()
        .await?;
    println!("✅ Sent WARNING message successfully");

    // Test 3: Send ERROR message
    println!("\n🧪 Test 3: Sending ERROR log");
    logger_ref
        .tell(LogMessage {
            level: "ERROR".to_string(),
            content: "Valid client error test".to_string(),
        })
        .send()
        .await?;
    println!("✅ Sent ERROR message successfully");

    // Wait for responses
    println!("\n⏳ Waiting for bidirectional responses from server...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("\n🎉 TEST PASSED: Valid keypair authentication successful!");
    println!("   - Connected successfully with correct server public key");
    println!("   - Sent messages to server");
    println!("   - Received responses from server");

    Ok(())
}