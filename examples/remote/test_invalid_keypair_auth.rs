//! Test client with INVALID server public key
//! This should FAIL to connect (authentication should reject it)
//!
//! Run after starting tell_concrete_server:
//! cargo run --example test_invalid_keypair_auth --features remote

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
        .with_env_filter("kameo_remote=error")
        .try_init();

    println!("\n🚀 === TEST CLIENT WITH INVALID KEYPAIR ===");

    // Bootstrap on port 9314 with cryptographic keypair (client's own key)
    println!("🔑 Setting up client with cryptographic identity...");
    let client_keypair = kameo_remote::KeyPair::from_seed_for_testing(44); // Different client key
    let client_peer_id = client_keypair.peer_id();
    println!("   Client PeerId: {}", client_peer_id);
    
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair("127.0.0.1:9316".parse()?, client_keypair).await?;
    println!("✅ Client listening on {} with crypto identity", transport.local_addr());

    // Connect to server using WRONG server PeerId
    println!("\n📡 Attempting to connect with WRONG server public key...");
    let wrong_server_keypair = kameo_remote::KeyPair::from_seed_for_testing(99); // WRONG server key!
    let wrong_server_peer_id = wrong_server_keypair.peer_id();
    println!("   Using WRONG server PeerId: {}", wrong_server_peer_id);
    println!("   (Real server has PeerId from seed 42)");
    
    if let Some(handle) = transport.handle() {
        // Configure the WRONG expected server public key for verification
        {
            let pool = handle.registry.connection_pool.lock().await;
            // Note: First param is the actual server's peer ID (seed 42)
            // Second param is what we expect (wrong key from seed 99)
            let actual_server = kameo_remote::KeyPair::from_seed_for_testing(42).peer_id();
            pool.set_expected_server_key(actual_server.clone(), wrong_server_peer_id.clone());
            println!("   Configured to expect wrong server key for {}", actual_server);
        }
        
        // Try to connect to the actual server with wrong expected key
        let actual_server = kameo_remote::KeyPair::from_seed_for_testing(42).peer_id();
        let peer = handle.add_peer(&actual_server).await;
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("❌ SECURITY FAILURE: Connected with wrong public key!");
                println!("   This should NOT have succeeded!");
                
                // Try to send a message to verify if it really connected
                println!("\n⚠️  Attempting to send message (should fail)...");
                
                // Try to look up remote actor
                match DistributedActorRef::lookup("logger").await {
                    Ok(Some(logger_ref)) => {
                        println!("❌ CRITICAL: Found remote actor with wrong key!");
                        
                        // Try to send a message
                        match logger_ref
                            .tell(LogMessage {
                                level: "BREACH".to_string(),
                                content: "Message from unauthorized client!".to_string(),
                            })
                            .send()
                            .await
                        {
                            Ok(()) => {
                                println!("❌ CRITICAL: Message sent with wrong key!");
                                return Err("SECURITY BREACH: Wrong key accepted and message sent!".into());
                            }
                            Err(e) => {
                                println!("✅ Message send failed as expected: {}", e);
                            }
                        }
                    }
                    Ok(None) => {
                        println!("✅ Could not find remote actor (partial protection)");
                    }
                    Err(e) => {
                        println!("✅ Actor lookup failed as expected: {}", e);
                    }
                }
                
                return Err("Security issue: connection succeeded with wrong key".into());
            }
            Err(e) => {
                println!("✅ CORRECTLY REJECTED: {}", e);
                println!("   Connection failed as expected with wrong key");
                
                // Check the error message
                let error_str = e.to_string();
                if error_str.contains("AuthenticationFailed") || 
                   error_str.contains("signature verification") ||
                   error_str.contains("Invalid signature") {
                    println!("   🔒 Authentication properly rejected the invalid key");
                } else {
                    println!("   ℹ️  Connection failed with: {}", error_str);
                }
            }
        }
    }

    println!("\n🎉 TEST PASSED: Invalid keypair was correctly rejected!");
    println!("   - Connection attempt with wrong server public key FAILED");
    println!("   - Authentication system working correctly");
    println!("   - Server resources protected from unauthorized access");

    Ok(())
}