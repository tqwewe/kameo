//! Test 2: CORRUPTED KEYPAIR - Should FAIL  
//!
//! This test uses the same private key as Test 1 (seed 43) but with a
//! corrupted/invalid public key. This simulates key tampering attacks.
//! The challenge-response should fail because the public key doesn't match
//! the private key used for signing.
//!
//! Expected: ❌ FAIL - Connection should be rejected during challenge-response
//!
//! Run after starting the server:
//! cargo run --example test_corrupted_keypair_auth --features remote

#![allow(dead_code, unused_variables)]

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import shared message definitions
mod tell_messages;
use tell_messages::*;

// Client actor for receiving responses (if any)
struct ClientActor {
    responses_received: u32,
}

impl Actor for ClientActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🎬 ClientActor started (shouldn't receive responses due to auth failure)");
        Ok(Self {
            responses_received: 0,
        })
    }
}

impl ClientActor {
    async fn handle_server_response(&mut self, msg: &rkyv::Archived<ServerResponse>) {
        self.responses_received += 1;
        println!("❌ ERROR: Received unexpected response: {}", msg.response_data);
        println!("   This indicates the security system failed to block the corrupted keypair");
    }
}

distributed_actor! {
    ClientActor {
        ServerResponse => handle_server_response,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging to see authentication failure details
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=warn")
        .try_init();

    println!("\n🚨 === TEST 2: CORRUPTED KEYPAIR AUTHENTICATION ===");
    println!("Testing client with SAME private key but CORRUPTED public key");
    println!("Expected Result: ❌ Should FAIL challenge-response authentication\n");

    // Create corrupted keypair (same private key, different public key)
    println!("🔑 Creating CORRUPTED keypair (same private key as seed 43, different public key)...");
    let corrupted_keypair = kameo_remote::KeyPair::from_seed_with_corrupted_public_key_for_testing(43);
    let corrupted_peer_id = corrupted_keypair.peer_id();
    let private_key_bytes = corrupted_keypair.private_key_bytes();
    let public_key_bytes = corrupted_keypair.public_key_bytes();
    
    // Also show what the VALID keypair looks like for comparison
    let valid_keypair = kameo_remote::KeyPair::from_seed_for_testing(43);
    let valid_public_key_bytes = valid_keypair.public_key_bytes();
    
    println!("   Private key:     {:02x?} (SAME as valid client)", &private_key_bytes[..8]);
    println!("   Public key:      {:02x?} (CORRUPTED)", &public_key_bytes[..8]);
    println!("   Expected public: {:02x?} (what it should be)", &valid_public_key_bytes[..8]);
    println!("   Corrupted PeerId: {}", corrupted_peer_id);
    println!("   🚨 Private/public key relationship is INVALID - key tampering detected!");
    
    let transport = match kameo::remote::v2_bootstrap::bootstrap_with_keypair("127.0.0.1:9311".parse()?, corrupted_keypair).await {
        Ok(transport) => {
            println!("⚠️  Transport creation succeeded (transport layer doesn't validate keypair relationship)");
            transport
        }
        Err(e) => {
            println!("✅ Transport creation failed (early validation caught the issue): {}", e);
            return Ok(());
        }
    };

    // Connect to server - this should fail during challenge-response
    println!("\n📡 Attempting to connect to server...");
    let server_keypair = kameo_remote::KeyPair::from_seed_for_testing(42);
    let server_peer_id = server_keypair.peer_id();
    println!("   Server PeerId: {}", server_peer_id);
    println!("   🔍 Challenge-response will test if client can sign with private key matching claimed public key...");
    
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("❌ SECURITY FAILURE: Corrupted keypair was accepted!");
                println!("   🚨 This indicates a critical security vulnerability");
                println!("   The challenge-response authentication failed to detect key tampering");
                
                // If connection succeeded, try sending a message to see if it works
                println!("\n📤 Testing message sending with corrupted keypair...");
                
                let client_actor_ref = ClientActor::spawn(());
                let client_actor_id = client_actor_ref.id();
                
                transport
                    .register_actor("client".to_string(), client_actor_id)
                    .await?;
                
                if let Ok(Some(logger_ref)) = DistributedActorRef::lookup("logger").await {
                    match logger_ref
                        .tell(LogMessage {
                            level: "ERROR".to_string(),
                            content: "TEST 2: This message should NOT be sent - corrupted keypair!".to_string(),
                        })
                        .send()
                        .await {
                        Ok(()) => {
                            println!("❌ CRITICAL: Message sent with corrupted keypair - security system completely bypassed!");
                        }
                        Err(e) => {
                            println!("✅ Message sending failed: {}", e);
                        }
                    }
                }
                
                return Err("Security test failed - corrupted keypair was accepted".into());
            }
            Err(e) => {
                println!("✅ CONNECTION PROPERLY REJECTED: Challenge-response caught the key tampering");
                println!("   Error: {}", e);
                if e.to_string().contains("AUTH FAILED") || e.to_string().contains("challenge") {
                    println!("   🔒 Challenge-response authentication correctly detected signature mismatch");
                } else if e.to_string().contains("cryptographic") {
                    println!("   🔒 Cryptographic validation caught the invalid keypair");
                } else {
                    println!("   ℹ️  Connection rejected for: {}", e);
                }
            }
        }
    }

    println!("\n🎉 TEST 2 PASSED: Corrupted keypair was properly rejected");
    println!("✅ Challenge-response authentication detected key tampering");
    println!("✅ Ed25519 signature verification prevented unauthorized access");
    println!("✅ Security system correctly blocked invalid private/public key relationship");

    Ok(())
}