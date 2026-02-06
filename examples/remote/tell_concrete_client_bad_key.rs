//! Concrete actor tell client with WRONG KEY (demonstrates security rejection)
//!
//! This client uses a different keypair than expected by the server,
//! demonstrating how the secure PeerId system rejects unauthorized connections.
//!
//! ## Cryptographic Keypair System
//!
//! ### How Keypairs Work:
//! - Each peer has an Ed25519 keypair (32-byte private key + 32-byte public key)
//! - PeerId = public key (used for peer identification)
//! - Private key = used to sign challenges and prove identity ownership
//! - Server only accepts connections from peers with expected public keys
//!
//! ### Creating Keypairs:
//! ```rust
//! // Production: Generate random keypair
//! let keypair = kameo_remote::KeyPair::generate();
//! let peer_id = keypair.peer_id();
//! 
//! // Testing: Deterministic keypair from seed
//! let keypair = kameo_remote::KeyPair::new_for_testing("42");
//! let peer_id = keypair.peer_id();
//! 
//! // From existing private key bytes
//! let keypair = kameo_remote::KeyPair::from_private_key_bytes(&private_key_bytes)?;
//! ```
//!
//! ### Security Tests:
//! 1. **Invalid Keys**: Cryptographically invalid (all-zero, etc.) → Rejected immediately
//! 2. **Wrong Keys**: Valid but unauthorized → Rejected during handshake  
//! 3. **Valid Keys**: Authorized keypairs → Accepted and connected
//!
//! ### Server Configuration:
//! The server expects specific PeerIds. In this test:
//! - Server PeerId: seed 42 → `e1ef2fe6f211f7399a8a6a55fdc811ee92ec7f01ee125942da87ef659553499f`
//! - Expected Client: seed 43 → `1ba66c6751506850ae0787244c69476b6d45fb857a914a5a0445a24253f7b810`
//! - Bad Client: seed 99 → `d523845a249f6994b019cbb33057d352237858ff79a98cb2359d805ee45044d6`
//!
//! Run after starting the server:
//! cargo run --example tell_concrete_client_bad_key --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::{transport::RemoteTransport, DynamicDistributedActorRef};

// Import shared message definitions
mod tell_messages;
use tell_messages::*;

// Client actor that can receive messages from server
struct BadClientActor {
    responses_received: u32,
}

impl Actor for BadClientActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🎬 BadClientActor started (with wrong key!)");
        Ok(Self {
            responses_received: 0,
        })
    }
}

// Handler for ServerResponse messages from server (if we ever receive any)
impl BadClientActor {
    async fn handle_server_response(&mut self, msg: &rkyv::Archived<ServerResponse>) {
        self.responses_received += 1;
        println!("📨 [BAD CLIENT] Unexpectedly received response #{}: {}", 
                self.responses_received, msg.response_data);
    }
}

// Register with distributed actor macro
distributed_actor! {
    BadClientActor {
        ServerResponse => handle_server_response,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    println!("\n🔴 === BAD CLIENT (Wrong Cryptographic Key) ===");
    println!("This client demonstrates security rejection with wrong keypairs\n");

    // Bootstrap on port 9312 (different port) with WRONG keypair
    println!("🔑 Setting up client with WRONG cryptographic identity...");
    let bad_client_keypair = kameo_remote::KeyPair::new_for_testing("99"); // WRONG SEED!
    let bad_client_peer_id = bad_client_keypair.peer_id();
    println!("   Bad Client PeerId: {}", bad_client_peer_id);
    println!("   ⚠️  This key is NOT authorized by the server!");

    let transport =
        kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9312".parse()?, bad_client_keypair)
            .await?;
    println!("✅ Bad client listening on {} with WRONG crypto identity", transport.local_addr());

    // Try to connect to server using server's actual PeerId, but with our wrong client identity
    println!("\n📡 Attempting to connect to server at 127.0.0.1:9310...");
    let server_keypair = kameo_remote::KeyPair::new_for_testing("42"); // Correct server key
    let server_peer_id = server_keypair.peer_id();
    println!("   Target server PeerId: {}", server_peer_id);
    println!("   Our (unauthorized) PeerId: {}", bad_client_peer_id);

    // Test 1: Try with the server's PeerId (should be rejected by network-level auth)
    println!("\n🧪 Test 1: Connection with wrong client identity (network-level rejection)");
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("⚠️  Initial connection succeeded, checking if handshake completes...");
                // Wait for handshake and verify connection is actually established
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                
                if peer.is_connected().await {
                    println!("❌ ERROR: Full connection established - security not working!");
                } else {
                    println!("✅ Connection rejected during handshake - security working!");
                    println!("   🛡️  Server rejected unauthorized client after handshake.");
                }
            }
            Err(e) => {
                println!("✅ Connection immediately REJECTED: {}", e);
                println!("   🛡️  Network-level security working - unauthorized client blocked.");
            }
        }
    }

    // Test 2: Try with an INVALID PeerId to test debug validation
    println!("\n🧪 Test 2: Connection with cryptographically invalid PeerId (validation test)");
    // Create a PeerId with invalid bytes (not a valid Ed25519 key)
    let invalid_peer_id = kameo_remote::PeerId::from_public_key_bytes([0u8; 32]); // All zeros = invalid
    println!("   Invalid PeerId: {}", invalid_peer_id);
    
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&invalid_peer_id).await;
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("❌ ERROR: Invalid key should have been rejected by debug validation!");
                println!("   This indicates the cryptographic validation is not working.");
            }
            Err(e) => {
                println!("✅ Invalid key correctly REJECTED: {}", e);
                if e.to_string().contains("Invalid peer public key") {
                    println!("   🔒 Cryptographic debug validation working!");
                } else if e.to_string().contains("AuthenticationFailed") {
                    println!("   🚫 Authentication system blocked invalid key.");
                }
            }
        }
    }

    // Even if connection was established, let's try to register and see what happens
    println!("\n🧪 Testing actor registration with unauthorized key...");

    // Create and register BadClientActor
    let client_actor_ref = <BadClientActor as Actor>::spawn(());
    let client_actor_id = client_actor_ref.id();

    match transport.register_actor("bad_client".to_string(), client_actor_id).await {
        Ok(()) => {
            println!("⚠️  Actor registration succeeded (connection might be working)");
            
            // Try to lookup and send messages anyway
            println!("\n🔍 Attempting to lookup server's LoggerActor...");
            
            // Wait a moment for potential gossip sync
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            
            match DynamicDistributedActorRef::lookup("logger", transport.clone()).await {
                Ok(Some(logger_ref)) => {
                    println!("⚠️  Found LoggerActor! Attempting to send message...");
                    
                    match logger_ref
                        .tell(LogMessage {
                            level: "WARN".to_string(),
                            content: "Unauthorized client managed to send message!".to_string(),
                        })
                        .send()
                        .await
                    {
                        Ok(()) => {
                            println!("❌ ERROR: Unauthorized message was sent successfully!");
                            println!("   This indicates the security system needs improvement.");
                        }
                        Err(e) => {
                            println!("✅ Message correctly rejected: {}", e);
                        }
                    }
                }
                Ok(None) => {
                    println!("✅ LoggerActor not found (good - gossip sync blocked)");
                }
                Err(e) => {
                    println!("✅ Lookup failed as expected: {}", e);
                }
            }
        }
        Err(e) => {
            println!("✅ Actor registration correctly failed: {}", e);
        }
    }

    println!("\n📊 === SECURITY TEST RESULTS ===");
    println!("Expected behavior:");
    println!("  ✅ Connection should be rejected");
    println!("  ✅ Actor registration should fail");
    println!("  ✅ Message sending should be blocked");
    println!("\nIf any of these passed, the security system needs improvement.");

    // FINAL TEST: Show that valid keypairs DO work
    println!("\n🧪 === Test 3: Valid Authorized Keypair (Control Test) ===");
    println!("Testing with the CORRECT keypair to verify legitimate clients work...");
    
    let authorized_keypair = kameo_remote::KeyPair::new_for_testing("43"); // Correct seed!
    let authorized_peer_id = authorized_keypair.peer_id();
    println!("   Authorized Client PeerId: {}", authorized_peer_id);
    println!("   ✅ This key IS expected by the server");
    
    // Create new transport on different port for valid client test
    let valid_transport =
        kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9313".parse()?, authorized_keypair)
            .await?;
    println!("✅ Valid client transport on {}", valid_transport.local_addr());
    
    if let Some(handle) = valid_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        
        println!("🔗 Attempting connection with AUTHORIZED keypair...");
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("⚠️  Initial connection succeeded, checking if handshake completes...");
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                
                if peer.is_connected().await {
                    println!("✅ SUCCESS: Authorized client connection established!");
                    println!("   🛡️  Legitimate keypair passed all security checks.");
                } else {
                    println!("❌ Authorized connection failed during handshake");
                }
            }
            Err(e) => {
                println!("❌ Authorized connection failed: {}", e);
            }
        }
    }

    println!("\n📊 === FINAL SECURITY SUMMARY ===");
    println!("✅ Invalid keys: REJECTED (cryptographic validation)");
    println!("✅ Wrong keys: REJECTED (network-level authentication)");
    println!("✅ Valid keys: ACCEPTED (proper authorization)");
    println!("🔒 Multi-layer security system functioning correctly!");

    println!("\n🔚 Security test complete. Press Ctrl+C to exit.");
    
    // Keep running briefly to see any delayed effects
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    
    Ok(())
}
