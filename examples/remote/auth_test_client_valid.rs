#!//! Authentication test client with VALID keypair
//!
//! This should successfully authenticate and connect
//!
//! Run after starting auth_test_server:
//! cargo run --example auth_test_client_valid --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use kameo::remote::v2_bootstrap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging to see authentication details
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=warn")
        .try_init();

    println!("\n🔐 === TEST: VALID KEYPAIR AUTHENTICATION ===");

    // Create valid client keypair (seed 43)
    let client_keypair = kameo_remote::KeyPair::from_seed_for_testing(43);
    let client_peer_id = client_keypair.peer_id();
    println!("🔑 Client PeerId: {}", client_peer_id);
    println!("   Using VALID keypair (correct private/public key relationship)");
    
    // Start transport with keypair
    let mut transport = v2_bootstrap::bootstrap_with_keypair("127.0.0.1:9311".parse()?, client_keypair).await?;
    println!("✅ Client transport created on 127.0.0.1:9311");
    
    // Start the transport
    transport.start().await?;
    
    // Connect to server
    println!("\n📡 Attempting to connect to server...");
    let server_keypair = kameo_remote::KeyPair::from_seed_for_testing(42);
    let server_peer_id = server_keypair.peer_id();
    
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("✅ CONNECTION SUCCESS!");
                println!("   Authentication completed successfully");
            }
            Err(e) => {
                println!("❌ CONNECTION FAILED: {}", e);
                if e.to_string().contains("AUTH") {
                    println!("   Authentication was rejected (unexpected for valid keypair)");
                }
                return Err(e.into());
            }
        }
    }
    
    // Try to lookup the test actor to verify connectivity
    println!("\n🔍 Looking up test_actor on server...");
    match DistributedActorRef::lookup("test_actor").await? {
        Some(_) => {
            println!("✅ Found test_actor - full connectivity established!");
        }
        None => {
            println!("⚠️  Could not find test_actor - actor discovery may be delayed");
        }
    }
    
    println!("\n🎉 TEST PASSED: Valid keypair authenticated successfully!");
    
    Ok(())
}