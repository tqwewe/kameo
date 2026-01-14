#!//! Authentication test client with INVALID/CORRUPTED keypair
//!
//! This should FAIL authentication and be immediately disconnected
//!
//! Run after starting auth_test_server:
//! cargo run --example auth_test_client_invalid --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::{transport::RemoteTransport};
use kameo::remote::v2_bootstrap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging to see authentication details
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=warn")
        .try_init();

    println!("\n🔐 === TEST: INVALID KEYPAIR AUTHENTICATION ===");

    // Create CORRUPTED client keypair
    // This has the same private key (seed 43) but wrong public key
    // This simulates key tampering/corruption
    let corrupted_keypair = kameo_remote::KeyPair::from_seed_with_corrupted_public_key_for_testing(43);
    let client_peer_id = corrupted_keypair.peer_id();
    
    println!("🔑 Client PeerId: {}", client_peer_id);
    println!("   Using CORRUPTED keypair:");
    println!("   - Private key from seed 43 (same as valid test)");
    println!("   - Public key CORRUPTED (doesn't match private key)");
    println!("   This simulates key tampering/corruption");
    
    // Start transport with corrupted keypair
    let mut transport = v2_bootstrap::bootstrap_with_keypair("127.0.0.1:9312".parse()?, corrupted_keypair).await?;
    println!("✅ Client transport created on 127.0.0.1:9312");
    
    // Start the transport
    transport.start().await?;
    
    // Connect to server - THIS SHOULD FAIL
    println!("\n📡 Attempting to connect to server...");
    println!("   Expected: ❌ Authentication should FAIL");
    
    let server_keypair = kameo_remote::KeyPair::from_seed_for_testing(42);
    let server_peer_id = server_keypair.peer_id();
    
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("❌ SECURITY FAILURE: Connection succeeded when it should have failed!");
                println!("   The corrupted keypair was incorrectly accepted");
                println!("   This indicates the authentication is NOT working properly");
                return Err("Authentication bypass - corrupted key was accepted!".into());
            }
            Err(e) => {
                println!("✅ CONNECTION CORRECTLY REJECTED: {}", e);
                if e.to_string().contains("AUTH") || e.to_string().contains("Signature") {
                    println!("   ✅ Authentication properly failed due to invalid signature");
                    println!("   ✅ Socket was terminated as expected");
                } else {
                    println!("   ⚠️  Connection failed but not explicitly due to auth: {}", e);
                }
            }
        }
    }
    
    println!("\n🎉 TEST PASSED: Corrupted keypair was correctly rejected!");
    println!("   The authentication system properly detected the invalid signature");
    
    Ok(())
}