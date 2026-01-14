//! Client with WRONG server public key
//! This should FAIL to connect
//!
//! Run after starting simple_auth_server:
//! cargo run --example simple_auth_client_wrong --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::v2_bootstrap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init();

    println!("\n🔐 === CLIENT WITH WRONG PUBLIC KEY ===");

    // Client needs its own keypair for identity
    let client_keypair = kameo_remote::KeyPair::from_seed_for_testing(43);
    
    // Get the WRONG server public key (from different seed)
    let wrong_server_keypair = kameo_remote::KeyPair::from_seed_for_testing(99);
    let wrong_server_peer_id = wrong_server_keypair.peer_id();
    let wrong_public_key = wrong_server_keypair.public_key_bytes();
    
    println!("🔑 Using WRONG server public key: {:02x?}", &wrong_public_key[..8]);
    println!("   This is NOT the server's actual public key!");
    
    // Start client with its own keypair
    let transport = v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9312".parse()?, 
        client_keypair
    ).await?;
    
    println!("✅ Client started on 127.0.0.1:9312");
    
    // Try to connect to server using the WRONG public key
    println!("\n📡 Attempting to connect with wrong public key...");
    if let Some(handle) = transport.handle() {
        // Configure the WRONG expected server public key for verification
        // We're expecting server to have the wrong key (seed 99) but it actually has key from seed 42
        {
            let pool = handle.registry.connection_pool.lock().await;
            // Note: First param is the actual server's peer ID (seed 42)
            // Second param is what we expect (wrong key from seed 99)
            let actual_server = kameo_remote::KeyPair::from_seed_for_testing(42).peer_id();
            pool.set_expected_server_key(actual_server, wrong_server_peer_id.clone());
        }
        
        // Connect to the actual server (not the wrong one)
        let actual_server = kameo_remote::KeyPair::from_seed_for_testing(42).peer_id();
        let peer = handle.add_peer(&actual_server).await;
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("❌ SECURITY FAILURE: Connected with wrong public key!");
                println!("   This should NOT have succeeded!");
                return Err("Security breach: wrong key accepted!".into());
            }
            Err(e) => {
                println!("✅ CORRECTLY REJECTED: {}", e);
                println!("   Connection failed as expected with wrong key");
            }
        }
    }
    
    println!("\n🎉 TEST PASSED: Wrong public key was correctly rejected!");
    
    Ok(())
}