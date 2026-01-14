//! Client with CORRECT server public key
//! This should successfully connect
//!
//! Run after starting simple_auth_server:
//! cargo run --example simple_auth_client_correct --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::v2_bootstrap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init();

    println!("\n🔐 === CLIENT WITH CORRECT PUBLIC KEY ===");

    // Client needs its own keypair for identity
    let client_keypair = kameo_remote::KeyPair::from_seed_for_testing(43);
    
    // Get the CORRECT server public key (from seed 42)
    let server_keypair = kameo_remote::KeyPair::from_seed_for_testing(42);
    let server_peer_id = server_keypair.peer_id();
    let server_public_key = server_keypair.public_key_bytes();
    
    println!("🔑 Using server public key: {:02x?}", &server_public_key[..8]);
    println!("   This is the CORRECT public key for the server");
    
    // Start client with its own keypair
    let transport = v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9311".parse()?, 
        client_keypair
    ).await?;
    
    println!("✅ Client started on 127.0.0.1:9311");
    
    // Connect to server using the correct public key
    println!("\n📡 Connecting to server...");
    if let Some(handle) = transport.handle() {
        // Configure the expected server public key for verification
        {
            let pool = handle.registry.connection_pool.lock().await;
            pool.set_expected_server_key(server_peer_id.clone(), server_peer_id.clone());
        }
        
        let peer = handle.add_peer(&server_peer_id).await;
        
        match peer.connect(&"127.0.0.1:9310".parse()?).await {
            Ok(()) => {
                println!("✅ SUCCESS: Connected with correct public key!");
            }
            Err(e) => {
                println!("❌ FAILED: {}", e);
                return Err(e.into());
            }
        }
    }
    
    println!("\n🎉 TEST PASSED: Correct public key authenticated successfully!");
    
    Ok(())
}