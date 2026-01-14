//! Simple server with private key
//! The server has the private key and can sign messages
//!
//! Run with:
//! cargo run --example simple_auth_server --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::v2_bootstrap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init();

    println!("\n🔐 === SERVER WITH PRIVATE KEY ===");

    // Server has the full keypair (private + public)
    let server_keypair = kameo_remote::KeyPair::from_seed_for_testing(42);
    let server_public_key = server_keypair.public_key_bytes();
    
    println!("🔑 Server Public Key: {:02x?}", &server_public_key[..8]);
    println!("   (Server also has the private key)");
    
    // Start server with its keypair
    let transport = v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9310".parse()?, 
        server_keypair
    ).await?;
    
    println!("✅ Server listening on 127.0.0.1:9310");
    println!("\n📡 Waiting for connections...");
    println!("   Clients should connect using this server's public key");
    
    // Keep running
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    
    Ok(())
}