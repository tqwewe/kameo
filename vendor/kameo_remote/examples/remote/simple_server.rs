use anyhow::Result;
use kameo_remote::{GossipConfig, GossipRegistryHandle, SecretKey, NodeId, RegistrationPriority};
use std::fs;
use std::path::Path;
use std::time::Duration;
use tracing::{info, warn};

/// Simple TLS-enabled server that can be run from command line
/// 
/// Usage: cargo run --example simple_server
/// 
/// This server demonstrates:
/// 1. Using TLS 1.3 encryption for all connections
/// 2. Ed25519 cryptographic identity verification
/// 3. Providing a simple echo service over secure connections
#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    
    tracing_subscriber::fmt().init();
    
    println!("🔐 Simple TLS-Enabled Server");
    println!("============================\n");
    
    // Load or generate server TLS keypair
    println!("1. Setting up TLS identity...");
    let key_path = "/tmp/kameo_tls/simple_server.key";
    let secret_key = load_or_generate_key(key_path).await?;
    let node_id = secret_key.public();
    
    println!("   Server NodeId: {}", node_id.fmt_short());
    println!("   Key file: {}", key_path);
    
    // Save public key for clients
    let pub_path = key_path.replace(".key", ".pub");
    fs::write(&pub_path, hex::encode(node_id.as_bytes()))?;
    println!("   Public key saved to: {}\n", pub_path);
    
    // Start TLS-enabled server
    let server_addr = "127.0.0.1:29100".parse()?;
    
    let config = GossipConfig::default();
    
    let registry = GossipRegistryHandle::new_with_tls(
        server_addr,
        secret_key,
        Some(config),
    ).await?;
    
    println!("Server Status:");
    println!("  ✅ Listening on: {}", server_addr);
    println!("  🔐 TLS encryption: ENABLED");
    println!("  🎯 Server NodeId: {}\n", node_id.fmt_short());
    
    // Register an echo service
    let echo_service_addr = "127.0.0.1:39100".parse()?;
    registry.register_urgent(
        "echo_service".to_string(),
        echo_service_addr,
        RegistrationPriority::Immediate,
    ).await?;
    
    println!("Services:");
    println!("  ✅ Registered 'echo_service' at {}", echo_service_addr);
    println!("  📡 Available via TLS to authorized clients\n");
    
    println!("Ready for TLS clients!");
    println!("  💡 Clients can connect using:");
    println!("     cargo run --example simple_client {}", pub_path);
    println!("  Press Ctrl+C to stop\n");
    
    // Keep server running and show periodic status
    let mut counter = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        counter += 1;
        
        if counter % 6 == 0 { // Every minute
            let stats = registry.registry.get_stats().await;
            println!("📊 Server Status ({}m running):", counter / 6);
            println!("   - NodeId: {}", node_id.fmt_short());
            println!("   - TLS peers: {}", stats.active_peers);
            println!("   - Service: echo_service");
            println!("   - Waiting for TLS clients...\n");
        }
    }
}

async fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);
    
    if key_path.exists() {
        println!("   Loading existing key from: {}", path);
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid key length: expected 32, got {}", key_bytes.len()));
        }
        
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        println!("   Generating new Ed25519 keypair...");
        let secret_key = SecretKey::generate();
        
        // Create directory if needed
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        // Save the key
        fs::write(key_path, hex::encode(secret_key.to_bytes()))?;
        println!("   Saved new key to: {}", path);
        
        Ok(secret_key)
    }
}