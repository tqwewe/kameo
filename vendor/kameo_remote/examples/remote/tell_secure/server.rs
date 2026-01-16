use anyhow::Result;
use std::collections::HashSet;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use kameo_remote::{GossipConfig, GossipRegistryHandle, SecretKey, NodeId, RegistrationPriority};
use tracing::{info, warn, error};

/// TLS-enabled secure server with Ed25519 certificate verification
/// 
/// Usage: cargo run --example tell_secure_server
/// 
/// This server:
/// 1. Uses TLS 1.3 for all connections
/// 2. Loads its own Ed25519 TLS certificate (generates if doesn't exist)
/// 3. Verifies client certificates during TLS handshake
/// 4. Provides mutual TLS (mTLS) authentication
#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    
    tracing_subscriber::fmt().init();
    
    println!("🔐 TLS-Enabled Secure Kameo Remote Server");
    println!("==========================================\n");
    
    // 1. Load or generate server TLS keypair
    println!("1. Setting up server TLS identity...");
    let server_key_path = "/tmp/kameo_tls/tell_secure_server.key";
    let server_secret = load_or_generate_tls_key(server_key_path).await?;
    let server_node_id = server_secret.public();
    
    println!("   Server NodeId: {}", server_node_id.fmt_short());
    println!("   Server TLS key loaded from: {}\n", server_key_path);
    
    // 2. Save server public key for clients
    println!("2. Saving server public key for clients...");
    let server_pub_path = server_key_path.replace(".key", ".pub");
    fs::write(&server_pub_path, hex::encode(server_node_id.as_bytes()))?;
    println!("   📋 Server public key saved to: {}", server_pub_path);
    println!("   📋 Clients can use this to verify server identity\n");
    
    // 3. Create TLS-enabled gossip registry
    println!("3. Starting TLS server...");
    let server_addr: SocketAddr = "127.0.0.1:29001".parse()?;
    
    let config = GossipConfig::default();
    
    let registry = GossipRegistryHandle::new_with_tls(
        server_addr,
        server_secret,
        Some(config),
    ).await?;
    
    println!("   ✅ Server listening on: {}", server_addr);
    println!("   ✅ Server NodeId: {}", server_node_id.fmt_short());
    println!("   ✅ TLS 1.3 encryption: ENABLED");
    println!("   ✅ Ed25519 certificates: ACTIVE\n");
    
    // 4. Register a service that clients can access
    println!("4. Registering secure services...");
    let service_addr = "127.0.0.1:39001".parse()?;
    
    registry.register_urgent(
        "secure_chat_service".to_string(),
        service_addr,
        RegistrationPriority::Immediate,
    ).await?;
    
    println!("   ✅ Registered 'secure_chat_service' at {}", service_addr);
    println!("   ✅ Service is now available to authorized clients\n");
    
    // 5. Wait for TLS client connections and show activity
    println!("5. Server ready - waiting for TLS client connections...");
    println!("   💡 Clients can connect using:");
    println!("      cargo run --example tell_secure_client {}", server_pub_path);
    println!("   Use Ctrl+C to stop the server\n");
    
    // Monitor connections
    let mut connection_count = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        // In a real implementation, you'd check actual connection status
        // For now, just show that the server is running
        connection_count += 1;
        
        if connection_count % 12 == 0 { // Every minute
            let stats = registry.registry.get_stats().await;
            info!("🔐 TLS Server running - {} active peers", stats.active_peers);
        }
        
        // Show status every 30 seconds
        if connection_count % 6 == 0 {
            let stats = registry.registry.get_stats().await;
            println!("   📊 TLS Server Status:");
            println!("      - Listening on: {}", server_addr);
            println!("      - Server NodeId: {}", server_node_id.fmt_short());
            println!("      - TLS peers: {}", stats.active_peers);
            println!("      - TLS 1.3: ENABLED");
            println!("      - Services: secure_chat_service");
            println!();
        }
    }
}

/// Load TLS keypair from file, or generate a new one if it doesn't exist
async fn load_or_generate_tls_key(key_path: &str) -> Result<SecretKey> {
    let path = Path::new(key_path);
    
    if path.exists() {
        println!("   📁 Loading existing TLS key from: {}", key_path);
        let key_hex = fs::read_to_string(path)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid key length: expected 32, got {}", key_bytes.len()));
        }
        
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        println!("   🔑 Generating new Ed25519 TLS keypair at: {}", key_path);
        
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        let secret_key = SecretKey::generate();
        
        // Save private key
        fs::write(path, hex::encode(secret_key.to_bytes()))?;
        
        println!("   💾 Saved TLS private key to: {}", key_path);
        
        Ok(secret_key)
    }
}