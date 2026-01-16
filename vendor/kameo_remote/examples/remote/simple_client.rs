use anyhow::Result;
use kameo_remote::{GossipConfig, GossipRegistryHandle, SecretKey, NodeId};
use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tracing::{info, warn};

/// Simple TLS-enabled client that connects to the simple_server
/// 
/// Usage: cargo run --example simple_client [server_pub_key_path]
/// 
/// This client demonstrates:
/// 1. Using TLS 1.3 encryption for all connections
/// 2. Ed25519 cryptographic identity verification
/// 3. Discovering and using services over secure connections
#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    
    tracing_subscriber::fmt().init();
    
    println!("🔐 Simple TLS-Enabled Client");
    println!("============================\n");
    
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let server_pub_path = if args.len() > 1 {
        args[1].clone()
    } else {
        "/tmp/kameo_tls/simple_server.pub".to_string()
    };
    
    // 1. Load server's public key for TLS verification
    println!("1. Loading server identity...");
    let server_node_id = load_node_id(&server_pub_path)?;
    println!("   Server NodeId: {}", server_node_id.fmt_short());
    println!("   Loaded from: {}\n", server_pub_path);
    
    // 2. Load or generate client keypair
    println!("2. Setting up client TLS identity...");
    let client_key_path = "/tmp/kameo_tls/simple_client.key";
    let client_secret = load_or_generate_key(client_key_path).await?;
    let client_node_id = client_secret.public();
    
    println!("   Client NodeId: {}", client_node_id.fmt_short());
    println!("   Key file: {}\n", client_key_path);
    
    // 3. Create TLS-enabled gossip registry
    println!("3. Starting TLS client...");
    let bind_addr = "0.0.0.0:0".parse()?;
    
    let config = GossipConfig::default();
    
    let registry = GossipRegistryHandle::new_with_tls(
        bind_addr,
        client_secret,
        Some(config),
    ).await?;
    
    let actual_addr = registry.registry.bind_addr;
    println!("   ✅ Client listening on: {}", actual_addr);
    println!("   ✅ TLS encryption: ENABLED\n");
    
    // 4. Connect to server with TLS verification
    println!("4. Connecting to TLS server...");
    let server_addr = "127.0.0.1:29100".parse()?;
    
    registry.registry.add_peer_with_node_id(server_addr, Some(server_node_id.clone())).await;
    
    println!("   🔗 Connecting to {} with NodeId verification", server_addr);
    println!("   ✅ Server NodeId: {}", server_node_id.fmt_short());
    
    // Wait for connection and gossip
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 5. Discover echo service
    println!("\n5. Discovering services...");
    
    let service_location = match registry.lookup("echo_service").await {
        Some(location) => {
            println!("   ✅ Found 'echo_service' at {} via gossip", location.address);
            location
        },
        None => {
            println!("   ⏳ Service not discovered yet, waiting...");
            tokio::time::sleep(Duration::from_secs(1)).await;
            
            match registry.lookup("echo_service").await {
                Some(location) => {
                    println!("   ✅ Found 'echo_service' at {}", location.address);
                    location
                },
                None => {
                    println!("   ❌ 'echo_service' not found");
                    println!("   💡 Make sure the server is running");
                    return Ok(());
                }
            }
        }
    };
    
    // 6. Show connection stats
    println!("\n6. Connection Statistics:");
    let stats = registry.registry.get_stats().await;
    println!("   Active TLS peers: {}", stats.active_peers);
    println!("   Known actors: {}", stats.known_actors);
    println!("   Gossip rounds: {}", stats.total_gossip_rounds);
    
    println!("\n✅ Successfully connected to TLS server!");
    println!("   All communication is encrypted with TLS 1.3");
    println!("   Server identity verified via Ed25519");
    
    // Keep client running
    println!("\nClient connected - press Ctrl+C to exit");
    
    let mut status_count = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        status_count += 1;
        
        if status_count % 3 == 0 { // Every 30 seconds
            let stats = registry.registry.get_stats().await;
            println!("📊 Client Status:");
            println!("   - NodeId: {}", client_node_id.fmt_short());
            println!("   - TLS peers: {}", stats.active_peers);
            println!("   - Service available: echo_service at {}", service_location.address);
            println!();
        }
    }
}

fn load_node_id(path: &str) -> Result<NodeId> {
    let pub_key_hex = fs::read_to_string(path)?;
    let pub_key_bytes = hex::decode(pub_key_hex.trim())?;
    
    if pub_key_bytes.len() != 32 {
        return Err(anyhow::anyhow!("Invalid public key length: expected 32, got {}", pub_key_bytes.len()));
    }
    
    NodeId::from_bytes(&pub_key_bytes)
        .map_err(|e| anyhow::anyhow!("Invalid NodeId: {}", e))
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
        
        // Also save public key
        let pub_path = path.replace(".key", ".pub");
        fs::write(&pub_path, hex::encode(secret_key.public().as_bytes()))?;
        println!("   Saved public key to: {}", pub_path);
        
        Ok(secret_key)
    }
}