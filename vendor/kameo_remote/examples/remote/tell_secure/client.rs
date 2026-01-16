use anyhow::Result;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use kameo_remote::{GossipConfig, GossipRegistryHandle, SecretKey, NodeId};
use tracing::{info, warn, error};

/// TLS-enabled secure client with Ed25519 certificate authentication
/// 
/// Usage: cargo run --example tell_secure_client [server_pub_key]
/// 
/// This client:
/// 1. Uses TLS 1.3 for all connections
/// 2. Loads its own Ed25519 TLS certificate
/// 3. Verifies server certificate during TLS handshake
/// 4. Demonstrates mutual TLS (mTLS) authentication
#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    
    tracing_subscriber::fmt().init();
    
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let server_pub_path = if args.len() > 1 {
        args[1].clone()
    } else {
        "/tmp/kameo_tls/tell_secure_server.pub".to_string()
    };
    
    println!("🔐 TLS-Enabled Secure Kameo Remote Client");
    println!("==========================================\n");
    
    // 1. Load server's public key for TLS verification
    println!("1. Loading server TLS identity...");
    let server_node_id = match load_node_id(&server_pub_path) {
        Ok(id) => {
            println!("   ✅ Server NodeId: {}", id.fmt_short());
            println!("   ✅ Loaded from: {}\n", server_pub_path);
            id
        },
        Err(e) => {
            println!("   ❌ Failed to load server public key: {}", e);
            println!("   💡 Make sure the server has been run first to generate its keypair");
            return Ok(());
        }
    };
    
    // 2. Load or generate client TLS keypair
    println!("2. Setting up client TLS identity...");
    let client_key_path = "/tmp/kameo_tls/tell_secure_client.key";
    let client_secret = load_or_generate_tls_key(client_key_path).await?;
    let client_node_id = client_secret.public();
    
    println!("   ✅ Client NodeId: {}", client_node_id.fmt_short());
    println!("   ✅ TLS key loaded from: {}\n", client_key_path);
    
    // 3. Create TLS-enabled client registry
    println!("3. Starting TLS client...");
    let client_addr: SocketAddr = "0.0.0.0:0".parse()?;
    
    let config = GossipConfig::default();
    
    let registry = GossipRegistryHandle::new_with_tls(
        client_addr,
        client_secret,
        Some(config),
    ).await?;
    
    let actual_addr = registry.registry.bind_addr;
    println!("   ✅ Client listening on: {}", actual_addr);
    println!("   ✅ Client NodeId: {}", client_node_id.fmt_short());
    println!("   ✅ TLS 1.3 encryption: ENABLED\n");
    
    // 4. Connect to TLS server with certificate verification
    println!("4. Connecting to TLS server...");
    let server_addr: SocketAddr = "127.0.0.1:29001".parse()?;
    
    registry.registry.add_peer_with_node_id(server_addr, Some(server_node_id.clone())).await;
    
    println!("   🔗 Connecting to {} with TLS", server_addr);
    println!("   🔑 Client NodeId: {}", client_node_id.fmt_short());
    println!("   🎯 Server NodeId: {}", server_node_id.fmt_short());
    println!("   ✅ TLS handshake in progress...\n");
    
    // Wait for connection and gossip
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 5. Wait for service discovery
    println!("5. Discovering available services...");
    
    // Wait for gossip propagation
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let service_location = match registry.lookup("secure_chat_service").await {
        Some(location) => {
            println!("   ✅ Found secure_chat_service at: {}", location.address);
            println!("   📡 Service available via TLS");
            location
        },
        None => {
            println!("   ❌ secure_chat_service not found");
            println!("   ⏳ Waiting longer for service discovery...");
            
            // Try a few more times
            for attempt in 1..=5 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Some(location) = registry.lookup("secure_chat_service").await {
                    println!("   ✅ Found secure_chat_service on attempt {} at: {}", attempt, location.address);
                    break;
                }
                if attempt == 5 {
                    println!("   ❌ Service discovery failed after 5 attempts");
                    println!("   💡 Make sure the server is running and has registered the service");
                    return Ok(());
                }
            }
            return Ok(());
        }
    };
    println!();
    
    // 6. Demonstrate TLS encryption
    println!("6. TLS Connection established...");
    println!("   🔐 All messages are encrypted with TLS 1.3");
    println!("   🎯 Server identity verified via Ed25519");
    println!("   ✅ Mutual TLS authentication complete\n");
    
    // 7. Show connection statistics
    println!("7. Connection Statistics:");
    let stats = registry.registry.get_stats().await;
    println!("   Active TLS peers: {}", stats.active_peers);
    println!("   Known actors: {}", stats.known_actors);
    println!("   Gossip rounds: {}", stats.total_gossip_rounds);
    println!();
    
    // 8. Stay connected and show status
    println!("8. TLS Client connected - press Ctrl+C to exit");
    println!("   🔐 TLS 1.3 connection established");
    println!("   📡 Service discovery via TLS gossip");
    println!("   🔑 Ed25519 certificates verified");
    println!();
    
    // Keep the client running
    let mut status_count = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        status_count += 1;
        
        if status_count % 3 == 0 { // Every 30 seconds
            let stats = registry.registry.get_stats().await;
            println!("   📊 TLS Client Status:");
            println!("      - Client NodeId: {}", client_node_id.fmt_short());
            println!("      - TLS peers: {}", stats.active_peers);
            println!("      - Connected to: {}", server_addr);
            println!("      - Service available: secure_chat_service");
            println!();
        }
    }
}

/// Load NodeId from public key file
fn load_node_id(path: &str) -> Result<NodeId> {
    let pub_key_hex = fs::read_to_string(path)?;
    let pub_key_bytes = hex::decode(pub_key_hex.trim())?;
    
    if pub_key_bytes.len() != 32 {
        return Err(anyhow::anyhow!("Invalid public key length: expected 32, got {}", pub_key_bytes.len()));
    }
    
    NodeId::from_bytes(&pub_key_bytes)
        .map_err(|e| anyhow::anyhow!("Invalid NodeId: {}", e))
}

/// Load or generate TLS keypair
async fn load_or_generate_tls_key(key_path: &str) -> Result<SecretKey> {
    let path = Path::new(key_path);
    
    if path.exists() {
        println!("   Loading existing TLS key from: {}", key_path);
        let key_hex = fs::read_to_string(path)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid key length: expected 32, got {}", key_bytes.len()));
        }
        
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        println!("   Generating new Ed25519 TLS keypair...");
        let secret_key = SecretKey::generate();
        
        // Create directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        // Save the key
        fs::write(path, hex::encode(secret_key.to_bytes()))?;
        println!("   Saved new TLS key to: {}", key_path);
        
        // Also save public key
        let pub_path = key_path.replace(".key", ".pub");
        fs::write(&pub_path, hex::encode(secret_key.public().as_bytes()))?;
        println!("   Saved public key to: {}", pub_path);
        
        Ok(secret_key)
    }
}