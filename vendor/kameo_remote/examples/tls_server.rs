use kameo_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// TLS-enabled gossip server - runs as a standalone process
///
/// Usage:
///   cargo run --example tls_server [port] [key_file]
///
/// Example:
///   cargo run --example tls_server 9001 /tmp/server.key
///   cargo run --example tls_server  # Uses defaults (port 9001, generates key)
///
/// This server:
/// 1. Loads or generates a persistent Ed25519 keypair
/// 2. Starts a TLS-enabled gossip registry
/// 3. Registers some test actors
/// 4. Accepts TLS connections from clients
/// 5. Displays connection status and gossip statistics
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "kameo_remote=info,tls_server=info".to_string()),
        )
        .init();

    println!("🔐 TLS Gossip Server");
    println!("====================\n");

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let port: u16 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(9001);
    let key_path = args
        .get(2)
        .map(|s| s.as_str())
        .unwrap_or("/tmp/kameo_tls_server.key");

    // Load or generate server keypair
    println!("📋 Server Configuration:");
    let secret_key = load_or_generate_key(key_path).await?;
    let node_id = secret_key.public();

    println!("   NodeId: {}", node_id.fmt_short());
    println!("   Full NodeId: {:?}", hex::encode(node_id.as_bytes()));
    println!("   Key file: {}", key_path);
    println!("   Port: {}", port);
    println!();

    // Save public key for clients to reference
    let pub_key_path = key_path.replace(".key", ".pub");
    fs::write(&pub_key_path, hex::encode(node_id.as_bytes()))?;
    println!("   Public key saved to: {}", pub_key_path);
    println!();

    // Create TLS-enabled gossip registry
    let bind_addr = format!("0.0.0.0:{}", port).parse()?;
    println!("🚀 Starting TLS gossip server on {}", bind_addr);

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(5),
        ..Default::default()
    };

    let registry = GossipRegistryHandle::new_with_tls(bind_addr, secret_key, Some(config)).await?;

    let actual_addr = registry.registry.bind_addr;
    println!("✅ Server listening on: {}", actual_addr);
    println!();

    // Register some test actors
    println!("📝 Registering test actors:");
    registry
        .register("server.health".to_string(), "127.0.0.1:10001".parse()?)
        .await?;
    println!("   - server.health -> 127.0.0.1:10001");

    registry
        .register("server.metrics".to_string(), "127.0.0.1:10002".parse()?)
        .await?;
    println!("   - server.metrics -> 127.0.0.1:10002");

    registry
        .register("server.config".to_string(), "127.0.0.1:10003".parse()?)
        .await?;
    println!("   - server.config -> 127.0.0.1:10003");
    println!();

    println!("💡 Clients can connect using:");
    println!(
        "   cargo run --example tls_client {} {}",
        port, pub_key_path
    );
    println!();
    println!("📊 Server Status (updates every 10 seconds):");
    println!("{}", "=".repeat(50));

    // Main loop - display statistics
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;

        let stats = registry.registry.get_stats().await;

        // Clear previous lines and reprint
        print!("\x1B[2K"); // Clear line
        print!("\x1B[1A"); // Move up
        print!("\x1B[2K");
        println!("{}", "=".repeat(50));

        println!("⏱  Uptime: {} seconds", stats.uptime_seconds);
        println!("👥 Active peers: {}", stats.active_peers);
        println!("❌ Failed peers: {}", stats.failed_peers);
        println!("📦 Local actors: {}", stats.local_actors);
        println!("🌍 Known actors: {}", stats.known_actors);
        println!("🔄 Gossip rounds: {}", stats.total_gossip_rounds);
        println!("📨 Delta exchanges: {}", stats.delta_exchanges);
        println!("📋 Full syncs: {}", stats.full_sync_exchanges);

        if stats.active_peers > 0 {
            println!();
            println!("✅ Connected peers:");
            // In real implementation, we'd list the actual peer NodeIds
            println!("   {} peer(s) connected via TLS", stats.active_peers);
        }

        println!("{}", "=".repeat(50));
    }
}

async fn load_or_generate_key(path: &str) -> Result<SecretKey, Box<dyn std::error::Error>> {
    let key_path = Path::new(path);

    if key_path.exists() {
        println!("   Loading existing key from: {}", path);
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;

        if key_bytes.len() != 32 {
            return Err(format!("Invalid key length: expected 32, got {}", key_bytes.len()).into());
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        println!("   Generating new key pair...");
        let secret_key = SecretKey::generate();

        // Create directory if it doesn't exist
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Save the private key
        let key_hex = hex::encode(secret_key.to_bytes());
        fs::write(key_path, key_hex)?;
        println!("   Saved new key to: {}", path);

        Ok(secret_key)
    }
}
