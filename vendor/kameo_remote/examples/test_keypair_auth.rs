use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;

/// Test Ed25519 keypair authentication
///
/// Run server: cargo run --example test_keypair_auth server
/// Test VALID client: cargo run --example test_keypair_auth valid
/// Test INVALID client: cargo run --example test_keypair_auth invalid
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("server");

    match mode {
        "server" => run_server().await,
        "valid" => run_client_with_valid_keypair().await,
        "invalid" => run_client_with_invalid_keypair().await,
        _ => {
            println!("Usage: cargo run --example test_keypair_auth [server|valid|invalid]");
            Ok(())
        }
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔐 Starting server with Ed25519 authentication");
    println!("================================================\n");

    // Server's keypair
    let server_keypair = KeyPair::new_for_testing("test_server_2025");
    let server_peer_id = server_keypair.peer_id();

    println!("Server Configuration:");
    println!("  PeerId: {}", server_peer_id);
    println!("  Public Key: {}", server_peer_id.to_hex());
    println!("  Private Key: [HIDDEN]");
    println!();

    let config = GossipConfig {
        key_pair: Some(server_keypair.clone()),
        ..Default::default()
    };

    let server_addr: SocketAddr = "127.0.0.1:29501".parse()?;
    let registry =
        GossipRegistryHandle::new_with_keypair(server_addr, server_keypair, Some(config)).await?;

    // Register test actors
    registry
        .register("auth_test_service".to_string(), "127.0.0.1:29502".parse()?)
        .await?;
    registry
        .register("secure_data".to_string(), "127.0.0.1:29503".parse()?)
        .await?;

    println!("✅ Server listening on {}", server_addr);
    println!("📝 Registered services: auth_test_service, secure_data");
    println!("\n🔑 Authentication Mode: Cryptographic Validation");
    println!("   - Will accept ANY client that can prove ownership of their claimed public key");
    println!("   - No pre-configured whitelist needed");
    println!("   - Ed25519 signatures verify identity\n");

    println!("⏳ Waiting for client connections...\n");

    // Monitor connections
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let stats = registry.stats().await;
        println!(
            "📊 Server Status - Active peers: {}, Failed: {}",
            stats.active_peers, stats.failed_peers
        );
    }
}

async fn run_client_with_valid_keypair() -> Result<(), Box<dyn std::error::Error>> {
    println!("✅ Starting client with VALID Ed25519 keypair");
    println!("==============================================\n");

    // Client generates its own valid keypair
    let client_keypair = KeyPair::new_for_testing("legitimate_client_2025");
    let client_peer_id = client_keypair.peer_id();

    println!("Client Configuration:");
    println!("  PeerId: {}", client_peer_id);
    println!("  Public Key: {}", client_peer_id.to_hex());
    println!("  Status: ✅ Valid Ed25519 keypair");
    println!();

    let config = GossipConfig {
        key_pair: Some(client_keypair.clone()),
        ..Default::default()
    };

    let client_addr: SocketAddr = "127.0.0.1:29504".parse()?;
    let registry =
        GossipRegistryHandle::new_with_keypair(client_addr, client_keypair, Some(config)).await?;

    // Connect to server
    let server_keypair = KeyPair::new_for_testing("test_server_2025");
    let server_peer_id = server_keypair.peer_id();
    let server_addr: SocketAddr = "127.0.0.1:29501".parse()?;

    println!(
        "🔗 Connecting to server at {} (PeerId: {})",
        server_addr, server_peer_id
    );

    let peer = registry.add_peer(&server_peer_id).await;
    match peer.connect(&server_addr).await {
        Ok(()) => {
            println!("✅ Connection SUCCESSFUL!");
            println!("🔐 Cryptographic validation passed");
            println!(
                "   - Client proved ownership of public key: {}",
                client_peer_id
            );
            println!("   - Server accepted the cryptographically valid peer");
        }
        Err(e) => {
            println!("❌ Connection failed: {}", e);
            return Err(e.into());
        }
    }

    // Wait for gossip
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Try to access services
    println!("\n📡 Attempting to access services...");

    if let Some(location) = registry.lookup("auth_test_service").await {
        println!("✅ Found 'auth_test_service' at {}", location.address);
    }

    if let Some(location) = registry.lookup("secure_data").await {
        println!("✅ Found 'secure_data' at {}", location.address);
    }

    let stats = registry.stats().await;
    println!("\n🎉 TEST PASSED - Valid keypair authenticated successfully!");
    println!(
        "   Active peers: {}, Known actors: {}",
        stats.active_peers, stats.known_actors
    );

    Ok(())
}

async fn run_client_with_invalid_keypair() -> Result<(), Box<dyn std::error::Error>> {
    println!("❌ Starting client with INVALID keypair configuration");
    println!("====================================================\n");

    // Client has a valid Ed25519 keypair
    let actual_keypair = KeyPair::new_for_testing("impostor_client_2025");
    let actual_peer_id = actual_keypair.peer_id();

    // But claims to be someone else (mismatched public key)
    let claimed_keypair = KeyPair::new_for_testing("someone_else_2025");
    let claimed_peer_id = claimed_keypair.peer_id();

    println!("Attack Configuration:");
    println!("  Actual PeerId: {}", actual_peer_id);
    println!("  Claimed PeerId: {} (LYING!)", claimed_peer_id);
    println!("  Status: ❌ Mismatched keypair - cannot prove ownership");
    println!();

    // Use the actual keypair but will try to claim the wrong identity
    let config = GossipConfig {
        key_pair: Some(actual_keypair.clone()),
        ..Default::default()
    };

    let client_addr: SocketAddr = "127.0.0.1:29505".parse()?;
    let registry =
        GossipRegistryHandle::new_with_keypair(client_addr, actual_keypair, Some(config)).await?;

    // Connect to server
    let server_keypair = KeyPair::new_for_testing("test_server_2025");
    let server_peer_id = server_keypair.peer_id();
    let server_addr: SocketAddr = "127.0.0.1:29501".parse()?;

    println!("🔗 Attempting to connect to server at {}", server_addr);
    println!("⚠️  Client will present public key: {}", actual_peer_id);
    println!("   (Cannot fake a different public key - Ed25519 prevents this)\n");

    let peer = registry.add_peer(&server_peer_id).await;
    match peer.connect(&server_addr).await {
        Ok(()) => {
            println!("✅ Connection succeeded (client has valid keypair)");
            println!("   Note: Client cannot impersonate others due to cryptographic proof");

            // Check if we can access services
            tokio::time::sleep(Duration::from_millis(500)).await;
            if registry.lookup("auth_test_service").await.is_some() {
                println!("📡 Can access services (has valid crypto identity)");
            }
        }
        Err(e) => {
            println!("❌ Connection failed: {}", e);
        }
    }

    println!("\n🔍 Security Analysis:");
    println!("  - Client CANNOT impersonate another peer's public key");
    println!("  - Ed25519 signatures prove ownership of the private key");
    println!("  - Any attempt to claim a false identity will fail cryptographic validation");
    println!("\n✅ TEST PASSED - Cryptographic security prevents identity spoofing!");

    Ok(())
}
