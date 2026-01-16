use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;

/// Test that demonstrates successful connection with valid keypair
/// Run server first: cargo run --example test_valid_keypair server
/// Then client: cargo run --example test_valid_keypair client
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let mode = if args.len() > 1 { &args[1] } else { "server" };

    match mode {
        "server" => run_server().await,
        "client" => run_client().await,
        _ => {
            println!("Usage: cargo run --example test_valid_keypair [server|client]");
            Ok(())
        }
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔐 Starting server with Ed25519 keypair authentication");

    // Generate server keypair
    let server_keypair = KeyPair::new_for_testing("server_key");
    let server_peer_id = server_keypair.peer_id();
    println!("Server PeerId: {}", server_peer_id);
    println!("Server public key: {}", server_peer_id.to_hex());

    // Create config with keypair
    let config = GossipConfig {
        key_pair: Some(server_keypair.clone()),
        ..Default::default()
    };

    let server_addr: SocketAddr = "127.0.0.1:28001".parse()?;
    let registry =
        GossipRegistryHandle::new_with_keypair(server_addr, server_keypair, Some(config)).await?;

    // Register a test actor
    registry
        .register("test_actor".to_string(), "127.0.0.1:28002".parse()?)
        .await?;

    println!(
        "✅ Server listening on {} with PeerId: {}",
        server_addr, server_peer_id
    );
    println!("📝 Registered actor: test_actor");
    println!("⏳ Waiting for client connections...");

    // Keep server running
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let stats = registry.stats().await;
        println!(
            "📊 Active peers: {}, Failed peers: {}",
            stats.active_peers, stats.failed_peers
        );
    }
}

async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔐 Starting client with valid Ed25519 keypair");

    // Generate client keypair
    let client_keypair = KeyPair::new_for_testing("client_key");
    let client_peer_id = client_keypair.peer_id();
    println!("Client PeerId: {}", client_peer_id);
    println!("Client public key: {}", client_peer_id.to_hex());

    // Generate expected server peer ID (must match server's keypair)
    let server_keypair = KeyPair::new_for_testing("server_key");
    let server_peer_id = server_keypair.peer_id();
    println!("Expected server PeerId: {}", server_peer_id);

    // Create config with keypair
    let config = GossipConfig {
        key_pair: Some(client_keypair.clone()),
        ..Default::default()
    };

    let client_addr: SocketAddr = "127.0.0.1:28003".parse()?;
    let registry =
        GossipRegistryHandle::new_with_keypair(client_addr, client_keypair, Some(config)).await?;

    println!(
        "✅ Client started on {} with PeerId: {}",
        client_addr, client_peer_id
    );

    // Add server as peer and connect
    let server_addr: SocketAddr = "127.0.0.1:28001".parse()?;
    let peer = registry.add_peer(&server_peer_id).await;

    println!("🔗 Attempting to connect to server at {}...", server_addr);
    match peer.connect(&server_addr).await {
        Ok(()) => {
            println!("✅ Successfully connected to server!");
            println!("🔑 Cryptographic authentication succeeded");
        }
        Err(e) => {
            println!("❌ Failed to connect: {}", e);
            return Err(e.into());
        }
    }

    // Wait for gossip to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Try to lookup the actor
    match registry.lookup("test_actor").await {
        Some(location) => {
            println!("✅ Found actor 'test_actor' at {}", location.address);
            println!("🎉 Test PASSED: Client with valid keypair connected successfully!");
        }
        None => {
            println!("❌ Actor 'test_actor' not found");
            println!("⚠️  Test FAILED: Connection may have succeeded but gossip didn't propagate");
        }
    }

    // Show stats
    let stats = registry.stats().await;
    println!("\n📊 Client stats:");
    println!("  Active peers: {}", stats.active_peers);
    println!("  Failed peers: {}", stats.failed_peers);
    println!("  Known actors: {}", stats.known_actors);

    Ok(())
}
