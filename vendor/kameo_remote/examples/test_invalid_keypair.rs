use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;

/// Test that demonstrates failed connection with invalid keypair
/// The client uses a different keypair than what the server expects
/// Run server first: cargo run --example test_invalid_keypair server
/// Then client: cargo run --example test_invalid_keypair client
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let mode = if args.len() > 1 { &args[1] } else { "server" };

    match mode {
        "server" => run_server().await,
        "client" => run_client_with_wrong_key().await,
        _ => {
            println!("Usage: cargo run --example test_invalid_keypair [server|client]");
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

    let server_addr: SocketAddr = "127.0.0.1:28101".parse()?;
    let registry =
        GossipRegistryHandle::new_with_keypair(server_addr, server_keypair, Some(config)).await?;

    // Register a test actor
    registry
        .register("test_actor".to_string(), "127.0.0.1:28102".parse()?)
        .await?;

    println!(
        "✅ Server listening on {} with PeerId: {}",
        server_addr, server_peer_id
    );
    println!("📝 Registered actor: test_actor");

    // Configure expected client (with correct public key)
    let expected_client_keypair = KeyPair::new_for_testing("client_key");
    let expected_client_peer_id = expected_client_keypair.peer_id();
    println!(
        "⚠️  Server expects client with PeerId: {}",
        expected_client_peer_id
    );

    // Add the expected client as a configured peer
    let _expected_client_addr: SocketAddr = "127.0.0.1:28103".parse()?;
    let _peer = registry.add_peer(&expected_client_peer_id).await;
    // Note: We don't connect TO the client, we just configure it as an expected peer

    println!("⏳ Waiting for client connections...");
    println!(
        "   Only clients with PeerId {} will be accepted",
        expected_client_peer_id
    );

    // Keep server running and monitor connections
    let mut iteration = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        iteration += 1;

        let stats = registry.stats().await;
        println!("\n📊 [{}s] Server status:", iteration * 5);
        println!("  Active peers: {}", stats.active_peers);
        println!("  Failed peers: {}", stats.failed_peers);

        if stats.failed_peers > 0 {
            println!("  ✅ Test PASSED: Invalid client was rejected!");
        }
    }
}

async fn run_client_with_wrong_key() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔐 Starting client with WRONG Ed25519 keypair");

    // Generate WRONG client keypair (different from what server expects)
    let wrong_client_keypair = KeyPair::new_for_testing("impostor_key"); // WRONG KEY!
    let wrong_client_peer_id = wrong_client_keypair.peer_id();
    println!("❌ Client using WRONG PeerId: {}", wrong_client_peer_id);
    println!("❌ Client public key: {}", wrong_client_peer_id.to_hex());

    // Show what the correct key would be
    let correct_client_keypair = KeyPair::new_for_testing("client_key");
    let correct_client_peer_id = correct_client_keypair.peer_id();
    println!("✅ Server expects PeerId: {}", correct_client_peer_id);

    // Create config with WRONG keypair
    let config = GossipConfig {
        key_pair: Some(wrong_client_keypair.clone()),
        ..Default::default()
    };

    let client_addr: SocketAddr = "127.0.0.1:28103".parse()?;
    let registry =
        GossipRegistryHandle::new_with_keypair(client_addr, wrong_client_keypair, Some(config))
            .await?;

    println!(
        "\n✅ Client started on {} with WRONG PeerId: {}",
        client_addr, wrong_client_peer_id
    );

    // Try to connect to server
    let server_keypair = KeyPair::new_for_testing("server_key");
    let server_peer_id = server_keypair.peer_id();
    let server_addr: SocketAddr = "127.0.0.1:28101".parse()?;

    // Add server as peer
    let peer = registry.add_peer(&server_peer_id).await;

    println!(
        "\n🔗 Attempting to connect to server at {} with WRONG credentials...",
        server_addr
    );
    match peer.connect(&server_addr).await {
        Ok(()) => {
            println!("❌ SECURITY ISSUE: Connection succeeded with wrong keypair!");
            println!("   This should NOT happen - authentication should have failed");

            // Try to lookup the actor to confirm connection works
            tokio::time::sleep(Duration::from_millis(500)).await;
            match registry.lookup("test_actor").await {
                Some(location) => {
                    println!(
                        "❌ CRITICAL: Found actor 'test_actor' at {}",
                        location.address
                    );
                    println!("   Impostor gained access to protected resources!");
                }
                None => {
                    println!("⚠️  Connection may have partially succeeded but gossip didn't work");
                }
            }
        }
        Err(e) => {
            println!("✅ Connection correctly REJECTED: {}", e);
            println!("🔐 Authentication system working properly!");
            println!("\n🎉 Test PASSED: Client with invalid keypair was rejected!");
        }
    }

    // Show stats
    let stats = registry.stats().await;
    println!("\n📊 Client stats:");
    println!("  Active peers: {}", stats.active_peers);
    println!("  Failed peers: {}", stats.failed_peers);
    println!("  Known actors: {}", stats.known_actors);

    if stats.active_peers > 0 {
        println!("\n❌ TEST FAILED: Should not have any active peers!");
    } else {
        println!("\n✅ TEST PASSED: No connections established with wrong keypair");
    }

    Ok(())
}
