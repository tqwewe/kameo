use kameo_remote::{GossipConfig, GossipRegistryHandle, NodeId, SecretKey};
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tracing::debug;

/// TLS-enabled gossip client - connects to TLS server
///
/// Usage:
///   cargo run --example tls_client <server_port> [server_pub_key] [client_key] [--wrong-key]
///
/// Examples:
///   # Connect with auto-generated client key
///   cargo run --example tls_client 9001 /tmp/kameo_tls_server.pub
///   
///   # Connect with specific client key
///   cargo run --example tls_client 9001 /tmp/kameo_tls_server.pub /tmp/client1.key
///   
///   # Test authentication failure with wrong NodeId
///   cargo run --example tls_client 9001 /tmp/kameo_tls_server.pub --wrong-key
///
/// This client:
/// 1. Loads or generates its own Ed25519 keypair
/// 2. Loads the server's public key (NodeId)
/// 3. Connects to the server using TLS with mutual authentication
/// 4. Registers some client actors
/// 5. Discovers server actors through gossip
/// 6. Can test authentication failures with --wrong-key flag
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
                .unwrap_or_else(|_| "kameo_remote=info,tls_client=info".to_string()),
        )
        .init();

    println!("🔐 TLS Gossip Client");
    println!("====================\n");

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!(
            "Usage: {} <server_port> [server_pub_key] [client_key] [--wrong-key]",
            args[0]
        );
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} 9001 /tmp/kameo_tls_server.pub", args[0]);
        eprintln!(
            "  {} 9001 /tmp/kameo_tls_server.pub /tmp/client1.key",
            args[0]
        );
        eprintln!("  {} 9001 /tmp/kameo_tls_server.pub --wrong-key", args[0]);
        return Ok(());
    }

    let server_port: u16 = args[1].parse()?;
    let server_pub_path = args
        .get(2)
        .map(|s| s.as_str())
        .unwrap_or("/tmp/kameo_tls_server.pub");

    // Check for --wrong-key flag
    let use_wrong_key = args.iter().any(|arg| arg == "--wrong-key");

    let client_key_path = if use_wrong_key {
        None // Will generate a random key
    } else {
        args.get(3)
            .filter(|s| *s != "--wrong-key")
            .map(|s| s.as_str())
    };

    // Load server's public key (NodeId)
    println!("📋 Loading server information:");
    let server_node_id = match load_server_node_id(server_pub_path) {
        Ok(node_id) => {
            println!("   Server NodeId: {}", node_id.fmt_short());
            println!("   Server public key from: {}", server_pub_path);
            node_id
        }
        Err(e) => {
            eprintln!(
                "❌ Error: Could not load server public key from {}",
                server_pub_path
            );
            eprintln!("   {}", e);
            eprintln!();
            eprintln!("💡 Make sure the server is running first:");
            eprintln!("   cargo run --example tls_server 9001");
            eprintln!();
            eprintln!("The server will generate its keypair and save the public key to:");
            eprintln!("   {}", server_pub_path);
            return Ok(());
        }
    };

    let server_addr: SocketAddr = format!("127.0.0.1:{}", server_port).parse()?;
    println!("   Server address: {}", server_addr);
    println!();

    // Load or generate client keypair
    println!("📋 Client Configuration:");
    let (client_secret_key, client_key_source) = if use_wrong_key {
        println!("   ⚠️  TESTING MODE: Using wrong NodeId for authentication failure test");
        let wrong_key = SecretKey::generate();
        (wrong_key, "generated for testing".to_string())
    } else {
        let key_path = client_key_path.unwrap_or("/tmp/kameo_tls_client.key");
        let key = load_or_generate_key(key_path).await?;
        (key, key_path.to_string())
    };

    let client_node_id = client_secret_key.public();
    println!("   Client NodeId: {}", client_node_id.fmt_short());
    println!("   Client key: {}", client_key_source);
    println!();

    // What NodeId to tell the server we are (for testing wrong key scenario)
    let claimed_server_node_id = if use_wrong_key {
        // Claim we're someone else to test authentication failure
        println!("   ❌ Will claim wrong server NodeId to test authentication failure");
        println!("   Expected NodeId: {}", server_node_id.fmt_short());
        let wrong_node_id = SecretKey::generate().public();
        println!("   Claiming NodeId: {}", wrong_node_id.fmt_short());
        wrong_node_id
    } else {
        server_node_id
    };

    // Create TLS-enabled gossip registry for client
    let bind_addr = "127.0.0.1:0".parse()?;
    println!("🚀 Starting TLS gossip client...");

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(5),
        ..Default::default()
    };

    let registry =
        GossipRegistryHandle::new_with_tls(bind_addr, client_secret_key, Some(config)).await?;

    let actual_addr = registry.registry.bind_addr;
    println!("✅ Client listening on: {}", actual_addr);
    println!();

    // Register some client actors
    println!("📝 Registering client actors:");
    registry
        .register("client.status".to_string(), "127.0.0.1:11001".parse()?)
        .await?;
    println!("   - client.status -> 127.0.0.1:11001");

    registry
        .register("client.data".to_string(), "127.0.0.1:11002".parse()?)
        .await?;
    println!("   - client.data -> 127.0.0.1:11002");
    println!();

    // Add server as peer with its NodeId for TLS verification
    println!(
        "🔗 Adding server as peer at {} with NodeId {}",
        server_addr,
        claimed_server_node_id.fmt_short()
    );
    registry
        .registry
        .add_peer_with_node_id(server_addr, Some(claimed_server_node_id))
        .await;

    if use_wrong_key {
        println!();
        println!("⏳ Attempting to connect to server (should fail due to public key mismatch)...");
        println!(
            "   We claim the server has NodeId: {}",
            claimed_server_node_id.fmt_short()
        );
        println!(
            "   But server actually has NodeId: {}",
            server_node_id.fmt_short()
        );

        // Actually try to connect to the server - this should fail!
        let connection_result = registry.registry.get_connection(server_addr).await;

        match connection_result {
            Ok(_) => {
                println!("❌ ERROR: Connection succeeded but should have failed!");
                println!("   This is a security vulnerability - authentication bypassed!");
                return Err(
                    "Security test failed: connection succeeded with wrong public key".into(),
                );
            }
            Err(e) => {
                println!("✅ Connection correctly rejected: {}", e);

                // Verify the error is about NodeId/PublicKey mismatch
                if e.to_string().contains("NodeId mismatch") {
                    println!("✅ TEST PASSED: Public key verification working correctly!");
                    println!();
                    println!("Explanation:");
                    println!("- NodeId IS the Ed25519 public key (they're the same thing)");
                    println!(
                        "- Server's certificate contains its public key: {}",
                        server_node_id.fmt_short()
                    );
                    println!(
                        "- We expected a different public key: {}",
                        claimed_server_node_id.fmt_short()
                    );
                    println!("- TLS handshake correctly detected the mismatch and rejected the connection");
                    println!();
                    println!("This prevents man-in-the-middle attacks where an attacker");
                    println!("tries to impersonate the server with their own key.");
                } else {
                    println!("⚠️  Connection failed but not for the expected reason");
                    println!("   Expected: NodeId/PublicKey mismatch error");
                    println!("   Got: {}", e);
                }
            }
        }

        return Ok(());
    }

    // Normal operation - wait for gossip and display status
    println!("⏳ Waiting for gossip propagation...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check what actors we've discovered
    println!();
    println!("🔍 Discovered actors from server:");

    let server_actors = vec!["server.health", "server.metrics", "server.config"];

    let mut found_count = 0;
    for actor_name in &server_actors {
        if let Some(location) = registry.lookup(actor_name).await {
            println!("   ✅ {} -> {:?}", actor_name, location);
            found_count += 1;
        } else {
            println!("   ⏳ {} -> not found yet", actor_name);
        }
    }

    println!();
    println!("📊 Connection Status:");
    let stats = registry.registry.get_stats().await;
    println!("   Active peers: {}", stats.active_peers);
    println!("   Failed peers: {}", stats.failed_peers);
    println!("   Local actors: {}", stats.local_actors);
    println!("   Known actors: {}", stats.known_actors);
    println!("   Gossip rounds: {}", stats.total_gossip_rounds);

    if found_count == server_actors.len() {
        println!();
        println!("✅ SUCCESS: All server actors discovered via TLS gossip!");
    } else {
        println!();
        println!(
            "⚠️  Only found {}/{} server actors",
            found_count,
            server_actors.len()
        );
        println!("   Try running the client again - gossip may need more time");
    }

    // Keep running to maintain connection
    println!();
    println!("💡 Client will keep running. Press Ctrl+C to stop.");
    println!("   The server should show this client as an active peer.");

    // Keep the client running
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Periodically check for new actors
        let stats = registry.registry.get_stats().await;
        if stats.known_actors > stats.local_actors {
            debug!(
                "Currently know {} actors ({} local, {} remote)",
                stats.known_actors,
                stats.local_actors,
                stats.known_actors - stats.local_actors
            );
        }
    }
}

fn load_server_node_id(path: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
    let pub_key_hex = fs::read_to_string(path)?;
    let pub_key_bytes = hex::decode(pub_key_hex.trim())?;

    if pub_key_bytes.len() != 32 {
        return Err(format!(
            "Invalid public key length: expected 32, got {}",
            pub_key_bytes.len()
        )
        .into());
    }

    NodeId::from_bytes(&pub_key_bytes).map_err(|e| format!("Invalid NodeId: {}", e).into())
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

        // Also save the public key
        let pub_path = path.replace(".key", ".pub");
        let pub_hex = hex::encode(secret_key.public().as_bytes());
        fs::write(&pub_path, pub_hex)?;
        println!("   Saved public key to: {}", pub_path);

        Ok(secret_key)
    }
}
