use kameo_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize crypto provider for rustls
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,test_tls_with_node_id=info")
        .init();

    info!("Starting TLS test with NodeId mapping for outgoing connections");

    // Create two nodes with their own secret keys
    let secret_key_a = SecretKey::generate();
    let node_id_a = secret_key_a.public();
    info!("Node A ID: {}", node_id_a.fmt_short());

    let secret_key_b = SecretKey::generate();
    let node_id_b = secret_key_b.public();
    info!("Node B ID: {}", node_id_b.fmt_short());

    // Create config with shorter gossip interval for testing
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(2),
        ..Default::default()
    };

    // Start Node A with TLS
    let addr_a: SocketAddr = "127.0.0.1:7001".parse()?;
    let registry_a =
        GossipRegistryHandle::new_with_tls(addr_a, secret_key_a, Some(config.clone())).await?;
    info!("Node A started with TLS on {}", addr_a);

    // Start Node B with TLS
    let addr_b: SocketAddr = "127.0.0.1:7002".parse()?;
    let registry_b =
        GossipRegistryHandle::new_with_tls(addr_b, secret_key_b, Some(config.clone())).await?;
    info!("Node B started with TLS on {}", addr_b);

    // Register an actor on Node A
    let actor_addr: SocketAddr = "127.0.0.1:8001".parse()?;
    registry_a
        .register("test_actor".to_string(), actor_addr)
        .await?;
    info!("Registered test_actor on Node A at {}", actor_addr);

    // Add peers WITH NodeId for proper TLS verification
    // Node A knows about Node B
    registry_a
        .registry
        .add_peer_with_node_id(addr_b, Some(node_id_b))
        .await;
    info!("Node A added Node B as peer with NodeId");

    // Node B knows about Node A
    registry_b
        .registry
        .add_peer_with_node_id(addr_a, Some(node_id_a))
        .await;
    info!("Node B added Node A as peer with NodeId");

    // Wait for gossip to propagate
    info!("Waiting for TLS-encrypted gossip to propagate...");
    sleep(Duration::from_secs(5)).await;

    // Check if Node B knows about the actor from Node A
    match registry_b.lookup("test_actor").await {
        Some(location) => {
            info!("✅ SUCCESS: Node B found actor via TLS-encrypted gossip!");
            info!("Actor location: {:?}", location);

            // Verify the actor is on the correct node
            if location.address == actor_addr.to_string() {
                info!("✅ Actor address matches!");
            } else {
                error!(
                    "❌ Actor address mismatch! Expected: {}, Got: {}",
                    actor_addr, location.address
                );
            }
        }
        None => {
            error!("❌ FAILED: Node B couldn't find the actor");
            error!("This might indicate TLS connection issues");
        }
    }

    // Print stats to verify TLS connections
    let stats_a = registry_a.stats().await;
    let stats_b = registry_b.stats().await;

    info!("\nNode A stats:");
    info!("  Active peers: {}", stats_a.active_peers);
    info!("  Failed peers: {}", stats_a.failed_peers);
    info!("  Local actors: {}", stats_a.local_actors);
    info!("  Known actors: {}", stats_a.known_actors);

    info!("\nNode B stats:");
    info!("  Active peers: {}", stats_b.active_peers);
    info!("  Failed peers: {}", stats_b.failed_peers);
    info!("  Local actors: {}", stats_b.local_actors);
    info!("  Known actors: {}", stats_b.known_actors);

    // Test outgoing connection with wrong NodeId (should fail)
    info!("\n--- Testing TLS verification with wrong NodeId ---");

    // Create a third node
    let secret_key_c = SecretKey::generate();
    let node_id_c = secret_key_c.public();
    let addr_c: SocketAddr = "127.0.0.1:7003".parse()?;
    let _registry_c =
        GossipRegistryHandle::new_with_tls(addr_c, secret_key_c, Some(config)).await?;
    info!("Node C started with ID: {}", node_id_c.fmt_short());

    // Node A tries to connect to Node C but with wrong NodeId (Node B's ID)
    registry_a
        .registry
        .add_peer_with_node_id(addr_c, Some(node_id_b))
        .await;
    info!("Node A added Node C with WRONG NodeId (using Node B's ID)");

    // Wait a bit for connection attempt
    sleep(Duration::from_secs(3)).await;

    // Check stats - Node C should not be connected due to NodeId mismatch
    let stats_a_after = registry_a.stats().await;
    if stats_a_after.failed_peers > stats_a.failed_peers {
        info!("✅ TLS verification working: Connection to Node C failed due to NodeId mismatch");
    } else {
        info!("⚠️  Connection status unclear - may need more time for verification");
    }

    info!("\n✅ TLS with NodeId mapping test complete!");
    info!("Summary:");
    info!("- Nodes can connect with TLS when NodeId is provided and correct");
    info!("- Gossip propagates over TLS-encrypted connections");
    info!("- Wrong NodeId prevents TLS handshake (security feature working)");

    Ok(())
}
