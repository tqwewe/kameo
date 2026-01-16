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
        .with_env_filter("kameo_remote=debug,test_bidirectional_tls=info")
        .init();

    info!("Starting bidirectional TLS communication test");

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
    let addr_a: SocketAddr = "127.0.0.1:7011".parse()?;
    let registry_a =
        GossipRegistryHandle::new_with_tls(addr_a, secret_key_a, Some(config.clone())).await?;
    info!("Node A started with TLS on {}", addr_a);

    // Start Node B with TLS
    let addr_b: SocketAddr = "127.0.0.1:7012".parse()?;
    let registry_b =
        GossipRegistryHandle::new_with_tls(addr_b, secret_key_b, Some(config.clone())).await?;
    info!("Node B started with TLS on {}", addr_b);

    // Register an actor on Node A
    registry_a
        .register("actor_on_a".to_string(), "127.0.0.1:8011".parse()?)
        .await?;
    info!("Registered actor_on_a on Node A");

    // Register an actor on Node B
    registry_b
        .register("actor_on_b".to_string(), "127.0.0.1:8012".parse()?)
        .await?;
    info!("Registered actor_on_b on Node B");

    // Add Node B as a peer of Node A with NodeId
    registry_a
        .registry
        .add_peer_with_node_id(addr_b, Some(node_id_b))
        .await;
    info!("Node A added Node B as peer with NodeId");

    // Add Node A as a peer of Node B with NodeId
    registry_b
        .registry
        .add_peer_with_node_id(addr_a, Some(node_id_a))
        .await;
    info!("Node B added Node A as peer with NodeId");

    info!("Waiting for TLS-encrypted bidirectional gossip to propagate...");
    sleep(Duration::from_secs(5)).await;

    // Test bidirectional communication:
    // 1. Check if Node A knows about actor_on_b
    let actor_on_b_from_a = registry_a.lookup("actor_on_b").await;
    let has_actor_on_b = match &actor_on_b_from_a {
        Some(location) => {
            info!(
                "✅ Node A successfully learned about actor_on_b at {:?}",
                location
            );
            true
        }
        None => {
            error!("❌ Node A did not learn about actor_on_b");
            false
        }
    };

    // 2. Check if Node B knows about actor_on_a
    let actor_on_a_from_b = registry_b.lookup("actor_on_a").await;
    let has_actor_on_a = match &actor_on_a_from_b {
        Some(location) => {
            info!(
                "✅ Node B successfully learned about actor_on_a at {:?}",
                location
            );
            true
        }
        None => {
            error!("❌ Node B did not learn about actor_on_a");
            false
        }
    };

    // 3. Check stats to see connection status
    let stats_a = registry_a.stats().await;
    info!(
        "Node A stats: known_actors={}, active_peers={}, failed_peers={}",
        stats_a.known_actors, stats_a.active_peers, stats_a.failed_peers
    );

    let stats_b = registry_b.stats().await;
    info!(
        "Node B stats: known_actors={}, active_peers={}, failed_peers={}",
        stats_b.known_actors, stats_b.active_peers, stats_b.failed_peers
    );

    // Check if bidirectional communication was successful
    let success =
        has_actor_on_b && has_actor_on_a && stats_a.failed_peers == 0 && stats_b.failed_peers == 0;

    if success {
        info!("🎉 Bidirectional TLS communication test PASSED!");
    } else {
        error!("❌ Bidirectional TLS communication test FAILED!");
        error!("Debug: Check the logs for TLS handshake errors or connection pool issues");
    }

    Ok(())
}
