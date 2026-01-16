use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // Initialize logging with debug level
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .init();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(10),
        ..Default::default()
    };

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");
    let peer_id_a = key_pair_a.peer_id();

    // Start Node A
    println!("Starting Node A on 127.0.0.1:8001...");
    let handle_a = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:8001".parse().unwrap(),
        key_pair_a,
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node A");
    println!("Node A started");

    // Wait a bit
    sleep(Duration::from_secs(1)).await;

    // Start Node B
    println!("Starting Node B on 127.0.0.1:8002...");
    let handle_b = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:8002".parse().unwrap(),
        key_pair_b,
        Some(config),
    )
    .await
    .expect("Failed to create node B");
    println!("Node B started");

    let peer_a = handle_b.add_peer(&peer_id_a).await;
    peer_a
        .connect(&"127.0.0.1:8001".parse().unwrap())
        .await
        .expect("Failed to connect node B to node A");

    // Monitor stats
    for i in 0..10 {
        sleep(Duration::from_secs(2)).await;

        let stats_a = handle_a.stats().await;
        let stats_b = handle_b.stats().await;

        println!("\n=== Round {} ===", i + 1);
        println!(
            "Node A - Active: {}, Failed: {}",
            stats_a.active_peers, stats_a.failed_peers
        );
        println!(
            "Node B - Active: {}, Failed: {}",
            stats_b.active_peers, stats_b.failed_peers
        );

        // Check connections
        let pool_a = handle_a.registry.connection_pool.lock().await;
        let pool_b = handle_b.registry.connection_pool.lock().await;

        println!("Node A connections: {}", pool_a.connection_count());
        println!("Node B connections: {}", pool_b.connection_count());
    }
}
