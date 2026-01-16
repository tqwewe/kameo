use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_node_name_based_connections() {
    // Initialize logging for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .try_init();

    // Create configs
    let config_a = GossipConfig {
        gossip_interval: Duration::from_secs(5),
        ..Default::default()
    };

    let config_b = GossipConfig {
        gossip_interval: Duration::from_secs(5),
        ..Default::default()
    };

    // Start Node A first with TLS
    let key_pair_a = KeyPair::new_for_testing("test_node_a");
    let peer_id_a = key_pair_a.peer_id();
    let node_a = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(), // Use port 0 for dynamic allocation
        key_pair_a,
        Some(config_a),
    )
    .await
    .expect("Failed to create node A");

    let node_a_addr = node_a.registry.bind_addr;
    println!("Node A started at {}", node_a_addr);

    // Start Node B with TLS
    let key_pair_b = KeyPair::new_for_testing("test_node_b");
    let peer_id_b = key_pair_b.peer_id();
    let node_b = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        key_pair_b,
        Some(config_b),
    )
    .await
    .expect("Failed to create node B");

    let node_b_addr = node_b.registry.bind_addr;
    println!("Node B started at {}", node_b_addr);

    // Node B: Add node A as peer and connect
    let peer_a = node_b.add_peer(&peer_id_a).await;
    peer_a
        .connect(&node_a_addr)
        .await
        .expect("Failed to connect to node A");

    // Node A: Add node B as peer and connect
    let peer_b = node_a.add_peer(&peer_id_b).await;
    peer_b
        .connect(&node_b_addr)
        .await
        .expect("Failed to connect to node B");

    // Give nodes time to connect
    sleep(Duration::from_secs(2)).await;

    // Check that Node B has the expected mappings
    {
        let pool = node_b.registry.connection_pool.lock().await;
        println!("Node B pool state:");
        println!("  Total connections: {}", pool.connection_count());
        println!("  Node mappings: {}", pool.peer_id_to_addr.len());

        // Verify Node B knows about Node A
        assert!(pool.peer_id_to_addr.contains_key(&peer_id_a));
        assert_eq!(pool.connection_count(), 1);
    }

    // Check that Node A has learned about Node B
    {
        let pool = node_a.registry.connection_pool.lock().await;
        println!("Node A pool state:");
        println!("  Total connections: {}", pool.connection_count());
        println!("  Node mappings: {}", pool.peer_id_to_addr.len());

        // After exchange, Node A should know about Node B
        assert!(pool.peer_id_to_addr.contains_key(&peer_id_b));
    }

    // Clean shutdown
    node_a.shutdown().await;
    node_b.shutdown().await;
}
