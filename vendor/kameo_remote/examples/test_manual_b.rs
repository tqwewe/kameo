use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use std::{net::SocketAddr, time::Duration};
use tokio::time::sleep;

// Test configuration constant
const TEST_GOSSIP_INTERVAL_SECS: u64 = 10;

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .init();

    let node_a_keypair = KeyPair::new_for_testing("node_a");
    let node_b_keypair = KeyPair::new_for_testing("node_b");
    let node_c_keypair = KeyPair::new_for_testing("node_c");

    let config = GossipConfig {
        key_pair: Some(node_b_keypair.clone()),
        gossip_interval: Duration::from_secs(TEST_GOSSIP_INTERVAL_SECS),
        ..Default::default()
    };

    println!("Starting Node B on 127.0.0.1:8002...");
    let handle = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:8002".parse().unwrap(),
        node_b_keypair.clone(),
        Some(config),
    )
    .await
    .expect("Failed to create node B");

    println!("Node B started at {}", handle.registry.bind_addr);

    // Add peers and attempt connections
    let peer_a = handle.add_peer(&node_a_keypair.peer_id()).await;
    let peer_c = handle.add_peer(&node_c_keypair.peer_id()).await;

    println!("Attempting to connect to node_a...");
    match peer_a.connect(&"127.0.0.1:8001".parse().unwrap()).await {
        Ok(_) => println!("Connected to node_a"),
        Err(e) => println!("Failed to connect to node_a: {}", e),
    }

    println!("Attempting to connect to node_c...");
    match peer_c.connect(&"127.0.0.1:8003".parse().unwrap()).await {
        Ok(_) => println!("Connected to node_c"),
        Err(e) => println!("Failed to connect to node_c: {}", e),
    }

    // Register a local actor with immediate priority for fast propagation
    handle
        .register_with_priority(
            "node_b".to_string(),
            "127.0.0.1:9002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .expect("Failed to register actor");
    println!("Registered node_b on Node B with immediate priority");

    // Monitor peer status
    loop {
        sleep(Duration::from_secs(2)).await;
        let stats = handle.stats().await;

        // Get detailed peer status
        let (peer_status, configured_peers, connected_peers) = {
            let gossip_state = handle.registry.gossip_state.lock().await;
            let pool = handle.registry.connection_pool.lock().await;
            let actor_state = handle.registry.actor_state.read().await;
            let mut status = Vec::new();
            let mut configured_count = 0;
            let mut connected_count = 0;

            // Check all configured peers (from peer_id_to_addr)
            for entry in pool.peer_id_to_addr.iter() {
                let peer_id = entry.key();
                let addr = entry.value();
                configured_count += 1;

                // Check if we have an active connection to this node
                let has_connection = pool.get_connection_by_peer_id(peer_id).is_some();
                if has_connection {
                    connected_count += 1;
                }

                // Find actors registered from this peer
                let mut peer_actors: Vec<String> = Vec::new();

                // Check known actors
                for (name, location) in &actor_state.known_actors {
                    if let Ok(actor_addr) = location.address.parse::<SocketAddr>() {
                        // Match by port since actors use different ports (9001, 9002, 9003)
                        match (addr.port(), actor_addr.port()) {
                            (8001, 9001) => peer_actors.push(name.clone()),
                            (8002, 9002) => peer_actors.push(name.clone()),
                            (8003, 9003) => peer_actors.push(name.clone()),
                            _ => {}
                        }
                    }
                }

                // Also check local actors
                for (name, location) in &actor_state.local_actors {
                    if let Ok(actor_addr) = location.address.parse::<SocketAddr>() {
                        match (addr.port(), actor_addr.port()) {
                            (8001, 9001) => peer_actors.push(name.clone()),
                            (8002, 9002) => peer_actors.push(name.clone()),
                            (8003, 9003) => peer_actors.push(name.clone()),
                            _ => {}
                        }
                    }
                }

                // Check failure status from gossip_state if available
                let failure_info = gossip_state.peers.get(addr);
                let is_failed = failure_info
                    .map(|info| info.failures >= handle.registry.config.max_peer_failures)
                    .unwrap_or(false);
                let failures = failure_info.map(|info| info.failures).unwrap_or(0);

                let status_str = if has_connection {
                    format!("CONNECTED [{}]", peer_actors.join(", "))
                } else if is_failed {
                    format!("FAILED (failures: {})", failures)
                } else {
                    "NOT_CONNECTED".to_string()
                };
                status.push(format!("  {} ({}) - {}", peer_id, addr, status_str));
            }
            (status, configured_count, connected_count)
        };

        println!(
            "Node B - Configured peers: {}, Connected: {}, Active: {}, Failed: {}, Known actors: {}, Local actors: {}",
            configured_peers, connected_peers, stats.active_peers, stats.failed_peers, stats.known_actors, stats.local_actors
        );
        println!("Peer Status:");
        for peer in peer_status {
            println!("{}", peer);
        }
    }
}
