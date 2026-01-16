use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RemoteActorLocation};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;

/// Helper function to create a test registry config
fn create_test_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_millis(25), // Very fast gossip for testing
        max_peer_failures: 3,
        dead_peer_timeout: Duration::from_secs(60),
        connection_timeout: Duration::from_secs(5),
        immediate_propagation_enabled: true, // Enable immediate gossip
        ..Default::default()
    }
}

fn test_keypair(name: &str) -> KeyPair {
    KeyPair::new_for_testing(name)
}

fn test_peer_id(name: &str) -> kameo_remote::PeerId {
    test_keypair(name).peer_id()
}

/// Helper function to create a test actor location
fn create_test_actor_location(addr: SocketAddr, peer_name: &str) -> RemoteActorLocation {
    let peer_id = test_peer_id(peer_name);
    RemoteActorLocation::new_with_peer(addr, peer_id)
}

#[tokio::test]
async fn test_register_actor_sync_real_single_peer() {
    // Test: Register with 1 real remote peer - should wait for real ACK

    println!("🚀 Starting real peer test...");

    // Create two registry handles on different ports
    let node1_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let node2_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let config = create_test_config();

    // Start first node
    let handle1 = GossipRegistryHandle::new_with_keypair(
        node1_addr,
        test_keypair("node1"),
        Some(config.clone()),
    )
    .await
    .expect("Failed to start node1");
    let node1_actual_addr = handle1.registry.bind_addr;
    println!("✅ Node1 started on {}", node1_actual_addr);

    // Start second node
    let handle2 = GossipRegistryHandle::new_with_keypair(
        node2_addr,
        test_keypair("node2"),
        Some(config.clone()),
    )
    .await
    .expect("Failed to start node2");
    let node2_actual_addr = handle2.registry.bind_addr;
    println!("✅ Node2 started on {}", node2_actual_addr);

    // Connect nodes as peers
    let peer2_id = test_peer_id("node2");
    let peer2 = handle1.add_peer(&peer2_id).await;
    peer2
        .connect(&node2_actual_addr)
        .await
        .expect("Failed to connect to node2");

    let peer1_id = test_peer_id("node1");
    let peer1 = handle2.add_peer(&peer1_id).await;
    peer1
        .connect(&node1_actual_addr)
        .await
        .expect("Failed to connect to node1");

    println!("🔗 Nodes connected as peers");

    // Wait for connection to stabilize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Register actor on node1 with sync - should wait for node2's ACK
    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location(node1_actual_addr, "node1");

    println!(
        "📝 Registering actor '{}' with sync on node1...",
        actor_name
    );

    let start = std::time::Instant::now();
    let result = timeout(
        Duration::from_secs(5), // Generous timeout
        handle1.registry.register_actor_sync(
            actor_name.clone(),
            actor_location,
            Duration::from_millis(2000), // Wait up to 2 seconds for ACK
        ),
    )
    .await;
    let elapsed = start.elapsed();

    println!("⏱️  Registration completed in {:?}", elapsed);

    // Verify result
    match result {
        Ok(Ok(())) => {
            println!("✅ Registration succeeded - got ACK from peer!");

            // Should have taken some time (not immediate) because it waited for ACK
            assert!(elapsed >= Duration::from_millis(10)); // At least some delay
            assert!(elapsed <= Duration::from_millis(3000)); // But not too long (real networking takes time)

            // Verify actor was registered on both nodes through gossip
            tokio::time::sleep(Duration::from_millis(1000)).await; // Let gossip propagate

            let node1_lookup = handle1.lookup(&actor_name).await;
            let node2_lookup = handle2.lookup(&actor_name).await;

            println!("🔍 Node1 lookup result: {:?}", node1_lookup.is_some());
            println!("🔍 Node2 lookup result: {:?}", node2_lookup.is_some());

            assert!(node1_lookup.is_some(), "Actor should be findable on node1");
            assert!(
                node2_lookup.is_some(),
                "Actor should be findable on node2 via gossip"
            );
        }
        Ok(Err(e)) => {
            panic!("Registration failed: {:?}", e);
        }
        Err(_) => {
            panic!("Registration timed out - no ACK received in 5 seconds");
        }
    }

    // Cleanup
    handle1.shutdown().await;
    handle2.shutdown().await;

    println!("✅ Test completed successfully");
}

#[tokio::test]
async fn test_register_actor_sync_real_no_peers_vs_with_peers() {
    // Test: Compare timing between no peers vs with peers

    println!("🚀 Starting no-peers vs with-peers comparison test...");

    let config = create_test_config();

    // === TEST 1: No peers - should be immediate ===
    let solo_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let solo_handle = GossipRegistryHandle::new_with_keypair(
        solo_addr,
        test_keypair("solo"),
        Some(config.clone()),
    )
    .await
    .expect("Failed to start solo node");
    let solo_actual_addr = solo_handle.registry.bind_addr;

    println!("✅ Solo node started on {}", solo_actual_addr);

    let actor_name_solo = "solo_actor".to_string();
    let actor_location_solo = create_test_actor_location(solo_actual_addr, "solo_node");

    let start_solo = std::time::Instant::now();
    let result_solo = solo_handle
        .registry
        .register_actor_sync(
            actor_name_solo.clone(),
            actor_location_solo,
            Duration::from_secs(1),
        )
        .await;
    let elapsed_solo = start_solo.elapsed();

    println!("⏱️  Solo registration completed in {:?}", elapsed_solo);
    assert!(result_solo.is_ok(), "Solo registration should succeed");
    assert!(
        elapsed_solo < Duration::from_millis(50),
        "Solo registration should be immediate"
    );

    // === TEST 2: With peer - should wait for ACK ===
    let node1_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let node2_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let handle1 = GossipRegistryHandle::new_with_keypair(
        node1_addr,
        test_keypair("node1"),
        Some(config.clone()),
    )
    .await
    .expect("Failed to start node1");
    let node1_actual_addr = handle1.registry.bind_addr;

    let handle2 = GossipRegistryHandle::new_with_keypair(
        node2_addr,
        test_keypair("node2"),
        Some(config.clone()),
    )
    .await
    .expect("Failed to start node2");
    let node2_actual_addr = handle2.registry.bind_addr;

    // Connect as peers
    let peer2_id = test_peer_id("node2");
    let peer2 = handle1.add_peer(&peer2_id).await;
    peer2
        .connect(&node2_actual_addr)
        .await
        .expect("Failed to connect to node2");

    let peer1_id = test_peer_id("node1");
    let peer1 = handle2.add_peer(&peer1_id).await;
    peer1
        .connect(&node1_actual_addr)
        .await
        .expect("Failed to connect to node1");

    // Wait for connection
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("🔗 Peer nodes connected");

    let actor_name_peer = "peer_actor".to_string();
    let actor_location_peer = create_test_actor_location(node1_actual_addr, "node1");

    let start_peer = std::time::Instant::now();
    let result_peer = timeout(
        Duration::from_secs(5),
        handle1.registry.register_actor_sync(
            actor_name_peer.clone(),
            actor_location_peer,
            Duration::from_millis(2000),
        ),
    )
    .await;
    let elapsed_peer = start_peer.elapsed();

    println!("⏱️  Peer registration completed in {:?}", elapsed_peer);

    match result_peer {
        Ok(Ok(())) => {
            // Should have taken longer than solo (waited for ACK)
            assert!(
                elapsed_peer >= Duration::from_millis(10),
                "Peer registration should take longer than solo (waited for ACK)"
            );
            assert!(
                elapsed_peer <= Duration::from_millis(3000),
                "Peer registration should not timeout (real networking takes time)"
            );

            println!("✅ Timing validation passed:");
            println!("   Solo (no peers): {:?} (immediate)", elapsed_solo);
            println!("   With peer: {:?} (waited for ACK)", elapsed_peer);

            // Verify gossip propagation
            tokio::time::sleep(Duration::from_millis(1000)).await;

            let node1_lookup = handle1.lookup(&actor_name_peer).await;
            let node2_lookup = handle2.lookup(&actor_name_peer).await;

            assert!(node1_lookup.is_some(), "Actor should be on node1");
            assert!(node2_lookup.is_some(), "Actor should propagate to node2");
        }
        Ok(Err(e)) => panic!("Peer registration failed: {:?}", e),
        Err(_) => panic!("Peer registration timed out"),
    }

    // Cleanup
    solo_handle.shutdown().await;
    handle1.shutdown().await;
    handle2.shutdown().await;

    println!("✅ Comparison test completed successfully");
}

#[tokio::test]
async fn test_register_actor_sync_real_multiple_peers() {
    // Test: Register with multiple real peers - should get ACK from first responder

    println!("🚀 Starting multiple peers test...");

    let config = create_test_config();

    // Create 4 nodes (1 registering + 3 peers)
    let mut handles = Vec::new();
    let mut addrs = Vec::new();

    for i in 0..4 {
        let addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let node_name = format!("node{}", i);
        let handle = GossipRegistryHandle::new_with_keypair(
            addr,
            test_keypair(&node_name),
            Some(config.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("Failed to start node{}", i));
        let actual_addr = handle.registry.bind_addr;

        handles.push(handle);
        addrs.push(actual_addr);
        println!("✅ Node{} started on {}", i, actual_addr);
    }

    // Connect all nodes as peers (full mesh)
    for (i, handle) in handles.iter().enumerate() {
        for (j, addr) in addrs.iter().enumerate() {
            if i != j {
                let peer_id = test_peer_id(&format!("node{}", j));
                let peer = handle.add_peer(&peer_id).await;
                peer.connect(addr)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to connect node{} to node{}", i, j));
            }
        }
    }

    println!("🔗 All nodes connected in full mesh");

    // Wait for connections to stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Register actor on node0 - should get ACK from any of the 3 peers
    let actor_name = "multi_peer_actor".to_string();
    let actor_location = create_test_actor_location(addrs[0], "node0");

    println!("📝 Registering actor with 3 peers...");

    let start = std::time::Instant::now();
    let result = timeout(
        Duration::from_secs(5),
        handles[0].registry.register_actor_sync(
            actor_name.clone(),
            actor_location,
            Duration::from_millis(3000),
        ),
    )
    .await;
    let elapsed = start.elapsed();

    println!("⏱️  Multi-peer registration completed in {:?}", elapsed);

    match result {
        Ok(Ok(())) => {
            println!("✅ Registration succeeded - got ACK from at least one peer!");

            // Should have waited for ACK but not timed out
            assert!(elapsed >= Duration::from_millis(10)); // Some delay
            assert!(elapsed <= Duration::from_millis(4000)); // Not timeout (real networking)

            // Wait for full gossip propagation
            tokio::time::sleep(Duration::from_millis(2000)).await;

            // Verify actor is visible on all nodes
            for (i, handle) in handles.iter().enumerate() {
                let lookup = handle.lookup(&actor_name).await;
                assert!(lookup.is_some(), "Actor should be visible on node{}", i);
                println!("🔍 Node{} can find actor: ✅", i);
            }
        }
        Ok(Err(e)) => panic!("Multi-peer registration failed: {:?}", e),
        Err(_) => panic!("Multi-peer registration timed out"),
    }

    // Cleanup
    for (i, handle) in handles.into_iter().enumerate() {
        handle.shutdown().await;
        println!("🔄 Node{} shut down", i);
    }

    println!("✅ Multiple peers test completed successfully");
}

#[tokio::test]
async fn test_register_actor_sync_real_peer_timeout() {
    // Test: Real peer that doesn't respond - should timeout gracefully

    println!("🚀 Starting peer timeout test...");

    let config = GossipConfig {
        gossip_interval: Duration::from_millis(50),
        max_peer_failures: 3,
        dead_peer_timeout: Duration::from_secs(60),
        connection_timeout: Duration::from_secs(5),
        immediate_propagation_enabled: false, // Disable immediate gossip to simulate slow peer
        ..Default::default()
    };

    // Start two nodes
    let handle1 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        test_keypair("node1_timeout"),
        Some(config.clone()),
    )
    .await
    .expect("Failed to start node1");
    let node1_addr = handle1.registry.bind_addr;

    let handle2 = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        test_keypair("node2_timeout"),
        Some(config.clone()),
    )
    .await
    .expect("Failed to start node2");
    let node2_addr = handle2.registry.bind_addr;

    // Connect as peers
    let peer2_id = test_peer_id("node2_timeout");
    let peer2 = handle1.add_peer(&peer2_id).await;
    peer2
        .connect(&node2_addr)
        .await
        .expect("Failed to connect to node2");

    let peer1_id = test_peer_id("node1_timeout");
    let peer1 = handle2.add_peer(&peer1_id).await;
    peer1
        .connect(&node1_addr)
        .await
        .expect("Failed to connect to node1");

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("🔗 Nodes connected, testing timeout scenario");

    // Simulate slow/unresponsive peer by shutting down node2 immediately
    handle2.shutdown().await;
    println!("🔇 Node2 shut down to simulate unresponsive peer");

    // Try to register on node1 - should timeout since peer is gone
    let actor_name = "timeout_actor".to_string();
    let actor_location = create_test_actor_location(node1_addr, "node1");

    let start = std::time::Instant::now();
    let result = handle1
        .registry
        .register_actor_sync(
            actor_name.clone(),
            actor_location,
            Duration::from_millis(500), // Short timeout
        )
        .await;
    let elapsed = start.elapsed();

    println!("⏱️  Registration with timeout completed in {:?}", elapsed);

    // Should succeed but take full timeout duration (graceful fallback)
    assert!(
        result.is_ok(),
        "Registration should succeed even on timeout"
    );
    assert!(
        elapsed >= Duration::from_millis(450),
        "Should wait roughly full timeout"
    );
    assert!(
        elapsed <= Duration::from_millis(800),
        "Should not exceed timeout by much"
    );

    // Actor should still be registered locally
    let lookup = handle1.lookup(&actor_name).await;
    assert!(
        lookup.is_some(),
        "Actor should be registered despite timeout"
    );

    println!("✅ Timeout behavior validated - graceful fallback works");

    // Cleanup
    handle1.shutdown().await;

    println!("✅ Timeout test completed successfully");
}
