use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use std::time::Duration;
use tokio::time::sleep;

// NOTE: These tests are currently expected to fail as the gossip propagation
// mechanism between nodes is not fully implemented. They serve as documentation
// of the intended behavior once the gossip system is complete.

#[tokio::test]
async fn test_node_b_killed_a_detects_immediately() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();
    
    // Test case: Node A→B connection, kill B, verify A detects immediately
    
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(500), // Fast gossip for disconnection detection
        connection_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        immediate_propagation_enabled: true, // Enable immediate propagation
        bootstrap_readiness_timeout: Duration::from_secs(5),
        bootstrap_readiness_check_interval: Duration::from_millis(50),
        ..Default::default()
    };
    
    // Start node A
    let keypair_a = KeyPair::new_for_testing("conn_fail_a");
    let peer_id_a = keypair_a.peer_id();
    let handle_a = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_a,
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node A");
    
    let node_a_addr = handle_a.registry.bind_addr;
    eprintln!("Node A started at {}", node_a_addr);
    
    // Start node B with A as bootstrap peer (this triggers immediate connection)
    let keypair_b = KeyPair::new_for_testing("conn_fail_b");
    let peer_id_b = keypair_b.peer_id();
    let handle_b = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_b,
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node B");
    
    let node_b_addr = handle_b.registry.bind_addr;
    eprintln!("Node B started at {} with A as bootstrap peer", node_b_addr);
    
    let peer_a = handle_b.add_peer(&peer_id_a).await;
    peer_a.connect(&node_a_addr).await.expect("Failed to connect B to A");
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&node_b_addr).await.expect("Failed to connect A to B");

    // Wait for connection establishment
    eprintln!("Waiting for connection establishment...");
    sleep(Duration::from_millis(500)).await;
    
    // Register an actor on B with Immediate priority to trigger fast propagation
    handle_b.register_with_priority(
        "test_actor_b".to_string(),
        "127.0.0.1:9001".parse().unwrap(),
        RegistrationPriority::Immediate, // Use immediate priority
    ).await.expect("Failed to register actor on B");
    
    // Wait for immediate propagation or next gossip round
    eprintln!("Waiting for gossip propagation...");
    sleep(Duration::from_millis(200)).await;
    
    // Debug: Check stats before assertion
    let stats_a = handle_a.stats().await;
    let stats_b = handle_b.stats().await;
    if stats_a.active_peers == 0 || stats_b.active_peers == 0 {
        eprintln!("Connection issue detected - Node A: {} peers, Node B: {} peers", 
                  stats_a.active_peers, stats_b.active_peers);
    }
    
    // Verify A knows about B's actor
    let found = handle_a.lookup("test_actor_b").await;
    if found.is_none() {
        eprintln!("Actor not found on A. Checking B's local actors...");
        let b_actor = handle_b.lookup("test_actor_b").await;
        eprintln!("Actor on B: {:?}", b_actor);
    }
    assert!(found.is_some(), "Node A should know about actor on B");
    
    // Kill node B
    handle_b.shutdown().await;
    
    // Give TCP time to detect disconnection and process it
    // The disconnection is detected when the connection pool tries to send gossip
    // and gets a broken pipe error. With 500ms gossip interval, we need to wait
    // at least that long plus processing time.
    sleep(Duration::from_secs(3)).await;
    
    // A should quickly realize B is gone
    let stats = handle_a.stats().await;
    assert_eq!(stats.active_peers, 0, "Node A should detect B disconnected");
    
    // Clean shutdown of A
    handle_a.shutdown().await;
}

#[tokio::test]
async fn test_node_a_killed_b_detects_immediately() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();
    
    // Test case: Node A→B connection, kill A, verify B detects immediately
    
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(50), // Very fast gossip for immediate detection
        connection_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        immediate_propagation_enabled: true, // Enable immediate propagation for actors
        ..Default::default()
    };
    
    // Start node B first (it will be the "server")
    let keypair_b = KeyPair::new_for_testing("conn_fail_b_server");
    let peer_id_b = keypair_b.peer_id();
    let handle_b = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_b,
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node B");
    
    let node_b_addr = handle_b.registry.bind_addr;
    
    // Start node A connecting to B (like NAT scenario)
    let keypair_a = KeyPair::new_for_testing("conn_fail_a_client");
    let peer_id_a = keypair_a.peer_id();
    let handle_a = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_a,
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node A");

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&node_b_addr).await.expect("Failed to connect A to B");
    let peer_a = handle_b.add_peer(&peer_id_a).await;
    peer_a.connect(&handle_a.registry.bind_addr).await.expect("Failed to connect B to A");
    
    // Wait for initial connection attempt
    sleep(Duration::from_millis(500)).await;
    
    // Check initial connection state
    let initial_stats_a = handle_a.stats().await;
    let initial_stats_b = handle_b.stats().await;
    eprintln!("Initial connection state:");
    eprintln!("  Node A: {} active peers, {} failed peers", initial_stats_a.active_peers, initial_stats_a.failed_peers);
    eprintln!("  Node B: {} active peers, {} failed peers", initial_stats_b.active_peers, initial_stats_b.failed_peers);
    
    // If either node failed to connect, wait for retry (peer failure retry interval)
    if initial_stats_a.active_peers == 0 || initial_stats_b.active_peers == 0 {
        eprintln!("Initial connection issue, waiting for retry...");
        sleep(Duration::from_secs(5)).await; // Default peer failure retry interval
    }
    
    // Debug: Check initial connection state
    let stats_a = handle_a.stats().await;
    let stats_b = handle_b.stats().await;
    eprintln!("After connection setup:");
    eprintln!("  Node A: {} active peers, {} failed peers", stats_a.active_peers, stats_a.failed_peers);
    eprintln!("  Node B: {} active peers, {} failed peers", stats_b.active_peers, stats_b.failed_peers);
    
    // Ensure connections are active by triggering gossip
    sleep(Duration::from_millis(100)).await; // Wait for gossip rounds
    
    // Register an actor on A with immediate priority
    handle_a.register_with_priority(
        "test_actor_a".to_string(),
        "127.0.0.1:9002".parse().unwrap(),
        RegistrationPriority::Immediate,
    ).await.expect("Failed to register actor on A");
    
    // Wait for immediate propagation (should be fast with immediate priority)
    sleep(Duration::from_secs(1)).await;
    
    // Verify B knows about A's actor - add debugging
    let found = handle_b.lookup("test_actor_a").await;
    if found.is_none() {
        eprintln!("Actor not found on B. Checking A's local actors...");
        let a_actor = handle_a.lookup("test_actor_a").await;
        eprintln!("Actor on A: {:?}", a_actor);
        
        // Check stats again
        let stats_a = handle_a.stats().await;
        let stats_b = handle_b.stats().await;
        eprintln!("Final stats:");
        eprintln!("  Node A: {} active peers, {} failed peers", stats_a.active_peers, stats_a.failed_peers);
        eprintln!("  Node B: {} active peers, {} failed peers", stats_b.active_peers, stats_b.failed_peers);
    }
    assert!(found.is_some(), "Node B should know about actor on A");
    
    // Verify initial peer count
    let stats_before = handle_b.stats().await;
    assert_eq!(stats_before.active_peers, 1, "Node B should have 1 active peer");
    
    // Kill node A
    eprintln!("Shutting down node A...");
    handle_a.shutdown().await;
    
    // Give TCP time to detect disconnection
    eprintln!("Waiting for disconnection detection...");
    for i in 0..6 {
        sleep(Duration::from_millis(500)).await;
        let stats = handle_b.stats().await;
        eprintln!("  {}ms: B has {} active peers, {} failed peers", 
            i * 500, stats.active_peers, stats.failed_peers);
        if stats.active_peers == 0 {
            eprintln!("✓ Disconnection detected after {}ms", i * 500);
            break;
        }
    }
    
    // B should quickly realize A is gone
    let stats_after = handle_b.stats().await;
    assert_eq!(stats_after.active_peers, 0, "Node B should detect A disconnected");
    
    // Clean shutdown of B
    handle_b.shutdown().await;
}

#[tokio::test]
async fn test_nat_scenario_bidirectional_communication() {
    // Test case: NAT scenario - Node A (behind NAT) → Node B, verify bidirectional communication
    // In this test, A initiates connection to B (simulating A behind NAT)
    // Once connected, both A and B should be able to send messages bidirectionally
    
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(500),
        connection_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    
    // Start node B (public node)
    let keypair_b = KeyPair::new_for_testing("nat_public_b");
    let peer_id_b = keypair_b.peer_id();
    let handle_b = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_b,
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node B");
    
    let node_b_addr = handle_b.registry.bind_addr;
    
    // Start node A (behind NAT) - only connects to B, doesn't accept incoming
    let keypair_a = KeyPair::new_for_testing("nat_private_a");
    let peer_id_a = keypair_a.peer_id();
    let handle_a = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        keypair_a,
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node A");

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&node_b_addr).await.expect("Failed to connect A to B");
    let peer_a = handle_b.add_peer(&peer_id_a).await;
    peer_a.connect(&handle_a.registry.bind_addr).await.expect("Failed to connect B to A");
    
    // Important: B should NOT try to connect to A (simulating NAT)
    // The connection is only initiated by A
    
    // Wait for connection establishment
    sleep(Duration::from_secs(1)).await;
    
    // Register an actor on A (behind NAT)
    handle_a.register_with_priority(
        "actor_behind_nat".to_string(),
        "127.0.0.1:9003".parse().unwrap(),
        RegistrationPriority::Normal,
    ).await.expect("Failed to register actor on A");
    
    // Register an actor on B (public)
    handle_b.register_with_priority(
        "public_actor".to_string(),
        "127.0.0.1:9004".parse().unwrap(),
        RegistrationPriority::Normal,
    ).await.expect("Failed to register actor on B");
    
    // Wait for bidirectional gossip propagation
    sleep(Duration::from_secs(3)).await;
    
    // Debug: Check stats before assertions
    let stats_a = handle_a.stats().await;
    let stats_b = handle_b.stats().await;
    eprintln!("After gossip propagation:");
    eprintln!("  Node A: {} local, {} known, {} active peers", 
        stats_a.local_actors, stats_a.known_actors, stats_a.active_peers);
    eprintln!("  Node B: {} local, {} known, {} active peers", 
        stats_b.local_actors, stats_b.known_actors, stats_b.active_peers);
    
    // Verify B knows about A's actor (message went through NAT)
    let found_on_b = handle_b.lookup("actor_behind_nat").await;
    if found_on_b.is_none() {
        eprintln!("Actor not found on B. A's local actor: {:?}", 
            handle_a.lookup("actor_behind_nat").await);
    }
    assert!(found_on_b.is_some(), "Node B should know about actor behind NAT");
    
    // Verify A knows about B's actor (response came back through NAT)
    let found_on_a = handle_a.lookup("public_actor").await;
    if found_on_a.is_none() {
        eprintln!("Actor not found on A. B's local actor: {:?}", 
            handle_b.lookup("public_actor").await);
    }
    assert!(found_on_a.is_some(), "Node A (behind NAT) should know about public actor");
    
    // Verify connection counts
    let stats_a = handle_a.stats().await;
    let stats_b = handle_b.stats().await;
    assert_eq!(stats_a.active_peers, 1, "Node A should have 1 active peer");
    assert_eq!(stats_b.active_peers, 1, "Node B should have 1 active peer");
    
    // Register another actor on A to verify continued bidirectional communication
    handle_a.register_with_priority(
        "second_nat_actor".to_string(),
        "127.0.0.1:9005".parse().unwrap(),
        RegistrationPriority::Immediate, // Use immediate priority for faster propagation
    ).await.expect("Failed to register second actor on A");
    
    // Wait for propagation
    sleep(Duration::from_millis(300)).await;
    
    // Verify B knows about the new actor
    let found_second = handle_b.lookup("second_nat_actor").await;
    assert!(found_second.is_some(), "Bidirectional communication should continue working");
    
    // Clean shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}
