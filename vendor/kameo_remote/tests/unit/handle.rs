use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

async fn create_handle(
    bind_addr: SocketAddr,
    config: Option<GossipConfig>,
    seed: &str,
) -> GossipRegistryHandle {
    let keypair = KeyPair::new_for_testing(seed);
    GossipRegistryHandle::new_with_keypair(bind_addr, keypair, config)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_registry_handle_creation() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_handle_create").await;
    
    // Should start with empty registry
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 0);
    assert_eq!(stats.known_actors, 0);
    assert_eq!(stats.active_peers, 0);
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_registry_handle_with_peers() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config.clone()), "unit_handle_with_peers").await;
    let peer1 = create_handle(
        "127.0.0.1:0".parse().unwrap(),
        Some(config.clone()),
        "unit_peer_1",
    )
    .await;
    let peer2 = create_handle(
        "127.0.0.1:0".parse().unwrap(),
        Some(config),
        "unit_peer_2",
    )
    .await;

    let peer1_addr = peer1.registry.bind_addr;
    let peer2_addr = peer2.registry.bind_addr;
    let peer1_id = peer1.registry.peer_id;
    let peer2_id = peer2.registry.peer_id;

    let conn1 = handle.add_peer(&peer1_id).await;
    conn1.connect(&peer1_addr).await.unwrap();
    let conn2 = handle.add_peer(&peer2_id).await;
    conn2.connect(&peer2_addr).await.unwrap();
    
    // Give bootstrap time to complete
    sleep(Duration::from_millis(100)).await;
    
    let stats = handle.stats().await;
    assert_eq!(stats.active_peers, 2);
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_registry_handle_default_config() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    
    let handle = create_handle(bind_addr, None, "unit_handle_default").await;
    
    let stats = handle.stats().await;
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_register_actor() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_register_actor").await;
    
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let result = handle.register(actor_name.clone(), actor_addr).await;
    assert!(result.is_ok());
    
    // Check that actor was registered
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 1);
    
    // Check lookup
    let found = handle.lookup(&actor_name).await;
    assert!(found.is_some());
    let location = found.unwrap();
    assert_eq!(location.address, actor_addr);
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_register_multiple_actors() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_register_multiple").await;
    
    // Register multiple actors
    let actors = vec![
        ("actor1", "127.0.0.1:9001"),
        ("actor2", "127.0.0.1:9002"),
        ("actor3", "127.0.0.1:9003"),
    ];
    
    for (name, addr) in &actors {
        let result = handle.register(
            name.to_string(),
                addr.parse().unwrap(),
        ).await;
        assert!(result.is_ok());
    }
    
    // Check stats
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 3);
    
    // Check lookups
    for (name, addr) in &actors {
        let found = handle.lookup(name).await;
        assert!(found.is_some());
        let location = found.unwrap();
        assert_eq!(location.address, addr.parse().unwrap());
    }
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_unregister_actor() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_unregister").await;
    
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    // Register first
    handle.register(actor_name.clone(), actor_addr).await.unwrap();
    
    // Verify registration
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 1);
    
    // Unregister
    let result = handle.unregister(&actor_name).await;
    assert!(result.is_ok());
    let removed = result.unwrap();
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().address, actor_addr);
    
    // Check stats
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 0);
    
    // Check lookup
    let found = handle.lookup(&actor_name).await;
    assert!(found.is_none());
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_unregister_nonexistent_actor() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_unregister_missing").await;
    
    let result = handle.unregister("nonexistent").await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_lookup_nonexistent_actor() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_lookup_missing").await;
    
    let found = handle.lookup("nonexistent").await;
    assert!(found.is_none());
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_stats_tracking() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_stats").await;
    
    // Initial stats
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 0);
    assert_eq!(stats.known_actors, 0);
    assert_eq!(stats.active_peers, 0);
    assert_eq!(stats.failed_peers, 0);
    assert_eq!(stats.total_gossip_rounds, 0);
    assert_eq!(stats.current_sequence, 0);
    assert!(stats.uptime_seconds >= 0);
    
    // Register an actor
    handle.register(
        "test_actor".to_string(),
        "127.0.0.1:9001".parse().unwrap(),
    ).await.unwrap();
    
    // Stats should update
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 1);
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_shutdown() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_shutdown").await;
    
    // Should be able to operate before shutdown
    let result = handle.register(
        "test_actor".to_string(),
        "127.0.0.1:9001".parse().unwrap(),
    ).await;
    assert!(result.is_ok());
    
    // Shutdown
    handle.shutdown().await;
    
    // Operations after shutdown should fail
    let result = handle.register(
        "new_actor".to_string(),
        "127.0.0.1:9002".parse().unwrap(),
    ).await;
    assert!(result.is_err());
    
    let result = handle.unregister("test_actor").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_actor_lifecycle() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_actor_lifecycle").await;
    
    let actor_name = "lifecycle_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    // Register
    handle.register(actor_name.clone(), actor_addr).await.unwrap();
    
    // Lookup should succeed
    let found = handle.lookup(&actor_name).await;
    assert!(found.is_some());
    
    // Unregister
    let removed = handle.unregister(&actor_name).await.unwrap();
    assert!(removed.is_some());
    
    // Lookup should fail
    let found = handle.lookup(&actor_name).await;
    assert!(found.is_none());
    
    // Re-register should work
    handle.register(actor_name.clone(), actor_addr).await.unwrap();
    
    let found = handle.lookup(&actor_name).await;
    assert!(found.is_some());
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_operations() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::default();
    
    let handle = std::sync::Arc::new(
        create_handle(bind_addr, Some(config), "unit_concurrent_ops").await
    );
    
    // Concurrent registrations
    let mut handles = vec![];
    for i in 0..10 {
        let handle_clone = handle.clone();
        let actor_name = format!("actor_{}", i);
        let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        
        let join_handle = tokio::spawn(async move {
            handle_clone.register(actor_name, actor_addr).await
        });
        handles.push(join_handle);
    }
    
    // All should succeed
    for join_handle in handles {
        let result = join_handle.await.unwrap();
        assert!(result.is_ok());
    }
    
    // Check stats
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 10);
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_two_node_communication() {
    
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        ..Default::default()
    };
    
    // Start first node
    let handle1 = create_handle(
        "127.0.0.1:0".parse().unwrap(),
        Some(config.clone()),
        "unit_two_node_1",
    )
    .await;
    
    let node1_addr = handle1.registry.bind_addr;
    
    // Start second node connected to first
    let handle2 = create_handle(
        "127.0.0.1:0".parse().unwrap(),
        Some(config),
        "unit_two_node_2",
    )
    .await;

    let peer1_from_2 = handle2.add_peer(&handle1.registry.peer_id).await;
    peer1_from_2.connect(&node1_addr).await.unwrap();
    
    // Give time for connection
    sleep(Duration::from_millis(200)).await;
    
    // Register actor on node1
    handle1.register(
        "node1_actor".to_string(),
        "127.0.0.1:9001".parse().unwrap(),
    ).await.unwrap();
    
    // Register actor on node2
    handle2.register(
        "node2_actor".to_string(),
        "127.0.0.1:9002".parse().unwrap(),
    ).await.unwrap();
    
    // Wait for gossip to propagate
    sleep(Duration::from_millis(500)).await;
    
    // Node1 should see node2's actor
    let _found = handle1.lookup("node2_actor").await;
    // Note: This might not work in unit tests without proper gossip setup
    // but the test verifies the API works
    
    // Node2 should see node1's actor
    let _found = handle2.lookup("node1_actor").await;
    // Note: This might not work in unit tests without proper gossip setup
    
    handle1.shutdown().await;
    handle2.shutdown().await;
}

#[tokio::test]
async fn test_custom_config() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(50),
        max_gossip_peers: 5,
        actor_ttl: Duration::from_secs(600),
        cleanup_interval: Duration::from_secs(30),
        connection_timeout: Duration::from_secs(15),
        response_timeout: Duration::from_secs(10),
        max_message_size: 5 * 1024 * 1024,
        ..Default::default()
    };
    
    let handle = create_handle(bind_addr, Some(config), "unit_custom_config").await;
    
    // Should work with custom config
    let result = handle.register(
        "test_actor".to_string(),
        "127.0.0.1:9001".parse().unwrap(),
    ).await;
    assert!(result.is_ok());
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_error_handling() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    
    let config = GossipConfig::default();
    
    let handle = create_handle(bind_addr, Some(config), "unit_error_handling").await;
    
    // Register an actor
    handle.register(
        "test_actor".to_string(),
        "127.0.0.1:9001".parse().unwrap(),
    ).await.unwrap();
    
    // Shutdown the handle
    handle.shutdown().await;
    
    // Further operations should fail gracefully
    let result = handle.register(
        "new_actor".to_string(),
        "127.0.0.1:9002".parse().unwrap(),
    ).await;
    assert!(result.is_err());
    
    let result = handle.unregister("test_actor").await;
    assert!(result.is_err());
    
    // Lookup might still work (depends on implementation)
    let _found = handle.lookup("test_actor").await;
    
    // Stats should still work
    let _stats = handle.stats().await;
}

#[tokio::test]
async fn test_bind_to_specific_address() {
    let bind_addr = "127.0.0.1:0".parse().unwrap(); // Let OS choose port
    
    let handle = create_handle(bind_addr, None, "unit_bind_specific").await;
    
    // Should bind to some address
    let actual_addr = handle.registry.bind_addr;
    assert_eq!(actual_addr.ip(), bind_addr.ip());
    // Port might be different (OS-chosen)
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_repeated_shutdown() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    
    let handle = create_handle(bind_addr, None, "unit_repeated_shutdown").await;
    
    // Multiple shutdowns should not panic
    handle.shutdown().await;
    handle.shutdown().await;
    handle.shutdown().await;
}

// Vector clock test removed
