use kameo_remote::registry::*;
use kameo_remote::{ActorLocation, GossipConfig, GossipError, KeyPair, RegistrationPriority};
use std::net::SocketAddr;
use std::time::Duration;

fn test_config(seed: &str) -> GossipConfig {
    GossipConfig {
        key_pair: Some(KeyPair::new_for_testing(seed)),
        ..Default::default()
    }
}

fn create_test_registry() -> GossipRegistry {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = test_config("unit_registry");
    GossipRegistry::new(bind_addr, config)
}

fn create_test_actor_location(addr: SocketAddr) -> ActorLocation {
    ActorLocation {
        address: addr,
        wall_clock_time: kameo_remote::current_timestamp(),
        priority: RegistrationPriority::Normal,
        local_registration_time: 0,
    }
}

#[tokio::test]
async fn test_registry_creation() {
    let bind_addr = "127.0.0.1:8080".parse().unwrap();
    let config = test_config("unit_registry_creation");
    
    let registry = GossipRegistry::new(bind_addr, config.clone());
    
    assert_eq!(registry.bind_addr, bind_addr);
    assert_eq!(registry.config.gossip_interval, config.gossip_interval);
    
    // Test initial state
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 0);
    assert_eq!(stats.known_actors, 0);
    assert_eq!(stats.active_peers, 0);
    assert!(!registry.is_shutdown().await);
}

#[tokio::test]
async fn test_register_actor() {
    let registry = create_test_registry();
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let result = registry.register_actor(
        actor_name.clone(),
        create_test_actor_location(actor_addr)
    ).await;
    
    assert!(result.is_ok());
    
    // Check stats
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 1);
    assert_eq!(stats.known_actors, 0);
    
    // Check lookup
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_some());
    let location = found.unwrap();
    assert_eq!(location.address, actor_addr);
}

#[tokio::test]
async fn test_register_actor_after_shutdown() {
    let registry = create_test_registry();
    registry.shutdown().await;
    
    let result = registry.register_actor(
        "test_actor".to_string(),
        create_test_actor_location("127.0.0.1:9001".parse().unwrap())
    ).await;
    
    assert!(matches!(result, Err(GossipError::Shutdown)));
}

#[tokio::test]
async fn test_unregister_actor() {
    let registry = create_test_registry();
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    // Register first
    let location = create_test_actor_location(actor_addr);
    registry.register_actor(actor_name.clone(), location.clone()).await.unwrap();
    
    // Unregister
    let result = registry.unregister_actor(&actor_name).await;
    assert!(result.is_ok());
    let removed = result.unwrap();
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().address, actor_addr);
    
    // Check stats
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 0);
    
    // Check lookup
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_none());
}

#[tokio::test]
async fn test_unregister_nonexistent_actor() {
    let registry = create_test_registry();
    
    let result = registry.unregister_actor("nonexistent").await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn test_unregister_actor_after_shutdown() {
    let registry = create_test_registry();
    registry.shutdown().await;
    
    let result = registry.unregister_actor("test_actor").await;
    assert!(matches!(result, Err(GossipError::Shutdown)));
}

#[tokio::test]
async fn test_lookup_actor_ttl() {
    let registry = create_test_registry();
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    // Create a known actor (simulating remote actor)
    let mut location = create_test_actor_location(actor_addr);
    location.wall_clock_time = kameo_remote::current_timestamp() - 1000; // 1000 seconds ago
    
    {
        let mut actor_state = registry.actor_state.write().await;
        actor_state.known_actors.insert(actor_name.clone(), location);
    }
    
    // Should not find expired actor
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_none());
}

#[tokio::test]
async fn test_lookup_actor_priority() {
    let registry = create_test_registry();
    let actor_name = "test_actor".to_string();
    let local_addr = "127.0.0.1:9001".parse().unwrap();
    let remote_addr = "127.0.0.1:9002".parse().unwrap();
    
    // Add both local and known actor with same name
    let local_location = create_test_actor_location(local_addr);
    let remote_location = create_test_actor_location(remote_addr);
    
    {
        let mut actor_state = registry.actor_state.write().await;
        actor_state.local_actors.insert(actor_name.clone(), local_location);
        actor_state.known_actors.insert(actor_name.clone(), remote_location);
    }
    
    // Should find local actor (priority)
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_some());
    assert_eq!(found.unwrap().address, local_addr);
}

#[tokio::test]
async fn test_add_bootstrap_peers() {
    let registry = create_test_registry();
    let peer1 = "127.0.0.1:8001".parse().unwrap();
    let peer2 = "127.0.0.1:8002".parse().unwrap();
    let peers = vec![peer1, peer2, registry.bind_addr]; // Include own address
    
    registry.add_bootstrap_peers(peers).await;
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.active_peers, 2); // Own address should be filtered out
}

#[tokio::test]
async fn test_add_peer() {
    let registry = create_test_registry();
    let peer_addr = "127.0.0.1:8001".parse().unwrap();
    
    registry.add_peer(peer_addr).await;
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.active_peers, 1);
    
    // Adding same peer again should not duplicate
    registry.add_peer(peer_addr).await;
    let stats = registry.get_stats().await;
    assert_eq!(stats.active_peers, 1);
    
    // Adding own address should be ignored
    registry.add_peer(registry.bind_addr).await;
    let stats = registry.get_stats().await;
    assert_eq!(stats.active_peers, 1);
}

#[tokio::test]
async fn test_apply_delta_actor_added() {
    let registry = create_test_registry();
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    let actor_name = "remote_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let actor_location = create_test_actor_location(actor_addr);
    
    let change = RegistryChange::ActorAdded {
        name: actor_name.clone(),
        location: actor_location,
        priority: RegistrationPriority::Normal,
    };
    
    let delta = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![change],
        sender_addr,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let result = registry.apply_delta(delta).await;
    assert!(result.is_ok());
    
    // Check that actor was added
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_some());
    assert_eq!(found.unwrap().address, actor_addr);
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.known_actors, 1);
}

#[tokio::test]
async fn test_apply_delta_actor_removed() {
    let registry = create_test_registry();
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    let actor_name = "remote_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    // First add an actor
    let actor_location = create_test_actor_location(actor_addr);
    {
        let mut actor_state = registry.actor_state.write().await;
        actor_state.known_actors.insert(actor_name.clone(), actor_location);
    }
    
    // Now remove it via delta
    let change = RegistryChange::ActorRemoved {
        name: actor_name.clone(),
        priority: RegistrationPriority::Normal,
    };
    
    let delta = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![change],
        sender_addr,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let result = registry.apply_delta(delta).await;
    assert!(result.is_ok());
    
    // Check that actor was removed
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_none());
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.known_actors, 0);
}

// VectorClock conflict resolution test removed

#[tokio::test]
async fn test_apply_delta_local_actor_protection() {
    let registry = create_test_registry();
    let actor_name = "local_actor".to_string();
    let local_addr = "127.0.0.1:9001".parse().unwrap();
    let remote_addr = "127.0.0.1:9002".parse().unwrap();
    
    // Register local actor
    let local_location = create_test_actor_location(local_addr);
    registry.register_actor(actor_name.clone(), local_location).await.unwrap();
    
    // Try to override with remote actor
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    
    let remote_location = create_test_actor_location(remote_addr);
    
    let change = RegistryChange::ActorAdded {
        name: actor_name.clone(),
        location: remote_location,
        priority: RegistrationPriority::Normal,
    };
    
    let delta = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![change],
        sender_addr,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let result = registry.apply_delta(delta).await;
    assert!(result.is_ok());
    
    // Should still have local actor
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_some());
    assert_eq!(found.unwrap().address, local_addr);
}

#[tokio::test]
async fn test_cleanup_stale_actors() {
    let registry = create_test_registry();
    let actor_name = "stale_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    // Add actor with old timestamp
    let mut location = create_test_actor_location(actor_addr);
    location.wall_clock_time = kameo_remote::current_timestamp() - 1000; // Very old
    
    {
        let mut actor_state = registry.actor_state.write().await;
        actor_state.known_actors.insert(actor_name.clone(), location);
    }
    
    // Cleanup should remove stale actor
    registry.cleanup_stale_actors().await;
    
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_none());
}

// Tests removed: test_record_node_activity, test_record_vector_clock_activity, test_vector_clock_gc
// These tests are no longer relevant after removing NodeId and VectorClock

#[tokio::test]
async fn test_prepare_gossip_round_no_peers() {
    let registry = create_test_registry();
    
    let result = registry.prepare_gossip_round().await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
async fn test_prepare_gossip_round_after_shutdown() {
    let registry = create_test_registry();
    registry.shutdown().await;
    
    let result = registry.prepare_gossip_round().await;
    assert!(matches!(result, Err(GossipError::Shutdown)));
}

#[tokio::test]
async fn test_prepare_gossip_round_with_peers() {
    let registry = create_test_registry();
    let peer_addr = "127.0.0.1:8001".parse().unwrap();
    
    // Add peer and some local actors
    registry.add_peer(peer_addr).await;
    registry.register_actor(
        "test_actor".to_string(),
        create_test_actor_location("127.0.0.1:9001".parse().unwrap())
    ).await.unwrap();
    
    let result = registry.prepare_gossip_round().await;
    assert!(result.is_ok());
    
    let tasks = result.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].peer_addr, peer_addr);
}

#[tokio::test]
async fn test_shutdown_and_is_shutdown() {
    let registry = create_test_registry();
    
    assert!(!registry.is_shutdown().await);
    
    registry.shutdown().await;
    assert!(registry.is_shutdown().await);
}

#[tokio::test]
async fn test_gossip_config_default() {
    let config = GossipConfig::default();
    
    assert_eq!(config.gossip_interval, Duration::from_secs(5));
    assert_eq!(config.max_gossip_peers, 3);
    assert_eq!(config.actor_ttl, Duration::from_secs(300));
    assert_eq!(config.max_message_size, 10 * 1024 * 1024);
    assert_eq!(config.max_delta_history, 100);
    assert_eq!(config.full_sync_interval, 50);
}

#[tokio::test]
async fn test_registry_stats_calculation() {
    let registry = create_test_registry();
    
    // Add some test data
    registry.add_peer("127.0.0.1:8001".parse().unwrap()).await;
    registry.add_peer("127.0.0.1:8002".parse().unwrap()).await;
    
    registry.register_actor(
        "local1".to_string(),
        create_test_actor_location("127.0.0.1:9001".parse().unwrap())
    ).await.unwrap();
    
    registry.register_actor(
        "local2".to_string(),
        create_test_actor_location("127.0.0.1:9002".parse().unwrap())
    ).await.unwrap();
    
    // Add known actor
    {
        let mut actor_state = registry.actor_state.write().await;
        actor_state.known_actors.insert(
            "remote1".to_string(),
            create_test_actor_location("127.0.0.1:9003".parse().unwrap())
        );
    }
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 2);
    assert_eq!(stats.known_actors, 1);
    assert_eq!(stats.active_peers, 2);
    assert_eq!(stats.failed_peers, 0);
}

// VectorClock deduplicate changes test removed
