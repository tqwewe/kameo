use kameo_remote::*;
use kameo_remote::registry::*;
use kameo_remote::RegistrationPriority;
use std::collections::HashMap;
use std::time::Duration;
use std::net::SocketAddr;

fn create_test_actor_location(addr: SocketAddr) -> ActorLocation {
    ActorLocation {
        address: addr,
        wall_clock_time: kameo_remote::current_timestamp(),
        priority: RegistrationPriority::Normal,
        local_registration_time: 0,
    }
}

fn test_config(seed: &str) -> GossipConfig {
    GossipConfig {
        key_pair: Some(KeyPair::new_for_testing(seed)),
        ..Default::default()
    }
}

#[test]
fn test_gossip_error_display() {
    let errors = vec![
        GossipError::Network(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused")),
        GossipError::Serialization(bincode::Error::new(bincode::ErrorKind::SizeLimit)),
        GossipError::MessageTooLarge { size: 2048, max: 1024 },
        GossipError::Timeout,
        GossipError::PeerNotFound("127.0.0.1:8001".parse().unwrap()),
        GossipError::ActorNotFound("missing_actor".to_string()),
        GossipError::Shutdown,
        GossipError::DeltaTooOld { requested: 5, oldest: 10 },
        GossipError::FullSyncRequired,
    ];
    
    for error in errors {
        let display = format!("{}", error);
        assert!(!display.is_empty());
        
        let debug = format!("{:?}", error);
        assert!(!debug.is_empty());
    }
}



#[test]
fn test_actor_location_edge_cases() {
    let addr = "127.0.0.1:9001".parse().unwrap();
    
    // Test creation
    let location = ActorLocation {
        address: addr,
        wall_clock_time: kameo_remote::current_timestamp(),
        priority: RegistrationPriority::Normal,
        local_registration_time: 0,
    };
    
    assert_eq!(location.address, addr);
    assert!(location.wall_clock_time > 0);
    
    // Test serialization
    let serialized = bincode::serialize(&location).unwrap();
    let deserialized: ActorLocation = bincode::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.address, location.address);
}

#[test]
fn test_gossip_config_edge_cases() {
    let config = GossipConfig::default();
    
    // Test reasonable defaults
    assert!(config.gossip_interval.as_millis() > 0);
    assert!(config.max_gossip_peers > 0);
    assert!(config.actor_ttl.as_secs() > 0);
    assert!(config.cleanup_interval.as_secs() > 0);
    assert!(config.connection_timeout.as_secs() > 0);
    assert!(config.response_timeout.as_secs() > 0);
    assert!(config.max_message_size > 0);
    assert!(config.max_peer_failures > 0);
    assert!(config.peer_retry_interval.as_secs() > 0);
    assert!(config.max_delta_history > 0);
    assert!(config.full_sync_interval > 0);
    
    // Test custom config
    let custom = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("edge_custom_config")),
        gossip_interval: Duration::from_millis(1),
        max_gossip_peers: 1,
        actor_ttl: Duration::from_secs(1),
        cleanup_interval: Duration::from_secs(1),
        connection_timeout: Duration::from_secs(1),
        response_timeout: Duration::from_secs(1),
        max_message_size: 1,
        max_peer_failures: 1,
        peer_retry_interval: Duration::from_secs(1),
        max_delta_history: 1,
        full_sync_interval: 1,
        ..Default::default()
    };
    
    assert_eq!(custom.gossip_interval.as_millis(), 1);
    assert_eq!(custom.max_gossip_peers, 1);
}

#[tokio::test]
async fn test_registry_with_empty_data() {
    let registry = GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("edge_empty_data"),
    );
    
    // Test with no peers
    let tasks = registry.prepare_gossip_round().await.unwrap();
    assert!(tasks.is_empty());
    
    // Test lookup on empty registry
    let found = registry.lookup_actor("nonexistent").await;
    assert!(found.is_none());
    
    // Test stats on empty registry
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 0);
    assert_eq!(stats.known_actors, 0);
    assert_eq!(stats.active_peers, 0);
    
    // Test cleanup on empty registry
    registry.cleanup_stale_actors().await;
    
    // Should still be empty
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 0);
    assert_eq!(stats.known_actors, 0);
}

#[tokio::test]
async fn test_registry_with_invalid_data() {
    let registry = GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("edge_invalid_data"),
    );
    
    // Test with invalid peer addresses (should be handled gracefully)
    registry.add_peer("127.0.0.1:8001".parse().unwrap()).await;
    registry.add_peer("127.0.0.1:8002".parse().unwrap()).await;
    
    // Test with zero port (should work)
    registry.add_peer("127.0.0.1:0".parse().unwrap()).await;
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.active_peers, 2); // Own address filtered out
}

#[tokio::test]
async fn test_delta_application_edge_cases() {
    let registry = GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("edge_delta_cases"),
    );
    
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    
    // Test empty delta
    let empty_delta = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![],
        sender_addr,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let result = registry.apply_delta(empty_delta).await;
    assert!(result.is_ok());
    
    // Test delta with invalid actor name (empty string)
    let invalid_change = RegistryChange::ActorAdded {
        name: "".to_string(),
        location: create_test_actor_location("127.0.0.1:9001".parse().unwrap()),
        priority: RegistrationPriority::Normal,
    };
    
    let delta_with_invalid = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![invalid_change],
        sender_addr,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let result = registry.apply_delta(delta_with_invalid).await;
    assert!(result.is_ok()); // Should handle gracefully
    
    // Check that empty name actor was added
    let found = registry.lookup_actor("").await;
    assert!(found.is_some());
}

#[tokio::test]
async fn test_registry_concurrent_modifications() {
    let registry = std::sync::Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("edge_concurrent_mods"),
    ));
    
    let mut handles = vec![];
    
    // Concurrent register operations
    for i in 0..10 {
        let reg = registry.clone();
        let handle = tokio::spawn(async move {
            let actor_name = format!("actor_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            let location = create_test_actor_location(actor_addr);
            reg.register_actor(actor_name, location).await
        });
        handles.push(handle);
    }
    
    // All should complete successfully
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 10);
}

#[tokio::test]
async fn test_registry_memory_pressure() {
    let registry = GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            max_delta_history: 3, // Very small history
            ..Default::default()
        },
    );
    
    // Add many actors to create memory pressure
    for i in 0..100 {
        let actor_name = format!("actor_{}", i);
        let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        let location = create_test_actor_location(actor_addr);
        registry.register_actor(actor_name, location).await.unwrap();
    }
    
    // Trigger gossip round to create delta history
    registry.add_peer("127.0.0.1:8001".parse().unwrap()).await;
    
    // Should handle memory pressure gracefully
    let _tasks = registry.prepare_gossip_round().await.unwrap();
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 100);
    assert!(stats.delta_history_size <= 3); // Should be limited
}

#[tokio::test]
async fn test_registry_time_edge_cases() {
    let registry = GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("edge_time_cases")),
            actor_ttl: Duration::from_millis(1), // Very short TTL
            ..Default::default()
        },
    );
    
    // Add a remote actor
    let actor_name = "remote_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    let location = create_test_actor_location(actor_addr);
    
    {
        let mut actor_state = registry.actor_state.write().await;
        actor_state.known_actors.insert(actor_name.clone(), location);
    }
    
    // Should find it immediately
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_some());
    
    // Wait for TTL to expire
    tokio::time::sleep(Duration::from_millis(5)).await;
    
    // Should not find expired actor
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_none());
}

#[tokio::test]
async fn test_registry_shutdown_edge_cases() {
    let registry = GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("edge_shutdown"),
    );
    
    // Multiple shutdowns should be idempotent
    registry.shutdown().await;
    registry.shutdown().await;
    registry.shutdown().await;
    
    assert!(registry.is_shutdown().await);
    
    // Operations after shutdown should fail
    let result = registry.register_actor(
        "test_actor".to_string(),
        create_test_actor_location("127.0.0.1:9001".parse().unwrap()),
    ).await;
    assert!(matches!(result, Err(GossipError::Shutdown)));
    
    let result = registry.unregister_actor("test_actor").await;
    assert!(matches!(result, Err(GossipError::Shutdown)));
    
    let result = registry.prepare_gossip_round().await;
    assert!(matches!(result, Err(GossipError::Shutdown)));
}

#[test]
fn test_serialization_edge_cases() {
    let addr = "127.0.0.1:9001".parse().unwrap();
    
    // Test with empty data
    let empty_message = RegistryMessage::FullSync {
        local_actors: HashMap::new(),
        known_actors: HashMap::new(),
        sender_addr: addr,
        sequence: 0,
        wall_clock_time: 0,
    };
    
    let serialized = bincode::serialize(&empty_message).unwrap();
    let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryMessage::FullSync { local_actors, known_actors, .. } => {
            assert!(local_actors.is_empty());
            assert!(known_actors.is_empty());
        }
        _ => panic!("Expected FullSync message"),
    }
    
    // Test with very large data
    let mut large_actors = HashMap::new();
    for i in 0..1000 {
        let actor_name = format!("actor_{}", i);
        let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        let location = create_test_actor_location(actor_addr);
        large_actors.insert(actor_name, location);
    }
    
    let large_message = RegistryMessage::FullSync {
        local_actors: large_actors,
        known_actors: HashMap::new(),
        sender_addr: addr,
        sequence: 0,
        wall_clock_time: 0,
    };
    
    let serialized = bincode::serialize(&large_message).unwrap();
    let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryMessage::FullSync { local_actors, .. } => {
            assert_eq!(local_actors.len(), 1000);
        }
        _ => panic!("Expected FullSync message"),
    }
}

// Test removed - vector clock merge no longer relevant

#[test]
fn test_current_timestamp_edge_cases() {
    let ts1 = kameo_remote::current_timestamp();
    let ts2 = kameo_remote::current_timestamp();
    
    // Timestamps should be reasonable
    assert!(ts1 > 0);
    assert!(ts2 >= ts1);
    
    // Should be Unix timestamp (seconds since epoch)
    assert!(ts1 > 1_000_000_000); // After year 2001
    assert!(ts1 < 10_000_000_000); // Before year 2286
}

#[tokio::test]
async fn test_registry_with_malformed_addresses() {
    let registry = GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("edge_malformed_addrs"),
    );
    
    // Test with port 0 (should work)
    registry.add_peer("127.0.0.1:0".parse().unwrap()).await;
    
    // Test with max port
    registry.add_peer("127.0.0.1:65535".parse().unwrap()).await;
    
    // Test with IPv6 loopback
    registry.add_peer("[::1]:8001".parse().unwrap()).await;
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.active_peers, 2); // Own address filtered out
}
