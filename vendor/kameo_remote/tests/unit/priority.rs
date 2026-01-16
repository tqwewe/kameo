use kameo_remote::registry::*;
use kameo_remote::{ActorLocation, GossipConfig, KeyPair, RegistrationPriority};
use std::net::SocketAddr;

fn create_test_registry() -> GossipRegistry {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("unit_priority")),
        ..Default::default()
    };
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

fn create_test_actor_location_with_priority(addr: SocketAddr, priority: RegistrationPriority) -> ActorLocation {
    ActorLocation {
        address: addr,
        wall_clock_time: kameo_remote::current_timestamp(),
        priority,
        local_registration_time: 0,
    }
}

#[test]
fn test_registration_priority_ordering() {
    assert!(RegistrationPriority::Normal < RegistrationPriority::Immediate);
}

#[test]
fn test_registration_priority_should_trigger_immediate_gossip() {
    assert!(!RegistrationPriority::Normal.should_trigger_immediate_gossip());
    assert!(RegistrationPriority::Immediate.should_trigger_immediate_gossip());
}

#[test]
fn test_registration_priority_is_critical() {
    assert!(!RegistrationPriority::Normal.is_critical());
    assert!(RegistrationPriority::Immediate.is_critical());
}

#[test]
fn test_registration_priority_fanout_multiplier() {
    assert_eq!(RegistrationPriority::Normal.fanout_multiplier(), 1.0);
    assert_eq!(RegistrationPriority::Immediate.fanout_multiplier(), 2.0);
}

#[test]
fn test_registration_priority_retry_count() {
    assert_eq!(RegistrationPriority::Normal.immediate_retry_count(), 0);
    assert_eq!(RegistrationPriority::Immediate.immediate_retry_count(), 3);
}

#[test]
fn test_registration_priority_default() {
    assert_eq!(RegistrationPriority::default(), RegistrationPriority::Normal);
}

#[test]
fn test_registration_priority_serialization() {
    let priorities = vec![
        RegistrationPriority::Normal,
        RegistrationPriority::Immediate,
    ];
    
    for priority in priorities {
        let serialized = bincode::serialize(&priority).unwrap();
        let deserialized: RegistrationPriority = bincode::deserialize(&serialized).unwrap();
        assert_eq!(priority, deserialized);
    }
}

#[tokio::test]
async fn test_registry_change_with_priority() {
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    let actor_location = create_test_actor_location(actor_addr);
    
    // Test ActorAdded with different priorities
    let priorities = vec![
        RegistrationPriority::Normal,
        RegistrationPriority::Immediate,
    ];
    
    for priority in priorities {
        let change = RegistryChange::ActorAdded {
            name: actor_name.clone(),
            location: actor_location.clone(),
            priority,
        };
        
        // Test serialization
        let serialized = bincode::serialize(&change).unwrap();
        let deserialized: RegistryChange = bincode::deserialize(&serialized).unwrap();
        
        match deserialized {
            RegistryChange::ActorAdded { priority: p, .. } => {
                assert_eq!(p, priority);
            }
            _ => panic!("Expected ActorAdded change"),
        }
    }
}

#[tokio::test]
async fn test_registry_change_actor_removed_with_priority() {
    let actor_name = "test_actor".to_string();
    
    let change = RegistryChange::ActorRemoved {
        name: actor_name.clone(),
        priority: RegistrationPriority::Immediate,
    };
    
    // Test serialization
    let serialized = bincode::serialize(&change).unwrap();
    let deserialized: RegistryChange = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryChange::ActorRemoved { name, priority, .. } => {
            assert_eq!(name, actor_name);
            assert_eq!(priority, RegistrationPriority::Immediate);
        }
        _ => panic!("Expected ActorRemoved change"),
    }
}

#[tokio::test]
async fn test_urgent_changes_queue() {
    let registry = create_test_registry();
    let actor_name = "urgent_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    let actor_location = create_test_actor_location(actor_addr);
    
    // Register with normal priority - should not go to urgent queue
    let result = registry.register_actor_with_priority(
        actor_name.clone(),
        actor_location.clone(),
        RegistrationPriority::Normal,
    ).await;
    assert!(result.is_ok());
    
    // Check gossip state - should not have urgent changes
    {
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.urgent_changes.is_empty());
        assert!(!gossip_state.pending_changes.is_empty());
    }
    
    // Unregister the actor first
    registry.unregister_actor(&actor_name).await.unwrap();
    
    // Register with immediate priority - should go to urgent queue
    let result = registry.register_actor_with_priority(
        actor_name.clone(),
        actor_location.clone(),
        RegistrationPriority::Immediate,
    ).await;
    assert!(result.is_ok());
    
    // Check gossip state - should have urgent changes
    {
        let gossip_state = registry.gossip_state.lock().await;
        assert!(!gossip_state.urgent_changes.is_empty());
        assert_eq!(gossip_state.urgent_changes.len(), 1);
        
        match &gossip_state.urgent_changes[0] {
            RegistryChange::ActorAdded { name, priority, .. } => {
                assert_eq!(name, &actor_name);
                assert_eq!(*priority, RegistrationPriority::Immediate);
            }
            _ => panic!("Expected urgent ActorAdded change"),
        }
    }
}

#[tokio::test]
async fn test_immediate_priority_registration() {
    let registry = create_test_registry();
    let actor_name = "immediate_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    let actor_location = create_test_actor_location(actor_addr);
    
    // Register with immediate priority
    let result = registry.register_actor_with_priority(
        actor_name.clone(),
        actor_location,
        RegistrationPriority::Immediate,
    ).await;
    assert!(result.is_ok());
    
    // Check gossip state - should have urgent changes
    {
        let gossip_state = registry.gossip_state.lock().await;
        assert!(!gossip_state.urgent_changes.is_empty());
        
        match &gossip_state.urgent_changes[0] {
            RegistryChange::ActorAdded { name, priority, .. } => {
                assert_eq!(name, &actor_name);
                assert_eq!(*priority, RegistrationPriority::Immediate);
            }
            _ => panic!("Expected immediate ActorAdded change"),
        }
    }
    
    // Verify lookup still works
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_some());
    assert_eq!(found.unwrap().address, actor_addr);
}

#[tokio::test]
async fn test_priority_unregistration() {
    let registry = create_test_registry();
    let actor_name = "priority_unregister_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    // Register with immediate priority so unregistration will be urgent
    let actor_location = create_test_actor_location_with_priority(actor_addr, RegistrationPriority::Immediate);
    
    registry.register_actor_with_priority(
        actor_name.clone(),
        actor_location,
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Clear urgent changes from registration
    {
        let mut gossip_state = registry.gossip_state.lock().await;
        gossip_state.urgent_changes.clear();
    }
    
    // Unregister - should use critical priority from the actor location
    let result = registry.unregister_actor(&actor_name).await;
    assert!(result.is_ok());
    
    // Check gossip state - should have urgent changes for unregistration
    {
        let gossip_state = registry.gossip_state.lock().await;
        assert!(!gossip_state.urgent_changes.is_empty());
        
        // Should have the urgent unregistration change
        let urgent_removals: Vec<_> = gossip_state.urgent_changes.iter()
            .filter_map(|change| match change {
                RegistryChange::ActorRemoved { name, priority, .. } => Some((name, priority)),
                _ => None,
            })
            .collect();
        
        assert_eq!(urgent_removals.len(), 1);
        assert_eq!(urgent_removals[0].0, &actor_name);
        assert_eq!(*urgent_removals[0].1, RegistrationPriority::Immediate);
    }
    
    // Verify actor is unregistered
    let found = registry.lookup_actor(&actor_name).await;
    assert!(found.is_none());
}

#[tokio::test]
async fn test_mixed_priority_changes() {
    let registry = create_test_registry();
    
    // Register multiple actors with different priorities
    let actors = vec![
        ("normal_actor", RegistrationPriority::Normal),
        ("immediate_actor", RegistrationPriority::Immediate),
    ];
    
    for (name, priority) in actors {
        let actor_addr = "127.0.0.1:9001".parse().unwrap();
        let actor_location = create_test_actor_location(actor_addr);
        
        registry.register_actor_with_priority(
            name.to_string(),
            actor_location,
            priority,
        ).await.unwrap();
    }
    
    // Check gossip state
    {
        let gossip_state = registry.gossip_state.lock().await;
        
        // Should have urgent changes for immediate priority
        assert_eq!(gossip_state.urgent_changes.len(), 1);
        
        // Check that urgent changes contain immediate priority actors
        let urgent_names: Vec<_> = gossip_state.urgent_changes.iter()
            .filter_map(|change| match change {
                RegistryChange::ActorAdded { name, priority, .. } => Some((name.clone(), *priority)),
                _ => None,
            })
            .collect();
        
        assert!(urgent_names.contains(&("immediate_actor".to_string(), RegistrationPriority::Immediate)));
        
        // Normal priority should be in pending changes
        assert!(!gossip_state.pending_changes.is_empty());
        let pending_names: Vec<_> = gossip_state.pending_changes.iter()
            .filter_map(|change| match change {
                RegistryChange::ActorAdded { name, priority, .. } => Some((name.clone(), *priority)),
                _ => None,
            })
            .collect();
        
        assert!(pending_names.contains(&("normal_actor".to_string(), RegistrationPriority::Normal)));
    }
    
    // Verify all actors are registered
    for (name, _) in [("normal_actor", RegistrationPriority::Normal), 
                     ("immediate_actor", RegistrationPriority::Immediate)] {
        let found = registry.lookup_actor(name).await;
        assert!(found.is_some());
    }
}

#[tokio::test]
async fn test_priority_delta_application() {
    let registry = create_test_registry();
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    let actor_name = "priority_remote_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let actor_location = create_test_actor_location_with_priority(actor_addr, RegistrationPriority::Immediate);
    
    // Test applying delta with immediate priority
    let change = RegistryChange::ActorAdded {
        name: actor_name.clone(),
        location: actor_location,
        priority: RegistrationPriority::Immediate,
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

// VectorClock priority change deduplication test removed
