use kameo_remote::registry::*;
use kameo_remote::{ActorLocation, GossipConfig, GossipError, KeyPair, RegistrationPriority};
use std::collections::HashMap;
use std::net::SocketAddr;

fn create_test_delta(sender_addr: SocketAddr, changes: Vec<RegistryChange>) -> RegistryDelta {
    RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes,
        sender_addr,
        wall_clock_time: kameo_remote::current_timestamp(),
    }
}

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
fn test_registry_message_serialization() {
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let actor_location = create_test_actor_location(actor_addr);
    
    let change = RegistryChange::ActorAdded {
        name: actor_name.clone(),
        location: actor_location.clone(),
        priority: RegistrationPriority::Normal,
    };
    
    let delta = create_test_delta(sender_addr, vec![change]);
    
    let message = RegistryMessage::DeltaGossip { delta: delta.clone() };
    
    // Test serialization
    let serialized = bincode::serialize(&message).unwrap();
    let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryMessage::DeltaGossip { delta: d } => {
            assert_eq!(d.sender_addr, sender_addr);
            assert_eq!(d.changes.len(), 1);
            match &d.changes[0] {
                RegistryChange::ActorAdded { name, location, priority: _, .. } => {
                    assert_eq!(name, &actor_name);
                    assert_eq!(location.address, actor_addr);
                }
                _ => panic!("Expected ActorAdded change"),
            }
        }
        _ => panic!("Expected DeltaGossip message"),
    }
}

#[test]
fn test_registry_change_actor_added() {
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    let actor_location = create_test_actor_location(actor_addr);
    
    let change = RegistryChange::ActorAdded {
        name: actor_name.clone(),
        location: actor_location.clone(),
        priority: RegistrationPriority::Normal,
    };
    
    // Test serialization
    let serialized = bincode::serialize(&change).unwrap();
    let deserialized: RegistryChange = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryChange::ActorAdded { name, location, priority: _ } => {
            assert_eq!(name, actor_name);
            assert_eq!(location.address, actor_addr);
        }
        _ => panic!("Expected ActorAdded change"),
    }
}

#[test]
fn test_registry_change_actor_removed() {
    let actor_name = "test_actor".to_string();
    
    let change = RegistryChange::ActorRemoved {
        name: actor_name.clone(),
        priority: RegistrationPriority::Normal,
    };
    
    // Test serialization
    let serialized = bincode::serialize(&change).unwrap();
    let deserialized: RegistryChange = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryChange::ActorRemoved { name, priority: _ } => {
            assert_eq!(name, actor_name);
        }
        _ => panic!("Expected ActorRemoved change"),
    }
}

#[test]
fn test_registry_delta_creation() {
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let actor_location = create_test_actor_location(actor_addr);
    
    let change = RegistryChange::ActorAdded {
        name: actor_name.clone(),
        location: actor_location,
        priority: RegistrationPriority::Normal,
    };
    
    let delta = RegistryDelta {
        since_sequence: 5,
        current_sequence: 10,
        changes: vec![change],
        sender_addr,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    assert_eq!(delta.since_sequence, 5);
    assert_eq!(delta.current_sequence, 10);
    assert_eq!(delta.changes.len(), 1);
    assert_eq!(delta.sender_addr, sender_addr);
    assert!(delta.wall_clock_time > 0);
}

#[test]
fn test_registry_message_types() {
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    let local_actors = HashMap::new();
    let known_actors = HashMap::new();
    let sequence = 42;
    let wall_clock_time = kameo_remote::current_timestamp();
    
    // Test DeltaGossip
    let delta = create_test_delta(sender_addr, vec![]);
    let msg = RegistryMessage::DeltaGossip { delta };
    let serialized = bincode::serialize(&msg).unwrap();
    assert!(!serialized.is_empty());
    
    // Test DeltaGossipResponse  
    let delta = create_test_delta(sender_addr, vec![]);
    let msg = RegistryMessage::DeltaGossipResponse { delta };
    let serialized = bincode::serialize(&msg).unwrap();
    assert!(!serialized.is_empty());
    
    // Test FullSyncRequest
    let msg = RegistryMessage::FullSyncRequest {
        sender_addr,
        sequence,
        wall_clock_time,
    };
    let serialized = bincode::serialize(&msg).unwrap();
    assert!(!serialized.is_empty());
    
    // Test FullSync
    let msg = RegistryMessage::FullSync {
        local_actors: local_actors.clone(),
        known_actors: known_actors.clone(),
        sender_addr,
        sequence,
        wall_clock_time,
    };
    let serialized = bincode::serialize(&msg).unwrap();
    assert!(!serialized.is_empty());
    
    // Test FullSyncResponse
    let msg = RegistryMessage::FullSyncResponse {
        local_actors,
        known_actors,
        sender_addr,
        sequence,
        wall_clock_time,
    };
    let serialized = bincode::serialize(&msg).unwrap();
    assert!(!serialized.is_empty());
}

#[test]
fn test_peer_info_creation() {
    let peer_addr = "127.0.0.1:8001".parse().unwrap();
    
    let peer_info = PeerInfo {
        address: peer_addr,
        peer_address: None,
        failures: 0,
        last_attempt: 0,
        last_success: 0,
        last_sequence: 0,
        last_sent_sequence: 0,
        consecutive_deltas: 0,
    };
    
    assert_eq!(peer_info.address, peer_addr);
    assert_eq!(peer_info.failures, 0);
    assert_eq!(peer_info.consecutive_deltas, 0);
}

#[test]
fn test_historical_delta() {
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    let actor_location = create_test_actor_location(actor_addr);
    
    let change = RegistryChange::ActorAdded {
        name: actor_name,
        location: actor_location,
        priority: RegistrationPriority::Normal,
    };
    
    let delta = HistoricalDelta {
        sequence: 1,
        changes: vec![change],
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    assert_eq!(delta.sequence, 1);
    assert_eq!(delta.changes.len(), 1);
    assert!(delta.wall_clock_time > 0);
}

#[test]
fn test_gossip_task_creation() {
    let peer_addr = "127.0.0.1:8001".parse().unwrap();
    let sender_addr = "127.0.0.1:8002".parse().unwrap();
    
    let message = RegistryMessage::FullSyncRequest {
        sender_addr,
        sequence: 42,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let task = GossipTask {
        peer_addr,
        message,
        current_sequence: 42,
    };
    
    assert_eq!(task.peer_addr, peer_addr);
    assert_eq!(task.current_sequence, 42);
    
    match task.message {
        RegistryMessage::FullSyncRequest { sequence, .. } => {
            assert_eq!(sequence, 42);
        }
        _ => panic!("Expected FullSyncRequest message"),
    }
}

#[test]
fn test_gossip_result_creation() {
    let peer_addr = "127.0.0.1:8001".parse().unwrap();
    let sent_sequence = 42;
    
    // Test successful result
    let success_result = GossipResult {
        peer_addr,
        sent_sequence,
        outcome: Ok(None),
    };
    
    assert_eq!(success_result.peer_addr, peer_addr);
    assert_eq!(success_result.sent_sequence, sent_sequence);
    assert!(success_result.outcome.is_ok());
    
    // Test error result
    let error_result = GossipResult {
        peer_addr,
        sent_sequence,
        outcome: Err(GossipError::Timeout),
    };
    
    assert_eq!(error_result.peer_addr, peer_addr);
    assert_eq!(error_result.sent_sequence, sent_sequence);
    assert!(error_result.outcome.is_err());
    
    match error_result.outcome {
        Err(GossipError::Timeout) => {},
        _ => panic!("Expected Timeout error"),
    }
}

#[test]
fn test_actor_state_creation() {
    let local_actors = HashMap::new();
    let known_actors = HashMap::new();
    
    let state = ActorState {
        local_actors,
        known_actors,
    };
    
    assert_eq!(state.local_actors.len(), 0);
    assert_eq!(state.known_actors.len(), 0);
}

#[test]
fn test_gossip_state_creation() {
    let state = GossipState {
        gossip_sequence: 0,
        pending_changes: Vec::new(),
        urgent_changes: Vec::new(),
        delta_history: Vec::new(),
        peers: HashMap::new(),
        delta_exchanges: 0,
        full_sync_exchanges: 0,
        shutdown: false,
        peer_to_actors: HashMap::new(),
    };
    
    assert_eq!(state.gossip_sequence, 0);
    assert!(state.pending_changes.is_empty());
    assert!(state.delta_history.is_empty());
    assert!(state.peers.is_empty());
    assert_eq!(state.delta_exchanges, 0);
    assert_eq!(state.full_sync_exchanges, 0);
    assert!(!state.shutdown);
}

#[test]
fn test_registry_stats_creation() {
    let current_time = kameo_remote::current_timestamp();
    
    let stats = RegistryStats {
        local_actors: 5,
        known_actors: 10,
        active_peers: 3,
        failed_peers: 1,
        total_gossip_rounds: 100,
        current_sequence: 50,
        uptime_seconds: 3600,
        last_gossip_timestamp: current_time,
        delta_exchanges: 75,
        full_sync_exchanges: 25,
        delta_history_size: 20,
        avg_delta_size: 2.5,
    };
    
    assert_eq!(stats.local_actors, 5);
    assert_eq!(stats.known_actors, 10);
    assert_eq!(stats.active_peers, 3);
    assert_eq!(stats.failed_peers, 1);
    assert_eq!(stats.total_gossip_rounds, 100);
    assert_eq!(stats.current_sequence, 50);
    assert_eq!(stats.uptime_seconds, 3600);
    assert_eq!(stats.last_gossip_timestamp, current_time);
    assert_eq!(stats.delta_exchanges, 75);
    assert_eq!(stats.full_sync_exchanges, 25);
    assert_eq!(stats.delta_history_size, 20);
    assert_eq!(stats.avg_delta_size, 2.5);
}

#[test]
fn test_message_size_estimation() {
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    let actor_name = "test_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let actor_location = create_test_actor_location(actor_addr);
    
    let change = RegistryChange::ActorAdded {
        name: actor_name,
        location: actor_location,
        priority: RegistrationPriority::Normal,
    };
    
    let delta = create_test_delta(sender_addr, vec![change]);
    let message = RegistryMessage::DeltaGossip { delta };
    
    let serialized = bincode::serialize(&message).unwrap();
    
    // Message should be reasonably sized
    assert!(serialized.len() > 100); // Has some content
    assert!(serialized.len() < 1024); // Not too large for simple message
}

#[test]
fn test_large_message_handling() {
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    
    // Create a message with many actors
    let mut local_actors = HashMap::new();
    let mut known_actors = HashMap::new();
    
    for i in 0..1000 {
        let actor_name = format!("actor_{}", i);
        let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        let actor_location = create_test_actor_location(actor_addr);
        
        if i % 2 == 0 {
            local_actors.insert(actor_name, actor_location);
        } else {
            known_actors.insert(actor_name, actor_location);
        }
    }
    
    let message = RegistryMessage::FullSync {
        local_actors,
        known_actors,
        sender_addr,
        sequence: 1,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let serialized = bincode::serialize(&message).unwrap();
    let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryMessage::FullSync { local_actors, known_actors, .. } => {
            assert_eq!(local_actors.len(), 500);
            assert_eq!(known_actors.len(), 500);
        }
        _ => panic!("Expected FullSync message"),
    }
}

#[test]
fn test_empty_message_handling() {
    let sender_addr = "127.0.0.1:8001".parse().unwrap();
    
    // Test empty delta
    let delta = create_test_delta(sender_addr, vec![]);
    let message = RegistryMessage::DeltaGossip { delta };
    
    let serialized = bincode::serialize(&message).unwrap();
    let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryMessage::DeltaGossip { delta } => {
            assert!(delta.changes.is_empty());
        }
        _ => panic!("Expected DeltaGossip message"),
    }
    
    // Test empty full sync
    let message = RegistryMessage::FullSync {
        local_actors: HashMap::new(),
        known_actors: HashMap::new(),
        sender_addr,
        sequence: 1,
        wall_clock_time: kameo_remote::current_timestamp(),
    };
    
    let serialized = bincode::serialize(&message).unwrap();
    let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
    
    match deserialized {
        RegistryMessage::FullSync { local_actors, known_actors, .. } => {
            assert!(local_actors.is_empty());
            assert!(known_actors.is_empty());
        }
        _ => panic!("Expected FullSync message"),
    }
}

#[test]
fn test_gossip_error_types() {
    // Test all error types can be created and displayed
    let errors = vec![
        GossipError::Network(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "test")),
        GossipError::Serialization(bincode::Error::new(bincode::ErrorKind::SizeLimit)),
        GossipError::MessageTooLarge { size: 1000, max: 500 },
        GossipError::Timeout,
        GossipError::PeerNotFound("127.0.0.1:8001".parse().unwrap()),
        GossipError::ActorNotFound("test_actor".to_string()),
        GossipError::Shutdown,
        GossipError::DeltaTooOld { requested: 10, oldest: 20 },
        GossipError::FullSyncRequired,
    ];
    
    for error in errors {
        let error_str = format!("{}", error);
        assert!(!error_str.is_empty());
        
        let debug_str = format!("{:?}", error);
        assert!(!debug_str.is_empty());
    }
}

#[tokio::test]
async fn test_complex_gossip_scenario() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = test_config("unit_gossip_complex");
    let registry = GossipRegistry::new(bind_addr, config);
    
    // Add some peers
    registry.add_peer("127.0.0.1:8001".parse().unwrap()).await;
    registry.add_peer("127.0.0.1:8002".parse().unwrap()).await;
    
    // Register some local actors
    registry.register_actor(
        "local1".to_string(),
        create_test_actor_location("127.0.0.1:9001".parse().unwrap())
    ).await.unwrap();
    
    registry.register_actor(
        "local2".to_string(),
        create_test_actor_location("127.0.0.1:9002".parse().unwrap())
    ).await.unwrap();
    
    // Apply some deltas to add remote actors
    let remote_addr = "127.0.0.1:8003".parse().unwrap();
    
    let change = RegistryChange::ActorAdded {
        name: "remote1".to_string(),
        location: create_test_actor_location("127.0.0.1:9003".parse().unwrap()),
        priority: RegistrationPriority::Normal,
    };
    
    let delta = create_test_delta(remote_addr, vec![change]);
    registry.apply_delta(delta).await.unwrap();
    
    // Prepare gossip round
    let tasks = registry.prepare_gossip_round().await.unwrap();
    assert_eq!(tasks.len(), 2); // Should have tasks for both peers
    
    // Verify stats
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 2);
    assert_eq!(stats.known_actors, 1);
    assert_eq!(stats.active_peers, 2);
    
    // Test cleanup
    registry.cleanup_stale_actors().await;
    
    // Stats should be unchanged (actors are not stale)
    let stats = registry.get_stats().await;
    assert_eq!(stats.known_actors, 1);
}

#[tokio::test]
async fn test_gossip_state_transitions() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = test_config("unit_gossip_multi");
    let registry = GossipRegistry::new(bind_addr, config);
    
    // Initial state
    assert!(!registry.is_shutdown().await);
    let stats = registry.get_stats().await;
    assert_eq!(stats.current_sequence, 0);
    
    // Register actor (should increment sequence)
    registry.register_actor(
        "test_actor".to_string(),
        create_test_actor_location("127.0.0.1:9001".parse().unwrap())
    ).await.unwrap();
    
    // Add peer and prepare gossip (should increment sequence)
    registry.add_peer("127.0.0.1:8001".parse().unwrap()).await;
    let tasks = registry.prepare_gossip_round().await.unwrap();
    assert_eq!(tasks.len(), 1);
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.current_sequence, 1);
    
    // Shutdown
    registry.shutdown().await;
    assert!(registry.is_shutdown().await);
    
    // Operations after shutdown should fail
    let result = registry.register_actor(
        "new_actor".to_string(),
        create_test_actor_location("127.0.0.1:9002".parse().unwrap())
    ).await;
    assert!(matches!(result, Err(GossipError::Shutdown)));
}
