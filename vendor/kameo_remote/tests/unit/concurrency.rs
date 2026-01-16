use kameo_remote::*;
use kameo_remote::registry::*;
use kameo_remote::RegistrationPriority;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn test_config(seed: &str) -> GossipConfig {
    GossipConfig {
        key_pair: Some(KeyPair::new_for_testing(seed)),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_concurrent_actor_registration() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    let mut handles = vec![];
    
    // Spawn multiple tasks registering actors concurrently
    for i in 0..50 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            let actor_name = format!("actor_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            let location = ActorLocation::new(actor_addr);
            registry_clone.register_actor(actor_name, location).await
        });
        handles.push(handle);
    }
    
    // Wait for all registrations to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
    
    // Verify all actors were registered
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 50);
}

#[tokio::test]
async fn test_concurrent_actor_lookup() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    // Register some actors first
    for i in 0..10 {
        let actor_name = format!("actor_{}", i);
        let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        let location = ActorLocation::new(actor_addr);
        registry.register_actor(actor_name, location).await.unwrap();
    }
    
    let mut handles = vec![];
    
    // Spawn multiple concurrent lookups
    for i in 0..100 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            let actor_name = format!("actor_{}", i % 10);
            registry_clone.lookup_actor(&actor_name).await
        });
        handles.push(handle);
    }
    
    // All lookups should complete successfully
    let mut found_count = 0;
    for handle in handles {
        let result = handle.await.unwrap();
        if result.is_some() {
            found_count += 1;
        }
    }
    
    assert_eq!(found_count, 100); // All should find actors
}

#[tokio::test]
async fn test_concurrent_register_and_unregister() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    let mut handles = vec![];
    
    // Spawn tasks that register and unregister actors
    for i in 0..20 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            let actor_name = format!("actor_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            let location = ActorLocation::new(actor_addr);
            
            // Register
            registry_clone.register_actor(actor_name.clone(), location).await.unwrap();
            
            // Small delay
            sleep(Duration::from_millis(1)).await;
            
            // Unregister
            registry_clone.unregister_actor(&actor_name).await.unwrap()
        });
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_some()); // Should have unregistered successfully
    }
    
    // All actors should be unregistered
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 0);
}

#[tokio::test]
async fn test_concurrent_delta_application() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    let mut handles = vec![];
    
    // Spawn tasks that apply deltas concurrently
    for i in 0..30 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            let sender_addr = format!("127.0.0.1:{}", 8000 + i).parse().unwrap();
            let actor_name = format!("remote_actor_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            
            let change = RegistryChange::ActorAdded {
                name: actor_name,
                location: ActorLocation::new(actor_addr),
                priority: RegistrationPriority::Normal,
            };
            
            let delta = RegistryDelta {
                since_sequence: 0,
                current_sequence: i as u64 + 1,
                changes: vec![change],
                sender_addr,
                wall_clock_time: kameo_remote::current_timestamp(),
            };
            
            registry_clone.apply_delta(delta).await
        });
        handles.push(handle);
    }
    
    // Wait for all delta applications
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
    
    // Should have all remote actors
    let stats = registry.get_stats().await;
    assert_eq!(stats.known_actors, 30);
}

#[tokio::test]
async fn test_concurrent_gossip_round_preparation() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    // Add some peers
    for i in 0..5 {
        let peer_addr = format!("127.0.0.1:{}", 8000 + i).parse().unwrap();
        registry.add_peer(peer_addr).await;
    }
    
    // Register some actors
    for i in 0..10 {
        let actor_name = format!("actor_{}", i);
        let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        let location = ActorLocation::new(actor_addr);
        registry.register_actor(actor_name, location).await.unwrap();
    }
    
    let mut handles = vec![];
    
    // Spawn multiple concurrent gossip round preparations
    for _i in 0..10 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            registry_clone.prepare_gossip_round().await
        });
        handles.push(handle);
    }
    
    // All should complete successfully
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        let tasks = result.unwrap();
        assert!(tasks.len() <= 5); // Should not exceed peer count
    }
}

#[tokio::test]
async fn test_concurrent_stats_access() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    // Add some data
    registry.add_peer("127.0.0.1:8001".parse().unwrap()).await;
    
    let location = ActorLocation::new("127.0.0.1:9001".parse().unwrap());
    registry.register_actor("test_actor".to_string(), location).await.unwrap();
    
    let mut handles = vec![];
    
    // Spawn multiple concurrent stats requests
    for _i in 0..50 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            registry_clone.get_stats().await
        });
        handles.push(handle);
    }
    
    // All should complete successfully
    for handle in handles {
        let stats = handle.await.unwrap();
        assert_eq!(stats.local_actors, 1);
        assert_eq!(stats.active_peers, 1);
    }
}

#[tokio::test]
async fn test_concurrent_cleanup_operations() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    // Add some stale remote actors
    {
        let mut actor_state = registry.actor_state.write().await;
        for i in 0..20 {
            let actor_name = format!("stale_actor_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            let mut location = ActorLocation::new(actor_addr);
            location.wall_clock_time = kameo_remote::current_timestamp() - 1000; // Very old
            actor_state.known_actors.insert(actor_name, location);
        }
    }
    
    let mut handles = vec![];
    
    // Spawn concurrent cleanup operations
    for _i in 0..10 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            registry_clone.cleanup_stale_actors().await;
        });
        handles.push(handle);
    }
    
    // Wait for all cleanups
    for handle in handles {
        handle.await.unwrap();
    }
    
    // All stale actors should be cleaned up
    let stats = registry.get_stats().await;
    assert_eq!(stats.known_actors, 0);
}

// Vector clock operations test removed - vector clocks no longer used in current implementation
// #[tokio::test]
// async fn test_concurrent_vector_clock_operations() {
//     // Test removed: VectorClock and node_id are no longer part of the API
// }

#[tokio::test]
async fn test_concurrent_shutdown_and_operations() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    let mut handles = vec![];
    
    // Spawn operations that will be interrupted by shutdown
    for i in 0..20 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            let actor_name = format!("actor_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            let location = ActorLocation::new(actor_addr);
            
            // Some operations might succeed before shutdown
            let _result = registry_clone.register_actor(actor_name, location).await;
            
            // Wait a bit
            sleep(Duration::from_millis(1)).await;
            
            // Try to prepare gossip round
            let _result = registry_clone.prepare_gossip_round().await;
        });
        handles.push(handle);
    }
    
    // Shutdown after a brief delay
    sleep(Duration::from_millis(5)).await;
    
    let shutdown_handle = {
        let registry_clone = registry.clone();
        tokio::spawn(async move {
            registry_clone.shutdown().await;
        })
    };
    
    // Wait for shutdown
    shutdown_handle.await.unwrap();
    
    // Wait for all operations (some may fail due to shutdown)
    for handle in handles {
        let _result = handle.await.unwrap();
        // Don't assert on results since shutdown can cause failures
    }
    
    // Registry should be shut down
    assert!(registry.is_shutdown().await);
    
    // New operations should fail
    let result = registry.register_actor(
        "post_shutdown_actor".to_string(),
        ActorLocation::new("127.0.0.1:9999".parse().unwrap()),
    ).await;
    assert!(matches!(result, Err(GossipError::Shutdown)));
}

#[tokio::test]
async fn test_concurrent_peer_management() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    let mut handles = vec![];
    
    // Spawn concurrent peer additions
    for i in 0..30 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            let peer_addr = format!("127.0.0.1:{}", 8000 + i).parse().unwrap();
            registry_clone.add_peer(peer_addr).await;
            
            // Also record peer activity
            registry_clone.record_peer_activity(peer_addr).await;
        });
        handles.push(handle);
    }
    
    // Wait for all peer operations
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify peer count
    let stats = registry.get_stats().await;
    assert_eq!(stats.active_peers, 30);
}

#[tokio::test]
async fn test_concurrent_full_sync_operations() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    let mut handles = vec![];
    
    // Spawn concurrent full sync operations
    for i in 0..20 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            let sender_addr = format!("127.0.0.1:{}", 8000 + i).parse().unwrap();
            
            let mut local_actors = std::collections::HashMap::new();
            let mut known_actors = std::collections::HashMap::new();
            
            // Add some actors
            for j in 0..5 {
                let actor_name = format!("sender_{}_actor_{}", i, j);
                let actor_addr = format!("127.0.0.1:{}", 9000 + i * 10 + j).parse().unwrap();
                let location = ActorLocation::new(actor_addr);
                
                if j % 2 == 0 {
                    local_actors.insert(actor_name, location);
                } else {
                    known_actors.insert(actor_name, location);
                }
            }
            
            registry_clone.merge_full_sync(
                local_actors,
                known_actors,
                sender_addr,
                i as u64 + 1,
                kameo_remote::current_timestamp(),
            ).await;
        });
        handles.push(handle);
    }
    
    // Wait for all full sync operations
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify state
    let stats = registry.get_stats().await;
    assert_eq!(stats.known_actors, 20 * 5); // 20 senders * 5 actors each
    assert_eq!(stats.active_peers, 20); // 20 peers
}

#[tokio::test]
async fn test_concurrent_handle_operations() {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = test_config("concurrency_default");
    
    let keypair = KeyPair::new_for_testing("unit_concurrency_handle");
    let handle = Arc::new(
        GossipRegistryHandle::new_with_keypair(bind_addr, keypair, Some(config))
            .await
            .unwrap(),
    );
    
    let mut handles = vec![];
    
    // Spawn concurrent handle operations
    for i in 0..30 {
        let handle_clone = handle.clone();
        let join_handle = tokio::spawn(async move {
            let actor_name = format!("handle_actor_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            
            // Register
            let result = handle_clone.register(actor_name.clone(), actor_addr).await;
            assert!(result.is_ok());
            
            // Lookup
            let found = handle_clone.lookup(&actor_name).await;
            assert!(found.is_some());
            
            // Stats
            let _stats = handle_clone.stats().await;
            
            // Unregister
            let result = handle_clone.unregister(&actor_name).await;
            assert!(result.is_ok());
        });
        handles.push(join_handle);
    }
    
    // Wait for all operations
    for join_handle in handles {
        join_handle.await.unwrap();
    }
    
    // All actors should be unregistered
    let stats = handle.stats().await;
    assert_eq!(stats.local_actors, 0);
    
    handle.shutdown().await;
}

#[tokio::test]
async fn test_deadlock_prevention() {
    let registry = Arc::new(GossipRegistry::new(
        "127.0.0.1:0".parse().unwrap(),
        test_config("concurrency_default"),
    ));
    
    let mut handles = vec![];
    
    // Spawn tasks that perform operations requiring multiple locks
    for i in 0..20 {
        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            // These operations require consistent lock ordering
            let actor_name = format!("deadlock_test_{}", i);
            let actor_addr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            let location = ActorLocation::new(actor_addr);
            
            // Register (requires vector_clock -> gossip_state -> actor_state)
            registry_clone.register_actor(actor_name.clone(), location).await.unwrap();
            
            // Prepare gossip round (requires vector_clock -> gossip_state)
            let _tasks = registry_clone.prepare_gossip_round().await;
            
            // Lookup (requires actor_state read)
            let _found = registry_clone.lookup_actor(&actor_name).await;
            
            // Unregister (requires vector_clock -> gossip_state -> actor_state)
            registry_clone.unregister_actor(&actor_name).await.unwrap();
        });
        handles.push(handle);
    }
    
    // All operations should complete without deadlock
    for handle in handles {
        handle.await.unwrap();
    }
    
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 0);
}
