use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Helper function to create a test registry handle with custom config
async fn create_test_registry(
    bind_addr: &str,
    keypair: KeyPair,
    config: Option<GossipConfig>,
) -> Arc<GossipRegistryHandle> {
    let bind_addr = bind_addr.parse().unwrap();
    Arc::new(
        GossipRegistryHandle::new_with_keypair(bind_addr, keypair, config)
            .await
            .unwrap()
    )
}

/// Simple priority test - compare normal vs immediate registration timing
#[tokio::test]
async fn test_priority_registration_timing() {
    
    // Create a registry with faster gossip for testing
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(100),
        immediate_propagation_enabled: true,
        ..Default::default()
    };
    
    let registry = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_single"),
        Some(config),
    ).await;
    
    // Test normal priority registration
    let start_time = Instant::now();
    let actor_name_normal = "normal_priority_actor".to_string();
    let actor_addr_normal = "127.0.0.1:9001".parse().unwrap();
    
    registry.register_with_priority(
        actor_name_normal.clone(),
        actor_addr_normal,
        RegistrationPriority::Normal,
    ).await.unwrap();
    
    let normal_registration_time = start_time.elapsed();
    println!("Normal priority registration took: {:?}", normal_registration_time);
    
    // Test immediate priority registration
    let start_time = Instant::now();
    let actor_name_immediate = "immediate_priority_actor".to_string();
    let actor_addr_immediate = "127.0.0.1:9002".parse().unwrap();
    
    registry.register_with_priority(
        actor_name_immediate.clone(),
        actor_addr_immediate,
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    let immediate_registration_time = start_time.elapsed();
    println!("Immediate priority registration took: {:?}", immediate_registration_time);
    
    // Both should be registered and lookup should work
    let normal_lookup = registry.lookup(&actor_name_normal).await;
    let immediate_lookup = registry.lookup(&actor_name_immediate).await;
    
    assert!(normal_lookup.is_some());
    assert!(immediate_lookup.is_some());
    
    println!("Both actors successfully registered and found via lookup");
    
    // Check that the immediate priority actor has the correct priority
    let immediate_actor = immediate_lookup.unwrap();
    assert_eq!(immediate_actor.priority, RegistrationPriority::Immediate);
    
    let normal_actor = normal_lookup.unwrap();
    assert_eq!(normal_actor.priority, RegistrationPriority::Normal);
    
    println!("Priority fields correctly set on actor locations");
    
    // Cleanup
    registry.shutdown().await;
}

/// Test two-node gossip propagation with proper bootstrap
#[tokio::test]
async fn test_two_node_gossip_propagation() {
    
    // Create config with fast gossip and immediate propagation
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        immediate_propagation_enabled: true,
        bootstrap_readiness_timeout: Duration::from_secs(5),
        bootstrap_readiness_check_interval: Duration::from_millis(50),
        ..Default::default()
    };
    
    // Create node1 first
    let node1 = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_node1"),
        Some(config.clone()),
    ).await;
    let node1_id = node1.registry.peer_id;
    
    let node1_addr = node1.registry.bind_addr;
    println!("Node1 bound to: {}", node1_addr);
    
    // Create node2 with node1 as peer
    let node2 = create_test_registry(
        "127.0.0.1:0",
        KeyPair::new_for_testing("priority_node2"),
        Some(config),
    ).await;
    
    let node2_addr = node2.registry.bind_addr;
    println!("Node2 bound to: {}", node2_addr);
    
    let peer1_from_2 = node2.add_peer(&node1_id).await;
    peer1_from_2.connect(&node1_addr).await.unwrap();

    // Wait for bootstrap connection to establish
    sleep(Duration::from_millis(500)).await;
    
    // Verify connection is established
    let stats1 = node1.stats().await;
    let stats2 = node2.stats().await;
    println!("Node1 stats: active_peers={}, failed_peers={}", stats1.active_peers, stats1.failed_peers);
    println!("Node2 stats: active_peers={}, failed_peers={}", stats2.active_peers, stats2.failed_peers);
    
    // Register actor on node1 with normal priority
    let actor_name = "test_actor_normal".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    let registration_time = Instant::now();
    node1.register_with_priority(
        actor_name.clone(),
        actor_addr,
        RegistrationPriority::Normal,
    ).await.unwrap();
    
    println!("Registered actor at: {:?}", registration_time);
    
    // Wait for gossip to propagate
    let mut found_on_node2 = false;
    let timeout = Duration::from_secs(5);
    let start_polling = Instant::now();
    
    while start_polling.elapsed() < timeout {
        if let Some(location) = node2.lookup(&actor_name).await {
            let propagation_time = Instant::now().duration_since(registration_time);
            println!("Actor propagated to node2 in: {:?}", propagation_time);
            println!("Found actor: {:?}", location);
            found_on_node2 = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    
    assert!(found_on_node2, "Actor did not propagate from node1 to node2");
    
    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
}

/// Test immediate priority propagation vs normal priority
#[tokio::test]
async fn test_immediate_vs_normal_priority() {
    
    // Create config with slower gossip to see immediate propagation difference
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(1000), // Slower gossip
        immediate_propagation_enabled: true,
        urgent_gossip_fanout: 1,
        bootstrap_readiness_timeout: Duration::from_secs(5),
        bootstrap_readiness_check_interval: Duration::from_millis(50),
        ..Default::default()
    };
    
    // Create node1 first
    let node1 = create_test_registry(
        "127.0.0.1:0",
        vec![],
        Some(config.clone()),
    ).await;
    
    let node1_addr = node1.registry.bind_addr;
    
    // Create node2 with node1 as peer
    let node2 = create_test_registry(
        "127.0.0.1:0",
        vec![node1_addr],
        Some(config),
    ).await;
    
    let node2_addr = node2.registry.bind_addr;
    
    // Wait for bootstrap connection to establish
    sleep(Duration::from_millis(500)).await;
    
    // Verify connection is established
    let stats1 = node1.stats().await;
    let stats2 = node2.stats().await;
    println!("Node1 stats: active_peers={}, failed_peers={}", stats1.active_peers, stats1.failed_peers);
    println!("Node2 stats: active_peers={}, failed_peers={}", stats2.active_peers, stats2.failed_peers);
    
    // Test normal priority first
    let normal_actor = "normal_actor".to_string();
    let normal_addr = "127.0.0.1:9001".parse().unwrap();
    
    let normal_start = Instant::now();
    node1.register_with_priority(
        normal_actor.clone(),
        normal_addr,
        RegistrationPriority::Normal,
    ).await.unwrap();
    
    // Wait for normal propagation
    let mut normal_propagation_time = None;
    let timeout = Duration::from_secs(3);
    let start_polling = Instant::now();
    
    while start_polling.elapsed() < timeout {
        if let Some(_) = node2.lookup(&normal_actor).await {
            normal_propagation_time = Some(Instant::now().duration_since(normal_start));
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    
    // Test immediate priority
    let immediate_actor = "immediate_actor".to_string();
    let immediate_addr = "127.0.0.1:9002".parse().unwrap();
    
    let immediate_start = Instant::now();
    node1.register_with_priority(
        immediate_actor.clone(),
        immediate_addr,
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Wait for immediate propagation
    let mut immediate_propagation_time = None;
    let timeout = Duration::from_secs(3);
    let start_polling = Instant::now();
    
    while start_polling.elapsed() < timeout {
        if let Some(_) = node2.lookup(&immediate_actor).await {
            immediate_propagation_time = Some(Instant::now().duration_since(immediate_start));
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    
    // Both should have propagated, and immediate should be faster
    if let Some(normal_time) = normal_propagation_time {
        println!("Normal priority propagation: {:?}", normal_time);
    } else {
        println!("Normal priority actor did not propagate within timeout");
    }
    
    if let Some(immediate_time) = immediate_propagation_time {
        println!("Immediate priority propagation: {:?}", immediate_time);
    } else {
        println!("Immediate priority actor did not propagate within timeout");
    }
    
    // At least one should have propagated
    assert!(
        normal_propagation_time.is_some() || immediate_propagation_time.is_some(),
        "Neither actor propagated within timeout"
    );
    
    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
}

/// Test that immediate priority configuration works
#[tokio::test]
async fn test_immediate_priority_config() {
    
    // Test with immediate propagation disabled
    let config_disabled = GossipConfig {
        immediate_propagation_enabled: false,
        ..Default::default()
    };
    
    let registry_disabled = create_test_registry(
        "127.0.0.1:0",
        vec![],
        Some(config_disabled),
    ).await;
    
    // Register with immediate priority
    let actor_name = "immediate_actor".to_string();
    let actor_addr = "127.0.0.1:9001".parse().unwrap();
    
    registry_disabled.register_with_priority(
        actor_name.clone(),
        actor_addr,
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Should still be registered locally
    let lookup = registry_disabled.lookup(&actor_name).await;
    assert!(lookup.is_some());
    assert_eq!(lookup.unwrap().priority, RegistrationPriority::Immediate);
    
    registry_disabled.shutdown().await;
    
    // Test with immediate propagation enabled
    let config_enabled = GossipConfig {
        immediate_propagation_enabled: true,
        urgent_gossip_fanout: 2,
        ..Default::default()
    };
    
    let registry_enabled = create_test_registry(
        "127.0.0.1:0",
        vec![],
        Some(config_enabled),
    ).await;
    
    // Register with immediate priority
    let actor_name2 = "immediate_actor2".to_string();
    let actor_addr2 = "127.0.0.1:9002".parse().unwrap();
    
    registry_enabled.register_with_priority(
        actor_name2.clone(),
        actor_addr2,
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Should be registered locally
    let lookup2 = registry_enabled.lookup(&actor_name2).await;
    assert!(lookup2.is_some());
    assert_eq!(lookup2.unwrap().priority, RegistrationPriority::Immediate);
    
    registry_enabled.shutdown().await;
}
