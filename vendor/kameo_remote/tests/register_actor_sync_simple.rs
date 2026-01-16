use kameo_remote::registry::*;
use kameo_remote::{GossipConfig, GossipError, KeyPair, RemoteActorLocation};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Helper function to create a test registry
fn create_test_registry(bind_addr: SocketAddr) -> GossipRegistry {
    let config = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("register_sync_simple")),
        gossip_interval: Duration::from_millis(100),
        max_peer_failures: 3,
        dead_peer_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    GossipRegistry::new(bind_addr, config)
}

/// Helper function to create a test actor location
fn create_test_actor_location(addr: SocketAddr) -> RemoteActorLocation {
    let peer_id = KeyPair::new_for_testing("test_peer").peer_id();
    RemoteActorLocation::new_with_peer(addr, peer_id)
}

/// Helper function to simulate a healthy peer in gossip state
async fn add_healthy_peer(registry: &GossipRegistry, peer_addr: SocketAddr) {
    registry.add_peer(peer_addr).await;
}

/// Helper function to simulate a failed peer in gossip state
async fn add_failed_peer(registry: &GossipRegistry, peer_addr: SocketAddr, max_failures: usize) {
    registry.add_peer(peer_addr).await;

    // Manually set peer as failed by accessing gossip state
    let mut gossip_state = registry.gossip_state.lock().await;
    if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
        peer_info.failures = max_failures;
    }
}

#[tokio::test]
async fn test_register_actor_sync_no_peers() {
    // Test: When there are no peers, register_actor_sync should return immediately
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    let start = std::time::Instant::now();
    let result = registry
        .register_actor_sync(
            actor_name.clone(),
            actor_location,
            Duration::from_secs(5), // Long timeout to ensure we don't hit it
        )
        .await;
    let elapsed = start.elapsed();

    // Should succeed immediately since no peers to wait for
    assert!(result.is_ok());
    // Should complete very quickly (less than 100ms)
    assert!(elapsed < Duration::from_millis(100));

    // Verify the actor was registered
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 1);

    // Verify we can lookup the actor
    let lookup_result = registry.lookup_actor(&actor_name).await;
    assert!(lookup_result.is_some());
}

#[tokio::test]
async fn test_register_actor_sync_with_healthy_peers() {
    // Test: With healthy peers, should wait for ACK
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    // Add a healthy peer
    add_healthy_peer(&registry, peer_addr).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    // Simulate sending the ACK after a short delay
    let registry_clone = Arc::new(registry.clone());
    let actor_name_clone = actor_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate receiving an ImmediateAck
        let mut pending_acks = registry_clone.pending_acks.lock().await;
        if let Some(sender) = pending_acks.remove(&actor_name_clone) {
            let _ = sender.send(true); // Send success ACK
        }
    });

    let start = std::time::Instant::now();
    let result = registry
        .register_actor_sync(actor_name.clone(), actor_location, Duration::from_secs(1))
        .await;
    let elapsed = start.elapsed();

    // Should succeed after receiving ACK
    assert!(result.is_ok());
    // Should take roughly 100ms (the delay we added)
    assert!(elapsed >= Duration::from_millis(90));
    assert!(elapsed <= Duration::from_millis(200));

    // Verify the actor was registered
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 1);
}

#[tokio::test]
async fn test_register_actor_sync_timeout() {
    // Test: Should timeout if no ACK received
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    // Add a healthy peer but don't send ACK
    add_healthy_peer(&registry, peer_addr).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    let start = std::time::Instant::now();
    let result = registry
        .register_actor_sync(
            actor_name.clone(),
            actor_location,
            Duration::from_millis(200), // Short timeout
        )
        .await;
    let elapsed = start.elapsed();

    // Should still succeed even after timeout (graceful fallback)
    assert!(result.is_ok());
    // Should take at least the timeout duration
    assert!(elapsed >= Duration::from_millis(180));
    assert!(elapsed <= Duration::from_millis(300));

    // Verify the actor was registered despite timeout
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 1);

    // Verify pending ACK was cleaned up
    let pending_acks = registry.pending_acks.lock().await;
    assert!(!pending_acks.contains_key(&actor_name));
}

#[tokio::test]
async fn test_register_actor_sync_peer_rejects() {
    // Test: Should handle peer rejection gracefully
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    // Add a healthy peer
    add_healthy_peer(&registry, peer_addr).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    // Simulate peer sending rejection ACK
    let registry_clone = Arc::new(registry.clone());
    let actor_name_clone = actor_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Simulate receiving an ImmediateAck with success=false
        let mut pending_acks = registry_clone.pending_acks.lock().await;
        if let Some(sender) = pending_acks.remove(&actor_name_clone) {
            let _ = sender.send(false); // Send rejection ACK
        }
    });

    let result = registry
        .register_actor_sync(actor_name.clone(), actor_location, Duration::from_secs(1))
        .await;

    // Should return error when peer rejects
    assert!(result.is_err());
    match result {
        Err(GossipError::Network(io_error)) => {
            let error_message = io_error.to_string();
            assert!(error_message.contains("Peer rejected registration"));
        }
        _ => panic!("Expected GossipError::Network with rejection message"),
    }
}

#[tokio::test]
async fn test_register_actor_sync_only_failed_peers() {
    // Test: Should behave like no peers when all peers are failed
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    // Add a peer that has exceeded max failures
    add_failed_peer(&registry, peer_addr, 5).await; // max_failures is 3 in our config

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    let start = std::time::Instant::now();
    let result = registry
        .register_actor_sync(actor_name.clone(), actor_location, Duration::from_secs(1))
        .await;
    let elapsed = start.elapsed();

    // Should succeed immediately like the no-peers case
    assert!(result.is_ok());
    // Should complete very quickly
    assert!(elapsed < Duration::from_millis(100));
}

#[tokio::test]
async fn test_register_actor_sync_channel_closed() {
    // Test: Handle case where ACK channel is closed unexpectedly
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    // Add a healthy peer
    add_healthy_peer(&registry, peer_addr).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    // Simulate channel being closed by removing and dropping the sender
    let registry_clone = Arc::new(registry.clone());
    let actor_name_clone = actor_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Remove the sender without calling send (simulates channel close)
        let mut pending_acks = registry_clone.pending_acks.lock().await;
        let _dropped_sender = pending_acks.remove(&actor_name_clone);
        // sender is dropped here, closing the channel
    });

    let result = registry
        .register_actor_sync(actor_name.clone(), actor_location, Duration::from_secs(1))
        .await;

    // Should succeed even if channel is closed (graceful fallback)
    assert!(result.is_ok());

    // Verify the actor was registered
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 1);
}
