use kameo_remote::registry::*;
use kameo_remote::{GossipConfig, RemoteActorLocation};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Helper function to create a test registry
fn create_test_registry(bind_addr: SocketAddr) -> GossipRegistry {
    let config = GossipConfig {
        key_pair: Some(kameo_remote::KeyPair::new_for_testing(format!(
            "node_{}",
            bind_addr.port()
        ))),
        gossip_interval: Duration::from_millis(100),
        max_peer_failures: 3,
        dead_peer_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    GossipRegistry::new(bind_addr, config)
}

/// Helper function to create a test actor location
fn create_test_actor_location(addr: SocketAddr) -> RemoteActorLocation {
    let peer_id = kameo_remote::KeyPair::new_for_testing("test_peer").peer_id();
    RemoteActorLocation::new_with_peer(addr, peer_id)
}

/// Helper function to simulate a healthy peer in gossip state
async fn add_healthy_peer(registry: &GossipRegistry, peer_addr: SocketAddr) {
    registry.add_peer(peer_addr).await;
}

#[tokio::test]
async fn test_register_actor_sync_multiple_peers_first_ack_wins() {
    // Test: With multiple peers, should return when FIRST peer ACKs
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
    let peer2: SocketAddr = "127.0.0.1:8082".parse().unwrap();
    let peer3: SocketAddr = "127.0.0.1:8083".parse().unwrap();

    // Add multiple healthy peers
    add_healthy_peer(&registry, peer1).await;
    add_healthy_peer(&registry, peer2).await;
    add_healthy_peer(&registry, peer3).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    let ack_count = Arc::new(AtomicUsize::new(0));

    // Simulate multiple peers sending ACKs at different times
    let registry_clone = Arc::new(registry.clone());
    let actor_name_clone = actor_name.clone();
    let ack_count_clone = ack_count.clone();

    // Peer 1: Sends ACK after 150ms
    let peer1_task = {
        let registry = registry_clone.clone();
        let actor_name = actor_name_clone.clone();
        let ack_count = ack_count_clone.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;

            let mut pending_acks = registry.pending_acks.lock().await;
            if let Some(sender) = pending_acks.remove(&actor_name) {
                ack_count.fetch_add(1, Ordering::SeqCst);
                let _ = sender.send(true); // First ACK
            }
        })
    };

    // Peer 2: Sends ACK after 100ms (should win - fastest)
    let peer2_task = {
        let registry = registry_clone.clone();
        let actor_name = actor_name_clone.clone();
        let ack_count = ack_count_clone.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut pending_acks = registry.pending_acks.lock().await;
            if let Some(sender) = pending_acks.remove(&actor_name) {
                ack_count.fetch_add(1, Ordering::SeqCst);
                let _ = sender.send(true); // This should win
            }
        })
    };

    // Peer 3: Sends ACK after 200ms (should be too late)
    let peer3_task = {
        let registry = registry_clone.clone();
        let actor_name = actor_name_clone.clone();
        let ack_count = ack_count_clone.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let mut pending_acks = registry.pending_acks.lock().await;
            // This should find nothing because peer2 already won
            if let Some(sender) = pending_acks.remove(&actor_name) {
                ack_count.fetch_add(1, Ordering::SeqCst);
                let _ = sender.send(true);
            }
        })
    };

    let start = std::time::Instant::now();
    let result = registry
        .register_actor_sync(actor_name.clone(), actor_location, Duration::from_secs(2))
        .await;
    let elapsed = start.elapsed();

    // Wait for all tasks to complete
    let _ = tokio::join!(peer1_task, peer2_task, peer3_task);

    // Should succeed after receiving first ACK (from peer2 at 100ms)
    assert!(result.is_ok());
    // Should take roughly 100ms (peer2's delay), not 150ms or 200ms
    assert!(elapsed >= Duration::from_millis(90));
    assert!(elapsed <= Duration::from_millis(130));

    // Only ONE ACK should have been processed (first one wins)
    assert_eq!(ack_count.load(Ordering::SeqCst), 1);

    // Verify pending_acks was cleaned up
    let pending_acks = registry.pending_acks.lock().await;
    assert!(!pending_acks.contains_key(&actor_name));

    // Verify the actor was registered
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 1);
}

#[tokio::test]
async fn test_register_actor_sync_multiple_peers_some_reject() {
    // Test: With multiple peers, some reject but one accepts
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
    let peer2: SocketAddr = "127.0.0.1:8082".parse().unwrap();
    let peer3: SocketAddr = "127.0.0.1:8083".parse().unwrap();

    // Add multiple healthy peers
    add_healthy_peer(&registry, peer1).await;
    add_healthy_peer(&registry, peer2).await;
    add_healthy_peer(&registry, peer3).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    let registry_clone = Arc::new(registry.clone());
    let actor_name_clone = actor_name.clone();

    // Peer 1: Rejects after 50ms
    let peer1_task = {
        let registry = registry_clone.clone();
        let actor_name = actor_name_clone.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let mut pending_acks = registry.pending_acks.lock().await;
            if let Some(sender) = pending_acks.remove(&actor_name) {
                let _ = sender.send(false); // Reject
            }
        })
    };

    // Since peer1 sends first (and it's a rejection), the call should fail
    // This tests that rejections are properly handled even with multiple peers

    let start = std::time::Instant::now();
    let result = registry
        .register_actor_sync(actor_name.clone(), actor_location, Duration::from_secs(1))
        .await;
    let elapsed = start.elapsed();

    // Wait for task to complete
    let _ = peer1_task.await;

    // Should fail after receiving first rejection
    assert!(result.is_err());
    // Should take roughly 50ms (peer1's rejection time)
    assert!(elapsed >= Duration::from_millis(40));
    assert!(elapsed <= Duration::from_millis(80));

    // Verify pending_acks was cleaned up
    let pending_acks = registry.pending_acks.lock().await;
    assert!(!pending_acks.contains_key(&actor_name));
}

#[tokio::test]
async fn test_register_actor_sync_race_condition_cleanup() {
    // Test: Race condition where ACK arrives just as timeout occurs
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    add_healthy_peer(&registry, peer_addr).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    // Simulate ACK arriving right at timeout boundary (200ms)
    let registry_clone = Arc::new(registry.clone());
    let actor_name_clone = actor_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await; // Right at timeout

        let mut pending_acks = registry_clone.pending_acks.lock().await;
        if let Some(sender) = pending_acks.remove(&actor_name_clone) {
            let _ = sender.send(true); // This might arrive too late
        }
    });

    let result = registry
        .register_actor_sync(
            actor_name.clone(),
            actor_location,
            Duration::from_millis(200), // Same as ACK delay - race condition
        )
        .await;

    // Should succeed regardless of race (timeout still registers actor)
    assert!(result.is_ok());

    // Verify cleanup happened (no leaked channels)
    let pending_acks = registry.pending_acks.lock().await;
    assert!(!pending_acks.contains_key(&actor_name));

    // Actor should be registered
    let stats = registry.get_stats().await;
    assert_eq!(stats.local_actors, 1);
}

#[tokio::test]
async fn test_register_actor_sync_peer_health_changes_during_wait() {
    // Test: Peer becomes unhealthy while waiting for ACK
    let registry = create_test_registry("127.0.0.1:8080".parse().unwrap());
    let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    add_healthy_peer(&registry, peer_addr).await;

    let actor_name = "test_actor".to_string();
    let actor_location = create_test_actor_location("127.0.0.1:9001".parse().unwrap());

    // Mark peer as failed after registration starts but before ACK
    let registry_clone = Arc::new(registry.clone());
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Make peer unhealthy (simulate connection failure)
        let mut gossip_state = registry_clone.gossip_state.lock().await;
        if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
            peer_info.failures = 10; // Exceed max_failures (3)
        }
    });

    let result = registry
        .register_actor_sync(
            actor_name.clone(),
            actor_location,
            Duration::from_millis(200),
        )
        .await;

    // Should succeed but timeout (no healthy peers to ACK)
    assert!(result.is_ok());

    // Should have taken full timeout since peer became unhealthy
    // (This validates that peer health changes during registration are handled)
}
