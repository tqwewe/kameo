use kameo_remote::{ClockOrdering, SecretKey, VectorClock};
use std::collections::HashSet;

#[test]
fn test_vector_clock_basic_operations() {
    // Create valid node IDs using SecretKey
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();

    let clock1 = VectorClock::new();
    let clock2 = VectorClock::new();

    // Initial state: both empty clocks are equal
    assert_eq!(clock1.compare(&clock2), ClockOrdering::Equal);

    // Node1 increments
    clock1.increment(node1);
    assert!(clock2.happens_before(&clock1));
    assert!(clock1.happens_after(&clock2));

    // Node2 increments
    clock2.increment(node2);
    assert!(clock1.is_concurrent(&clock2));

    // Node2 merges with node1's clock and increments
    clock2.merge(&clock1);
    clock2.increment(node2);
    assert!(clock1.happens_before(&clock2));
}

#[test]
fn test_vector_clock_merge() {
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let key3 = SecretKey::from_bytes(&[3u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();
    let node3 = key3.public();

    let clock1 = VectorClock::new();
    clock1.increment(node1);
    clock1.increment(node1); // node1: 2

    let clock2 = VectorClock::new();
    clock2.increment(node2);
    clock2.increment(node2);
    clock2.increment(node2); // node2: 3

    let clock3 = VectorClock::new();
    clock3.increment(node3); // node3: 1

    // Merge all clocks
    clock1.merge(&clock2);
    clock1.merge(&clock3);

    // clock1 should now have: node1: 2, node2: 3, node3: 1
    assert_eq!(clock1.get(&node1), 2);
    assert_eq!(clock1.get(&node2), 3);
    assert_eq!(clock1.get(&node3), 1);
}

#[test]
fn test_vector_clock_comparison() {
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();

    let clock1 = VectorClock::new();
    clock1.increment(node1);
    clock1.increment(node2); // node1: 1, node2: 1

    let clock2 = VectorClock::new();
    clock2.increment(node1); // node1: 1, node2: 0

    // clock2 is before clock1 (clock1 has everything clock2 has and more)
    assert_eq!(clock2.compare(&clock1), ClockOrdering::Before);
    assert_eq!(clock1.compare(&clock2), ClockOrdering::After);

    // Make clock2 higher in node2
    clock2.increment(node2);
    clock2.increment(node2); // node1: 1, node2: 2

    // clock1 is before clock2 (clock2 has everything clock1 has and more)
    assert_eq!(clock1.compare(&clock2), ClockOrdering::Before);
    assert_eq!(clock2.compare(&clock1), ClockOrdering::After);

    // Create a concurrent scenario
    let clock4 = VectorClock::new();
    clock4.increment(node1);
    clock4.increment(node1); // node1: 2, node2: 0
    let clock5 = VectorClock::new();
    clock5.increment(node2);
    clock5.increment(node2); // node1: 0, node2: 2

    // These are concurrent (neither dominates the other)
    assert_eq!(clock4.compare(&clock5), ClockOrdering::Concurrent);

    // Equal clocks
    let clock3 = clock1.clone();
    assert_eq!(clock1.compare(&clock3), ClockOrdering::Equal);
}

#[test]
fn test_vector_clock_gc() {
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let key3 = SecretKey::from_bytes(&[3u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();
    let node3 = key3.public();

    let clock = VectorClock::new();
    clock.increment(node1);
    clock.increment(node2);
    clock.increment(node3);

    // Should have 3 nodes
    assert_eq!(clock.get_nodes().len(), 3);

    // GC with only node1 and node2 as active
    let mut active_nodes = std::collections::HashSet::new();
    active_nodes.insert(node1);
    active_nodes.insert(node2);

    clock.gc_old_nodes(&active_nodes);

    // Should only have 2 nodes now
    assert_eq!(clock.get_nodes().len(), 2);
    assert_eq!(clock.get(&node1), 1);
    assert_eq!(clock.get(&node2), 1);
    assert_eq!(clock.get(&node3), 0); // node3 was removed
}

#[test]
fn test_vector_clock_empty_check() {
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let node1 = key1.public();

    let clock = VectorClock::new();
    assert!(clock.is_effectively_empty());

    // The new empty clock is effectively empty
    assert!(clock.is_effectively_empty());

    // Increment
    clock.increment(node1);
    assert!(!clock.is_effectively_empty());
}

#[test]
fn test_vector_clock_large_scale_no_truncation() {
    // Test that vector clocks can grow large without truncation
    let clock = VectorClock::new();

    // Add 1500 different nodes (past the old truncation limit)
    for i in 0..1500 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = (i >> 8) as u8;
        key_bytes[1] = (i & 0xFF) as u8;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();
        clock.increment(node);
    }

    // Verify all 1500 entries are preserved (no truncation)
    assert_eq!(
        clock.len(),
        1500,
        "Vector clock should preserve all 1500 entries"
    );

    // Verify we can still query all nodes
    for i in 0..1500 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = (i >> 8) as u8;
        key_bytes[1] = (i & 0xFF) as u8;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();
        assert_eq!(clock.get(&node), 1, "Each node should have value 1");
    }
}

#[test]
fn test_vector_clock_concurrent_add_remove_consistency() {
    // Test that concurrent add/remove operations are handled consistently
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();

    // Create concurrent vector clocks
    let clock1 = VectorClock::new();
    clock1.increment(node1); // node1: 1

    let clock2 = VectorClock::new();
    clock2.increment(node2); // node2: 1

    // These clocks are concurrent
    assert_eq!(clock1.compare(&clock2), ClockOrdering::Concurrent);

    // In our fixed implementation, concurrent operations should use node_id as tiebreaker
    // This ensures all nodes make the same decision

    // Simulate a scenario where both nodes make concurrent updates
    let clock1_copy = clock1.clone();
    let clock2_copy = clock2.clone();

    // Node1 advances
    clock1_copy.increment(node1); // node1: 2

    // Node2 advances
    clock2_copy.increment(node2); // node2: 2

    // Still concurrent but at different levels
    assert_eq!(clock1_copy.compare(&clock2_copy), ClockOrdering::Concurrent);
}

#[test]
fn test_vector_clock_merge_preserves_causality() {
    // Test that merging preserves causal relationships
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let key3 = SecretKey::from_bytes(&[3u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();
    let node3 = key3.public();

    // Create a causal chain: clock1 -> clock2 -> clock3
    let clock1 = VectorClock::new();
    clock1.increment(node1); // node1: 1

    let clock2 = clock1.clone();
    clock2.increment(node2); // node1: 1, node2: 1

    let clock3 = clock2.clone();
    clock3.increment(node3); // node1: 1, node2: 1, node3: 1

    // Verify causal relationships
    assert_eq!(clock1.compare(&clock2), ClockOrdering::Before);
    assert_eq!(clock2.compare(&clock3), ClockOrdering::Before);
    assert_eq!(clock1.compare(&clock3), ClockOrdering::Before);

    // Create a concurrent branch
    let clock4 = clock1.clone();
    clock4.increment(node3); // node1: 1, node3: 1

    // clock4 is concurrent with clock2 (clock2 has node2:1, clock4 has node3:1)
    assert_eq!(clock4.compare(&clock2), ClockOrdering::Concurrent);
    // clock3 happens after clock4 (clock3 has everything clock4 has plus node2)
    assert_eq!(clock4.compare(&clock3), ClockOrdering::Before);

    // Merge concurrent clocks
    let merged = clock3.clone();
    merged.merge(&clock4);

    // After merging clock4 into clock3, merged should equal clock3
    // because clock3 already has all of clock4's values (and more)
    assert_eq!(merged.compare(&clock3), ClockOrdering::Equal);
    assert_eq!(merged.compare(&clock4), ClockOrdering::After);

    // And should have max values from both
    assert_eq!(merged.get(&node1), 1);
    assert_eq!(merged.get(&node2), 1);
    assert_eq!(merged.get(&node3), 1);
}

#[test]
fn test_vector_clock_gc_preserves_active_nodes() {
    // Test that GC only removes truly inactive nodes
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let key3 = SecretKey::from_bytes(&[3u8; 32]).unwrap();
    let key4 = SecretKey::from_bytes(&[4u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();
    let node3 = key3.public();
    let node4 = key4.public();

    let clock = VectorClock::new();
    clock.increment(node1);
    clock.increment(node2);
    clock.increment(node3);
    clock.increment(node4);

    // All nodes have entries
    assert_eq!(clock.len(), 4);

    // Mark node2 and node4 as active
    let mut active_nodes = HashSet::new();
    active_nodes.insert(node2);
    active_nodes.insert(node4);

    // Run GC
    clock.gc_old_nodes(&active_nodes);

    // Only active nodes should remain
    assert_eq!(clock.len(), 2);
    assert_eq!(clock.get(&node1), 0); // Removed
    assert_eq!(clock.get(&node2), 1); // Kept (active)
    assert_eq!(clock.get(&node3), 0); // Removed
    assert_eq!(clock.get(&node4), 1); // Kept (active)
}

#[test]
fn test_vector_clock_deterministic_ordering() {
    // Test that node ID ordering provides deterministic conflict resolution
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();

    // Create two concurrent clocks
    let clock1 = VectorClock::new();
    clock1.increment(node1);

    let clock2 = VectorClock::new();
    clock2.increment(node2);

    // They should be concurrent
    assert_eq!(clock1.compare(&clock2), ClockOrdering::Concurrent);

    // For deterministic resolution, we rely on node_id comparison
    // This test verifies that SecretKey provides consistent ordering
    assert_ne!(
        node1 > node2,
        node2 > node1,
        "Node IDs should have consistent ordering"
    );
}

#[test]
fn test_vector_clock_merge_idempotent() {
    // Test that merging is idempotent
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();

    let clock1 = VectorClock::new();
    clock1.increment(node1);
    clock1.increment(node1); // node1: 2

    let clock2 = VectorClock::new();
    clock2.increment(node2);
    clock2.increment(node2);
    clock2.increment(node2); // node2: 3

    // Merge clock2 into clock1
    clock1.merge(&clock2);
    let first_merge = clock1.clone();

    // Merge again - should be idempotent
    clock1.merge(&clock2);

    assert_eq!(clock1, first_merge, "Multiple merges should be idempotent");
    assert_eq!(clock1.get(&node1), 2);
    assert_eq!(clock1.get(&node2), 3);
}

#[tokio::test]
async fn test_vector_clock_in_actor_location() {
    use kameo_remote::RemoteActorLocation;
    use std::net::SocketAddr;

    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let peer_id = kameo_remote::KeyPair::new_for_testing("test_peer").peer_id();

    let location1 = RemoteActorLocation::new_with_peer(addr, peer_id.clone());
    let location2 = RemoteActorLocation::new_with_peer(addr, peer_id.clone());

    // Increment vector clocks differently
    location1.vector_clock.increment(location1.node_id);
    location2.vector_clock.increment(location2.node_id);
    location2.vector_clock.increment(location2.node_id);

    // location2 should be "after" location1
    assert!(location1
        .vector_clock
        .happens_before(&location2.vector_clock));

    // Merge and verify
    location1.vector_clock.merge(&location2.vector_clock);
    location1.vector_clock.increment(location1.node_id);

    // Now location1 should be after location2
    assert!(location2
        .vector_clock
        .happens_before(&location1.vector_clock));
}

#[test]
fn test_vector_clock_conflict_resolution() {
    use kameo_remote::RemoteActorLocation;
    use std::net::SocketAddr;

    let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    let peer1 = kameo_remote::KeyPair::new_for_testing("peer1").peer_id();
    let peer2 = kameo_remote::KeyPair::new_for_testing("peer2").peer_id();

    let location1 = RemoteActorLocation::new_with_peer(addr1, peer1);
    let location2 = RemoteActorLocation::new_with_peer(addr2, peer2);

    // Simulate concurrent updates
    location1.vector_clock.increment(location1.node_id);
    location2.vector_clock.increment(location2.node_id);

    // They should be concurrent
    assert!(location1
        .vector_clock
        .is_concurrent(&location2.vector_clock));

    // In case of concurrent updates, we use node_id as tiebreaker
    // The one with higher node_id should win
    let should_use_location1 = location1.node_id > location2.node_id;
    let should_use_location2 = location2.node_id > location1.node_id;

    assert!(should_use_location1 != should_use_location2); // One must win
}

#[test]
fn test_actor_removed_vector_clock_ordering() {
    use kameo_remote::{ClockOrdering, SecretKey, VectorClock};

    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();

    // Scenario 1: Removal is outdated (Before)
    {
        let current_clock = VectorClock::new();
        current_clock.increment(node1);
        current_clock.increment(node1); // node1: 2

        let removal_clock = VectorClock::new();
        removal_clock.increment(node1); // node1: 1

        // Removal is before current state - should NOT apply
        assert_eq!(removal_clock.compare(&current_clock), ClockOrdering::Before);
    }

    // Scenario 2: Removal is newer (After)
    {
        let current_clock = VectorClock::new();
        current_clock.increment(node1); // node1: 1

        let removal_clock = VectorClock::new();
        removal_clock.increment(node1);
        removal_clock.increment(node1); // node1: 2

        // Removal is after current state - should apply
        assert_eq!(removal_clock.compare(&current_clock), ClockOrdering::After);
    }

    // Scenario 3: Removal is concurrent
    {
        let current_clock = VectorClock::new();
        current_clock.increment(node1); // node1: 1, node2: 0

        let removal_clock = VectorClock::new();
        removal_clock.increment(node2); // node1: 0, node2: 1

        // Removal is concurrent - conservative approach, should apply
        assert_eq!(
            removal_clock.compare(&current_clock),
            ClockOrdering::Concurrent
        );
    }

    // Scenario 4: Complex case - removal after re-add
    {
        // Actor was added by node1
        let add_clock = VectorClock::new();
        add_clock.increment(node1); // node1: 1

        // Actor was removed by node2 BEFORE seeing the add
        let early_removal_clock = VectorClock::new();
        early_removal_clock.increment(node2); // node2: 1, doesn't know about node1

        // This removal is concurrent with the add - would apply conservatively
        assert_eq!(
            early_removal_clock.compare(&add_clock),
            ClockOrdering::Concurrent
        );

        // Actor was removed by node2 AFTER seeing the add
        let late_removal_clock = add_clock.clone();
        late_removal_clock.increment(node2); // node1: 1, node2: 1

        // This removal is after the add - should definitely apply
        assert_eq!(late_removal_clock.compare(&add_clock), ClockOrdering::After);
    }
}
