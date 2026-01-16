use kameo_remote::{GossipRegistryHandle, SecretKey};
use std::time::Duration;

/// Test mutual authentication between two TLS-enabled nodes
#[tokio::test]
async fn test_mutual_authentication() {
    // Install the crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .try_init()
        .ok();

    // Generate keypairs for both nodes
    let secret_key_a = SecretKey::generate();
    let node_id_a = secret_key_a.public();

    let secret_key_b = SecretKey::generate();
    let node_id_b = secret_key_b.public();

    // Create TLS-enabled registries
    let registry_a =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_a, None)
            .await
            .expect("Failed to create registry A");

    let registry_b =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_b, None)
            .await
            .expect("Failed to create registry B");

    // Get their addresses
    let addr_a = registry_a.registry.bind_addr;
    let addr_b = registry_b.registry.bind_addr;

    // Add peers with NodeIds for TLS
    registry_a
        .registry
        .add_peer_with_node_id(addr_b, Some(node_id_b))
        .await;
    registry_b
        .registry
        .add_peer_with_node_id(addr_a, Some(node_id_a))
        .await;

    // Register an actor on A
    registry_a
        .register("test_actor".to_string(), "127.0.0.1:8000".parse().unwrap())
        .await
        .expect("Failed to register actor");

    // Wait for gossip to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify B can find the actor
    let location = registry_b.lookup("test_actor").await;
    assert!(location.is_some(), "Actor should be found on registry B");

    // Both registries should be connected with TLS
    let stats_a = registry_a.registry.get_stats().await;
    let stats_b = registry_b.registry.get_stats().await;

    assert_eq!(
        stats_a.active_peers, 1,
        "Registry A should have 1 connected peer"
    );
    assert_eq!(
        stats_b.active_peers, 1,
        "Registry B should have 1 connected peer"
    );
}

/// Test that impersonation is prevented - wrong NodeId rejects connection
#[tokio::test]
async fn test_impersonation_prevention() {
    // Install the crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init()
        .ok();

    // Generate keypairs
    let secret_key_a = SecretKey::generate();
    let node_id_a = secret_key_a.public();

    let secret_key_b = SecretKey::generate();
    let _node_id_b = secret_key_b.public();

    // Generate a third key that will try to impersonate
    let secret_key_imposter = SecretKey::generate();
    let node_id_imposter = secret_key_imposter.public();

    // Create registries
    let registry_a =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_a, None)
            .await
            .expect("Failed to create registry A");

    let registry_imposter = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_key_imposter,
        None,
    )
    .await
    .expect("Failed to create imposter registry");

    let addr_a = registry_a.registry.bind_addr;
    let addr_imposter = registry_imposter.registry.bind_addr;

    // Registry A expects node_id_b but imposter has different key
    // This should fail during TLS handshake
    registry_a
        .registry
        .add_peer_with_node_id(addr_imposter, Some(node_id_a))
        .await; // Wrong NodeId!
    registry_imposter
        .registry
        .add_peer_with_node_id(addr_a, Some(node_id_imposter))
        .await;

    // Register an actor on imposter
    registry_imposter
        .register(
            "secret_actor".to_string(),
            "127.0.0.1:9000".parse().unwrap(),
        )
        .await
        .expect("Failed to register actor");

    // Wait a bit for connection attempts
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The actor should NOT propagate because TLS handshake should fail
    let location = registry_a.lookup("secret_actor").await;
    assert!(
        location.is_none(),
        "Actor from imposter should not be found"
    );

    // Stats should show failed connection
    let stats_a = registry_a.registry.get_stats().await;
    assert_eq!(
        stats_a.active_peers, 0,
        "Should have no connected peers due to NodeId mismatch"
    );
}

/// Test that nodes can communicate bidirectionally over TLS
#[tokio::test]
async fn test_bidirectional_tls_communication() {
    // Install the crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .try_init()
        .ok();

    let secret_key_a = SecretKey::generate();
    let _node_id_a = secret_key_a.public();

    let secret_key_b = SecretKey::generate();
    let node_id_b = secret_key_b.public();

    // Create registries
    let registry_a =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_a, None)
            .await
            .expect("Failed to create registry A");

    let registry_b =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_b, None)
            .await
            .expect("Failed to create registry B");

    let _addr_a = registry_a.registry.bind_addr;
    let addr_b = registry_b.registry.bind_addr;

    // Only A knows about B initially
    registry_a
        .registry
        .add_peer_with_node_id(addr_b, Some(node_id_b))
        .await;

    // Register actors on both sides
    registry_a
        .register("actor_a".to_string(), "127.0.0.1:10001".parse().unwrap())
        .await
        .expect("Failed to register actor A");

    registry_b
        .register("actor_b".to_string(), "127.0.0.1:10002".parse().unwrap())
        .await
        .expect("Failed to register actor B");

    // Wait for bidirectional discovery and gossip
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Both should know about each other's actors
    let location_b_on_a = registry_a.lookup("actor_b").await;
    let location_a_on_b = registry_b.lookup("actor_a").await;

    assert!(location_b_on_a.is_some(), "A should know about B's actor");
    assert!(location_a_on_b.is_some(), "B should know about A's actor");
}

/// Test multiple nodes with TLS in a chain topology
#[tokio::test]
async fn test_multi_node_tls_chain() {
    // Install the crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init()
        .ok();

    // Create 3 nodes in a chain: A -> B -> C
    let secret_key_a = SecretKey::generate();
    let _node_id_a = secret_key_a.public();

    let secret_key_b = SecretKey::generate();
    let node_id_b = secret_key_b.public();

    let secret_key_c = SecretKey::generate();
    let node_id_c = secret_key_c.public();

    let registry_a =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_a, None)
            .await
            .expect("Failed to create registry A");

    let registry_b =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_b, None)
            .await
            .expect("Failed to create registry B");

    let registry_c =
        GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse().unwrap(), secret_key_c, None)
            .await
            .expect("Failed to create registry C");

    let _addr_a = registry_a.registry.bind_addr;
    let addr_b = registry_b.registry.bind_addr;
    let addr_c = registry_c.registry.bind_addr;

    // Set up chain topology
    registry_a
        .registry
        .add_peer_with_node_id(addr_b, Some(node_id_b))
        .await;
    registry_b
        .registry
        .add_peer_with_node_id(addr_c, Some(node_id_c))
        .await;

    // Register actors on each node
    registry_a
        .register("actor_a".to_string(), "127.0.0.1:11001".parse().unwrap())
        .await
        .unwrap();
    registry_b
        .register("actor_b".to_string(), "127.0.0.1:11002".parse().unwrap())
        .await
        .unwrap();
    registry_c
        .register("actor_c".to_string(), "127.0.0.1:11003".parse().unwrap())
        .await
        .unwrap();

    // Wait for gossip to propagate through the chain
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check that information propagates through the chain
    // B should know about A's and C's actors
    let b_knows_a = registry_b.lookup("actor_a").await;
    let b_knows_c = registry_b.lookup("actor_c").await;

    assert!(b_knows_a.is_some(), "B should know about A's actor");
    assert!(b_knows_c.is_some(), "B should know about C's actor");

    // A and C might not directly know about each other (depends on gossip)
    // But they should at least know about B
    let a_knows_b = registry_a.lookup("actor_b").await;
    let c_knows_b = registry_c.lookup("actor_b").await;

    assert!(a_knows_b.is_some(), "A should know about B's actor");
    assert!(c_knows_b.is_some(), "C should know about B's actor");
}

/// Test that TLS connections handle disconnection and reconnection
#[tokio::test]
async fn test_tls_reconnection() {
    // Install the crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init()
        .ok();

    let secret_key_a = SecretKey::generate();
    let node_id_a = secret_key_a.public();

    let secret_key_b = SecretKey::generate();
    let node_id_b = secret_key_b.public();

    // Create registry A
    let registry_a = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_key_a.clone(),
        None,
    )
    .await
    .expect("Failed to create registry A");

    // Create registry B
    let registry_b = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_key_b.clone(),
        None,
    )
    .await
    .expect("Failed to create registry B");

    let addr_a = registry_a.registry.bind_addr;
    let addr_b = registry_b.registry.bind_addr;

    // Connect them
    registry_a
        .registry
        .add_peer_with_node_id(addr_b, Some(node_id_b))
        .await;
    registry_b
        .registry
        .add_peer_with_node_id(addr_a, Some(node_id_a))
        .await;

    // Register an actor on A
    registry_a
        .register(
            "persistent_actor".to_string(),
            "127.0.0.1:12000".parse().unwrap(),
        )
        .await
        .unwrap();

    // Wait for propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify B knows about it
    let location = registry_b.lookup("persistent_actor").await;
    assert!(location.is_some(), "B should know about A's actor");

    // Shutdown B
    drop(registry_b);

    // Wait a bit
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A should detect disconnection
    let stats = registry_a.registry.get_stats().await;
    assert_eq!(
        stats.active_peers, 0,
        "A should have no connected peers after B shutdown"
    );

    // Restart B with same key and port
    let registry_b_new = GossipRegistryHandle::new_with_tls(addr_b, secret_key_b, None)
        .await
        .expect("Failed to recreate registry B");

    // Wait for reconnection
    tokio::time::sleep(Duration::from_secs(6)).await; // Retry interval is 5 seconds

    // They should reconnect and B should learn about the actor again
    let location = registry_b_new.lookup("persistent_actor").await;
    assert!(
        location.is_some(),
        "B should know about A's actor after reconnection"
    );

    let stats = registry_a.registry.get_stats().await;
    assert_eq!(stats.active_peers, 1, "A should have reconnected to B");
}

/// Test DNS name encoding and decoding for NodeIds
#[tokio::test]
async fn test_node_id_dns_encoding() {
    use kameo_remote::tls::name;

    // Test with various NodeIds
    for _ in 0..10 {
        let secret_key = SecretKey::generate();
        let node_id = secret_key.public();

        // Encode to DNS name
        let dns_name = name::encode(&node_id);

        // Should be valid DNS name format
        assert!(dns_name.ends_with(".kameo.invalid"));
        assert!(dns_name.len() < 255); // DNS name length limit

        // Decode back
        let decoded = name::decode(&dns_name);
        assert!(decoded.is_some(), "Should decode successfully");
        assert_eq!(
            decoded.unwrap(),
            node_id,
            "Round-trip should preserve NodeId"
        );
    }

    // Test invalid DNS names
    assert!(name::decode("invalid.name").is_none());
    assert!(name::decode("").is_none());
    assert!(name::decode("not-base32.kameo.invalid").is_none());
}

/// Test certificate generation and basic validation
#[tokio::test]
async fn test_certificate_generation() {
    // Install the crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    // Generate multiple certificates and verify they're created correctly
    for _ in 0..5 {
        let secret_key = SecretKey::generate();
        let node_id = secret_key.public();

        // Create a TLS config which internally creates certificates
        let tls_config =
            kameo_remote::tls::TlsConfig::new(secret_key).expect("Failed to create TLS config");

        // Verify the node_id matches
        assert_eq!(
            tls_config.node_id, node_id,
            "TLS config should have correct NodeId"
        );

        // Verify we can create connector and acceptor
        let _connector = tls_config.connector();
        let _acceptor = tls_config.acceptor();
    }
}
