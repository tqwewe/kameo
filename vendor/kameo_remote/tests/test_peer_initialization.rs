use kameo_remote::*;
use std::time::Duration;
use tokio::time::sleep;

/// Test the proposed fix: Allow specifying peer names when initializing
#[tokio::test]
async fn test_peer_initialization_with_names() {
    println!("🧪 Testing proposed peer initialization with names");

    let node1_addr = "127.0.0.1:35003".parse().unwrap();
    let node2_addr = "127.0.0.1:35004".parse().unwrap();
    let node3_addr = "127.0.0.1:35005".parse().unwrap();

    let node1_keypair = KeyPair::new_for_testing("node1");
    let node2_keypair = KeyPair::new_for_testing("node2");
    let node3_keypair = KeyPair::new_for_testing("node3");

    let handle1 = GossipRegistryHandle::new_with_tls(
        node1_addr,
        node1_keypair.to_secret_key(),
        Some(GossipConfig {
            key_pair: Some(node1_keypair.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let handle2 = GossipRegistryHandle::new_with_tls(
        node2_addr,
        node2_keypair.to_secret_key(),
        Some(GossipConfig {
            key_pair: Some(node2_keypair.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let handle3 = GossipRegistryHandle::new_with_tls(
        node3_addr,
        node3_keypair.to_secret_key(),
        Some(GossipConfig {
            key_pair: Some(node3_keypair.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Now add peers with correct names
    // Node1 knows Node2 and Node3
    let peer2_from_1 = handle1.add_peer(&node2_keypair.peer_id()).await;
    let peer3_from_1 = handle1.add_peer(&node3_keypair.peer_id()).await;
    peer2_from_1.connect(&node2_addr).await.unwrap();
    peer3_from_1.connect(&node3_addr).await.unwrap();

    // Node2 knows Node1 and Node3
    let peer1_from_2 = handle2.add_peer(&node1_keypair.peer_id()).await;
    let peer3_from_2 = handle2.add_peer(&node3_keypair.peer_id()).await;
    peer1_from_2.connect(&node1_addr).await.unwrap();
    peer3_from_2.connect(&node3_addr).await.unwrap();

    // Node3 knows Node1 and Node2
    let peer1_from_3 = handle3.add_peer(&node1_keypair.peer_id()).await;
    let peer2_from_3 = handle3.add_peer(&node2_keypair.peer_id()).await;
    peer1_from_3.connect(&node1_addr).await.unwrap();
    peer2_from_3.connect(&node2_addr).await.unwrap();

    // Wait for connections
    sleep(Duration::from_millis(200)).await;

    // Register actors
    handle1
        .register("service1".to_string(), "127.0.0.1:46001".parse().unwrap())
        .await
        .unwrap();
    handle2
        .register("service2".to_string(), "127.0.0.1:46002".parse().unwrap())
        .await
        .unwrap();
    handle3
        .register("service3".to_string(), "127.0.0.1:46003".parse().unwrap())
        .await
        .unwrap();

    // Wait for gossip
    sleep(Duration::from_millis(500)).await;

    // Test discovery from all nodes
    println!("\n🔍 Testing full mesh discovery:");

    // Each node should discover all other services
    for (handle, node_name) in [
        (&handle1, "node1"),
        (&handle2, "node2"),
        (&handle3, "node3"),
    ] {
        println!("\nFrom {}:", node_name);
        for service in ["service1", "service2", "service3"] {
            let result = handle.lookup(service).await;
            println!("  lookup('{}') = {:?}", service, result.is_some());
            assert!(
                result.is_some(),
                "{} should discover {}",
                node_name,
                service
            );
        }
    }

    // Cleanup
    handle1.shutdown().await;
    handle2.shutdown().await;
    handle3.shutdown().await;
}
