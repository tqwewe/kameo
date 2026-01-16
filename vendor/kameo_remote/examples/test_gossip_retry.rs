use kameo_remote::*;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,test_gossip_retry=info")
        .init();

    let node1_addr = "127.0.0.1:37001".parse().unwrap();
    let node2_addr = "127.0.0.1:37002".parse().unwrap();

    info!("🚀 Testing gossip retry mechanism");

    let node1_keypair = KeyPair::new_for_testing("node1");
    let node2_keypair = KeyPair::new_for_testing("node2");

    // Start node1 with a peer configured for node2 (which isn't running yet)
    info!("Starting node1 with node2 as a peer (node2 not running yet)");
    let handle1 = GossipRegistryHandle::new_with_keypair(
        node1_addr,
        node1_keypair.clone(),
        Some(GossipConfig {
            gossip_interval: Duration::from_secs(2), // Gossip every 2 seconds for faster testing
            peer_retry_interval: Duration::from_secs(5), // Retry failed peers every 5 seconds
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Add node2 as a peer and try to connect (should fail)
    let peer2 = handle1.add_peer(&node2_keypair.peer_id()).await;
    match peer2.connect(&node2_addr).await {
        Ok(_) => info!("Unexpectedly connected to node2"),
        Err(e) => info!("Expected failure connecting to node2: {}", e),
    }

    // Register an actor on node1
    handle1
        .register("actor1".to_string(), "127.0.0.1:47001".parse().unwrap())
        .await
        .unwrap();
    info!("Registered actor1 on node1");

    // Wait a bit to see retry attempts
    info!("Waiting 10 seconds to observe retry attempts...");
    sleep(Duration::from_secs(10)).await;

    // Now start node2
    info!("Starting node2 after 10 seconds");
    let handle2 = GossipRegistryHandle::new_with_keypair(
        node2_addr,
        node2_keypair.clone(),
        Some(GossipConfig {
            gossip_interval: Duration::from_secs(2),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Add node1 as a peer
    let peer1 = handle2.add_peer(&node1_keypair.peer_id()).await;
    peer1.connect(&node1_addr).await.unwrap();

    // Register an actor on node2
    handle2
        .register("actor2".to_string(), "127.0.0.1:47002".parse().unwrap())
        .await
        .unwrap();
    info!("Registered actor2 on node2");

    // Wait for gossip to propagate (should happen quickly now)
    info!("Waiting for gossip propagation...");
    sleep(Duration::from_secs(5)).await;

    // Check if actors are visible on both nodes
    let actor1_from_2 = handle2.lookup("actor1").await;
    let actor2_from_1 = handle1.lookup("actor2").await;

    info!("Node2 sees actor1: {}", actor1_from_2.is_some());
    info!("Node1 sees actor2: {}", actor2_from_1.is_some());

    if actor1_from_2.is_some() && actor2_from_1.is_some() {
        info!("✅ SUCCESS: Gossip retry worked! Both nodes discovered each other's actors");
    } else {
        info!("❌ FAILURE: Gossip retry did not work properly");
    }

    // Show stats
    let stats1 = handle1.stats().await;
    let stats2 = handle2.stats().await;

    info!("Node1 stats: {:?}", stats1);
    info!("Node2 stats: {:?}", stats2);

    // Cleanup
    handle1.shutdown().await;
    handle2.shutdown().await;
}
