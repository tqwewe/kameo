use kameo_remote::{GossipRegistryHandle, SecretKey};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install default crypto provider (ring)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Enable debug logging
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .init();

    println!("Starting TLS-enabled gossip test...\n");

    // Generate keys for both nodes
    let node1_key = SecretKey::generate();
    let node1_id = node1_key.public();
    println!("Node1 ID: {}", node1_id.fmt_short());

    let node2_key = SecretKey::generate();
    let node2_id = node2_key.public();
    println!("Node2 ID: {}", node2_id.fmt_short());

    // Create addresses
    let addr1: SocketAddr = "127.0.0.1:9001".parse()?;
    let addr2: SocketAddr = "127.0.0.1:9002".parse()?;

    // Create node 1 with TLS enabled
    println!("\nStarting Node1 with TLS...");
    let handle1 = GossipRegistryHandle::new_with_tls(addr1, node1_key, None).await?;

    // Create node 2 with TLS enabled
    println!("Starting Node2 with TLS...");
    let handle2 = GossipRegistryHandle::new_with_tls(addr2, node2_key, None).await?;

    // Add peers
    println!("\nAdding peers...");
    let peer1 = handle1.add_peer(&node2_id.to_peer_id()).await;
    peer1.connect(&addr2).await?;

    let peer2 = handle2.add_peer(&node1_id.to_peer_id()).await;
    peer2.connect(&addr1).await?;

    // Register actors on each node
    println!("\nRegistering actors...");
    handle1.register("actor1".to_string(), addr1).await?;
    handle2.register("actor2".to_string(), addr2).await?;

    // Wait for gossip to propagate
    println!("\nWaiting for gossip propagation...");
    sleep(Duration::from_secs(2)).await;

    // Check that each node knows about the other's actors
    let actor1_on_node2 = handle2.lookup("actor1").await;
    let actor2_on_node1 = handle1.lookup("actor2").await;

    println!("\nResults:");
    println!("Node2 knows about actor1: {:?}", actor1_on_node2);
    println!("Node1 knows about actor2: {:?}", actor2_on_node1);

    if actor1_on_node2.is_some() && actor2_on_node1.is_some() {
        println!("\n✓ TLS-encrypted gossip successful!");
        println!("✓ Both nodes exchanged actor information over encrypted connections");
    } else {
        println!("\n✗ Gossip failed - actors not propagated");
    }

    // Get stats
    let stats1 = handle1.stats().await;
    let stats2 = handle2.stats().await;

    println!("\nNode1 stats: {:?}", stats1);
    println!("Node2 stats: {:?}", stats2);

    Ok(())
}
