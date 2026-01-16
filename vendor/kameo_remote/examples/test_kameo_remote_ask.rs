use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9002".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");
    let peer_id_a = key_pair_a.peer_id();
    let peer_id_b = key_pair_b.peer_id();

    let config_a = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let config_b = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    // Start nodes
    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config_a))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config_b))
        .await
        .unwrap();

    // Connect nodes - both directions
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    let peer_a = handle_b.add_peer(&peer_id_a).await;
    peer_a.connect(&addr_a).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    println!("Test: Basic ask with correlation tracking");

    // Get connection and test ask
    let conn = handle_a.get_connection(addr_b).await.unwrap();

    println!("Sending ask request...");
    let request = b"ECHO:Hello from Node A";
    match conn.ask_with_timeout(request, Duration::from_secs(2)).await {
        Ok(response) => {
            println!("✅ Got response: {:?}", String::from_utf8_lossy(&response));
            assert_eq!(response, b"ECHOED:Hello from Node A");
        }
        Err(e) => {
            println!("❌ Ask failed: {:?}", e);
        }
    }

    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}
