mod common;

use common::{connect_bidirectional, create_tls_node, parse_addr, wait_for_condition};
use kameo_remote::GossipConfig;
use std::time::Duration;

#[tokio::test]
async fn test_local_actor_wins_over_remote_update() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        ..Default::default()
    };

    let node_a = create_tls_node(config.clone()).await?;
    let node_b = create_tls_node(config.clone()).await?;
    connect_bidirectional(&node_a, &node_b).await?;

    let addr_a = parse_addr("127.0.0.1:9401");
    let addr_b = parse_addr("127.0.0.1:9402");

    let (res_a, res_b) = tokio::join!(
        node_a.register("actor.concurrent".to_string(), addr_a),
        node_b.register("actor.concurrent".to_string(), addr_b)
    );
    res_a?;
    res_b?;

    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            node_a.lookup("actor.concurrent").await.is_some()
                && node_b.lookup("actor.concurrent").await.is_some()
        })
        .await,
        "both nodes should see the actor registration"
    );

    let addr_a_str = addr_a.to_string();
    let addr_b_str = addr_b.to_string();
    let final_a = node_a
        .lookup("actor.concurrent")
        .await
        .expect("node A actor location")
        .address;
    let final_b = node_b
        .lookup("actor.concurrent")
        .await
        .expect("node B actor location")
        .address;

    assert_eq!(
        final_a, addr_a_str,
        "node A should retain its local actor registration"
    );
    assert_eq!(
        final_b, addr_b_str,
        "node B should retain its local actor registration"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;

    Ok(())
}
