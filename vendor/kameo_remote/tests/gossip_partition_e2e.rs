mod common;

use common::{connect_bidirectional, create_tls_node, force_disconnect, wait_for_condition};
use kameo_remote::GossipConfig;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_partition_heal_flow() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        // Keep automatic peer retries suppressed long enough for the forced
        // partition to remain in place until we manually reconnect node B and
        // node C. Short retry windows (the default 300ms) caused node B to
        // reconnect on its own, letting the actor propagate prematurely.
        peer_retry_interval: Duration::from_secs(5),
        ..Default::default()
    };

    let node_a = create_tls_node(config.clone()).await?;
    let node_b = create_tls_node(config.clone()).await?;
    let node_c = create_tls_node(config.clone()).await?;

    connect_bidirectional(&node_a, &node_b).await?;
    connect_bidirectional(&node_b, &node_c).await?;

    node_c
        .register("actor.before".to_string(), "127.0.0.1:9601".parse()?)
        .await?;
    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            node_a.lookup("actor.before").await.is_some()
        })
        .await,
        "pre-partition actor should propagate to node A"
    );

    force_disconnect(&node_b, &node_c).await;
    sleep(Duration::from_millis(100)).await;

    node_c
        .register("actor.partitioned".to_string(), "127.0.0.1:9602".parse()?)
        .await?;

    assert!(
        !wait_for_condition(Duration::from_millis(500), || async {
            node_a.lookup("actor.partitioned").await.is_some()
        })
        .await,
        "actor should not cross partition before heal"
    );

    connect_bidirectional(&node_b, &node_c).await?;

    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            node_a.lookup("actor.partitioned").await.is_some()
        })
        .await,
        "actor should propagate after heal"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;

    Ok(())
}
