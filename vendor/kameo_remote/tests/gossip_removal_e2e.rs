mod common;

use common::{connect_bidirectional, create_tls_node, wait_for_actor, wait_for_condition};
use kameo_remote::{GossipConfig, RegistrationPriority};
use std::time::Duration;

#[tokio::test]
async fn test_actor_removal_propagates() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        ..Default::default()
    };

    let node_a = create_tls_node(config.clone()).await?;
    let node_b = create_tls_node(config.clone()).await?;
    connect_bidirectional(&node_a, &node_b).await?;

    node_a
        .register_with_priority(
            "actor.remove".to_string(),
            "127.0.0.1:9501".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;

    assert!(
        wait_for_actor(&node_b, "actor.remove", Duration::from_secs(2)).await,
        "actor should propagate before removal"
    );

    node_a.unregister("actor.remove").await?;

    assert!(
        wait_for_condition(Duration::from_secs(5), || async {
            node_b.lookup("actor.remove").await.is_none()
        })
        .await,
        "actor removal should propagate to peer"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}
