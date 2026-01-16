mod common;

use common::{connect_bidirectional, create_tls_node, wait_for_condition};
use kameo_remote::{GossipConfig, RegistrationPriority};
use std::time::Duration;

#[tokio::test]
async fn test_gossip_matrix_convergence_line_topology() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        ..Default::default()
    };

    let node_a = create_tls_node(config.clone()).await?;
    let node_b = create_tls_node(config.clone()).await?;
    let node_c = create_tls_node(config.clone()).await?;
    let node_d = create_tls_node(config.clone()).await?;

    connect_bidirectional(&node_a, &node_b).await?;
    connect_bidirectional(&node_b, &node_c).await?;
    connect_bidirectional(&node_c, &node_d).await?;

    node_a
        .register_urgent(
            "actor.a".to_string(),
            "127.0.0.1:9301".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node_b
        .register_urgent(
            "actor.b".to_string(),
            "127.0.0.1:9302".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node_c
        .register_urgent(
            "actor.c".to_string(),
            "127.0.0.1:9303".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;
    node_d
        .register_urgent(
            "actor.d".to_string(),
            "127.0.0.1:9304".parse()?,
            RegistrationPriority::Immediate,
        )
        .await?;

    let actors = ["actor.a", "actor.b", "actor.c", "actor.d"];
    let nodes = [&node_a, &node_b, &node_c, &node_d];

    let converged = wait_for_condition(Duration::from_secs(5), || async {
        for node in nodes {
            for actor in &actors {
                if node.lookup(actor).await.is_none() {
                    return false;
                }
            }
        }
        true
    })
    .await;

    if !converged {
        for (idx, node) in nodes.iter().enumerate() {
            let mut missing = Vec::new();
            for actor in &actors {
                if node.lookup(actor).await.is_none() {
                    missing.push(*actor);
                }
            }
            let stats = node.stats().await;
            eprintln!("node {} missing actors: {:?}", idx, missing);
            eprintln!(
                "node {} stats: local_actors={} known_actors={} active_peers={} failed_peers={}",
                idx, stats.local_actors, stats.known_actors, stats.active_peers, stats.failed_peers
            );
        }
    }

    assert!(converged, "all nodes should converge on all actors in line topology");

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
    node_d.shutdown().await;

    Ok(())
}
