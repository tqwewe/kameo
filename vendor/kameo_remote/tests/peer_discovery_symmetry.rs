use kameo_remote::registry::{GossipRegistry, PeerInfoGossip};
use kameo_remote::{GossipConfig, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

fn peer_discovery_config(seed: &str) -> GossipConfig {
    GossipConfig {
        key_pair: Some(KeyPair::new_for_testing(seed)),
        enable_peer_discovery: true,
        allow_loopback_discovery: true,
        allow_link_local_discovery: true,
        max_peers: 5,
        mesh_formation_target: 1,
        fail_ttl: Duration::from_secs(1),
        pending_ttl: Duration::from_secs(1),
        stale_ttl: Duration::from_secs(1),
        ..Default::default()
    }
}

fn peer_info(addr: &str, last_attempt: u64, last_success: u64) -> PeerInfoGossip {
    PeerInfoGossip {
        address: addr.to_string(),
        peer_address: None,
        node_id: None,
        failures: 0,
        last_attempt,
        last_success,
    }
}

#[tokio::test]
async fn test_peer_list_cleanup_symmetric() -> Result<(), Box<dyn std::error::Error>> {
    let registry_a =
        GossipRegistry::new("127.0.0.1:0".parse()?, peer_discovery_config("peer_sym_a"));
    let registry_b =
        GossipRegistry::new("127.0.0.1:0".parse()?, peer_discovery_config("peer_sym_b"));

    let now = kameo_remote::current_timestamp();
    let stale = peer_info(
        "127.0.0.1:4100",
        now.saturating_sub(5),
        now.saturating_sub(5),
    );

    registry_a
        .on_peer_list_gossip(vec![stale.clone()], "127.0.0.1:5001", now)
        .await;
    registry_b
        .on_peer_list_gossip(vec![stale], "127.0.0.1:5002", now)
        .await;

    let stats_a = registry_a.get_stats().await;
    let stats_b = registry_b.get_stats().await;
    assert_eq!(stats_a.discovered_peers, 1);
    assert_eq!(stats_b.discovered_peers, 1);

    sleep(Duration::from_secs(2)).await;

    registry_a.prune_stale_peers().await;
    registry_b.prune_stale_peers().await;

    let stats_a = registry_a.get_stats().await;
    let stats_b = registry_b.get_stats().await;
    assert_eq!(stats_a.discovered_peers, 0);
    assert_eq!(stats_b.discovered_peers, 0);

    Ok(())
}

#[tokio::test]
async fn test_mesh_formation_metric_symmetric() -> Result<(), Box<dyn std::error::Error>> {
    let registry_a = GossipRegistry::new(
        "127.0.0.1:0".parse()?,
        peer_discovery_config("peer_sym_metric_a"),
    );
    let registry_b = GossipRegistry::new(
        "127.0.0.1:0".parse()?,
        peer_discovery_config("peer_sym_metric_b"),
    );

    registry_a
        .mark_peer_connected("127.0.0.1:5100".parse::<SocketAddr>()?)
        .await;
    registry_b
        .mark_peer_connected("127.0.0.1:5200".parse::<SocketAddr>()?)
        .await;

    let stats_a = registry_a.get_stats().await;
    let stats_b = registry_b.get_stats().await;
    assert!(stats_a.mesh_formation_time_ms.is_some());
    assert!(stats_b.mesh_formation_time_ms.is_some());

    let stats_a_repeat = registry_a.get_stats().await;
    let stats_b_repeat = registry_b.get_stats().await;
    assert_eq!(
        stats_a.mesh_formation_time_ms,
        stats_a_repeat.mesh_formation_time_ms
    );
    assert_eq!(
        stats_b.mesh_formation_time_ms,
        stats_b_repeat.mesh_formation_time_ms
    );

    Ok(())
}
