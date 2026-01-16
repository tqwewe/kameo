use kameo_remote::{registry::GossipRegistry, GossipConfig, KeyPair};
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::test]
async fn test_peer_retry_backoff_gate() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("backoff_gate")),
        peer_retry_interval: Duration::from_secs(1),
        max_peer_failures: 2,
        gossip_interval: Duration::from_millis(200),
        ..Default::default()
    };

    let registry = GossipRegistry::new("127.0.0.1:0".parse()?, config.clone());
    let peer_addr: SocketAddr = "127.0.0.1:9701".parse()?;

    registry.add_peer(peer_addr).await;
    registry
        .register_actor(
            "actor.backoff".to_string(),
            kameo_remote::RemoteActorLocation::new_with_peer(
                "127.0.0.1:9702".parse()?,
                registry.peer_id.clone(),
            ),
        )
        .await?;

    let now = kameo_remote::current_timestamp();
    {
        let mut state = registry.gossip_state.lock().await;
        if let Some(peer) = state.peers.get_mut(&peer_addr) {
            peer.failures = config.max_peer_failures;
            peer.last_attempt = now;
        }
    }

    let tasks = registry.prepare_gossip_round().await?;
    assert!(
        tasks.is_empty(),
        "peer should be gated by retry interval immediately after failure"
    );

    {
        let mut state = registry.gossip_state.lock().await;
        if let Some(peer) = state.peers.get_mut(&peer_addr) {
            peer.last_attempt = now.saturating_sub(config.peer_retry_interval.as_secs() + 1);
        }
    }

    let tasks = registry.prepare_gossip_round().await?;
    assert!(
        !tasks.is_empty(),
        "peer should be eligible after retry interval elapses"
    );

    Ok(())
}
