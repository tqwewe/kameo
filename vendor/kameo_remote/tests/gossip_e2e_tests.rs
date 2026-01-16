use kameo_remote::{GossipConfig, GossipRegistryHandle, RegistrationPriority, SecretKey};
use std::net::SocketAddr;
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio::time::sleep;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install crypto provider");
    });
}

async fn create_node(
    config: GossipConfig,
) -> Result<GossipRegistryHandle, Box<dyn std::error::Error>> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let node = GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse()?, secret_key, Some(config))
        .await?;
    Ok(node)
}

async fn connect_pair(a: &GossipRegistryHandle, b: &GossipRegistryHandle) {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;

    a.registry.add_peer(addr_b).await;
    b.registry.add_peer(addr_a).await;

    a.bootstrap_non_blocking(vec![addr_b]).await;
    b.bootstrap_non_blocking(vec![addr_a]).await;
}

async fn wait_for_actor(node: &GossipRegistryHandle, actor: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if node.lookup(actor).await.is_some() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_for_exchange(
    node: &GossipRegistryHandle,
    initial_delta: u64,
    initial_full: u64,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let stats = node.stats().await;
        if stats.delta_exchanges > initial_delta || stats.full_sync_exchanges > initial_full {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn test_full_sync_for_small_cluster() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        small_cluster_threshold: 10,
        ..Default::default()
    };

    let node_a = create_node(config.clone()).await?;
    let node_b = create_node(config.clone()).await?;
    connect_pair(&node_a, &node_b).await;

    node_a
        .register("actor.fullsync".to_string(), "127.0.0.1:9001".parse()?)
        .await?;

    assert!(
        wait_for_actor(&node_b, "actor.fullsync", Duration::from_secs(2)).await,
        "actor should propagate via full sync in small cluster"
    );

    let stats_b = node_b.stats().await;
    assert!(
        stats_b.full_sync_exchanges > 0,
        "full sync exchanges should be recorded for small cluster"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_delta_gossip_after_initial_full_sync() -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        small_cluster_threshold: 0,
        full_sync_interval: 1000,
        ..Default::default()
    };

    let node_a = create_node(config.clone()).await?;
    let node_b = create_node(config.clone()).await?;
    connect_pair(&node_a, &node_b).await;

    node_a
        .register("actor.delta.1".to_string(), "127.0.0.1:9101".parse()?)
        .await?;
    assert!(
        wait_for_actor(&node_b, "actor.delta.1", Duration::from_secs(2)).await,
        "initial actor should propagate"
    );

    let stats_before = node_b.stats().await;

    node_a
        .register("actor.delta.2".to_string(), "127.0.0.1:9102".parse()?)
        .await?;
    assert!(
        wait_for_actor(&node_b, "actor.delta.2", Duration::from_secs(2)).await,
        "second actor should propagate after initial sync"
    );

    assert!(
        wait_for_exchange(
            &node_b,
            stats_before.delta_exchanges,
            stats_before.full_sync_exchanges,
            Duration::from_secs(2)
        )
        .await,
        "gossip exchange counters should advance after second update"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_immediate_propagation_for_urgent_registration(
) -> Result<(), Box<dyn std::error::Error>> {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(3),
        immediate_propagation_enabled: true,
        urgent_gossip_fanout: 1,
        ..Default::default()
    };

    let node_a = create_node(config.clone()).await?;
    let node_b = create_node(config.clone()).await?;
    connect_pair(&node_a, &node_b).await;

    node_a
        .register_with_priority(
            "actor.immediate".to_string(),
            "127.0.0.1:9201".parse::<SocketAddr>()?,
            RegistrationPriority::Immediate,
        )
        .await?;

    assert!(
        wait_for_actor(&node_b, "actor.immediate", Duration::from_secs(1)).await,
        "urgent registration should propagate before normal gossip interval"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}
