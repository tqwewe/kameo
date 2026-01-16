use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, SecretKey};
use std::future::Future;
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

#[allow(dead_code)]
pub async fn create_tls_node(
    mut config: GossipConfig,
) -> Result<GossipRegistryHandle, Box<dyn std::error::Error>> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let private_bytes = secret_key.to_bytes();
    let key_pair = KeyPair::from_private_key_bytes(&private_bytes)
        .map_err(Box::<dyn std::error::Error>::from)?;
    config.key_pair = Some(key_pair);
    let node = GossipRegistryHandle::new_with_tls("127.0.0.1:0".parse()?, secret_key, Some(config))
        .await?;
    Ok(node)
}

#[allow(dead_code)]
pub async fn connect_bidirectional(
    a: &GossipRegistryHandle,
    b: &GossipRegistryHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;
    let peer_id_a = a.registry.peer_id.clone();
    let peer_id_b = b.registry.peer_id.clone();

    a.registry
        .configure_peer(peer_id_b.clone(), addr_b)
        .await;
    b.registry
        .configure_peer(peer_id_a.clone(), addr_a)
        .await;

    let peer_b = a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await?;

    let peer_a = b.add_peer(&peer_id_a).await;
    peer_a.connect(&addr_a).await?;

    Ok(())
}

#[allow(dead_code)]
pub async fn force_disconnect(a: &GossipRegistryHandle, b: &GossipRegistryHandle) {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;

    let _ = a.registry.handle_peer_connection_failure(addr_b).await;
    let _ = b.registry.handle_peer_connection_failure(addr_a).await;
}

#[allow(dead_code)]
pub async fn wait_for_condition<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check().await {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

#[allow(dead_code)]
pub async fn wait_for_actor(node: &GossipRegistryHandle, actor: &str, timeout: Duration) -> bool {
    wait_for_condition(
        timeout,
        || async move { node.lookup(actor).await.is_some() },
    )
    .await
}

#[allow(dead_code)]
pub async fn wait_for_active_peers(
    node: &GossipRegistryHandle,
    min_peers: usize,
    timeout: Duration,
) -> bool {
    wait_for_condition(timeout, || async move {
        node.stats().await.active_peers >= min_peers
    })
    .await
}

#[allow(dead_code)]
pub fn parse_addr(addr: &str) -> SocketAddr {
    addr.parse().expect("valid socket addr")
}
