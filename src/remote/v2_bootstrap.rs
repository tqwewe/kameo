//! Bootstrap functions for kameo_remote transport
//!
//! Provides simple one-line setup for development and production use

use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use super::distributed_message_handler::DistributedMessageHandler;
use super::transport::{BoxError, MessageHandler};

use super::kameo_transport::KameoTransport;
use super::transport::{RemoteTransport, TransportConfig};
use futures::future::BoxFuture;
use kameo_remote::{GossipConfig, GossipRegistryHandle};
use kameo_remote::registry::{ActorResponse, GossipRegistry};

type GossipHandlerFuture<'a> = Pin<
    Box<
        dyn Future<Output = kameo_remote::Result<Option<kameo_remote::registry::ActorResponse>>>
            + Send
            + 'a,
    >,
>;

/// Bridge implementation that connects kameo_remote's ActorMessageHandler to our DistributedMessageHandler
struct KameoActorMessageHandler {
    distributed_handler: Arc<DistributedMessageHandler>,
}

impl KameoActorMessageHandler {
    fn new(distributed_handler: Arc<DistributedMessageHandler>) -> Self {
        Self {
            distributed_handler,
        }
    }
}

struct RemoteLinkDisconnectHandler {
    registry: Arc<GossipRegistry>,
}

struct RemoteLinkConnectHandler;

impl kameo_remote::registry::PeerDisconnectHandler for RemoteLinkDisconnectHandler {
    fn handle_peer_disconnect(
        &self,
        peer_addr: SocketAddr,
        peer_id: Option<kameo_remote::PeerId>,
    ) -> BoxFuture<'_, ()> {
        let registry = self.registry.clone();
        Box::pin(async move {
            let resolved = peer_id
                .or_else(|| registry.connection_pool.get_peer_id_by_addr(&peer_addr));
            if let Some(peer_id) = resolved.as_ref() {
                super::remote_link::notify_peer_disconnected_by_id(peer_id).await;
                return;
            }
            tracing::warn!(
                peer_addr = %peer_addr,
                "peer disconnect without peer_id; remote link notifications skipped"
            );
        })
    }
}

impl kameo_remote::registry::PeerConnectHandler for RemoteLinkConnectHandler {
    fn handle_peer_connect(
        &self,
        peer_addr: SocketAddr,
        peer_id: Option<kameo_remote::PeerId>,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Some(peer_id) = peer_id.as_ref() {
                super::remote_link::auto_link_peer(peer_addr, peer_id);
            }
        })
    }
}

impl kameo_remote::registry::ActorMessageHandler for KameoActorMessageHandler {
    fn handle_actor_message(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: kameo_remote::AlignedBytes,
        correlation_id: Option<u16>,
    ) -> GossipHandlerFuture<'_> {
        let distributed_handler = self.distributed_handler.clone();

        Box::pin(async move {
            let actor_id = crate::actor::ActorId::from_u64(actor_id);

            // CRITICAL_PATH: bridge aligned payloads into kameo message handlers.
            match (correlation_id, payload) {
                (Some(_), payload) => {
                    match MessageHandler::handle_ask_typed(
                        distributed_handler.as_ref(),
                        actor_id,
                        type_hash,
                        payload,
                    )
                    .await
                    {
                        Ok(reply) => Ok(Some(ActorResponse::Bytes(reply))),
                        Err(e) => Err(kameo_remote::GossipError::Network(std::io::Error::other(
                            format!("Ask handler error: {:?}", e),
                        ))),
                    }
                }
                (None, payload) => {
                    match MessageHandler::handle_tell_typed(
                        distributed_handler.as_ref(),
                        actor_id,
                        type_hash,
                        payload,
                    )
                    .await
                    {
                        Ok(()) => Ok(None),
                        Err(e) => Err(kameo_remote::GossipError::Network(std::io::Error::other(
                            format!("Tell handler error: {:?}", e),
                        ))),
                    }
                }
            }
        })
    }
}

/// Global distributed message handler instance
static GLOBAL_DISTRIBUTED_HANDLER: std::sync::LazyLock<Arc<DistributedMessageHandler>> =
    std::sync::LazyLock::new(|| Arc::new(DistributedMessageHandler::new()));

/// Get access to the global distributed message handler for registering actors
pub fn get_distributed_handler() -> &'static Arc<DistributedMessageHandler> {
    &GLOBAL_DISTRIBUTED_HANDLER
}

/// Set the KAMEO_USE_V2_TRANSPORT environment variable safely
///
/// Warns if the variable is already set to a different value before overwriting.
fn set_v2_transport_env_var() {
    const ENV_VAR_NAME: &str = "KAMEO_USE_V2_TRANSPORT";
    match std::env::var(ENV_VAR_NAME) {
        Ok(val) if val != "true" => {
            tracing::warn!(
                existing_value = %val,
                "KAMEO_USE_V2_TRANSPORT already set to different value, overwriting to 'true'"
            );
        }
        _ => {}
    }
    // SAFETY: This is called during bootstrap before any threads are spawned
    // that might read this environment variable concurrently.
    unsafe { std::env::set_var(ENV_VAR_NAME, "true") };
}

/// Bootstrap a kameo_remote transport with a specific keypair for TLS authentication
///
/// This enables TLS encryption and authentication using the provided keypair.
///
/// # Arguments
/// * `addr` - The socket address to bind to
/// * `keypair` - The Ed25519 keypair for TLS authentication
///
/// # Example
/// ```no_run
/// use kameo::remote::v2_bootstrap;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let keypair = kameo_remote::KeyPair::new_for_testing("42");
///     let addr: std::net::SocketAddr = "127.0.0.1:8080".parse()?;
///     let transport = v2_bootstrap::bootstrap_with_keypair(addr, keypair)
///         .await
///         .expect("bootstrap failed");
///     // Now use the transport with TLS enabled
///     Ok(())
/// }
/// ```
pub async fn bootstrap_with_keypair(
    addr: SocketAddr,
    keypair: kameo_remote::KeyPair,
) -> Result<Box<KameoTransport>, BoxError> {
    // Ensure the rustls CryptoProvider is installed (required for TLS)
    // This uses the ring provider which is enabled in kameo_remote's Cargo.toml
    // We need to call this through kameo_remote since it has the rustls dependency
    kameo_remote::tls::ensure_crypto_provider();

    // Create a custom config with TLS enabled
    let config = TransportConfig {
        bind_addr: addr,
        enable_encryption: true, // Enable TLS
        ..Default::default()
    };

    // Use bootstrap_with_config but with a custom keypair
    // We need to modify the transport after creation to use the custom keypair

    // Force kameo_remote transport for v2 bootstrap
    set_v2_transport_env_var();

    let mut transport = Box::new(KameoTransport::new(config));

    // Create gossip config without the keypair (we'll use it for TLS separately)
    let gossip_config = GossipConfig {
        key_pair: Some(keypair.clone()),
        gossip_interval: std::time::Duration::from_secs(5),
        max_gossip_peers: 3,
        ..Default::default()
    };

    // Convert the keypair to a secret key for TLS
    let secret_key = keypair.to_secret_key();

    // Create and start the gossip registry with TLS enabled using new_with_tls.
    //
    // In some sandboxed macOS environments, socket syscalls can fail transiently with EPERM
    // ("Operation not permitted") under load. Retry only for that specific error so our
    // integration tests are deterministic without masking real failures.
    let handle = {
        // NOTE: `kameo_remote` already has a short in-function retry for EPERM during bind().
        // Keep this outer loop small so we don't multiply the retry windows.
        const MAX_EPERM_RETRIES: usize = 5;
        const EPERM_RETRY_SLEEP_MS: u64 = 10;

        let mut attempt = 0usize;
        loop {
            match GossipRegistryHandle::new_with_tls(
                addr,
                secret_key.clone(),
                Some(gossip_config.clone()),
            )
            .await
            {
                Ok(h) => break h,
                Err(e) => {
                    let is_eperm = matches!(
                        e,
                        kameo_remote::GossipError::Network(ref io)
                            if io.kind() == std::io::ErrorKind::PermissionDenied
                                || io.raw_os_error() == Some(1)
                    );
                    if is_eperm && attempt < MAX_EPERM_RETRIES {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(
                            EPERM_RETRY_SLEEP_MS,
                        ))
                        .await;
                        continue;
                    }
                    return Err(BoxError::from(e));
                }
            }
        }
    };

    // Use the set_handle method if available, or we need to add it
    transport.set_handle(Arc::new(handle));

    // Register the distributed message handler with kameo_remote
    if let Some(handle) = transport.handle() {
        let bridge_handler = Arc::new(KameoActorMessageHandler::new(
            GLOBAL_DISTRIBUTED_HANDLER.clone(),
        ));
        handle
            .registry
            .set_actor_message_handler(bridge_handler)
            .await;
        let disconnect_handler = Arc::new(RemoteLinkDisconnectHandler {
            registry: handle.registry.clone(),
        });
        handle
            .registry
            .set_peer_disconnect_handler(disconnect_handler)
            .await;
        let connect_handler = Arc::new(RemoteLinkConnectHandler);
        handle
            .registry
            .set_peer_connect_handler(connect_handler)
            .await;
        tracing::debug!("Distributed message handler registered successfully");
    } else {
        tracing::error!(
            "Failed to register distributed message handler - transport handle not available"
        );
        return Err(BoxError::from(
            "Transport handle not available for handler registration",
        ));
    }

    // Automatically set the global transport for DistributedActorRef::lookup
    super::distributed_actor_ref::set_global_transport(transport.clone());

    tracing::info!(
        local_addr = %addr,
        "Kameo remote transport started with TLS"
    );

    Ok(transport)
}

// REMOVED: bootstrap_with_peers - This function was removed because it doesn't properly establish
// peer connections. Users should use bootstrap_with_keypair() and then manually add peers using the
// proper kameo_remote API pattern:
//
// Example:
//   let transport = bootstrap_with_keypair(addr, keypair).await?;
//   let handle = transport.get_handle()?;
//   let peer_id = peer_keypair.peer_id();
//   let peer = handle.add_peer(&peer_id).await;
//   peer.connect(peer_addr).await?;

/// Create a deterministic test keypair for local testing.
pub fn test_keypair(seed: u64) -> kameo_remote::KeyPair {
    kameo_remote::KeyPair::new_for_testing(seed.to_string())
}

/// Bootstrap on a specific address with TLS enabled using an explicit keypair.
pub async fn bootstrap_on(
    addr: SocketAddr,
    keypair: kameo_remote::KeyPair,
) -> Result<Box<KameoTransport>, BoxError> {
    bootstrap_with_keypair(addr, keypair).await
}

/// Bootstrap a kameo_remote transport with custom configuration
///
/// # Arguments
/// * `config` - The transport configuration
///
/// # Example
/// ```no_run
/// use kameo::remote::transport::TransportConfig;
/// use kameo::remote::v2_bootstrap;
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = TransportConfig {
///         bind_addr: "127.0.0.1:0".parse()?,
///         max_connections: 500,
///         connection_timeout: Duration::from_secs(10),
///         enable_encryption: true,
///         keypair: Some(v2_bootstrap::test_keypair(1)),
///         ..Default::default()
///     };
///     let transport = v2_bootstrap::bootstrap_with_config(config)
///         .await
///         .expect("bootstrap failed");
///     Ok(())
/// }
/// ```
pub async fn bootstrap_with_config(
    config: TransportConfig,
) -> Result<Box<KameoTransport>, BoxError> {
    // Force kameo_remote transport for v2 bootstrap
    set_v2_transport_env_var();

    // Capture encryption setting before config is consumed
    let enable_encryption = config.enable_encryption;

    let mut transport = Box::new(KameoTransport::new(config));
    transport.start().await?;

    // Register the distributed message handler with kameo_remote
    if let Some(handle) = transport.handle() {
        let bridge_handler = Arc::new(KameoActorMessageHandler::new(
            GLOBAL_DISTRIBUTED_HANDLER.clone(),
        ));
        handle
            .registry
            .set_actor_message_handler(bridge_handler)
            .await;
        tracing::debug!("Distributed message handler registered successfully");
    } else {
        tracing::error!(
            "Failed to register distributed message handler - transport handle not available"
        );
        return Err(BoxError::from(
            "Transport handle not available for handler registration",
        ));
    }

    // Log the local address
    let local_addr = transport.local_addr();
    tracing::info!(
        local_addr = %local_addr,
        encryption = enable_encryption,
        "Kameo remote transport started"
    );

    // Automatically set the global transport for DistributedActorRef::lookup
    super::distributed_actor_ref::set_global_transport(transport.clone());

    Ok(transport)
}

/// Bootstrap multiple kameo_remote nodes for testing purposes.
///
/// # Warning
///
/// **Currently creates ISOLATED nodes** - they are not automatically connected
/// to each other. Peer connection requires manual setup after bootstrap.
///
/// This is useful for:
/// - Unit testing individual nodes
/// - Setting up nodes that will be connected via external orchestration
/// - Development scenarios where you need multiple isolated transports
///
/// # Arguments
/// * `count` - Number of nodes to create
/// * `base_port` - Starting port number (each node gets base_port + index)
///
/// # Example
/// ```no_run
/// use kameo::remote::v2_bootstrap;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Create 3 isolated nodes on ports 8080, 8081, 8082
///     let transports = v2_bootstrap::bootstrap_cluster(3, 8080)
///         .await
///         .expect("bootstrap failed");
///
///     // IMPORTANT: Nodes are NOT connected to each other!
///     // To connect them, use the peer connection API:
///     // let handle = transports[0].handle().unwrap();
///     // handle.connect_to("127.0.0.1:8081".parse()?).await?;
///
///     Ok(())
/// }
/// ```
///
/// # Note
///
/// For a fully connected cluster, you must manually establish peer connections
/// after calling this function. See the kameo_remote documentation for the
/// peer connection API.
pub async fn bootstrap_cluster(
    count: usize,
    base_port: u16,
) -> Result<Vec<Box<KameoTransport>>, BoxError> {
    tracing::warn!(
        node_count = count,
        base_port = base_port,
        "bootstrap_cluster creates ISOLATED nodes - peer connections must be established manually"
    );

    let mut transports = Vec::with_capacity(count);
    let mut addrs = Vec::with_capacity(count);

    // Create all transports with TLS
    for i in 0..count {
        let addr: SocketAddr = format!("127.0.0.1:{}", base_port + i as u16).parse()?;
        addrs.push(addr);
        // Use TLS-enabled bootstrap with deterministic test keypair
        let transport = bootstrap_with_keypair(addr, test_keypair(i as u64)).await?;
        transports.push(transport);
    }

    // Connect each transport to all others
    // Note: This requires kameo_remote to support adding peers after startup
    // For now, this just creates isolated nodes
    // TODO: Add peer connection support when kameo_remote API allows it

    tracing::info!(
        node_count = count,
        addresses = ?addrs,
        "Created {} isolated transport nodes",
        count
    );

    Ok(transports)
}
