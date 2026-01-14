//! Bootstrap functions for kameo_remote transport
//!
//! Provides simple one-line setup for development and production use

use std::net::SocketAddr;
use std::sync::Arc;

use super::distributed_message_handler::DistributedMessageHandler;
use super::transport::{BoxError, MessageHandler};

use super::kameo_transport::KameoTransport;
use super::transport::{RemoteTransport, TransportConfig};
use kameo_remote::{GossipConfig, GossipRegistryHandle};

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

/// Type alias for the actor message handler future return type
type ActorMessageFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = kameo_remote::Result<Option<Vec<u8>>>> + Send>,
>;

/// Create an immediate error future for invalid actor IDs
///
/// This helper simplifies error handling in the message handler by providing
/// a clean way to return parse errors without nested async blocks.
fn invalid_actor_id_error(actor_id: &str, err: std::num::ParseIntError) -> ActorMessageFuture {
    let msg = format!("Invalid actor_id '{}': {}", actor_id, err);
    Box::pin(std::future::ready(Err(kameo_remote::GossipError::Network(
        std::io::Error::other(msg),
    ))))
}

impl kameo_remote::registry::ActorMessageHandler for KameoActorMessageHandler {
    fn handle_actor_message(
        &self,
        actor_id: &str,
        type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = kameo_remote::Result<Option<Vec<u8>>>> + Send + '_>,
    > {
        let distributed_handler = self.distributed_handler.clone();
        // OPTIMIZATION: Parse actor_id directly without string allocation
        let actor_id_u64 = match actor_id.parse::<u64>() {
            Ok(id) => id,
            Err(e) => {
                return invalid_actor_id_error(actor_id, e);
            }
        };

        // OPTIMIZATION: Use Bytes for zero-copy payload
        let payload = bytes::Bytes::copy_from_slice(payload);

        Box::pin(async move {
            // Use the pre-parsed actor_id
            let actor_id = crate::actor::ActorId::from_u64(actor_id_u64);

            if correlation_id.is_some() {
                // This is an ask operation (has correlation_id)
                match MessageHandler::handle_ask_typed(
                    distributed_handler.as_ref(),
                    actor_id,
                    type_hash,
                    payload,
                )
                .await
                {
                    Ok(reply) => {
                        // OPTIMIZATION: reply is already Bytes, convert to Vec without clone
                        Ok(Some(reply.into()))
                    }
                    Err(e) => Err(kameo_remote::GossipError::Network(std::io::Error::other(
                        format!("Ask handler error: {:?}", e),
                    ))),
                }
            } else {
                // This is a tell operation (no correlation_id)
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
    std::env::set_var(ENV_VAR_NAME, "true");
}

// COMMENTED OUT: Non-TLS bootstrap methods - migrating to TLS-only
// /// Bootstrap a simple kameo_remote transport with default configuration (LEGACY - NO TLS)
// ///
// /// DEPRECATED: This method does not use TLS. Use bootstrap_with_keypair instead.
// /// Starts a transport on a random port with sensible defaults for development and testing.
// ///
// /// # Example
// /// ```no_run
// /// use kameo::remote::v2_bootstrap;
// ///
// /// #[tokio::main]
// /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
// ///     let transport = v2_bootstrap::bootstrap_legacy_no_tls().await?;
// ///     // Now use the transport for remote actors
// ///     Ok(())
// /// }
// /// ```
// pub async fn bootstrap_legacy_no_tls() -> Result<Box<KameoTransport>, BoxError> {
//     let config = TransportConfig::default();
//     bootstrap_with_config(config).await
// }

// /// Bootstrap a kameo_remote transport on a specific address (LEGACY - NO TLS)
// ///
// /// DEPRECATED: This method does not use TLS. Use bootstrap_with_keypair instead.
// /// # Arguments
// /// * `addr` - The socket address to bind to
// ///
// /// # Example
// /// ```no_run
// /// use kameo::remote::v2_bootstrap;
// ///
// /// #[tokio::main]
// /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
// ///     let transport = v2_bootstrap::bootstrap_on_legacy_no_tls("127.0.0.1:8080".parse()?).await?;
// ///     // Now use the transport for remote actors
// ///     Ok(())
// /// }
// /// ```
// pub async fn bootstrap_on_legacy_no_tls(addr: SocketAddr) -> Result<Box<KameoTransport>, BoxError> {
//     let config = TransportConfig {
//         bind_addr: addr,
//         ..Default::default()
//     };
//     bootstrap_with_config(config).await  // This will set the global transport automatically
// }

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
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let keypair = kameo_remote::KeyPair::new_for_testing("test");
///     let transport = v2_bootstrap::bootstrap_with_keypair(
///         "127.0.0.1:8080".parse()?,
///         keypair
///     ).await?;
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
        key_pair: Some(keypair.clone()), // Keep for backward compatibility
        gossip_interval: std::time::Duration::from_secs(5),
        max_gossip_peers: 3,
        ..Default::default()
    };

    // Convert the keypair to a secret key for TLS
    let secret_key = kameo_remote::migration::migrate_keypair_to_secret_key(keypair.clone())
        .map_err(|e| BoxError::from(format!("Failed to extract secret key: {}", e)))?;

    // Create and start the gossip registry with TLS enabled using new_with_tls
    let handle = GossipRegistryHandle::new_with_tls(addr, secret_key, Some(gossip_config))
        .await
        .map_err(BoxError::from)?;

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
    super::DistributedActorRef::set_global_transport(transport.clone());

    tracing::info!(
        local_addr = %addr,
        "Kameo remote transport started with TLS"
    );

    Ok(transport)
}

// REMOVED: bootstrap_with_peers - This function was removed because it doesn't properly establish
// peer connections. Users should use bootstrap_on() and then manually add peers using the
// proper kameo_remote API pattern:
//
// Example:
//   let transport = bootstrap_on(addr).await?;
//   let handle = transport.get_handle()?;
//   let peer = handle.add_peer(&PeerId::new("peer_node_name")).await;
//   peer.connect(peer_addr).await?;

/// Bootstrap on a specific address with TLS enabled
///
/// # Arguments
/// * `addr` - The socket address to bind to
///
/// # Example
/// ```no_run
/// use kameo::remote::v2_bootstrap;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let transport = v2_bootstrap::bootstrap_on("127.0.0.1:9330".parse()?).await?;
///     Ok(())
/// }
/// ```
pub async fn bootstrap_on(addr: SocketAddr) -> Result<Box<KameoTransport>, BoxError> {
    // Use TLS-enabled bootstrap with generated keypair
    bootstrap_with_keypair(addr, kameo_remote::KeyPair::generate()).await
}

/// Bootstrap a kameo_remote transport with custom configuration
///
/// # Arguments
/// * `config` - The transport configuration
///
/// # Example
/// ```no_run
/// use kameo::remote::v2_bootstrap;
/// use kameo::remote::transport::TransportConfig;
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let config = TransportConfig {
///         bind_addr: "127.0.0.1:0".parse()?,
///         max_connections: 500,
///         connection_timeout: Duration::from_secs(10),
///         enable_encryption: false, // For testing
///         peers: Vec::new(),
///     };
///     let transport = v2_bootstrap::bootstrap_with_config(config).await?;
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
    super::DistributedActorRef::set_global_transport(transport.clone());

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
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     // Create 3 isolated nodes on ports 8080, 8081, 8082
///     let transports = v2_bootstrap::bootstrap_cluster(3, 8080).await?;
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
        // Use TLS-enabled bootstrap with generated keypair
        let transport = bootstrap_with_keypair(addr, kameo_remote::KeyPair::generate()).await?;
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
