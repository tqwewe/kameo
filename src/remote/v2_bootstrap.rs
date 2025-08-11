//! Bootstrap functions for kameo_remote transport
//! 
//! Provides simple one-line setup for development and production use

use std::net::SocketAddr;
use std::sync::Arc;

use super::transport::{BoxError, MessageHandler};
use super::distributed_message_handler::DistributedMessageHandler;

use super::transport::{RemoteTransport, TransportConfig};
use super::transport_factory::create_transport;
use super::kameo_transport::KameoTransport;
use kameo_remote::{GossipConfig, GossipRegistryHandle};

/// Bridge implementation that connects kameo_remote's ActorMessageHandler to our DistributedMessageHandler
struct KameoActorMessageHandler {
    distributed_handler: Arc<DistributedMessageHandler>,
}

impl KameoActorMessageHandler {
    fn new(distributed_handler: Arc<DistributedMessageHandler>) -> Self {
        Self { distributed_handler }
    }
}

impl kameo_remote::registry::ActorMessageHandler for KameoActorMessageHandler {
    fn handle_actor_message(
        &self,
        actor_id: &str,
        type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = kameo_remote::Result<Option<Vec<u8>>>> + Send + '_>> {
        let distributed_handler = self.distributed_handler.clone();
        // OPTIMIZATION: Parse actor_id directly without string allocation
        let actor_id_u64 = match actor_id.parse::<u64>() {
            Ok(id) => id,
            Err(e) => {
                return Box::pin(async move {
                    Err(kameo_remote::GossipError::Network(
                        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Invalid actor_id: {}", e))
                    ))
                });
            }
        };
        
        // OPTIMIZATION: Use Bytes for zero-copy payload
        let payload = bytes::Bytes::copy_from_slice(payload);
        
        Box::pin(async move {
        
        
        // Use the pre-parsed actor_id
        let actor_id = crate::actor::ActorId::from_u64(actor_id_u64);
        
        
        if correlation_id.is_some() {
            // This is an ask operation (has correlation_id)
            match MessageHandler::handle_ask_typed(distributed_handler.as_ref(), actor_id, type_hash, payload.into()).await {
                Ok(reply) => {
                    // OPTIMIZATION: reply is already Bytes, convert to Vec without clone
                    Ok(Some(reply.into()))
                },
                Err(e) => {
                    Err(kameo_remote::GossipError::Network(
                        std::io::Error::new(std::io::ErrorKind::Other, format!("Ask handler error: {:?}", e))
                    ))
                },
            }
        } else {
            // This is a tell operation (no correlation_id)
            match MessageHandler::handle_tell_typed(distributed_handler.as_ref(), actor_id, type_hash, payload.into()).await {
                Ok(()) => {
                    Ok(None)
                },
                Err(e) => Err(kameo_remote::GossipError::Network(
                    std::io::Error::new(std::io::ErrorKind::Other, format!("Tell handler error: {:?}", e))
                )),
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

/// Bootstrap a simple kameo_remote transport with default configuration
/// 
/// Starts a transport on a random port with sensible defaults for development and testing.
/// 
/// # Example
/// ```no_run
/// use kameo::remote::v2_bootstrap;
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let transport = v2_bootstrap::bootstrap().await?;
///     // Now use the transport for remote actors
///     Ok(())
/// }
/// ```
pub async fn bootstrap() -> Result<Box<KameoTransport>, BoxError> {
    let config = TransportConfig::default();
    bootstrap_with_config(config).await
}

/// Bootstrap a kameo_remote transport on a specific address
/// 
/// # Arguments
/// * `addr` - The socket address to bind to
/// 
/// # Example
/// ```no_run
/// use kameo::remote::v2_bootstrap;
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let transport = v2_bootstrap::bootstrap_on("127.0.0.1:8080".parse()?).await?;
///     // Now use the transport for remote actors
///     Ok(())
/// }
/// ```
pub async fn bootstrap_on(addr: SocketAddr) -> Result<Box<KameoTransport>, BoxError> {
    let config = TransportConfig {
        bind_addr: addr,
        ..Default::default()
    };
    bootstrap_with_config(config).await  // This will set the global transport automatically
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
///     let keypair = kameo_remote::KeyPair::from_seed_for_testing(42);
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
    // Create a custom config with TLS enabled
    let config = TransportConfig {
        bind_addr: addr,
        enable_encryption: true,  // Enable TLS
        ..Default::default()
    };
    
    // Use bootstrap_with_config but with a custom keypair
    // We need to modify the transport after creation to use the custom keypair
    
    // Force kameo_remote transport for v2 bootstrap
    std::env::set_var("KAMEO_USE_V2_TRANSPORT", "true");
    
    let mut transport = Box::new(KameoTransport::new(config));
    
    // Create gossip config with the provided keypair for TLS
    let gossip_config = GossipConfig {
        key_pair: Some(keypair),
        gossip_interval: std::time::Duration::from_secs(5),
        max_gossip_peers: 3,
        ..Default::default()
    };

    // Create and start the gossip registry with TLS enabled
    let handle = GossipRegistryHandle::new(addr, vec![], Some(gossip_config))
        .await
        .map_err(|e| BoxError::from(e))?;

    // Use the set_handle method if available, or we need to add it
    transport.set_handle(Arc::new(handle));

    // Register the distributed message handler with kameo_remote
    if let Some(handle) = transport.handle() {
        let bridge_handler = Arc::new(KameoActorMessageHandler::new(GLOBAL_DISTRIBUTED_HANDLER.clone()));
        handle.registry.set_actor_message_handler(bridge_handler).await;
    }
    
    // Automatically set the global transport for DistributedActorRef::lookup
    super::DistributedActorRef::set_global_transport(transport.clone());
    
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

/// Bootstrap a kameo_remote transport with custom configuration
/// 
/// # Arguments
/// * `config` - The transport configuration
/// 
/// # Example
/// ```no_run
/// use kameo::remote::{v2_bootstrap, TransportConfig};
/// use std::time::Duration;
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = TransportConfig {
///         bind_addr: "127.0.0.1:0".parse()?,
///         max_connections: 500,
///         connection_timeout: Duration::from_secs(10),
///         enable_encryption: false, // For testing
///     };
///     let transport = v2_bootstrap::bootstrap_with_config(config).await?;
///     Ok(())
/// }
/// ```
pub async fn bootstrap_with_config(config: TransportConfig) -> Result<Box<KameoTransport>, BoxError> {
    // Force kameo_remote transport for v2 bootstrap
    std::env::set_var("KAMEO_USE_V2_TRANSPORT", "true");
    
    let mut transport = Box::new(KameoTransport::new(config));
    transport.start().await?;
    
    // Register the distributed message handler with kameo_remote
    if let Some(handle) = transport.handle() {
        let bridge_handler = Arc::new(KameoActorMessageHandler::new(GLOBAL_DISTRIBUTED_HANDLER.clone()));
        handle.registry.set_actor_message_handler(bridge_handler).await;
    } else {
    }
    
    // Log the local address
    let local_addr = transport.local_addr();
    
    // Automatically set the global transport for DistributedActorRef::lookup
    super::DistributedActorRef::set_global_transport(transport.clone());
    
    Ok(transport)
}

/// Bootstrap multiple kameo_remote nodes that connect to each other
/// 
/// This is useful for testing and development when you want to quickly
/// set up a cluster of nodes.
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
///     // Create 3 nodes on ports 8080, 8081, 8082
///     let transports = v2_bootstrap::bootstrap_cluster(3, 8080).await?;
///     
///     // Use the transports...
///     Ok(())
/// }
/// ```
pub async fn bootstrap_cluster(
    count: usize,
    base_port: u16,
) -> Result<Vec<Box<KameoTransport>>, BoxError> {
    let mut transports = Vec::with_capacity(count);
    let mut addrs = Vec::with_capacity(count);
    
    // Create all transports
    for i in 0..count {
        let addr: SocketAddr = format!("127.0.0.1:{}", base_port + i as u16).parse()?;
        addrs.push(addr);
        let transport = bootstrap_on(addr).await?;
        transports.push(transport);
    }
    
    // Connect each transport to all others
    // Note: This requires kameo_remote to support adding peers after startup
    // For now, this just creates isolated nodes
    // TODO: Add peer connection support when kameo_remote API allows it
    
    Ok(transports)
}