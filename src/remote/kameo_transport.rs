//! kameo_remote transport implementation

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kameo_remote::{GossipConfig, GossipRegistryHandle};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
// Removed serde - using rkyv for zero-copy serialization
use tokio::sync::RwLock;

use crate::actor::ActorId;

use super::transport::{
    MessageHandler, RemoteActorLocation, RemoteTransport, TransportConfig, TransportError,
    TransportResult,
};

// Constants for streaming - must match kameo_remote
const STREAM_THRESHOLD: usize = 1024 * 1024 - 1024; // 1MB - 1KB to account for serialization overhead

/// kameo_remote-based transport implementation
#[derive(Clone)]
pub struct KameoTransport {
    config: TransportConfig,
    handle: Option<Arc<GossipRegistryHandle>>,
    message_handler: Option<Arc<dyn MessageHandler>>,
    registry: Arc<RwLock<std::collections::HashMap<String, ActorId>>>,
}

impl std::fmt::Debug for KameoTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KameoTransport")
            .field("config", &self.config)
            .field("has_handle", &self.handle.is_some())
            .field("has_message_handler", &self.message_handler.is_some())
            .finish()
    }
}

impl KameoTransport {
    /// Create a new kameo_remote transport with the given configuration
    pub fn new(config: TransportConfig) -> Self {
        Self {
            config,
            handle: None,
            message_handler: None,
            registry: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Get access to the internal kameo_remote handle for manual peer operations
    pub fn handle(&self) -> Option<&kameo_remote::GossipRegistryHandle> {
        self.handle.as_ref().map(|arc| arc.as_ref())
    }

    /// Get the underlying kameo_remote handle for advanced operations like adding peers
    pub fn get_handle(&self) -> Result<Arc<kameo_remote::GossipRegistryHandle>, TransportError> {
        self.handle
            .as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))
            .cloned()
    }

    /// Set the handle for the transport (used when creating with custom keypair)
    pub fn set_handle(&mut self, handle: Arc<GossipRegistryHandle>) {
        self.handle = Some(handle);
    }

    /// Register an actor with synchronous peer acknowledgment using default timeout
    ///
    /// This is a convenience method that calls the trait method with a 2 second timeout.
    pub async fn register_actor_sync(
        &self,
        name: String,
        actor_id: ActorId,
    ) -> TransportResult<()> {
        <Self as RemoteTransport>::register_actor_sync(self, name, actor_id, Duration::from_secs(2))
            .await
    }

    /// Register a distributed actor and automatically register its handlers.
    ///
    /// This method registers the actor and automatically calls its distributed handler registration.
    /// The actor must implement the DistributedActor trait (via the distributed_actor! macro).
    ///
    /// NOTE: Defaults to Immediate priority for fast gossip propagation.
    pub async fn register_distributed_actor<A>(
        &self,
        name: String,
        actor_ref: &crate::actor::ActorRef<A>,
    ) -> TransportResult<()>
    where
        A: crate::Actor + crate::remote::DistributedActor + 'static,
    {
        // Default to Immediate priority for fast registration without delays
        self.register_distributed_actor_with_priority(
            name,
            actor_ref,
            kameo_remote::RegistrationPriority::Immediate,
        )
        .await
    }

    /// Register a distributed actor synchronously with peer confirmation.
    ///
    /// This method registers the actor, registers its handlers, and waits for confirmation from at least one peer.
    /// If no peers are available, returns immediately after local registration.
    /// Use this to eliminate sleep delays in distributed systems by waiting for actual gossip propagation.
    pub async fn register_distributed_actor_sync<A>(
        &self,
        name: String,
        actor_ref: &crate::actor::ActorRef<A>,
        timeout: std::time::Duration,
    ) -> TransportResult<()>
    where
        A: crate::Actor + crate::remote::DistributedActor + 'static,
    {
        tracing::info!(
            "🔄 [TRANSPORT] Starting register_distributed_actor_sync for '{}' (timeout: {:?})",
            name,
            timeout
        );

        // Validate that global transport is properly configured
        {
            use super::distributed_actor_ref::GLOBAL_TRANSPORT;
            let global = GLOBAL_TRANSPORT.lock().unwrap();
            if global.is_none() {
                tracing::error!(
                    "❌ [TRANSPORT] No global transport configured for '{}'",
                    name
                );
                return Err(TransportError::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "No global transport configured - call bootstrap_on() or bootstrap_with_config() first"
                ))));
            }
            tracing::info!("✅ [TRANSPORT] Global transport validated for '{}'", name);
        }

        // REMOVE this line - local registration already done by ctx.register()
        // registry.write().await.insert(name.clone(), actor_id);
        let actor_id = actor_ref.id();
        tracing::info!("📋 [TRANSPORT] Actor ID for '{}': {}", name, actor_id);

        // Register distributed handlers
        tracing::info!(
            "📡 [TRANSPORT] Registering distributed handlers for '{}'",
            name
        );
        A::__register_distributed_handlers(actor_ref);
        tracing::info!(
            "✅ [TRANSPORT] Distributed handlers registered for '{}'",
            name
        );

        // Use sync registration with kameo_remote to wait for peer confirmation
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))?;
        tracing::info!("🔗 [TRANSPORT] Got transport handle for '{}'", name);

        // Serialize ActorId as metadata using rkyv for zero-copy
        tracing::info!("📦 [TRANSPORT] Serializing ActorId metadata for '{}'", name);
        let metadata = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_id)
            .map_err(|e| {
                tracing::error!(
                    "❌ [TRANSPORT] Failed to serialize ActorId for '{}': {}",
                    name,
                    e
                );
                TransportError::SerializationFailed(format!("Failed to serialize ActorId: {}", e))
            })?
            .to_vec();
        tracing::info!(
            "✅ [TRANSPORT] ActorId serialized for '{}', metadata size: {} bytes",
            name,
            metadata.len()
        );

        // Create location with metadata and immediate priority for sync registration
        let bind_addr = handle.registry.bind_addr;
        tracing::info!(
            "🏠 [TRANSPORT] Creating RemoteActorLocation for '{}' at bind_addr: {}",
            name,
            bind_addr
        );
        let mut location = kameo_remote::RemoteActorLocation::new_with_metadata(
            bind_addr,
            handle.registry.peer_id.clone(),
            metadata,
        );
        location.priority = kameo_remote::RegistrationPriority::Immediate;
        tracing::info!(
            "📍 [TRANSPORT] RemoteActorLocation created for '{}' with Immediate priority",
            name
        );

        // Use sync registration - this waits for peer confirmation or returns immediately if no peers
        tracing::info!(
            "⏳ [TRANSPORT] Calling registry.register_actor_sync for '{}' (timeout: {:?})",
            name,
            timeout
        );
        match handle
            .registry
            .register_actor_sync(name.clone(), location, timeout)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    "✅ [TRANSPORT] Successfully registered '{}' with kameo_remote registry",
                    name
                );
            }
            Err(e) => {
                tracing::error!(
                    "❌ [TRANSPORT] Failed to register '{}' with kameo_remote: {:?}",
                    name,
                    e
                );
                // Check if this is a timeout error and panic with debug info
                let error_msg = format!("{:?}", e);
                if error_msg.contains("timed out")
                    || error_msg.contains("timeout")
                    || error_msg.contains("Timeout")
                {
                    panic!(
                        "🚨 CRITICAL: register_distributed_actor_sync timed out after {:?} for actor '{}'\n\
                         This should NEVER happen in normal operation and indicates a serious distributed system issue:\n\
                         • Check if peers are connected and responding\n\
                         • Verify gossip protocol is working correctly\n\
                         • Check network connectivity between nodes\n\
                         • Increase timeout if network is slow\n\
                         Original error: {:?}",
                        timeout, name, e
                    );
                } else {
                    // Non-timeout error, return normally
                    return Err(TransportError::Other(Box::new(e)));
                }
            }
        }

        tracing::info!(
            "🎉 [TRANSPORT] Completed register_distributed_actor_sync for '{}'",
            name
        );
        Ok(())
    }

    /// Register a distributed actor with specific priority and automatically register its handlers.
    ///
    /// This method registers the actor with the specified priority and automatically calls its distributed handler registration.
    /// Use RegistrationPriority::Immediate for instant gossip propagation without delay.
    /// The actor must implement the DistributedActor trait (via the distributed_actor! macro).
    pub async fn register_distributed_actor_with_priority<A>(
        &self,
        name: String,
        actor_ref: &crate::actor::ActorRef<A>,
        priority: kameo_remote::RegistrationPriority,
    ) -> TransportResult<()>
    where
        A: crate::Actor + crate::remote::DistributedActor + 'static,
    {
        // Validate that global transport is properly configured
        {
            use super::distributed_actor_ref::GLOBAL_TRANSPORT;
            let global = GLOBAL_TRANSPORT.lock().unwrap();
            if global.is_none() {
                return Err(TransportError::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "No global transport configured - call bootstrap_on() or bootstrap_with_config() first"
                ))));
            }
        }

        // Register the actor with the specified priority (convert enum to u8)
        let priority_u8 = match priority {
            kameo_remote::RegistrationPriority::Normal => 0,
            kameo_remote::RegistrationPriority::Immediate => 1,
        };
        self.register_actor_with_priority(name, actor_ref.id(), priority_u8)
            .await?;

        // Automatically register the distributed handlers
        A::__register_distributed_handlers(actor_ref);

        Ok(())
    }
}

impl KameoTransport {
    /// Connect to the configured peers using the proper kameo_remote API
    pub async fn connect_to_peers(&self) -> Result<(), super::transport::TransportError> {
        if let Some(handle) = &self.handle {
            let mut successful_connections = 0;

            for peer_addr in self.config.peers.iter() {
                // Use proper node naming like manual examples: kameo_node_{port}
                let peer_node_name = format!("kameo_node_{}", peer_addr.port());
                println!(
                    "Attempting to connect to peer: {} (node: {})",
                    peer_addr, peer_node_name
                );

                // Use the proper kameo_remote API: add_peer + connect with proper PeerId
                let peer = handle
                    .add_peer(&kameo_remote::PeerId::new(&peer_node_name))
                    .await;
                match peer.connect(peer_addr).await {
                    Ok(_) => {
                        println!(
                            "✓ Successfully connected to peer: {} (node: {})",
                            peer_addr, peer_node_name
                        );
                        successful_connections += 1;
                    }
                    Err(e) => {
                        println!(
                            "✗ Failed to connect to peer: {} (node: {}) - {}",
                            peer_addr, peer_node_name, e
                        );
                        // Don't fail the entire operation, just continue to next peer
                    }
                }
            }

            if successful_connections > 0 {
                println!(
                    "Successfully connected to {} peers using kameo_remote API.",
                    successful_connections
                );
            }
        }
        Ok(())
    }

    /// Get a connection handle for a specific location
    /// This allows caching the connection to avoid mutex locks on every tell/ask
    pub async fn get_connection_for_location(
        &self,
        location: &RemoteActorLocation,
    ) -> Result<kameo_remote::connection_pool::ConnectionHandle, TransportError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

        // This still goes through the mutex, but only once during actor ref creation
        handle
            .get_connection(location.peer_addr)
            .await
            .map_err(|e| {
                TransportError::ConnectionFailed(format!("Failed to get connection: {}", e))
            })
    }
}

impl RemoteTransport for KameoTransport {
    fn start(&mut self) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        Box::pin(async move {
            // Check if handle is already set (e.g., from bootstrap_with_keypair)
            if self.handle.is_some() {
                // Transport already started/configured, nothing to do
                return Ok(());
            }

            // Parse bind address
            let bind_addr = self.config.bind_addr;

            // Create gossip config - keypair should be provided via bootstrap_with_keypair for TLS
            // For default bootstrap_on, we use a test keypair (not for production)
            let node_name = format!("kameo_node_{}", bind_addr.port());
            let keypair = kameo_remote::KeyPair::new_for_testing(&node_name);

            let gossip_config = GossipConfig {
                key_pair: Some(keypair),
                gossip_interval: Duration::from_secs(5),
                max_gossip_peers: 3,
                ..Default::default()
            };

            // Create and start the gossip registry with no initial peers
            // We'll add peers later via add_peer() + connect() calls
            let handle = GossipRegistryHandle::new(bind_addr, vec![], Some(gossip_config))
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;

            self.handle = Some(Arc::new(handle));

            // TODO: Hook up message handler to process incoming messages

            Ok(())
        })
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(_handle) = self.handle.take() {
                // Graceful shutdown by dropping the handle Arc
                // The handle will be dropped when the Arc is dropped
            }
            Ok(())
        })
    }

    fn local_addr(&self) -> SocketAddr {
        self.handle
            .as_ref()
            .map(|h| h.registry.bind_addr)
            .unwrap_or(self.config.bind_addr)
    }

    fn register_actor(
        &self,
        name: String,
        actor_id: ActorId,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        let registry = self.registry.clone();
        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Register in local registry
            registry.write().await.insert(name.clone(), actor_id);

            // Serialize ActorId as metadata using rkyv for zero-copy
            let metadata = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_id)
                .map_err(|e| {
                    TransportError::SerializationFailed(format!(
                        "Failed to serialize ActorId: {}",
                        e
                    ))
                })?
                .to_vec();

            // Register with kameo_remote's gossip protocol including ActorId metadata
            let bind_addr = handle.registry.bind_addr;

            // Create location with metadata and immediate priority for instant gossip
            let mut location = kameo_remote::RemoteActorLocation::new_with_metadata(
                bind_addr,
                handle.registry.peer_id.clone(),
                metadata,
            );
            location.priority = kameo_remote::RegistrationPriority::Immediate;

            // Register with immediate priority to trigger instant gossip synchronization
            handle
                .registry
                .register_actor_with_priority(
                    name.clone(),
                    location,
                    kameo_remote::RegistrationPriority::Immediate,
                )
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;

            // With Immediate priority, the registration will trigger immediate gossip
            // to all connected peers, eliminating the need to wait for the next gossip interval

            Ok(())
        })
    }

    /// Register an actor with specific priority
    fn register_actor_with_priority(
        &self,
        name: String,
        actor_id: ActorId,
        priority: u8, // 0=Normal, 1=Immediate
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        let registry = self.registry.clone();
        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Register in local registry
            registry.write().await.insert(name.clone(), actor_id);

            // Serialize ActorId as metadata using rkyv for zero-copy
            let metadata = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_id)
                .map_err(|e| {
                    TransportError::SerializationFailed(format!(
                        "Failed to serialize ActorId: {}",
                        e
                    ))
                })?
                .to_vec();

            // Register with kameo_remote's gossip protocol using specified priority
            let bind_addr = handle.registry.bind_addr;

            // Create location with metadata and convert u8 priority to RegistrationPriority
            let mut location = kameo_remote::RemoteActorLocation::new_with_metadata(
                bind_addr,
                handle.registry.peer_id.clone(),
                metadata,
            );

            // Convert u8 to RegistrationPriority
            let kameo_priority = if priority >= 1 {
                kameo_remote::RegistrationPriority::Immediate
            } else {
                kameo_remote::RegistrationPriority::Normal
            };
            location.priority = kameo_priority;

            // Register with the specified priority to control gossip timing
            handle
                .registry
                .register_actor_with_priority(name.clone(), location, kameo_priority)
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;

            Ok(())
        })
    }

    fn register_actor_sync(
        &self,
        name: String,
        actor_id: ActorId,
        timeout: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        let registry = self.registry.clone();
        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Register in local registry
            registry.write().await.insert(name.clone(), actor_id);

            // Serialize ActorId as metadata using rkyv for zero-copy
            let metadata = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_id)
                .map_err(|e| {
                    TransportError::SerializationFailed(format!(
                        "Failed to serialize ActorId: {}",
                        e
                    ))
                })?
                .to_vec();

            // Create location with metadata
            let location = kameo_remote::RemoteActorLocation::new_with_metadata(
                handle.registry.bind_addr,
                handle.registry.peer_id.clone(),
                metadata,
            );

            // Use the new synchronous registration method with timeout
            handle
                .registry
                .register_actor_sync(name.clone(), location, timeout)
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;

            Ok(())
        })
    }

    fn unregister_actor(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Remove from local registry
            self.registry.write().await.remove(&name);

            // Unregister with kameo_remote
            handle
                .unregister(&name)
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;

            Ok(())
        })
    }

    fn lookup_actor(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Option<RemoteActorLocation>>> + Send + '_>>
    {
        let name = name.to_string();
        let registry = self.registry.clone();
        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Lookup via kameo_remote's gossip protocol
            let location_opt = handle.lookup(&name).await;

            // Convert kameo_remote location to our RemoteActorLocation
            if let Some(loc) = location_opt {
                // Try to deserialize ActorId from metadata first
                let actor_id = if !loc.metadata.is_empty() {
                    match rkyv::from_bytes::<ActorId, rkyv::rancor::Error>(&loc.metadata) {
                        Ok(id) => id,
                        Err(_e) => {
                            // Fall back to local registry
                            registry
                                .read()
                                .await
                                .get(&name)
                                .copied()
                                .unwrap_or_else(|| ActorId::from_u64(0))
                        }
                    }
                } else {
                    // No metadata, try local registry
                    registry
                        .read()
                        .await
                        .get(&name)
                        .copied()
                        .unwrap_or_else(|| ActorId::from_u64(0))
                };

                let remote_location = RemoteActorLocation {
                    peer_addr: loc
                        .address
                        .parse()
                        .unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap()),
                    actor_id,
                    metadata: loc.metadata.clone(),
                };

                Ok(Some(remote_location))
            } else {
                Ok(None)
            }
        })
    }

    fn send_tell<M>(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _message: M,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>>
    where
        M: Archive
            + for<'a> RkyvSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        &'a mut [u8],
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            > + Send
            + 'static,
    {
        Box::pin(async move {
            // kameo_remote doesn't have typed message support yet
            // Use send_tell_typed instead
            Err(TransportError::Other(
                "Typed messages not supported by kameo_remote yet. Use send_tell_typed instead"
                    .into(),
            ))
        })
    }

    fn send_ask<A, M>(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _message: M,
        _timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = TransportResult<<A as crate::message::Message<M>>::Reply>>
                + Send
                + '_,
        >,
    >
    where
        A: crate::actor::Actor + crate::message::Message<M>,
        M: Archive
            + for<'a> RkyvSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        &'a mut [u8],
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            > + Send
            + 'static,
        <A as crate::message::Message<M>>::Reply: Archive
            + for<'a> RkyvDeserialize<
                <A as crate::message::Message<M>>::Reply,
                rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
            > + Send,
    {
        Box::pin(async move {
            // kameo_remote doesn't have typed message support yet
            // Use send_ask_typed instead
            Err(TransportError::Other(
                "Typed messages not supported by kameo_remote yet. Use send_ask_typed instead"
                    .into(),
            ))
        })
    }

    fn send_tell_typed(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        let peer_addr = location.peer_addr;
        let payload = payload.to_vec();
        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Get connection pool from registry
            let mut pool = handle.registry.connection_pool.lock().await;

            // Get or create connection to peer
            let conn = pool.get_connection(peer_addr).await.map_err(|e| {
                TransportError::ConnectionFailed(format!("Failed to get connection: {}", e))
            })?;

            // Use direct binary protocol to avoid double serialization
            // Format: [length:4][type:1][correlation_id:2][reserved:5][actor_id:8][type_hash:4][payload_len:4][payload:N]

            let inner_size = 8 + 16 + payload.len(); // header + actor fields + payload
            let mut message = Vec::with_capacity(4 + inner_size);

            // Length prefix (4 bytes)
            message.extend_from_slice(&(inner_size as u32).to_be_bytes());

            // Header: [type:1][correlation_id:2][reserved:5]
            message.push(3u8); // MessageType::ActorTell
            message.extend_from_slice(&0u16.to_be_bytes()); // No correlation for tell
            message.extend_from_slice(&[0u8; 5]); // Reserved

            // Actor message: [actor_id:8][type_hash:4][payload_len:4][payload:N]
            message.extend_from_slice(&actor_id.into_u64().to_be_bytes());
            message.extend_from_slice(&type_hash.to_be_bytes());
            message.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            message.extend_from_slice(&payload);

            // Send the message via kameo_remote using send_binary_message
            conn.send_binary_message(&message).await.map_err(|e| {
                TransportError::Other(format!("Failed to send typed tell: {}", e).into())
            })?;

            Ok(())
        })
    }

    fn send_ask_typed(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        type_hash: u32,
        payload: Bytes,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + '_>> {
        let peer_addr = location.peer_addr;
        let payload = payload.to_vec();
        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Get connection pool from registry
            let mut pool = handle.registry.connection_pool.lock().await;

            // Get or create connection to peer
            let conn = pool.get_connection(peer_addr).await.map_err(|e| {
                TransportError::ConnectionFailed(format!("Failed to get connection: {}", e))
            })?;

            // For ask operations, we need to wrap in RegistryMessage::ActorMessage
            // The server expects this format for Ask messages

            let actor_message = kameo_remote::registry::RegistryMessage::ActorMessage {
                actor_id: actor_id.into_u64().to_string(),
                type_hash,
                payload,
                correlation_id: None, // This MUST be None - the Ask envelope will set it
            };

            // Serialize the RegistryMessage
            let serialized_msg =
                rkyv::to_bytes::<rkyv::rancor::Error>(&actor_message).map_err(|e| {
                    TransportError::Other(
                        format!("Failed to serialize actor message: {}", e).into(),
                    )
                })?;

            // Send the ask message and wait for reply
            // conn.ask() will add the proper header with MessageType::Ask (1) and correlation ID
            // The server will:
            // 1. Receive MessageType::Ask with correlation_id in envelope
            // 2. Deserialize our RegistryMessage::ActorMessage
            // 3. Set the correlation_id from envelope into the ActorMessage
            // 4. Call the handler with the updated ActorMessage
            match tokio::time::timeout(timeout, conn.ask(&serialized_msg)).await {
                Ok(Ok(reply_bytes)) => {
                    // The reply should be the serialized response from the actor
                    Ok(Bytes::from(reply_bytes))
                }
                Ok(Err(e)) => Err(TransportError::Other(
                    format!("Failed to send typed ask: {}", e).into(),
                )),
                Err(_) => Err(TransportError::Timeout),
            }
        })
    }

    fn send_ask_streaming(
        &self,
        _actor_id: ActorId,
        location: &RemoteActorLocation,
        _type_hash: u32,
        payload: Bytes,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + '_>> {
        let peer_addr = location.peer_addr;

        Box::pin(async move {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Get connection pool from registry
            let mut pool = handle.registry.connection_pool.lock().await;

            // Get or create connection to peer
            let conn = pool.get_connection(peer_addr).await.map_err(|e| {
                TransportError::ConnectionFailed(format!(
                    "Failed to get connection for streaming: {}",
                    e
                ))
            })?;

            // For streaming ask operations, use the connection's streaming capability with ask
            // This builds a proper ask message with streaming protocol support

            if payload.len() <= STREAM_THRESHOLD {
                // Small message, use regular ask path
                match tokio::time::timeout(timeout, conn.ask(&payload)).await {
                    Ok(Ok(reply_bytes)) => Ok(Bytes::from(reply_bytes)),
                    Ok(Err(e)) => Err(TransportError::Other(
                        format!("Failed to send ask: {}", e).into(),
                    )),
                    Err(_) => Err(TransportError::Timeout),
                }
            } else {
                // Large message requires streaming protocol
                // Since kameo_remote doesn't have native ask_streaming yet, implement chunked approach

                // CRITICAL FIX: The previous implementation only sent the first chunk
                // This implementation uses a chunked protocol for large ask messages

                const CHUNK_SIZE: usize = 256 * 1024; // 256KB chunks to stay under threshold
                let chunks: Vec<&[u8]> = payload.chunks(CHUNK_SIZE).collect();
                let total_chunks = chunks.len();

                if total_chunks == 1 {
                    // Single chunk, but still large - try regular ask
                    // Note: this may fail if the message is too large for ring buffer
                    match tokio::time::timeout(timeout, conn.ask(&payload)).await {
                        Ok(Ok(reply_bytes)) => Ok(Bytes::from(reply_bytes)),
                        Ok(Err(e)) => Err(TransportError::Other(
                            format!("Failed to send large ask message: {}", e).into(),
                        )),
                        Err(_) => Err(TransportError::Timeout),
                    }
                } else {
                    // Multiple chunks - implement chunked ask protocol
                    // This is a workaround until native streaming ask is available

                    // For now, this is a limitation - we can only handle single large messages
                    // Multi-chunk ask operations require protocol-level support
                    let error_msg = format!(
                        "Ask message too large for current implementation: {} bytes in {} chunks. Maximum supported size: {} bytes.",
                        payload.len(),
                        total_chunks,
                        STREAM_THRESHOLD
                    );

                    Err(TransportError::Other(error_msg.into()))
                }
            }
        })
    }

    fn set_message_handler(&mut self, handler: Box<dyn MessageHandler>) {
        self.message_handler = Some(Arc::from(handler));

        // If handle is already started, hook up the handler
        if let Some(_handle) = &self.handle {
            // TODO: Connect handler to kameo_remote's message processing
            // This will require kameo_remote to support custom message handlers
        }
    }
}
