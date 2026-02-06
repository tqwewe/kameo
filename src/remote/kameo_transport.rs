//! kameo_remote transport implementation

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kameo_remote::{GossipConfig, GossipRegistryHandle};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::RwLock;

use crate::actor::ActorId;

use super::location_metadata;
use super::transport::{
    MessageHandler, RemoteActorLocation, RemoteTransport, TransportConfig, TransportError,
    TransportResult,
};

type LocationMetadataV1 = location_metadata::LocationMetadataV1;

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
                    "No global transport configured - call bootstrap_with_keypair() or bootstrap_with_config() (with keypair) first",
                ))));
            }
            tracing::info!("✅ [TRANSPORT] Global transport validated for '{}'", name);
        }

        // REMOVE this line - local registration already done by ctx.register()
        // registry.write().await.insert(name.clone(), actor_id);
        let actor_id = actor_ref.id();
        tracing::info!("📋 [TRANSPORT] Actor ID for '{}': {}", name, actor_id);

        let name_for_link = name.clone();

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
        super::remote_link::register_local_actor(
            name_for_link,
            actor_ref.id(),
            actor_ref.weak_signal_mailbox(),
        );
        if let Some(handle) = self.handle.as_ref() {
            let peers = handle.registry.connection_pool.get_connected_peers();
            for peer_addr in peers {
                if let Some(peer_id) =
                    handle.registry.connection_pool.get_peer_id_by_addr(&peer_addr)
                {
                    super::remote_link::auto_link_peer(peer_addr, &peer_id);
                }
            }
        }

        // Use sync registration with kameo_remote to wait for peer confirmation
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))?;
        tracing::info!("🔗 [TRANSPORT] Got transport handle for '{}'", name);

        // Serialize metadata (ActorId + PeerId) so clients can reconnect by PeerId (TLS-verified).
        tracing::info!("📦 [TRANSPORT] Serializing actor metadata for '{}'", name);
        let metadata = location_metadata::encode_v1(actor_id, handle.registry.peer_id.clone())
            .map_err(|e| {
                tracing::error!(
                    "❌ [TRANSPORT] Failed to serialize metadata for '{}': {}",
                    name,
                    e
                );
                TransportError::SerializationFailed(format!("Failed to serialize metadata: {}", e))
            })?;
        tracing::info!(
            "✅ [TRANSPORT] Actor metadata serialized for '{}', metadata size: {} bytes",
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
                    "No global transport configured - call bootstrap_with_keypair() or bootstrap_with_config() (with keypair) first",
                ))));
            }
        }

        // Register the actor with the specified priority (convert enum to u8)
        let priority_u8 = match priority {
            kameo_remote::RegistrationPriority::Normal => 0,
            kameo_remote::RegistrationPriority::Immediate => 1,
        };
        let name_for_link = name.clone();
        self.register_actor_with_priority(name, actor_ref.id(), priority_u8)
            .await?;

        // Automatically register the distributed handlers
        A::__register_distributed_handlers(actor_ref);
        super::remote_link::register_local_actor(
            name_for_link,
            actor_ref.id(),
            actor_ref.weak_signal_mailbox(),
        );
        if let Some(handle) = self.handle.as_ref() {
            let peers = handle.registry.connection_pool.get_connected_peers();
            for peer_addr in peers {
                if let Some(peer_id) =
                    handle.registry.connection_pool.get_peer_id_by_addr(&peer_addr)
                {
                    super::remote_link::auto_link_peer(peer_addr, &peer_id);
                }
            }
        }

        Ok(())
    }
}

impl KameoTransport {
    /// Connect to the configured peers using the proper kameo_remote API
    pub async fn connect_to_peers(&self) -> Result<(), super::transport::TransportError> {
        if let Some(handle) = &self.handle {
            let mut successful_connections = 0;

            for peer in self.config.peers.iter() {
                println!(
                    "Attempting to connect to peer: {} (peer_id: {})",
                    peer.addr, peer.peer_id
                );

                let peer_handle = handle.add_peer(&peer.peer_id).await;
                match peer_handle.connect(&peer.addr).await {
                    Ok(_) => {
                        println!(
                            "✓ Successfully connected to peer: {} (peer_id: {})",
                            peer.addr, peer.peer_id
                        );
                        successful_connections += 1;
                    }
                    Err(e) => {
                        println!(
                            "✗ Failed to connect to peer: {} (peer_id: {}) - {}",
                            peer.addr, peer.peer_id, e
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
        self.connection_for_location(location).await
    }

    /// Force the underlying transport to drop any existing connection to `peer_addr`.
    ///
    /// This is used on error paths (timeouts, socket glitches) to avoid retry loops on a
    /// potentially "zombie" socket that the OS still considers open.
    pub async fn force_drop_connection(&self, peer_addr: SocketAddr) -> Result<(), TransportError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

        handle
            .registry
            .handle_peer_connection_failure(peer_addr)
            .await
            .map_err(|e| TransportError::ConnectionFailed(format!("force drop failed: {e}")))?;

        Ok(())
    }

    async fn force_drop_connection_for_location(
        &self,
        location: &RemoteActorLocation,
    ) -> Result<(), TransportError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

        // Fast path: if we know the peer id, do a hard pool disconnect to avoid
        // lazy eviction races.
        if let Ok(meta) = location_metadata::decode_v1(&location.metadata) {
            handle
                .registry
                .connection_pool
                .disconnect_connection_by_peer_id(&meta.peer_id);
            return Ok(());
        }

        // Fallback: address-only failure handling.
        self.force_drop_connection(location.peer_addr).await
    }

    /// Get a connection for a location, optionally forcing a new socket/IO task.
    ///
    /// Guarantees:
    /// - Never returns `Err` solely because the pool handed us a closed handle.
    /// - If `force_new` is true, evicts the current connection before dialing.
    pub async fn get_connection_for_location_fresh(
        &self,
        location: &RemoteActorLocation,
        force_new: bool,
    ) -> Result<kameo_remote::connection_pool::ConnectionHandle, TransportError> {
        if force_new {
            // Best-effort: even if eviction fails, we still attempt to dial.
            let _ = self.force_drop_connection_for_location(location).await;
        }

        let conn = self.get_connection_for_location(location).await?;
        if !conn.is_closed() {
            return Ok(conn);
        }

        // Pool handed us a closed handle. Evict and try one more time.
        let _ = self.force_drop_connection_for_location(location).await;
        self.get_connection_for_location(location).await
    }

    /// Helper that returns an active connection handle for a remote actor location.
    ///
    /// Prefer dialing by `PeerId` when the location metadata includes it, otherwise fall back to
    /// address-only dialing (best-effort, primarily for compatibility).
    async fn connection_for_location(
        &self,
        location: &RemoteActorLocation,
    ) -> Result<kameo_remote::connection_pool::ConnectionHandle, TransportError> {
        if let Ok(meta) = location_metadata::decode_v1(&location.metadata) {
            let handle = self
                .handle
                .as_ref()
                .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

            // Ensure the peer is configured with its current address (id-based dial is TLS-verified).
            // Avoid `Peer::connect` here: it mutates gossip state and can interact badly with
            // failure consensus during forced reconnects.
            handle
                .registry
                .configure_peer(meta.peer_id.clone(), location.peer_addr)
                .await;

            let peer_ref = handle.lookup_peer(&meta.peer_id).await.map_err(|e| {
                TransportError::ConnectionFailed(format!(
                    "Failed to lookup peer by id {}: {}",
                    meta.peer_id, e
                ))
            })?;

            let conn_arc = peer_ref.connection_ref().ok_or_else(|| {
                TransportError::ConnectionFailed(format!(
                    "No active connection available for peer {}",
                    meta.peer_id
                ))
            })?;

            return Ok(Arc::as_ref(conn_arc).clone());
        }

        self.connection_for_peer(location.peer_addr).await
    }

    /// Helper that returns an active connection handle for the given peer address.
    async fn connection_for_peer(
        &self,
        peer_addr: SocketAddr,
    ) -> Result<kameo_remote::connection_pool::ConnectionHandle, TransportError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))?;

        let remote_ref = handle.lookup_address(peer_addr).await.map_err(|e| {
            TransportError::ConnectionFailed(format!("Failed to lookup peer {}: {}", peer_addr, e))
        })?;

        let conn_arc = remote_ref.connection_ref().ok_or_else(|| {
            TransportError::ConnectionFailed(format!(
                "No active connection available for peer {}",
                peer_addr,
            ))
        })?;

        Ok(Arc::as_ref(conn_arc).clone())
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

            let bind_addr = self.config.bind_addr;
            let keypair = self
                .config
                .keypair
                .clone()
                .ok_or_else(|| {
                    TransportError::Other(
                        "Missing keypair. Use v2_bootstrap::bootstrap_with_keypair or set TransportConfig.keypair."
                            .into(),
                    )
                })?;

            let gossip_config = GossipConfig {
                key_pair: Some(keypair.clone()),
                gossip_interval: Duration::from_secs(5),
                max_gossip_peers: 3,
                schema_hash: self.config.schema_hash,
                ..Default::default()
            };

            if !self.config.enable_encryption {
                return Err(TransportError::Other(
                    "TLS is required for remote transport; enable_encryption must be true".into(),
                ));
            }

            kameo_remote::tls::ensure_crypto_provider();
            let secret_key = keypair.to_secret_key();
            let handle =
                GossipRegistryHandle::new_with_tls(bind_addr, secret_key, Some(gossip_config))
                    .await
                    .map_err(|e| TransportError::Other(Box::new(e)))?;

            self.handle = Some(Arc::new(handle));
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

            // Serialize metadata (ActorId + PeerId) so clients can reconnect by PeerId (TLS-verified).
            let metadata = location_metadata::encode_v1(actor_id, handle.registry.peer_id.clone())
                .map_err(|e| {
                    TransportError::SerializationFailed(format!(
                        "Failed to serialize metadata: {}",
                        e
                    ))
                })?
                ;

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

            // Serialize metadata (ActorId + PeerId) so clients can reconnect by PeerId (TLS-verified).
            let metadata = location_metadata::encode_v1(actor_id, handle.registry.peer_id.clone())
                .map_err(|e| {
                    TransportError::SerializationFailed(format!(
                        "Failed to serialize metadata: {}",
                        e
                    ))
                })?
                ;

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

            // Serialize metadata (ActorId + PeerId) so clients can reconnect by PeerId (TLS-verified).
            let metadata = location_metadata::encode_v1(actor_id, handle.registry.peer_id.clone())
                .map_err(|e| {
                    TransportError::SerializationFailed(format!(
                        "Failed to serialize metadata: {}",
                        e
                    ))
                })?
                ;

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

            // Lookup via kameo_remote's gossip protocol (returns RemoteActorRef)
            let location_opt = handle.lookup(&name).await;

            // Convert kameo_remote RemoteActorRef to our RemoteActorLocation
            if let Some(remote_ref) = location_opt {
                let location_info = remote_ref.location.clone();
                let metadata = location_info.metadata.clone();

                // Extract ActorId from metadata (supports both V1 struct and legacy ActorId-only bytes).
                let actor_id = if !metadata.is_empty() {
                    if let Ok(meta) = location_metadata::decode_v1(&metadata) {
                        meta.actor_id
                    } else if let Ok(id) = location_metadata::decode_legacy_actor_id(&metadata) {
                        id
                    } else {
                        registry
                            .read()
                            .await
                            .get(&name)
                            .copied()
                            .unwrap_or_else(|| ActorId::from_u64(0))
                    }
                } else {
                    registry
                        .read()
                        .await
                        .get(&name)
                        .copied()
                        .unwrap_or_else(|| ActorId::from_u64(0))
                };

                // Upgrade metadata to include PeerId for TLS-verified reconnects.
                let metadata = location_metadata::encode_v1(actor_id, location_info.peer_id.clone())
                    .unwrap_or(metadata);

                let remote_location = RemoteActorLocation {
                    peer_addr: location_info
                        .address
                        .parse()
                        .unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap()),
                    actor_id,
                    metadata,
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
        let payload = payload.clone();
        Box::pin(async move {
            let conn = self.connection_for_peer(peer_addr).await?;

            conn.tell_actor_frame(actor_id.into_u64(), type_hash, payload)
                .await
                .map_err(|e| {
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
        let payload = payload.clone();
        Box::pin(async move {
            let conn = self.connection_for_peer(peer_addr).await?;

            match conn
                .ask_actor_frame(actor_id.into_u64(), type_hash, payload, timeout)
                .await
            {
                Ok(reply_bytes) => Ok(reply_bytes),
                Err(kameo_remote::GossipError::Timeout) => Err(TransportError::Timeout),
                Err(e) => Err(TransportError::Other(
                    format!("Failed to send typed ask: {}", e).into(),
                )),
            }
        })
    }

    fn send_ask_streaming(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        type_hash: u32,
        payload: Bytes,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + '_>> {
        let peer_addr = location.peer_addr;

        Box::pin(async move {
            let conn = self.connection_for_peer(peer_addr).await?;

            let threshold = conn.streaming_threshold();

            if payload.len() <= threshold {
                match conn
                    .ask_actor_frame(actor_id.into_u64(), type_hash, payload, timeout)
                    .await
                {
                    Ok(reply_bytes) => Ok(reply_bytes),
                    Err(kameo_remote::GossipError::Timeout) => Err(TransportError::Timeout),
                    Err(e) => Err(TransportError::Other(
                        format!("Failed to send ask: {}", e).into(),
                    )),
                }
            } else {
                // Large message: use zero-copy streaming ask protocol
                match conn
                    .ask_streaming_bytes(payload, type_hash, actor_id.into_u64(), timeout)
                    .await
                {
                    Ok(reply_bytes) => Ok(reply_bytes),
                    Err(kameo_remote::GossipError::Timeout) => Err(TransportError::Timeout),
                    Err(e) => Err(TransportError::Other(
                        format!("Failed to send streaming ask: {}", e).into(),
                    )),
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
