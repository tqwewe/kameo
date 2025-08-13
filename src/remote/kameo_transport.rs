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

use super::transport::BoxError;
use crate::actor::{Actor, ActorId};

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
        self.handle.as_ref()
            .ok_or_else(|| TransportError::Other("Transport not started".into()))
            .map(|h| h.clone())
    }
    
    /// Set the handle for the transport (used when creating with custom keypair)
    pub fn set_handle(&mut self, handle: Arc<GossipRegistryHandle>) {
        self.handle = Some(handle);
    }
    
    /// Register an actor with synchronous peer acknowledgment using default timeout
    /// 
    /// This is a convenience method that calls the trait method with a 2 second timeout.
    pub async fn register_actor_sync(&self, name: String, actor_id: ActorId) -> TransportResult<()> {
        <Self as RemoteTransport>::register_actor_sync(self, name, actor_id, Duration::from_secs(2)).await
    }
    
    /// Register a distributed actor and automatically register its handlers.
    /// 
    /// This method registers the actor and automatically calls its distributed handler registration.
    /// The actor must implement the DistributedActor trait (via the distributed_actor! macro).
    pub async fn register_distributed_actor<A>(&self, name: String, actor_ref: &crate::actor::ActorRef<A>) -> TransportResult<()> 
    where
        A: crate::Actor + crate::remote::DistributedActor + 'static,
    {
        // Register the actor normally
        self.register_actor(name, actor_ref.id()).await?;
        
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
                let peer = handle.add_peer(&kameo_remote::PeerId::new(&peer_node_name)).await;
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
                println!("Successfully connected to {} peers using kameo_remote API.", successful_connections);
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
        handle.get_connection(location.peer_addr).await
            .map_err(|e| TransportError::ConnectionFailed(format!("Failed to get connection: {}", e)))
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
            let metadata = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_id).map_err(|e| {
                TransportError::SerializationFailed(format!("Failed to serialize ActorId: {}", e))
            })?.to_vec();

            // Register with kameo_remote's gossip protocol including ActorId metadata
            let bind_addr = handle.registry.bind_addr;
            
            // Create location with metadata and immediate priority for instant gossip
            let mut location = kameo_remote::RemoteActorLocation::new_with_metadata(
                bind_addr, 
                handle.registry.peer_id.clone(), 
                metadata
            );
            location.priority = kameo_remote::RegistrationPriority::Immediate;
            
            // Register with immediate priority to trigger instant gossip synchronization
            handle.registry
                .register_actor_with_priority(name.clone(), location, kameo_remote::RegistrationPriority::Immediate)
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;


            // With Immediate priority, the registration will trigger immediate gossip
            // to all connected peers, eliminating the need to wait for the next gossip interval

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
            let metadata = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_id).map_err(|e| {
                TransportError::SerializationFailed(format!("Failed to serialize ActorId: {}", e))
            })?.to_vec();

            // Create location with metadata
            let location = kameo_remote::RemoteActorLocation::new_with_metadata(
                handle.registry.bind_addr, 
                handle.registry.peer_id.clone(), 
                metadata
            );
            
            
            // Use the new synchronous registration method with timeout
            handle.registry
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
                        Ok(id) => {
                            id
                        },
                        Err(e) => {
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
                        .unwrap_or_else(|| {
                            ActorId::from_u64(0)
                        })
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
        M: Archive + for<'a> RkyvSerialize<
            rkyv::rancor::Strategy<rkyv::ser::Serializer<&'a mut [u8], rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>
        > + Send + 'static,
    {
        Box::pin(async move {
            // kameo_remote doesn't have typed message support yet
            // Use send_tell_typed instead
            Err(TransportError::Other(
                "Typed messages not supported by kameo_remote yet. Use send_tell_typed instead".into(),
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
        M: Archive + for<'a> RkyvSerialize<
            rkyv::rancor::Strategy<rkyv::ser::Serializer<&'a mut [u8], rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>
        > + Send + 'static,
        <A as crate::message::Message<M>>::Reply: Archive + for<'a> RkyvDeserialize<
            <A as crate::message::Message<M>>::Reply,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>
        > + Send,
    {
        Box::pin(async move {
            // kameo_remote doesn't have typed message support yet
            // Use send_ask_typed instead
            Err(TransportError::Other(
                "Typed messages not supported by kameo_remote yet. Use send_ask_typed instead".into(),
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
            let conn = pool.get_connection(peer_addr).await
                .map_err(|e| TransportError::ConnectionFailed(format!("Failed to get connection: {}", e)))?;

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
            conn.send_binary_message(&message).await
                .map_err(|e| TransportError::Other(format!("Failed to send typed tell: {}", e).into()))?;
            
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
            let conn = pool.get_connection(peer_addr).await
                .map_err(|e| TransportError::ConnectionFailed(format!("Failed to get connection: {}", e)))?;
            

            // For ask operations, we need to wrap in RegistryMessage::ActorMessage
            // The server expects this format for Ask messages
            
            let actor_message = kameo_remote::registry::RegistryMessage::ActorMessage {
                actor_id: actor_id.into_u64().to_string(),
                type_hash,
                payload,
                correlation_id: None, // This MUST be None - the Ask envelope will set it
            };
            
            // Serialize the RegistryMessage
            let serialized_msg = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_message)
                .map_err(|e| TransportError::Other(format!("Failed to serialize actor message: {}", e).into()))?;
            
            
            
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
                Ok(Err(e)) => {
                    Err(TransportError::Other(format!("Failed to send typed ask: {}", e).into()))
                }
                Err(_) => {
                    Err(TransportError::Timeout)
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

