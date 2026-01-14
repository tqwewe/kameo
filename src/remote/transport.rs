//! Transport abstraction layer for remote actors
//!
//! This module provides a trait-based abstraction over different transport implementations,
//! allowing Kameo to support multiple networking backends for distributed actor communication.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use bytes::Bytes;
use rkyv::{Archive, Serialize as RkyvSerialize};

use crate::actor::{Actor, ActorId};
use crate::message::Message;

/// Type alias for boxed errors
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Remote-specific errors
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// The requested actor is not registered with the remote transport
    #[error("actor not registered: {0}")]
    ActorNotRegistered(String),

    /// The operation timed out waiting for a response
    #[error("timeout occurred")]
    Timeout,

    /// The underlying swarm/transport has stopped
    #[error("swarm stopped")]
    SwarmStopped,

    /// Other errors from the transport layer
    #[error(transparent)]
    Other(#[from] BoxError),
}

/// Result type for transport operations
pub type TransportResult<T> = Result<T, TransportError>;

/// Errors that can occur during transport operations
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Failed to establish a connection to a remote peer
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to serialize or deserialize message data
    #[error("serialization failed: {0}")]
    SerializationFailed(String),

    /// The requested actor could not be found
    #[error("actor not found: {0}")]
    ActorNotFound(String),

    /// The operation timed out
    #[error("timeout occurred")]
    Timeout,

    /// The transport has been shut down
    #[error("transport shutdown")]
    Shutdown,

    /// Other transport-related errors
    #[error(transparent)]
    Other(#[from] BoxError),
}

impl From<TransportError> for RemoteError {
    fn from(err: TransportError) -> Self {
        match err {
            TransportError::ActorNotFound(name) => RemoteError::ActorNotRegistered(name),
            TransportError::Timeout => RemoteError::Timeout,
            TransportError::Shutdown => RemoteError::SwarmStopped,
            _ => RemoteError::Other(err.into()),
        }
    }
}

/// Trait for transport implementations
///
/// This trait abstracts over different networking backends (kameo_remote, iroh, etc.)
/// to provide a unified interface for remote actor communication.
pub trait RemoteTransport: Send + Sync + 'static {
    /// Start the transport layer
    fn start(&mut self) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>>;

    /// Shutdown the transport layer
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>>;

    /// Get the local peer ID/address
    fn local_addr(&self) -> SocketAddr;

    /// Register an actor with a given name
    fn register_actor(
        &self,
        name: String,
        actor_id: ActorId,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>>;

    /// Register an actor with synchronous confirmation from peers
    fn register_actor_sync(
        &self,
        name: String,
        actor_id: ActorId,
        timeout: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>>;

    /// Register an actor with specific priority (optional - default to register_actor)
    fn register_actor_with_priority(
        &self,
        name: String,
        actor_id: ActorId,
        _priority: u8, // Use u8 to avoid import issues for now - 0=Normal, 1=Immediate
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        // Default implementation just calls register_actor
        self.register_actor(name, actor_id)
    }

    /// Unregister an actor
    fn unregister_actor(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>>;

    /// Lookup a remote actor by name
    fn lookup_actor(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Option<RemoteActorLocation>>> + Send + '_>>;

    /// Send a tell message to a remote actor
    fn send_tell<M>(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        message: M,
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
            + 'static;

    /// Send an ask message to a remote actor and wait for reply
    #[allow(clippy::type_complexity)]
    fn send_ask<A, M>(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        message: M,
        timeout: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<<A as Message<M>>::Reply>> + Send + '_>>
    where
        A: Actor + Message<M>,
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
        <A as Message<M>>::Reply: Archive
            + for<'a> rkyv::Deserialize<
                <A as Message<M>>::Reply,
                rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
            > + Send;

    /// Send a tell message with explicit type hash (for generic actors)
    fn send_tell_typed(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _type_hash: u32,
        _payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        // Default implementation delegates to send_tell with a dummy message
        // Transports can override for better efficiency
        Box::pin(async move {
            Err(TransportError::Other(
                "Typed messages not supported by this transport".into(),
            ))
        })
    }

    /// Send an ask message with explicit type hash (for generic actors)
    fn send_ask_typed(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _type_hash: u32,
        _payload: Bytes,
        _timeout: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + '_>> {
        // Default implementation delegates to send_ask with a dummy message
        // Transports can override for better efficiency
        Box::pin(async move {
            Err(TransportError::Other(
                "Typed messages not supported by this transport".into(),
            ))
        })
    }

    /// Send an ask message with streaming protocol for large messages (>1MB)
    ///
    /// This method should be used for messages that exceed the streaming threshold
    /// to ensure optimal performance and avoid memory issues with large payloads.
    fn send_ask_streaming(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        type_hash: u32,
        payload: Bytes,
        timeout: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + '_>> {
        // Default implementation falls back to regular typed ask
        // Transports should override for true streaming support
        self.send_ask_typed(actor_id, location, type_hash, payload, timeout)
    }

    /// Handle incoming messages for local actors
    fn set_message_handler(&mut self, handler: Box<dyn MessageHandler>);
}

/// Information about a remote actor's location
#[derive(Debug, Clone, Archive, RkyvSerialize, rkyv::Deserialize)]
#[rkyv(derive(Debug))]
pub struct RemoteActorLocation {
    /// The peer address hosting the actor
    pub peer_addr: SocketAddr,
    /// The actor's ID on that peer
    pub actor_id: ActorId,
    /// Additional transport-specific data
    pub metadata: Vec<u8>,
}

/// Handler for incoming messages
pub trait MessageHandler: Send + Sync {
    /// Handle an incoming tell message
    fn handle_tell(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>>;

    /// Handle an incoming ask message
    fn handle_ask(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, BoxError>> + Send + '_>>;

    /// Handle an incoming tell message with type hash (for generic actors)
    fn handle_tell_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        // Default implementation converts to string for backward compatibility
        let message_type = format!("hash:{:08x}", type_hash);
        self.handle_tell(actor_id, &message_type, &payload)
    }

    /// Handle an incoming ask message with type hash (for generic actors)
    fn handle_ask_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, BoxError>> + Send + '_>> {
        // Default implementation converts to string for backward compatibility
        let message_type = format!("hash:{:08x}", type_hash);
        Box::pin(async move {
            let result = self.handle_ask(actor_id, &message_type, &payload).await?;
            Ok(Bytes::from(result))
        })
    }
}

/// Transport configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Local address to bind to
    pub bind_addr: SocketAddr,
    /// Maximum number of connections
    pub max_connections: usize,
    /// Connection timeout
    pub connection_timeout: std::time::Duration,
    /// Whether to enable encryption
    pub enable_encryption: bool,
    /// Peer addresses to connect to
    pub peers: Vec<SocketAddr>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 1000,
            connection_timeout: std::time::Duration::from_secs(30),
            enable_encryption: true,
            peers: Vec::new(),
        }
    }
}

// Implement RemoteTransport for Box<T> to allow boxed transports
impl<T: RemoteTransport + ?Sized> RemoteTransport for Box<T> {
    fn start(&mut self) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        (**self).start()
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        (**self).shutdown()
    }

    fn local_addr(&self) -> SocketAddr {
        (**self).local_addr()
    }

    fn register_actor(
        &self,
        name: String,
        actor_id: ActorId,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        (**self).register_actor(name, actor_id)
    }

    fn register_actor_sync(
        &self,
        name: String,
        actor_id: ActorId,
        timeout: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        (**self).register_actor_sync(name, actor_id, timeout)
    }

    fn unregister_actor(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        (**self).unregister_actor(name)
    }

    fn lookup_actor(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Option<RemoteActorLocation>>> + Send + '_>>
    {
        (**self).lookup_actor(name)
    }

    fn send_tell<M>(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        message: M,
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
        (**self).send_tell(actor_id, location, message)
    }

    fn send_ask<A, M>(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        message: M,
        timeout: std::time::Duration,
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
            + for<'a> rkyv::Deserialize<
                <A as crate::message::Message<M>>::Reply,
                rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
            > + Send,
    {
        (**self).send_ask::<A, M>(actor_id, location, message, timeout)
    }

    fn send_tell_typed(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        (**self).send_tell_typed(actor_id, location, type_hash, payload)
    }

    fn send_ask_typed(
        &self,
        actor_id: ActorId,
        location: &RemoteActorLocation,
        type_hash: u32,
        payload: Bytes,
        timeout: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + '_>> {
        (**self).send_ask_typed(actor_id, location, type_hash, payload, timeout)
    }

    fn set_message_handler(&mut self, handler: Box<dyn MessageHandler>) {
        (**self).set_message_handler(handler)
    }
}
