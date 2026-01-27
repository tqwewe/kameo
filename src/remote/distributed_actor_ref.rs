//! Zero-cost distributed actor reference for optimal remote messaging
//!
//! This module provides a DistributedActorRef that uses compile-time monomorphization
//! to eliminate all dynamic dispatch overhead while maintaining a simple, ergonomic API.
//! Each message type gets its own specialized code path for maximum performance.

use std::marker::PhantomData;
use std::time::Duration;

use bytes::{BufMut, Bytes};
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

use crate::actor::{Actor, ActorId};
use crate::error::SendError;
use crate::message::Message;

use super::transport::{RemoteActorLocation, RemoteTransport};
use super::type_hash::HasTypeHash;
use kameo_remote::{connection_pool::ConnectionHandle, registry::RegistryMessage, GossipError};

/// A reference to a remote distributed actor
///
/// This actor reference uses compile-time monomorphization
///
/// Each call to `.tell()` or `.ask()` is monomorphized by the message type, allowing the compiler
/// to optimize away all runtime type checks and generate the most efficient code possible.
///
/// This eliminates dynamic dispatch overhead
#[derive(Debug)]
pub struct DistributedActorRef<A, T = Box<super::kameo_transport::KameoTransport>>
where
    A: Actor,
{
    /// The actor's ID
    pub(crate) actor_id: ActorId,
    /// The actor's location
    pub(crate) location: RemoteActorLocation,
    /// The transport to use for communication
    pub(crate) transport: T,
    /// Cached connection handle for lock-free access (only for KameoTransport)
    pub(crate) connection: Option<ConnectionHandle>,
    /// The actor type, used for message type inference
    pub(crate) _actor_type: PhantomData<A>,
}

impl<A, T> Clone for DistributedActorRef<A, T>
where
    A: Actor,
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            actor_id: self.actor_id,
            location: self.location.clone(),
            transport: self.transport.clone(),
            connection: self.connection.clone(),
            _actor_type: PhantomData,
        }
    }
}

impl<A, T> DistributedActorRef<A, T>
where
    A: Actor,
    T: RemoteTransport,
{
    /// Create a new zero-cost distributed actor reference
    ///
    /// This method is private to prevent creating instances without cached connections.
    /// Use `lookup()` instead which ensures a cached connection is available.
    fn new_with_connection(
        actor_id: ActorId,
        location: RemoteActorLocation,
        transport: T,
        connection: kameo_remote::connection_pool::ConnectionHandle,
    ) -> Self {
        Self {
            actor_id,
            location,
            transport,
            connection: Some(connection),
            _actor_type: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn __new_for_tests(
        actor_id: ActorId,
        location: RemoteActorLocation,
        transport: T,
        connection: kameo_remote::connection_pool::ConnectionHandle,
    ) -> Self {
        Self::new_with_connection(actor_id, location, transport, connection)
    }

    #[doc(hidden)]
    pub fn __new_without_connection_for_tests(
        actor_id: ActorId,
        location: RemoteActorLocation,
        transport: T,
    ) -> Self {
        Self {
            actor_id,
            location,
            transport,
            connection: None,
            _actor_type: PhantomData,
        }
    }

    /// Get the actor's ID
    pub fn id(&self) -> ActorId {
        self.actor_id
    }

    /// Get the remote actor location
    pub fn location(&self) -> &RemoteActorLocation {
        &self.location
    }

    /// Send a tell message to the remote actor with zero-cost abstraction.
    ///
    /// This method is monomorphized for each message type M, allowing the compiler
    /// to generate specialized, high-performance code with no dynamic dispatch overhead.
    /// All type hashes, serialization, and message handling is resolved at compile time.
    pub fn tell<M>(&self, message: M) -> DistributedTellRequest<'_, A, M, T>
    where
        M: HasTypeHash
            + Send
            + 'static
            + Archive
            + for<'a> RSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        DistributedTellRequest {
            actor_ref: self,
            message,
            timeout: None,
            _message_type: PhantomData,
        }
    }

    /// Send an ask message to the remote actor with zero-cost abstraction.
    ///
    /// This method is monomorphized for each message type M and reply type R,
    /// generating specialized code with no runtime overhead.
    pub fn ask<M>(&self, message: M) -> DistributedAskRequest<'_, A, M, T>
    where
        A: Actor + Message<M>,
        M: HasTypeHash
            + Send
            + 'static
            + Archive
            + for<'a> RSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
        for<'b> <A as Message<M>>::Reply: Archive
            + RSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'b>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
        for<'b> <<A as Message<M>>::Reply as Archive>::Archived: RDeserialize<
                <A as Message<M>>::Reply,
                rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
            > + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'b>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        DistributedAskRequest {
            actor_ref: self,
            message,
            timeout: None,
            _message_type: PhantomData,
        }
    }
}

// Global transport cache for lookup without parameters
use std::sync::{Arc, LazyLock, Mutex};

/// Global transport instance for distributed actor lookups
///
/// This is set by the bootstrap functions and used by DistributedActorRef::lookup()
/// to enable convenient actor lookups without explicit transport parameters.
pub static GLOBAL_TRANSPORT: LazyLock<
    Arc<Mutex<Option<Box<super::kameo_transport::KameoTransport>>>>,
> = LazyLock::new(|| Arc::new(Mutex::new(None)));

// Specialized implementation for KameoTransport to cache connections
impl<A> DistributedActorRef<A, Box<super::kameo_transport::KameoTransport>>
where
    A: Actor,
{
    /// Look up a distributed actor by name (uses cached global transport)
    pub async fn lookup(
        name: &str,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let transport = {
            let global = GLOBAL_TRANSPORT.lock().unwrap();
            global.clone().ok_or(
                "No global transport set - did you call bootstrap_with_keypair() or bootstrap_with_config() (with keypair)?",
            )?
        };
        if let Some(location) = transport.lookup_actor(name).await? {
            // Try to get a cached connection for zero-cost abstraction
            // If connection fails, gracefully return None (actor found but not reachable)
            let connection = match transport.get_connection_for_location(&location).await {
                Ok(conn) => conn,
                Err(_) => {
                    // Connection failed - actor exists in gossip but is not reachable
                    return Ok(None);
                }
            };

            Ok(Some(Self::new_with_connection(
                location.actor_id,
                location,
                transport,
                connection,
            )))
        } else {
            Ok(None)
        }
    }
}

/// Set the global transport for lookup calls without transport parameter
/// This is private - only callable from within the kameo crate (by bootstrap functions)
pub(crate) fn set_global_transport(transport: Box<super::kameo_transport::KameoTransport>) {
    let mut global = GLOBAL_TRANSPORT.lock().unwrap();
    *global = Some(transport);
}

/// A pending tell request to a distributed actor (zero-cost version).
///
/// This struct is monomorphized for each message type M, allowing the compiler
/// to generate specialized code with compile-time constants and optimized paths.
#[derive(Debug)]
pub struct DistributedTellRequest<'a, A, M, T = Box<super::kameo_transport::KameoTransport>>
where
    A: Actor,
{
    actor_ref: &'a DistributedActorRef<A, T>,
    message: M,
    timeout: Option<Duration>,
    _message_type: PhantomData<M>,
}

impl<'a, A, M, T> DistributedTellRequest<'a, A, M, T>
where
    A: Actor,
    M: HasTypeHash
        + Send
        + 'static
        + Archive
        + for<'b> RSerialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'b>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    T: RemoteTransport,
{
    /// Set a timeout for the tell operation
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Send the tell message with zero-cost abstraction.
    ///
    /// This method is fully monomorphized for message type M, generating specialized
    /// code with compile-time constants and no dynamic dispatch. The type hash,
    /// serialization strategy, and all optimizations are resolved at compile time.
    pub async fn send(self) -> Result<(), SendError> {
        // Compile-time constant - no runtime lookup!
        let type_hash = M::TYPE_HASH.as_u32();

        // Optimized serialization - can be specialized per message type
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&self.message)
            .map_err(|_e| SendError::ActorStopped)?;

        // Use streaming for large messages automatically
        if let Some(ref conn) = self.actor_ref.connection {
            // Get actual threshold from connection
            let threshold = conn.streaming_threshold();

            // Use streaming for large messages
            if payload.len() > threshold {
                return conn
                    .stream_large_message(&payload, type_hash, self.actor_ref.actor_id.into_u64())
                    .await
                    .map_err(|_| SendError::ActorStopped);
            }

            // Small message - use ring buffer
            // ZERO-COPY: Use BytesMut for efficient message building
            let inner_size = 12 + 16 + payload.len(); // header (type+corr+reserved=12) + actor fields + payload
            let mut message = bytes::BytesMut::with_capacity(4 + inner_size);

            // All constants - compiler can optimize these away completely
            message.put_u32(inner_size as u32);

            // Header: [type:1][correlation_id:2][reserved:9]
            message.put_u8(3); // MessageType::ActorTell - compile-time constant
            message.put_u16(0); // No correlation for tell
            message.put_slice(&[0u8; 9]); // 9 reserved bytes for 32-byte alignment

            // Actor message: [actor_id:8][type_hash:4][payload_len:4][payload:N]
            message.put_u64(self.actor_ref.actor_id.into_u64());
            message.put_u32(type_hash); // Compile-time constant!
            message.put_u32(payload.len() as u32);
            message.put_slice(payload.as_slice());

            // ZERO-COPY: Convert to Bytes and write directly without copy
            let message_bytes = message.freeze();

            // Use the ConnectionHandle's zero-copy method
            return conn
                .send_bytes_zero_copy(message_bytes)
                .await
                .map_err(|_| SendError::ActorStopped);
        }

        Err(SendError::MissingConnection)
    }
}

/// A pending ask request to a distributed actor (zero-cost version).
///
/// This struct is monomorphized for each message type M and reply type R,
/// generating specialized code with no runtime overhead.
#[derive(Debug)]
pub struct DistributedAskRequest<'a, A: Actor, M, T = Box<super::kameo_transport::KameoTransport>> {
    actor_ref: &'a DistributedActorRef<A, T>,
    message: M,
    timeout: Option<Duration>,
    _message_type: PhantomData<M>,
}

impl<'a, A, M, T> DistributedAskRequest<'a, A, M, T>
where
    A: Actor + Message<M>,
    M: HasTypeHash
        + Send
        + 'static
        + Archive
        + for<'b> RSerialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'b>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    for<'b> <A as Message<M>>::Reply: Archive
        + RSerialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'b>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    for<'b> <<A as Message<M>>::Reply as Archive>::Archived: RDeserialize<
            <A as Message<M>>::Reply,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        > + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'b>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    T: RemoteTransport,
{
    /// Set a timeout for the ask operation
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Send the ask message and wait for reply with zero-cost abstraction.
    ///
    /// This method is fully monomorphized for message type M and reply type R,
    /// generating specialized code with no dynamic dispatch overhead.
    pub async fn send(self) -> Result<<A as Message<M>>::Reply, SendError> {
        // Get the raw bytes using the zero-cost implementation
        let reply_bytes = self.send_raw().await?;

        // Deserialize the reply using rkyv - monomorphized for reply type R
        let reply =
            match rkyv::from_bytes::<<A as Message<M>>::Reply, rkyv::rancor::Error>(&reply_bytes) {
                Ok(r) => r,
                Err(_e) => {
                    return Err(SendError::ActorStopped);
                }
            };

        Ok(reply)
    }

    /// Send the ask message and wait for reply - returns raw bytes for zero-copy access.
    ///
    /// This method is fully monomorphized and optimized for the specific message type M.
    pub async fn send_raw(self) -> Result<bytes::Bytes, SendError> {
        // Compile-time constant - no runtime lookup!
        let type_hash = M::TYPE_HASH.as_u32();

        // Optimized serialization - specialized per message type
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&self.message)
            .map_err(|_e| SendError::ActorStopped)?;

        // Default timeout if not specified
        let timeout = self.timeout.unwrap_or(Duration::from_secs(2));

        if let Some(conn) = self.actor_ref.connection.as_ref() {
            let threshold = conn.streaming_threshold();
            if payload.len() > threshold {
                let reply = conn
                    .ask_streaming_bytes(
                        bytes::Bytes::from(payload.into_vec()),
                        type_hash,
                        self.actor_ref.actor_id.into_u64(),
                        timeout,
                    )
                    .await
                    .map_err(|e| match e {
                        GossipError::Timeout => SendError::Timeout(None),
                        _ => SendError::ActorStopped,
                    })?;

                return Ok(reply);
            }

            let actor_message = RegistryMessage::ActorMessage {
                actor_id: self.actor_ref.actor_id.into_u64().to_string(),
                type_hash,
                payload: payload.to_vec(),
                correlation_id: None,
            };

            let serialized_msg = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_message)
                .map_err(|_e| SendError::ActorStopped)?;

            let reply = match tokio::time::timeout(timeout, conn.ask(&serialized_msg)).await {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(_)) => return Err(SendError::ActorStopped),
                Err(_) => return Err(SendError::Timeout(None)),
            };

            return Ok(Bytes::from(reply));
        }

        Err(SendError::MissingConnection)
    }
}
