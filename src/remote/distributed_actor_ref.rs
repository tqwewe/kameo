//! Zero-cost distributed actor reference for optimal remote messaging
//!
//! This module provides a DistributedActorRef that uses compile-time monomorphization
//! to eliminate all dynamic dispatch overhead while maintaining a simple, ergonomic API.
//! Each message type gets its own specialized code path for maximum performance.

use std::marker::PhantomData;
use std::time::Duration;

use bytes::Bytes;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

use crate::actor::{Actor, ActorId, ActorRef};
use crate::error::SendError;
use crate::message::Message;

use super::transport::{RemoteActorLocation, RemoteTransport};
use super::type_hash::HasTypeHash;
use crate::remote::remote_link;
use kameo_remote::{GossipError, connection_pool::ConnectionHandle};

use super::location_metadata;

type LocationMetadataV1 = location_metadata::LocationMetadataV1;

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
    /// The actor's registered name (if known)
    pub(crate) name: Option<String>,
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
            name: self.name.clone(),
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
            name: None,
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
            name: None,
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

    /// Get the actor's registered name (if known)
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[cfg(any(test, feature = "remote_integration_tests"))]
    #[doc(hidden)]
    pub fn __cached_connection_for_tests(&self) -> Option<ConnectionHandle> {
        self.connection.clone()
    }

    /// Link a local actor to this remote actor. The local actor receives
    /// `on_link_died` with `PeerDisconnected` when the peer socket disconnects.
    pub async fn link<B: Actor>(&self, local_actor: &ActorRef<B>) {
        if let Ok(meta) = location_metadata::decode_v1(&self.location.metadata) {
            remote_link::link_with_peer_id(
                &meta.peer_id,
                self.location.peer_addr,
                self.location.actor_id,
                local_actor.id(),
                local_actor.weak_signal_mailbox(),
            );
        } else {
            remote_link::link(
                self.location.peer_addr,
                self.location.actor_id,
                local_actor.id(),
                local_actor.weak_signal_mailbox(),
            );
        }
    }

    /// Blockingly link a local actor to this remote actor.
    pub fn blocking_link<B: Actor>(&self, local_actor: &ActorRef<B>) {
        if let Ok(meta) = location_metadata::decode_v1(&self.location.metadata) {
            remote_link::link_with_peer_id(
                &meta.peer_id,
                self.location.peer_addr,
                self.location.actor_id,
                local_actor.id(),
                local_actor.weak_signal_mailbox(),
            );
        } else {
            remote_link::link(
                self.location.peer_addr,
                self.location.actor_id,
                local_actor.id(),
                local_actor.weak_signal_mailbox(),
            );
        }
    }

    /// Unlink a local actor from this remote actor.
    pub async fn unlink<B: Actor>(&self, local_actor: &ActorRef<B>) {
        remote_link::unlink(
            self.location.peer_addr,
            self.location.actor_id,
            local_actor.id(),
        );
    }

    /// Blockingly unlink a local actor from this remote actor.
    pub fn blocking_unlink<B: Actor>(&self, local_actor: &ActorRef<B>) {
        remote_link::unlink(
            self.location.peer_addr,
            self.location.actor_id,
            local_actor.id(),
        );
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
            let connection = match transport
                .get_connection_for_location_fresh(&location, false)
                .await
            {
                Ok(conn) => conn,
                Err(_) => {
                    // Connection failed - actor exists in gossip but is not reachable
                    return Ok(None);
                }
            };

            if connection.is_closed() {
                // A closed handle from the pool must not abort lookup; treat as unreachable.
                return Ok(None);
            }
            let mut actor_ref = Self::new_with_connection(
                location.actor_id,
                location,
                transport,
                connection,
            );
            actor_ref.name = Some(name.to_string());
            Ok(Some(actor_ref))
        } else {
            Ok(None)
        }
    }

    /// Refresh this reference by re-looking up the actor by name.
    ///
    /// Returns `Ok(None)` if the name is unknown or the actor is not reachable.
    pub async fn refresh(&self) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_inner(false).await
    }

    /// Refresh and force a new underlying connection before returning the reference.
    ///
    /// Use this on error paths (timeouts, suspected zombie sockets) to avoid retry loops
    /// on a connection the OS still considers open.
    pub async fn refresh_force_new(
        &self,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_inner(true).await
    }

    async fn refresh_inner(
        &self,
        force_new: bool,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let name = match self.name.as_deref() {
            Some(name) => name,
            None => return Ok(None),
        };
        let transport = self.transport.clone();
        // Prefer fresh lookup by name, but fall back to the last known location. This keeps
        // refresh robust when gossip temporarily suppresses a peer during reconnection.
        let location = transport
            .lookup_actor(name)
            .await?
            .unwrap_or_else(|| self.location.clone());

        let connection = match transport
            .get_connection_for_location_fresh(&location, force_new)
            .await
        {
            Ok(conn) => conn,
            Err(_) => {
                return Ok(None);
            }
        };

        if connection.is_closed() {
            return Ok(None);
        }
        let mut actor_ref = Self::new_with_connection(location.actor_id, location, transport, connection);
        actor_ref.name = Some(name.to_string());
        Ok(Some(actor_ref))
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
                    .stream_large_message(
                        payload.as_ref(),
                        type_hash,
                        self.actor_ref.actor_id.into_u64(),
                    )
                    .await
                    .map_err(|e| match e {
                        GossipError::ConnectionClosed(_) | GossipError::Shutdown => {
                            SendError::ConnectionClosed
                        }
                        _ => SendError::ActorStopped,
                    });
            }

            let payload_bytes = Bytes::from(payload.into_vec());

            return conn
                .tell_actor_frame(self.actor_ref.actor_id.into_u64(), type_hash, payload_bytes)
                .await
                .map_err(|e| match e {
                    GossipError::ConnectionClosed(_) | GossipError::Shutdown => {
                        SendError::ConnectionClosed
                    }
                    _ => SendError::ActorStopped,
                });
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
        let default_timeout = Duration::from_secs(2);

        if let Some(conn) = self.actor_ref.connection.as_ref() {
            let threshold = conn.streaming_threshold();
            let payload_len = payload.len();

            if payload_len > threshold {
                // For streaming, scale timeout based on payload size if not explicitly set.
                // Use at least 30s base + 1s per MB to handle large payloads over the network.
                let streaming_timeout = self.timeout.unwrap_or_else(|| {
                    let payload_mb = payload_len as u64 / (1024 * 1024);
                    Duration::from_secs(30 + payload_mb)
                });

                // NOTE: One O(n) copy occurs here. rkyv's AlignedVec uses a 16-byte aligned
                // custom allocator that cannot transfer ownership to Vec's global allocator.
                // Both into_vec() and copy_from_slice() require this copy - it's unavoidable
                // with rkyv 0.8. After this point, Bytes enables zero-copy chunking via slice().
                let reply = conn
                    .ask_streaming_bytes(
                        Bytes::from(payload.into_vec()),
                        type_hash,
                        self.actor_ref.actor_id.into_u64(),
                        streaming_timeout,
                    )
                    .await
                    .map_err(|e| match e {
                        GossipError::Timeout => SendError::Timeout(None),
                        GossipError::ConnectionClosed(_) | GossipError::Shutdown => {
                            SendError::ConnectionClosed
                        }
                        _ => SendError::ActorStopped,
                    })?;

                return Ok(reply);
            }

            let payload_bytes = Bytes::from(payload.into_vec());
            let timeout = self.timeout.unwrap_or(default_timeout);

            let reply = conn
                .ask_actor_frame(
                    self.actor_ref.actor_id.into_u64(),
                    type_hash,
                    payload_bytes,
                    timeout,
                )
                .await
                .map_err(|e| match e {
                    GossipError::Timeout => SendError::Timeout(None),
                    GossipError::ConnectionClosed(_) | GossipError::Shutdown => {
                        SendError::ConnectionClosed
                    }
                    _ => SendError::ActorStopped,
                })?;

            return Ok(reply);
        }

        Err(SendError::MissingConnection)
    }
}
