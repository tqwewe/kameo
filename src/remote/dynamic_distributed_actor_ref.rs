//! Dynamic remote actor reference for distributed actors
//!
//! This module provides a DynamicDistributedActorRef that works without requiring
//! the actor type to implement Message<M>, enabling clients to send messages
//! to remote actors without defining the full distributed_actor! macro.

use std::marker::PhantomData;
use std::time::Duration;

use bytes::Bytes;
use rkyv::{Archive, Serialize as RSerialize};

use crate::actor::{Actor, ActorId, ActorRef};
use crate::error::SendError;

use super::transport::{RemoteActorLocation, RemoteTransport};
use super::type_hash::HasTypeHash;
use crate::remote::remote_link;

use super::location_metadata;

type LocationMetadataV1 = location_metadata::LocationMetadataV1;
use kameo_remote::{GossipError, connection_pool::ConnectionHandle};

/// A reference to a remote distributed actor (dynamic version)
///
/// Unlike DistributedActorRef, this doesn't require A: Message<M> constraints,
/// allowing clients to send messages without defining the actor locally.
#[derive(Debug)]
pub struct DynamicDistributedActorRef<T = Box<super::kameo_transport::KameoTransport>> {
    /// The actor's ID
    pub(crate) actor_id: ActorId,
    /// The actor's location
    pub(crate) location: RemoteActorLocation,
    /// The actor's registered name (if known)
    pub(crate) name: Option<String>,
    /// The transport to use for communication (stored for future use)
    #[allow(dead_code)]
    pub(crate) transport: T,
    /// Cached connection handle for lock-free access (only for KameoTransport)
    pub(crate) connection: Option<ConnectionHandle>,
}

impl<T> DynamicDistributedActorRef<T>
where
    T: RemoteTransport,
{
    /// Create a new dynamic distributed actor reference
    pub fn new(actor_id: ActorId, location: RemoteActorLocation, transport: T) -> Self {
        Self {
            actor_id,
            location,
            name: None,
            transport,
            connection: None,
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

    /// Send a tell message to the remote actor
    ///
    /// Note: This uses dynamic dispatch - no actor type constraints required
    pub fn tell<M>(&self, message: M) -> DynamicTellRequest<'_, M, T>
    where
        M: HasTypeHash + Send + 'static,
    {
        DynamicTellRequest {
            actor_ref: self,
            message,
            timeout: None,
        }
    }

    /// Send an ask message to the remote actor
    ///
    /// Note: This uses dynamic dispatch - no actor type constraints required
    pub fn ask<M, R>(&self, message: M) -> DynamicAskRequest<'_, M, R, T>
    where
        M: HasTypeHash + Send + 'static,
    {
        DynamicAskRequest {
            actor_ref: self,
            message,
            timeout: None,
            _phantom_reply: PhantomData,
        }
    }
}

// Specialized implementation for KameoTransport to cache connections
impl DynamicDistributedActorRef<Box<super::kameo_transport::KameoTransport>> {
    /// Look up a distributed actor by name (with connection caching for optimal performance)
    pub async fn lookup(
        name: &str,
        transport: Box<super::kameo_transport::KameoTransport>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(location) = transport.lookup_actor(name).await? {
            // Try to get the connection for caching
            let connection = match transport
                .get_connection_for_location_fresh(&location, false)
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
            Ok(Some(Self {
                actor_id: location.actor_id,
                location,
                name: Some(name.to_string()),
                transport,
                connection: Some(connection),
            }))
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
        let location = transport
            .lookup_actor(name)
            .await?
            .unwrap_or_else(|| self.location.clone());

        let connection = match transport
            .get_connection_for_location_fresh(&location, force_new)
            .await
        {
            Ok(conn) => conn,
            Err(_) => return Ok(None),
        };

        if connection.is_closed() {
            return Ok(None);
        }
        Ok(Some(Self {
            actor_id: location.actor_id,
            location,
            name: Some(name.to_string()),
            transport,
            connection: Some(connection),
        }))
    }
}

/// A pending tell request to a distributed actor (dynamic version)
#[derive(Debug)]
pub struct DynamicTellRequest<'a, M, T = Box<super::kameo_transport::KameoTransport>> {
    actor_ref: &'a DynamicDistributedActorRef<T>,
    message: M,
    timeout: Option<Duration>,
}

impl<'a, M, T> DynamicTellRequest<'a, M, T>
where
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

    /// Send the tell message
    pub async fn send(self) -> Result<(), SendError> {
        // Serialize the message with rkyv
        let message_ref = &self.message;
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(message_ref)
            .map_err(|_e| SendError::ActorStopped)?;

        // Get the message type hash
        let type_hash = M::TYPE_HASH.as_u32();

        // Try to use cached connection first if available
        if let Some(ref conn) = self.actor_ref.connection {
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

/// A pending ask request to a distributed actor (dynamic version)
#[derive(Debug)]
pub struct DynamicAskRequest<'a, M, R, T = Box<super::kameo_transport::KameoTransport>> {
    actor_ref: &'a DynamicDistributedActorRef<T>,
    message: M,
    timeout: Option<Duration>,
    _phantom_reply: PhantomData<R>,
}

impl<'a, M, R, T> DynamicAskRequest<'a, M, R, T>
where
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
    R: Archive
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
    <R as Archive>::Archived: for<'b> rkyv::Deserialize<R, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>
        + for<'b> rkyv::bytecheck::CheckBytes<
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

    /// Send the ask message and wait for reply
    pub async fn send(self) -> Result<R, SendError> {
        // Get the raw bytes
        let reply_bytes = self.send_raw().await?;

        // Deserialize the reply using rkyv
        let reply = match rkyv::from_bytes::<R, rkyv::rancor::Error>(&reply_bytes) {
            Ok(r) => r,
            Err(_e) => {
                return Err(SendError::ActorStopped);
            }
        };

        Ok(reply)
    }

    /// Send the ask message and wait for reply - returns raw bytes for zero-copy access
    pub async fn send_raw(self) -> Result<bytes::Bytes, SendError> {
        // Serialize the message with rkyv
        let message_ref = &self.message;
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(message_ref)
            .map_err(|_e| SendError::ActorStopped)?;

        // Get the message type hash
        let type_hash = M::TYPE_HASH.as_u32();

        // Default timeout if not specified
        let default_timeout = Duration::from_secs(2);

        let reply_bytes = if let Some(ref conn) = self.actor_ref.connection {
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
            match conn
                .ask_actor_frame(
                    self.actor_ref.actor_id.into_u64(),
                    type_hash,
                    payload_bytes,
                    timeout,
                )
                .await
            {
                Ok(reply) => reply,
                Err(kameo_remote::GossipError::Timeout) => return Err(SendError::Timeout(None)),
                Err(kameo_remote::GossipError::ConnectionClosed(_))
                | Err(kameo_remote::GossipError::Shutdown) => {
                    return Err(SendError::ConnectionClosed)
                }
                Err(_e) => return Err(SendError::ActorStopped),
            }
        } else {
            return Err(SendError::MissingConnection);
        };

        Ok(reply_bytes)
    }
}
