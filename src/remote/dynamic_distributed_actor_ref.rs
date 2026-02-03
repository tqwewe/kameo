//! Dynamic remote actor reference for distributed actors
//!
//! This module provides a DynamicDistributedActorRef that works without requiring
//! the actor type to implement Message<M>, enabling clients to send messages
//! to remote actors without defining the full distributed_actor! macro.

use std::marker::PhantomData;
use std::time::Duration;

use bytes::Bytes;
use rkyv::{Archive, Serialize as RSerialize};

use crate::actor::ActorId;
use crate::error::SendError;

use super::transport::{RemoteActorLocation, RemoteTransport};
use super::type_hash::HasTypeHash;
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
            let connection = match transport.get_connection_for_location(&location).await {
                Ok(conn) => conn,
                Err(_) => {
                    return Ok(None);
                }
            };

            Ok(Some(Self {
                actor_id: location.actor_id,
                location,
                transport,
                connection: Some(connection),
            }))
        } else {
            Ok(None)
        }
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
            // Direct binary protocol - no wrapper object, no double serialization!
            // Format: [length:4][type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]

            let inner_size = 12 + 16 + payload.len(); // header (type+corr+reserved=12) + actor fields + payload
            let mut message = Vec::with_capacity(4 + inner_size);

            // Length prefix (4 bytes) - this is the size AFTER the length prefix
            message.extend_from_slice(&(inner_size as u32).to_be_bytes());

            // Header: [type:1][correlation_id:2][reserved:9]
            message.push(3u8); // MessageType::ActorTell
            message.extend_from_slice(&0u16.to_be_bytes()); // No correlation for tell
            message.extend_from_slice(&[0u8; 9]); // 9 reserved bytes for 32-byte alignment

            // Actor message: [actor_id:8][type_hash:4][payload_len:4][payload:N]
            message.extend_from_slice(&self.actor_ref.actor_id.into_u64().to_be_bytes());
            message.extend_from_slice(&type_hash.to_be_bytes());
            message.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            message.extend_from_slice(payload.as_slice());

            // Try to send using cached connection - direct ring buffer access!
            return conn
                .send_binary_message(&message)
                .await
                .map_err(|_| SendError::ActorStopped);
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
            if payload.len() > threshold {
                // For streaming, scale timeout based on payload size if not explicitly set.
                // Use at least 30s base + 1s per MB to handle large payloads over the network.
                let streaming_timeout = self.timeout.unwrap_or_else(|| {
                    let payload_mb = payload.len() as u64 / (1024 * 1024);
                    Duration::from_secs(30 + payload_mb)
                });

                // NOTE: One O(n) copy occurs here. rkyv's AlignedVec uses a 16-byte aligned
                // custom allocator that cannot transfer ownership to Vec's global allocator.
                // Both into_vec() and copy_from_slice() require this copy - it's unavoidable
                // with rkyv 0.8. After this point, Bytes enables zero-copy chunking via slice().
                let reply = conn
                    .ask_streaming_bytes(
                        bytes::Bytes::from(payload.into_vec()),
                        type_hash,
                        self.actor_ref.actor_id.into_u64(),
                        streaming_timeout,
                    )
                    .await
                    .map_err(|e| match e {
                        GossipError::Timeout => SendError::Timeout(None),
                        _ => SendError::ActorStopped,
                    })?;
                return Ok(reply);
            }

            // Try to use cached connection first - no locks!
            let actor_message = kameo_remote::registry::RegistryMessage::ActorMessage {
                actor_id: self.actor_ref.actor_id.into_u64().to_string(),
                type_hash,
                payload: payload.as_slice().into(),
                correlation_id: None, // conn.ask() will handle correlation ID
            };

            // Serialize using rkyv
            if let Ok(message_bytes) = rkyv::to_bytes::<rkyv::rancor::Error>(&actor_message) {
                // Try to send using cached connection - direct ring buffer access!
                let timeout = self.timeout.unwrap_or(default_timeout);
                match tokio::time::timeout(timeout, conn.ask(&message_bytes)).await {
                    Ok(Ok(reply)) => Bytes::from(reply),
                    Ok(Err(_e)) => return Err(SendError::ActorStopped),
                    Err(_) => return Err(SendError::Timeout(None)),
                }
            } else {
                return Err(SendError::ActorStopped);
            }
        } else {
            return Err(SendError::MissingConnection);
        };

        Ok(reply_bytes)
    }
}
