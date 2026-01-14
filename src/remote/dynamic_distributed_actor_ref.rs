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

use super::transport::{RemoteActorLocation, RemoteTransport, TransportError};
use super::type_hash::HasTypeHash;
use kameo_remote::connection_pool::ConnectionHandle;

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
    /// The transport to use for communication
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

    /// Look up a distributed actor by name (without connection caching)
    ///
    /// WARNING: This method does not cache the connection, which means every tell/ask
    /// will fall back to the lock-based transport method. For better performance,
    /// use `lookup_with_connection_cache()` instead.
    #[deprecated(
        since = "0.17.3",
        note = "Use lookup_with_connection_cache() for better performance"
    )]
    pub async fn uncached_lookup(
        name: &str,
        transport: T,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(location) = transport.lookup_actor(name).await? {
            Ok(Some(Self::new(location.actor_id, location, transport)))
        } else {
            Ok(None)
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
            let connection = transport.get_connection_for_location(&location).await.ok();

            Ok(Some(Self {
                actor_id: location.actor_id,
                location,
                transport,
                connection,
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
            // Format: [length:4][type:1][correlation_id:2][reserved:5][actor_id:8][type_hash:4][payload_len:4][payload:N]

            let inner_size = 8 + 16 + payload.len(); // header + actor fields + payload
            let mut message = Vec::with_capacity(4 + inner_size);

            // Length prefix (4 bytes) - this is the size AFTER the length prefix
            message.extend_from_slice(&(inner_size as u32).to_be_bytes());

            // Header: [type:1][correlation_id:2][reserved:5]
            message.push(3u8); // MessageType::ActorTell
            message.extend_from_slice(&0u16.to_be_bytes()); // No correlation for tell
            message.extend_from_slice(&[0u8; 5]); // Reserved

            // Actor message: [actor_id:8][type_hash:4][payload_len:4][payload:N]
            message.extend_from_slice(&self.actor_ref.actor_id.into_u64().to_be_bytes());
            message.extend_from_slice(&type_hash.to_be_bytes());
            message.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            message.extend_from_slice(payload.as_slice());

            // Try to send using cached connection - direct ring buffer access!
            match conn.send_binary_message(&message).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(_e) => {
                    // Fall through to transport method
                }
            }
        }

        // Fallback: use transport's tell method (which will lock)
        self.actor_ref
            .transport
            .send_tell_typed(
                self.actor_ref.actor_id,
                &self.actor_ref.location,
                type_hash,
                Bytes::copy_from_slice(payload.as_slice()),
            )
            .await
            .map_err(|_| SendError::ActorStopped)?;

        Ok(())
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
        let timeout = self.timeout.unwrap_or(Duration::from_secs(2));

        let reply_bytes = if let Some(ref conn) = self.actor_ref.connection {
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
                match tokio::time::timeout(timeout, conn.ask(&message_bytes)).await {
                    Ok(Ok(reply)) => Bytes::from(reply),
                    Ok(Err(_e)) => {
                        // Fall through to transport method
                        self.actor_ref
                            .transport
                            .send_ask_typed(
                                self.actor_ref.actor_id,
                                &self.actor_ref.location,
                                type_hash,
                                Bytes::copy_from_slice(payload.as_slice()),
                                timeout,
                            )
                            .await
                            .map_err(|e| match e {
                                TransportError::Timeout => SendError::Timeout(None),
                                _ => SendError::ActorStopped,
                            })?
                    }
                    Err(_) => {
                        return Err(SendError::Timeout(None));
                    }
                }
            } else {
                // Serialization failed, use transport
                self.actor_ref
                    .transport
                    .send_ask_typed(
                        self.actor_ref.actor_id,
                        &self.actor_ref.location,
                        type_hash,
                        Bytes::copy_from_slice(payload.as_slice()),
                        timeout,
                    )
                    .await
                    .map_err(|e| match e {
                        TransportError::Timeout => SendError::Timeout(None),
                        _ => SendError::ActorStopped,
                    })?
            }
        } else {
            // No cached connection, use transport
            self.actor_ref
                .transport
                .send_ask_typed(
                    self.actor_ref.actor_id,
                    &self.actor_ref.location,
                    type_hash,
                    Bytes::copy_from_slice(payload.as_slice()),
                    timeout,
                )
                .await
                .map_err(|e| match e {
                    TransportError::Timeout => SendError::Timeout(None),
                    _ => SendError::ActorStopped,
                })?
        };

        Ok(reply_bytes)
    }
}
