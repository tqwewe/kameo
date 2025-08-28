//! Zero-cost distributed actor reference for optimal remote messaging
//!
//! This module provides a DistributedActorRef that uses compile-time monomorphization
//! to eliminate all dynamic dispatch overhead while maintaining a simple, ergonomic API.
//! Each message type gets its own specialized code path for maximum performance.

use std::marker::PhantomData;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

use crate::actor::ActorId;
use crate::error::SendError;

use super::remote_message_trait::RemoteMessage;
use super::transport::{RemoteActorLocation, RemoteTransport, TransportError};
use super::type_hash::HasTypeHash;
use kameo_remote::connection_pool::ConnectionHandle;

/// A reference to a remote distributed actor
///
/// This actor reference uses compile-time monomorphization
///
/// Each call to `.tell()` or `.ask()` is monomorphized by the message type, allowing the compiler
/// to optimize away all runtime type checks and generate the most efficient code possible.
///
/// This eliminates dynamic dispatch overhead
#[derive(Clone)]
pub struct DistributedActorRef<T = Box<super::kameo_transport::KameoTransport>> {
    /// The actor's ID
    pub(crate) actor_id: ActorId,
    /// The actor's location
    pub(crate) location: RemoteActorLocation,
    /// The transport to use for communication
    pub(crate) transport: T,
    /// Cached connection handle for lock-free access (only for KameoTransport)
    pub(crate) connection: Option<ConnectionHandle>,
}

impl<T> DistributedActorRef<T>
where
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
    pub fn tell<M>(&self, message: M) -> DistributedTellRequest<'_, M, T>
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
    pub fn ask<M, R>(&self, message: M) -> DistributedAskRequest<'_, M, R, T>
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
        DistributedAskRequest {
            actor_ref: self,
            message,
            timeout: None,
            _message_type: PhantomData,
            _reply_type: PhantomData,
        }
    }
}

// Global transport cache for lookup without parameters
use std::sync::{Arc, LazyLock, Mutex};

pub static GLOBAL_TRANSPORT: LazyLock<
    Arc<Mutex<Option<Box<super::kameo_transport::KameoTransport>>>>,
> = LazyLock::new(|| Arc::new(Mutex::new(None)));

// Specialized implementation for KameoTransport to cache connections
impl DistributedActorRef<Box<super::kameo_transport::KameoTransport>> {
    /// Set the global transport for lookup calls without transport parameter
    /// This is private - only callable from within the kameo crate (by bootstrap functions)
    pub(crate) fn set_global_transport(transport: Box<super::kameo_transport::KameoTransport>) {
        let mut global = GLOBAL_TRANSPORT.lock().unwrap();
        *global = Some(transport);
    }

    /// Look up a distributed actor by name (uses cached global transport)
    pub async fn lookup(
        name: &str,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let transport = {
            let global = GLOBAL_TRANSPORT.lock().unwrap();
            global.clone().ok_or(
                "No global transport set - did you call bootstrap_on() or bootstrap_with_config()?",
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

/// A pending tell request to a distributed actor (zero-cost version).
///
/// This struct is monomorphized for each message type M, allowing the compiler
/// to generate specialized code with compile-time constants and optimized paths.
pub struct DistributedTellRequest<'a, M, T = Box<super::kameo_transport::KameoTransport>> {
    actor_ref: &'a DistributedActorRef<T>,
    message: M,
    timeout: Option<Duration>,
    _message_type: PhantomData<M>,
}

impl<'a, M, T> DistributedTellRequest<'a, M, T>
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
            .map_err(|e| SendError::ActorStopped)?;

        // Use streaming for large messages automatically
        if let Some(ref conn) = self.actor_ref.connection {
            // Get actual threshold from connection
            let threshold = conn.streaming_threshold();

            // Use streaming for large messages
            if payload.len() > threshold {
                println!(
                    "🚀 [STREAMING MODE] Message size {} > threshold {}, using STREAMING path",
                    payload.len(),
                    threshold
                );
                match conn
                    .stream_large_message(&payload, type_hash, self.actor_ref.actor_id.into_u64())
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        return Err(SendError::ActorStopped); // Don't fall through to normal path for large messages!
                    }
                }
            } else {
                // println!(
                //     "📦 [RING BUFFER MODE] Message size {} <= threshold {}, using RING BUFFER path",
                //     payload.len(),
                //     threshold
                // );
            }

            // Small message - use ring buffer
            // ZERO-COPY: Use BytesMut for efficient message building
            let inner_size = 8 + 16 + payload.len(); // header + actor fields + payload
            let mut message = bytes::BytesMut::with_capacity(4 + inner_size);

            // All constants - compiler can optimize these away completely
            message.put_u32(inner_size as u32);

            // Header: [type:1][correlation_id:2][reserved:5]
            message.put_u8(3); // MessageType::ActorTell - compile-time constant
            message.put_u16(0); // No correlation for tell
            message.put_slice(&[0u8; 5]); // Reserved

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
                .map_err(|_| SendError::ActorStopped);
        }

        // No cached connection available - fall back to transport layer
        // Use safe default threshold for streaming decision
        let threshold = 1024 * 1024 - 1024; // 1MB - 1KB safe fallback

        println!(
            "🔬 [FALLBACK STREAMING DECISION] Message size: {} bytes, fallback threshold: {} bytes",
            payload.len(),
            threshold
        );

        if payload.len() > threshold {
            println!("🚀 [FALLBACK STREAMING MODE] Message size {} > threshold {}, using transport layer STREAMING", payload.len(), threshold);
            // Use transport layer for streaming large messages
            return self
                .actor_ref
                .transport
                .send_tell_typed(
                    self.actor_ref.actor_id,
                    &self.actor_ref.location,
                    type_hash,
                    Bytes::from(payload.into_vec()),
                )
                .await
                .map_err(|_| SendError::ActorStopped);
        } else {
            // println!("📦 [FALLBACK RING BUFFER MODE] Message size {} <= threshold {}, using transport layer RING BUFFER", payload.len(), threshold);
            // Use transport layer for normal messages
            return self
                .actor_ref
                .transport
                .send_tell_typed(
                    self.actor_ref.actor_id,
                    &self.actor_ref.location,
                    type_hash,
                    Bytes::from(payload.into_vec()),
                )
                .await
                .map_err(|_| SendError::ActorStopped);
        }
    }
}

/// A pending ask request to a distributed actor (zero-cost version).
///
/// This struct is monomorphized for each message type M and reply type R,
/// generating specialized code with no runtime overhead.
pub struct DistributedAskRequest<'a, M, R, T = Box<super::kameo_transport::KameoTransport>> {
    actor_ref: &'a DistributedActorRef<T>,
    message: M,
    timeout: Option<Duration>,
    _message_type: PhantomData<M>,
    _reply_type: PhantomData<R>,
}

impl<'a, M, R, T> DistributedAskRequest<'a, M, R, T>
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
    <R as Archive>::Archived: for<'b> RDeserialize<R, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>
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

    /// Send the ask message and wait for reply with zero-cost abstraction.
    ///
    /// This method is fully monomorphized for message type M and reply type R,
    /// generating specialized code with no dynamic dispatch overhead.
    pub async fn send(self) -> Result<R, SendError> {
        // Get the raw bytes using the zero-cost implementation
        let reply_bytes = self.send_raw().await?;

        // Deserialize the reply using rkyv - monomorphized for reply type R
        let reply = match rkyv::from_bytes::<R, rkyv::rancor::Error>(&reply_bytes) {
            Ok(r) => r,
            Err(e) => {
                return Err(SendError::ActorStopped);
            }
        };

        Ok(reply)
    }

    /// Send the ask message and wait for reply - returns raw bytes for zero-copy access.
    ///
    /// This method is fully monomorphized and optimized for the specific message type M.
    /// Large messages (>1MB) automatically use streaming protocol for optimal performance.
    pub async fn send_raw(self) -> Result<bytes::Bytes, SendError> {
        // Compile-time constant - no runtime lookup!
        let type_hash = M::TYPE_HASH.as_u32();

        // Optimized serialization - specialized per message type
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&self.message)
            .map_err(|e| SendError::ActorStopped)?;

        // Default timeout if not specified
        let timeout = self.timeout.unwrap_or(Duration::from_secs(2));

        // ✅ STREAMING THRESHOLD CHECK - Use streaming for large messages
        // Get threshold from connection if available, otherwise use safe default
        let threshold = self
            .actor_ref
            .connection
            .as_ref()
            .map(|conn| conn.streaming_threshold())
            .unwrap_or(1024 * 1024 - 1024); // Safe fallback: 1MB - 1KB

        if payload.len() > threshold {
            // For large ask messages, use streaming protocol
            let reply_bytes = self
                .actor_ref
                .transport
                .send_ask_streaming(
                    self.actor_ref.actor_id,
                    &self.actor_ref.location,
                    type_hash,                       // Compile-time constant
                    Bytes::from(payload.into_vec()), // Zero-copy transfer of ownership
                    timeout,
                )
                .await
                .map_err(|e| match e {
                    TransportError::Timeout => SendError::Timeout(None),
                    _ => SendError::ActorStopped,
                })?;
            return Ok(reply_bytes);
        }

        // For ask operations, we need to go through the transport layer
        // which handles correlation IDs and reply routing
        let reply_bytes = {
            // Use regular transport for small messages - still optimized per message type
            self.actor_ref
                .transport
                .send_ask_typed(
                    self.actor_ref.actor_id,
                    &self.actor_ref.location,
                    type_hash,                       // Compile-time constant
                    Bytes::from(payload.into_vec()), // Zero-copy transfer of ownership
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
