//! Zero-cost distributed actor reference for optimal remote messaging
//!
//! This module provides a DistributedActorRef that uses compile-time monomorphization
//! to eliminate all dynamic dispatch overhead while maintaining a simple, ergonomic API.
//! Each message type gets its own specialized code path for maximum performance.

use std::marker::PhantomData;
use std::time::Duration;

use bytes::{Bytes, BytesMut, BufMut};
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

use crate::actor::ActorId;
use crate::error::SendError;

use super::remote_message_trait::RemoteMessage;
use super::transport::{RemoteActorLocation, RemoteTransport, TransportError};
use super::type_hash::HasTypeHash;
use kameo_remote::connection_pool::ConnectionHandle;

// Constants for streaming - must match kameo_remote
const STREAM_THRESHOLD: usize = 1024 * 1024 - 1024; // 1MB - 1KB to account for serialization overhead

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
        connection: kameo_remote::connection_pool::ConnectionHandle
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
        M: HasTypeHash + Send + 'static + Archive + for<'a> RSerialize<
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
        M: HasTypeHash + Send + 'static + Archive + for<'a> RSerialize<
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
use std::sync::{Arc, Mutex, LazyLock};

static GLOBAL_TRANSPORT: LazyLock<Arc<Mutex<Option<Box<super::kameo_transport::KameoTransport>>>>> = 
    LazyLock::new(|| Arc::new(Mutex::new(None)));

// Specialized implementation for KameoTransport to cache connections
impl DistributedActorRef<Box<super::kameo_transport::KameoTransport>> {
    /// Set the global transport for lookup calls without transport parameter
    /// This is private - only callable from within the kameo crate (by bootstrap functions)
    pub(crate) fn set_global_transport(transport: Box<super::kameo_transport::KameoTransport>) {
        let mut global = GLOBAL_TRANSPORT.lock().unwrap();
        *global = Some(transport);
    }
    
    /// Look up a distributed actor by name (uses cached global transport)
    pub async fn lookup(name: &str) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let transport = {
            let global = GLOBAL_TRANSPORT.lock().unwrap();
            global.clone().ok_or("No global transport set - did you call bootstrap_on() or bootstrap_with_config()?")?
        };
        if let Some(location) = transport.lookup_actor(name).await? {
            // MUST get a cached connection for zero-cost abstraction
            let connection = transport.get_connection_for_location(&location).await
                .map_err(|e| format!("Zero-cost abstraction requires cached connection but failed to get connection: {}", e))?;

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
    M: HasTypeHash + Send + 'static + Archive + for<'b> RSerialize<
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
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&self.message).map_err(|e| {
            SendError::ActorStopped
        })?;

        // Use streaming for large messages automatically
        if payload.len() > STREAM_THRESHOLD {
            if let Some(ref conn) = self.actor_ref.connection {
                match conn.stream_large_message(&payload, type_hash, self.actor_ref.actor_id.into_u64()).await {
                    Ok(_) => {
                        return Ok(())
                    },
                    Err(e) => {
                        return Err(SendError::ActorStopped); // Don't fall through to normal path for large messages!
                    }
                }
            } else {
                return Err(SendError::ActorStopped); // Don't attempt normal path for large messages
            }
        }

        // Try to use cached connection first if available - zero overhead path
        if let Some(ref conn) = self.actor_ref.connection {
            // ZERO-COPY: Use BytesMut for efficient message building
            let inner_size = 8 + 16 + payload.len(); // header + actor fields + payload
            let mut message = bytes::BytesMut::with_capacity(4 + inner_size);

            // All constants - compiler can optimize these away completely
            message.put_u32(inner_size as u32);
            
            // Header: [type:1][correlation_id:2][reserved:5]
            message.put_u8(3); // MessageType::ActorTell - compile-time constant
            eprintln!("[CLIENT SEND] Writing ActorTell header: type=3, size={}, first bytes: {:02x} {:02x} {:02x} {:02x}", 
                     inner_size, 3u8, 0u8, 0u8, 0u8);
            message.put_u16(0); // No correlation for tell
            message.put_slice(&[0u8; 5]); // Reserved

            // Actor message: [actor_id:8][type_hash:4][payload_len:4][payload:N]
            message.put_u64(self.actor_ref.actor_id.into_u64());
            message.put_u32(type_hash); // Compile-time constant!
            message.put_u32(payload.len() as u32);
            message.put_slice(payload.as_slice());

            // ZERO-COPY: Convert to Bytes and write directly without copy
            let message_bytes = message.freeze();
            
            if payload.len() > STREAM_THRESHOLD {
                eprintln!("📤 CLIENT: Sending LARGE message: {} bytes total, {} payload", message_bytes.len(), payload.len());
            }
            
            // Use the ConnectionHandle's zero-copy method
            eprintln!("[TELL DEBUG] Sending {} bytes via send_bytes_zero_copy", message_bytes.len());
            eprintln!("[TELL DEBUG] Message type hash: {:08x}, Actor ID: {:?}", type_hash, self.actor_ref.actor_id);
            let result = conn.send_bytes_zero_copy(message_bytes).map_err(|e| {
                eprintln!("[TELL DEBUG] send_bytes_zero_copy failed: {:?}", e);
                SendError::ActorStopped
            });
            eprintln!("[TELL DEBUG] send_bytes_zero_copy returned: {:?}", result);
            return result;
        }

        // Zero-cost abstraction requires cached connection - this should never happen
        panic!("Zero-cost abstraction failed: no cached connection available. Now this shouldn't happen...");
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
    M: HasTypeHash + Send + 'static + Archive + for<'b> RSerialize<
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
    pub async fn send_raw(self) -> Result<bytes::Bytes, SendError> {
        // Compile-time constant - no runtime lookup!
        let type_hash = M::TYPE_HASH.as_u32();
        
        // Optimized serialization - specialized per message type
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&self.message).map_err(|e| {
            SendError::ActorStopped
        })?;

        // Default timeout if not specified
        let timeout = self.timeout.unwrap_or(Duration::from_secs(2));

        // For ask operations, we need to go through the transport layer
        // which handles correlation IDs and reply routing
        let reply_bytes = {
            // No cached connection, use transport - still optimized per message type
            self
                .actor_ref
                .transport
                .send_ask_typed(
                    self.actor_ref.actor_id,
                    &self.actor_ref.location,
                    type_hash, // Compile-time constant
                    Bytes::copy_from_slice(payload.as_slice()),
                    timeout,
                )
                .await
                .map_err(|e| {
                    match e {
                        TransportError::Timeout => SendError::Timeout(None),
                        _ => SendError::ActorStopped,
                    }
                })?
        };

        Ok(reply_bytes)
    }
}