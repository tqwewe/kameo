//! Zero-cost RemoteMessage trait for optimized distributed actor messaging
//!
//! This module provides the RemoteMessage trait that enables compile-time optimization
//! of remote message sending, eliminating all dynamic dispatch overhead.

use bytes::Bytes;
use rkyv::{Archive, Serialize as RSerialize};

use super::type_hash::{HasTypeHash, TypeHash};

/// Trait for messages that can be sent to remote actors with zero-cost abstractions.
///
/// This trait provides compile-time constants and optimized serialization methods
/// that allow the compiler to generate specialized, high-performance code for each
/// message type while maintaining a simple, ergonomic API.
///
/// # Example
///
/// ```ignore
/// use kameo::RemoteMessage;
/// use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
///
/// #[derive(RemoteMessage, Archive, RSerialize, RDeserialize)]
/// struct LogMessage {
///     level: String,
///     content: String,
/// }
/// ```
pub trait RemoteMessage:
    HasTypeHash
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
    >
{
    /// Compile-time constant: the type hash as a u32
    const TYPE_HASH_U32: u32 = Self::TYPE_HASH.as_u32();

    /// Compile-time constant: the type name for debugging
    const TYPE_NAME: &'static str;

    /// Optional hint about the typical serialized size for pre-allocation optimization
    const SERIALIZED_SIZE_HINT: Option<usize> = None;

    /// Optimized serialization method that can be specialized per message type.
    ///
    /// The default implementation uses standard rkyv serialization, but message types
    /// can override this for custom optimization strategies.
    fn serialize_optimized(&self) -> Result<rkyv::util::AlignedVec, rkyv::rancor::Error>
    where
        Self: Sized,
    {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
    }

    /// Convert the serialized data to Bytes efficiently.
    ///
    /// This can be overridden for message types that want to use custom
    /// memory management or zero-copy strategies.
    fn to_bytes_optimized(&self) -> Result<Bytes, rkyv::rancor::Error>
    where
        Self: Sized,
    {
        let vec = self.serialize_optimized()?;
        Ok(Bytes::copy_from_slice(vec.as_slice()))
    }
}

/// Helper trait to provide compile-time message type information
pub trait MessageTypeInfo {
    /// Get the compile-time type hash
    const TYPE_HASH: TypeHash;

    /// Get the type name for debugging
    const TYPE_NAME: &'static str;
}
