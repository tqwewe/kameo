//! Pluggable codec traits for remote message serialization.
//!
//! These traits replace the hardcoded `serde` + `rmp-serde` bounds previously
//! used for encoding/decoding actor messages over the network.  Consumer crates
//! choose a concrete codec via feature flags:
//!
//! - `rkyv-codec`  — blanket impls via `rkyv`
//! - `serde-codec` — blanket impls via `rmp-serde` (backward-compatible)

/// Encode a value into bytes for remote transmission.
pub trait RemoteEncode: Send + 'static {
    /// Serialize this value into a byte vector for remote transmission.
    fn remote_encode(&self) -> Result<Vec<u8>, RemoteCodecError>;
}

/// Decode bytes received from the network into a value.
pub trait RemoteDecode: Sized + Send + 'static {
    /// Deserialize a value from the given byte slice.
    fn remote_decode(bytes: &[u8]) -> Result<Self, RemoteCodecError>;
}

/// Codec error type.
#[derive(Debug)]
pub struct RemoteCodecError(pub String);

impl core::fmt::Display for RemoteCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "remote codec error: {}", self.0)
    }
}

impl std::error::Error for RemoteCodecError {}

// ---------------------------------------------------------------------------
// rkyv blanket impls (behind `rkyv-codec` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "rkyv-codec")]
impl<T> RemoteEncode for T
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        > + Send
        + 'static,
{
    fn remote_encode(&self) -> Result<Vec<u8>, RemoteCodecError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|v| v.to_vec())
            .map_err(|e| RemoteCodecError(e.to_string()))
    }
}

#[cfg(feature = "rkyv-codec")]
impl<T> RemoteDecode for T
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    fn remote_decode(bytes: &[u8]) -> Result<Self, RemoteCodecError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
            .map_err(|e| RemoteCodecError(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// serde blanket impls (behind `serde-codec` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "serde-codec")]
impl<T> RemoteEncode for T
where
    T: serde::Serialize + Send + 'static,
{
    fn remote_encode(&self) -> Result<Vec<u8>, RemoteCodecError> {
        rmp_serde::to_vec_named(self).map_err(|e| RemoteCodecError(e.to_string()))
    }
}

#[cfg(feature = "serde-codec")]
impl<T> RemoteDecode for T
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    fn remote_decode(bytes: &[u8]) -> Result<Self, RemoteCodecError> {
        rmp_serde::decode::from_slice(bytes).map_err(|e| RemoteCodecError(e.to_string()))
    }
}
