//! Pluggable codec traits for remote message serialization.
//!
//! The [`Encode`] and [`Decode`] traits abstract over the serialization
//! framework used for actor messages on the wire.  A blanket implementation
//! is provided behind the `serde-codec` feature flag so that consumer types
//! only need to derive `serde::Serialize` / `serde::Deserialize`.
//!
//! Downstream crates can provide their own blanket or manual implementations
//! for alternative serialization frameworks.

use std::{error, fmt};

// ---------------------------------------------------------------------------
// CodecError
// ---------------------------------------------------------------------------

/// An error that occurs during codec encode/decode operations.
///
/// Wraps an [`anyhow::Error`] for flexible error propagation while
/// implementing the standard [`Error`](error::Error) trait.
pub struct CodecError(anyhow::Error);

impl CodecError {
    /// Create a new codec error from any displayable value.
    ///
    /// This uses [`anyhow::Error::msg`] internally, which accepts any type
    /// implementing `Display + Debug + Send + Sync + 'static`.  Notably this
    /// includes `rkyv::rancor::Error` which does *not* implement
    /// `std::error::Error`.
    pub fn new(err: impl fmt::Display + fmt::Debug + Send + Sync + 'static) -> Self {
        CodecError(anyhow::Error::msg(err))
    }

    /// Consumes the error and returns the inner [`anyhow::Error`].
    pub fn into_inner(self) -> anyhow::Error {
        self.0
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl error::Error for CodecError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.0.source()
    }
}

impl From<String> for CodecError {
    fn from(s: String) -> Self {
        CodecError(anyhow::anyhow!("{s}"))
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for CodecError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for CodecError {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(CodecError::from(s))
    }
}

// ---------------------------------------------------------------------------
// Encode / Decode traits
// ---------------------------------------------------------------------------

/// Encode a value into bytes for remote transmission.
pub trait Encode: Send + 'static {
    /// Serialize this value into a byte vector.
    fn encode(&self) -> Result<Vec<u8>, CodecError>;
}

/// Decode bytes received from the network into a value.
pub trait Decode: Sized + Send + 'static {
    /// Deserialize a value from the given byte slice.
    fn decode(bytes: &[u8]) -> Result<Self, CodecError>;
}

// ---------------------------------------------------------------------------
// serde-codec blanket impls
// ---------------------------------------------------------------------------

#[cfg(feature = "serde-codec")]
impl<T> Encode for T
where
    T: serde::Serialize + Send + 'static,
{
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        rmp_serde::to_vec_named(self).map_err(|e| CodecError::new(e.to_string()))
    }
}

#[cfg(feature = "serde-codec")]
impl<T> Decode for T
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        rmp_serde::decode::from_slice(bytes).map_err(|e| CodecError::new(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "serde-codec")]
mod tests {
    use super::*;

    #[test]
    fn serde_encode_decode_roundtrip() {
        let value: u64 = 12345;
        let bytes = value.encode().unwrap();
        let decoded = u64::decode(&bytes).unwrap();
        assert_eq!(decoded, 12345);
    }

    #[test]
    fn serde_decode_bad_bytes_returns_error() {
        let result = u64::decode(&[0xFF, 0xFF]);
        assert!(result.is_err());
    }
}
