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
// rkyv-codec blanket impls
// ---------------------------------------------------------------------------

#[cfg(feature = "rkyv-codec")]
impl<T> Encode for T
where
    T: rkyv::Archive
        + for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        > + Send
        + 'static,
{
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|v| v.to_vec())
            .map_err(|e| CodecError::new(e.to_string()))
    }
}

#[cfg(feature = "rkyv-codec")]
impl<T> Decode for T
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
            .map_err(|e| CodecError::new(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// KameoRkyvCodec — thin wrapper over libp2p_rkyv_codec for kameo wire types
// ---------------------------------------------------------------------------

#[cfg(feature = "rkyv-codec")]
mod rkyv_transport {
    use std::io;

    use async_trait::async_trait;
    use futures::prelude::*;
    use libp2p::{StreamProtocol, request_response};
    use libp2p_rkyv_codec::RkyvCodec;

    use super::super::messaging::{Config, SwarmRequest, SwarmResponse};

    /// Length-prefixed rkyv codec for kameo's remote messaging protocol.
    ///
    /// `SwarmRequest`/`SwarmResponse` use wire-friendly field types and derive
    /// rkyv directly, so this is a thin delegation to [`RkyvCodec`] with
    /// construction from a [`Config`].
    pub struct KameoRkyvCodec {
        inner: RkyvCodec<SwarmRequest, SwarmResponse>,
    }

    impl KameoRkyvCodec {
        /// Creates a new `KameoRkyvCodec` from the messaging configuration.
        pub fn new(config: &Config) -> Self {
            KameoRkyvCodec {
                inner: RkyvCodec::new(
                    config.request_size_maximum(),
                    config.response_size_maximum(),
                ),
            }
        }
    }

    impl Clone for KameoRkyvCodec {
        fn clone(&self) -> Self {
            Self {
                inner: RkyvCodec::new(
                    self.inner.request_size_maximum,
                    self.inner.response_size_maximum,
                ),
            }
        }
    }

    impl std::fmt::Debug for KameoRkyvCodec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("KameoRkyvCodec")
                .field("request_size_maximum", &self.inner.request_size_maximum)
                .field("response_size_maximum", &self.inner.response_size_maximum)
                .finish()
        }
    }

    #[async_trait]
    impl request_response::Codec for KameoRkyvCodec {
        type Protocol = StreamProtocol;
        type Request = SwarmRequest;
        type Response = SwarmResponse;

        async fn read_request<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Request>
        where
            T: AsyncRead + Unpin + Send,
        {
            self.inner.read_request(p, io).await
        }

        async fn read_response<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Response>
        where
            T: AsyncRead + Unpin + Send,
        {
            self.inner.read_response(p, io).await
        }

        async fn write_request<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
            req: Self::Request,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            self.inner.write_request(p, io, req).await
        }

        async fn write_response<T>(
            &mut self,
            p: &Self::Protocol,
            io: &mut T,
            resp: Self::Response,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            self.inner.write_response(p, io, resp).await
        }
    }
}

#[cfg(feature = "rkyv-codec")]
pub use rkyv_transport::KameoRkyvCodec;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "serde-codec")]
mod serde_tests {
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

#[cfg(test)]
#[cfg(feature = "rkyv-codec")]
mod rkyv_tests {
    use super::*;

    #[test]
    fn rkyv_encode_decode_roundtrip() {
        let value: u64 = 12345;
        let bytes = value.encode().unwrap();
        let decoded = u64::decode(&bytes).unwrap();
        assert_eq!(decoded, 12345);
    }

    #[test]
    fn rkyv_decode_bad_bytes_returns_error() {
        let result = u64::decode(&[0xFF, 0xFF]);
        assert!(result.is_err());
    }
}
