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
    /// The concrete error type produced by this codec's encoding.
    type CodecError: std::error::Error + Send + Sync + 'static;

    /// Serialize this value into a byte vector for remote transmission.
    fn remote_encode(&self) -> Result<Vec<u8>, RemoteCodecError<Self::CodecError>>;
}

/// Decode bytes received from the network into a value.
pub trait RemoteDecode: Sized + Send + 'static {
    /// The concrete error type produced by this codec's decoding.
    type CodecError: std::error::Error + Send + Sync + 'static;

    /// Deserialize a value from the given byte slice.
    fn remote_decode(bytes: &[u8]) -> Result<Self, RemoteCodecError<Self::CodecError>>;
}

/// Simple string-based error for cases without a typed source error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CodecMessage(pub String);

/// Codec error wrapping a concrete error type.
///
/// The type parameter `E` defaults to [`CodecMessage`] for backward
/// compatibility with code that writes `RemoteCodecError` without specifying a
/// type parameter.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RemoteCodecError<E: std::error::Error + Send + Sync + 'static = CodecMessage>(
    #[source] pub E,
);

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
    type CodecError = rkyv::rancor::Error;

    fn remote_encode(&self) -> Result<Vec<u8>, RemoteCodecError<Self::CodecError>> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|v| v.to_vec())
            .map_err(RemoteCodecError)
    }
}

#[cfg(feature = "rkyv-codec")]
impl<T> RemoteDecode for T
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    type CodecError = rkyv::rancor::Error;

    fn remote_decode(bytes: &[u8]) -> Result<Self, RemoteCodecError<Self::CodecError>> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned).map_err(RemoteCodecError)
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
    type CodecError = rmp_serde::encode::Error;

    fn remote_encode(&self) -> Result<Vec<u8>, RemoteCodecError<Self::CodecError>> {
        rmp_serde::to_vec_named(self).map_err(RemoteCodecError)
    }
}

#[cfg(feature = "serde-codec")]
impl<T> RemoteDecode for T
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    type CodecError = rmp_serde::decode::Error;

    fn remote_decode(bytes: &[u8]) -> Result<Self, RemoteCodecError<Self::CodecError>> {
        rmp_serde::decode::from_slice(bytes).map_err(RemoteCodecError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn codec_message_displays_inner_string() {
        let msg = CodecMessage("something went wrong".into());
        assert_eq!(msg.to_string(), "something went wrong");
    }

    #[test]
    fn remote_codec_error_preserves_source_error() {
        let inner = CodecMessage("root cause".into());
        let err = RemoteCodecError(inner);
        // Display delegates to inner
        assert_eq!(err.to_string(), "root cause");
        // source() returns the inner error
        let source = err.source().expect("source should be Some");
        assert_eq!(source.to_string(), "root cause");
    }

    #[test]
    fn remote_codec_error_default_type_param() {
        // RemoteCodecError without explicit type param defaults to CodecMessage
        let err: RemoteCodecError = RemoteCodecError(CodecMessage("default".into()));
        assert_eq!(err.to_string(), "default");
    }

    #[test]
    fn remote_codec_error_with_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = RemoteCodecError(io_err);
        assert_eq!(err.to_string(), "file missing");
        // source chain works
        let source = err.source().unwrap();
        assert_eq!(source.to_string(), "file missing");
    }

    #[cfg(feature = "rkyv-codec")]
    mod rkyv_codec_tests {
        use super::*;

        #[test]
        fn rkyv_encode_decode_roundtrip() {
            let value: u64 = 12345;
            let bytes = value.remote_encode().unwrap();
            let decoded = u64::remote_decode(&bytes).unwrap();
            assert_eq!(decoded, 12345);
        }

        #[test]
        fn rkyv_decode_bad_bytes_preserves_error_type() {
            let result = u64::remote_decode(&[0xFF]);
            let err = result.unwrap_err();
            // The inner error is rkyv::rancor::Error, not a String
            assert!(!err.to_string().is_empty());
            // source chain is intact
            assert!(err.source().is_some());
        }
    }
}
