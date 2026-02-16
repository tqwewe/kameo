//! Pluggable codec for libp2p `request_response::Codec`.
//!
//! Downstream crates register a [`SwarmCodecFns`] function table via the
//! [`SWARM_CODEC`] distributed slice.  [`TransportCodec`] delegates all
//! encoding/decoding to the registered codec, adding length-prefixed framing.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::{StreamProtocol, request_response};
use linkme::distributed_slice;

use super::messaging::{Config, SwarmRequest, SwarmResponse};

// ---------------------------------------------------------------------------
// Pluggable codec registration
// ---------------------------------------------------------------------------

/// Function table for encoding/decoding [`SwarmRequest`] / [`SwarmResponse`].
///
/// Downstream crates register exactly one of these via the [`SWARM_CODEC`]
/// distributed slice.
#[derive(Copy, Clone, Debug)]
pub struct SwarmCodecFns {
    /// Encode a [`SwarmRequest`] into bytes.
    pub encode_request: fn(&SwarmRequest) -> io::Result<Vec<u8>>,
    /// Decode bytes into a [`SwarmRequest`].
    pub decode_request: fn(&[u8]) -> io::Result<SwarmRequest>,
    /// Encode a [`SwarmResponse`] into bytes.
    pub encode_response: fn(&SwarmResponse) -> io::Result<Vec<u8>>,
    /// Decode bytes into a [`SwarmResponse`].
    pub decode_response: fn(&[u8]) -> io::Result<SwarmResponse>,
}

/// Distributed slice for codec registration.
///
/// Exactly one [`SwarmCodecFns`] should be registered by a downstream crate
/// (e.g. via `#[linkme::distributed_slice(kameo::remote::SWARM_CODEC)]`).
#[distributed_slice]
pub static SWARM_CODEC: [SwarmCodecFns];

/// Returns the registered codec, panicking if none (or more than one) is registered.
fn codec() -> &'static SwarmCodecFns {
    match SWARM_CODEC.len() {
        0 => panic!("no SwarmCodec registered — link a crate that provides one"),
        1 => &SWARM_CODEC[0],
        n => panic!("expected exactly 1 SwarmCodec registration, found {n}"),
    }
}

// ---------------------------------------------------------------------------
// TransportCodec — libp2p request_response::Codec implementation
// ---------------------------------------------------------------------------

/// Length-prefixed transport codec that delegates serialization to the
/// registered [`SwarmCodecFns`].
#[derive(Debug, Clone)]
pub struct TransportCodec {
    request_size_maximum: u64,
    response_size_maximum: u64,
}

impl TransportCodec {
    pub(super) fn new(config: Config) -> Self {
        // Eagerly validate that a codec is registered.
        let _ = codec();
        TransportCodec {
            request_size_maximum: config.request_size_maximum(),
            response_size_maximum: config.response_size_maximum(),
        }
    }
}

#[async_trait]
impl request_response::Codec for TransportCodec {
    type Protocol = StreamProtocol;
    type Request = SwarmRequest;
    type Response = SwarmResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let data = read_length_prefixed(io, self.request_size_maximum).await?;
        (codec().decode_request)(&data)
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let data = read_length_prefixed(io, self.response_size_maximum).await?;
        (codec().decode_response)(&data)
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = (codec().encode_request)(&req)?;
        write_length_prefixed(io, &data).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = (codec().encode_response)(&resp)?;
        write_length_prefixed(io, &data).await
    }
}

// ---------------------------------------------------------------------------
// Length-prefixed I/O helpers
// ---------------------------------------------------------------------------

async fn read_length_prefixed<T: AsyncRead + Unpin>(
    io: &mut T,
    max_len: u64,
) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as u64;
    if len > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} > {max_len}"),
        ));
    }
    let mut data = vec![0u8; len as usize];
    io.read_exact(&mut data).await?;
    Ok(data)
}

async fn write_length_prefixed<T: AsyncWrite + Unpin>(io: &mut T, data: &[u8]) -> io::Result<()> {
    let len = data.len() as u32;
    io.write_all(&len.to_le_bytes()).await?;
    io.write_all(data).await?;
    io.close().await?;
    Ok(())
}
