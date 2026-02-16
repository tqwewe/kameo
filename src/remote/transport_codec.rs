//! Pluggable codec for libp2p `request_response::Codec`.
//!
//! A [`SwarmCodecFns`] function table is injected via constructor into
//! [`TransportCodec`], which delegates all encoding/decoding to it,
//! adding length-prefixed framing.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::{StreamProtocol, request_response};

use super::messaging::{Config, SwarmRequest, SwarmResponse};

// ---------------------------------------------------------------------------
// Pluggable codec
// ---------------------------------------------------------------------------

/// Function table for encoding/decoding [`SwarmRequest`] / [`SwarmResponse`].
///
/// Constructed by the downstream crate that owns the serialization format
/// (e.g. rkyv) and passed through the constructor chain into [`TransportCodec`].
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

// ---------------------------------------------------------------------------
// TransportCodec — libp2p request_response::Codec implementation
// ---------------------------------------------------------------------------

/// Length-prefixed transport codec that delegates serialization to the
/// injected [`SwarmCodecFns`].
#[derive(Debug, Clone)]
pub struct TransportCodec {
    codec: SwarmCodecFns,
    request_size_maximum: u64,
    response_size_maximum: u64,
}

impl TransportCodec {
    pub(super) fn new(config: Config, codec: SwarmCodecFns) -> Self {
        TransportCodec {
            codec,
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
        (self.codec.decode_request)(&data)
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
        (self.codec.decode_response)(&data)
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
        let data = (self.codec.encode_request)(&req)?;
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
        let data = (self.codec.encode_response)(&resp)?;
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
