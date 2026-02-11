//! Manual binary codec for libp2p `request_response::Codec`.
//!
//! Replaces the previous CBOR codec so that internal transport types
//! (`SwarmRequest`, `SwarmResponse`) are serialized without depending
//! on any serialization framework.
//!
//! Wire format: `[total_len: u32 LE][variant_tag: u8][field bytes...]`
//!
//! Field encoding:
//! - `ActorId`:           `[len:u16 LE][bytes...]` via `to_bytes()` / `from_bytes()`
//! - `Cow<str>` / String: `[len:u32 LE][utf8 bytes...]`
//! - `Vec<u8>`:           `[len:u32 LE][bytes...]`
//! - `Option<Duration>`:  `[0u8]` for None, `[1u8][secs:u64 LE][nanos:u32 LE]` for Some
//! - `bool`:              `[0u8]` or `[1u8]`
//! - `ActorStopReason`:   variant tag + recursive encoding
//! - `PanicError`:        error string + reason tag
//! - `RemoteSendError`:   variant tag + payload

use std::{borrow::Cow, io, time::Duration};

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::{StreamProtocol, request_response};

use crate::{
    actor::ActorId,
    error::{ActorStopReason, Infallible, PanicError, PanicReason, RemoteSendError},
};

use super::messaging::{Config, SwarmRequest, SwarmResponse};

/// Manual binary codec for `SwarmRequest` / `SwarmResponse`.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct TransportCodec {
    request_size_maximum: u64,
    response_size_maximum: u64,
}

impl TransportCodec {
    pub(super) fn new(config: Config) -> Self {
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
        decode_swarm_request(&data)
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
        decode_swarm_response(&data)
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
        let data = encode_swarm_request(&req);
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
        let data = encode_swarm_response(&resp);
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

// ---------------------------------------------------------------------------
// Binary buffer helpers
// ---------------------------------------------------------------------------

struct Buf(Vec<u8>);

impl Buf {
    fn new() -> Self {
        Buf(Vec::with_capacity(256))
    }

    fn put_u8(&mut self, v: u8) {
        self.0.push(v);
    }

    fn put_u16_le(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn put_u32_le(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn put_u64_le(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn put_bytes(&mut self, data: &[u8]) {
        self.put_u32_le(data.len() as u32);
        self.0.extend_from_slice(data);
    }

    fn put_str(&mut self, s: &str) {
        self.put_bytes(s.as_bytes());
    }

    fn put_actor_id(&mut self, id: &ActorId) {
        let bytes = id.to_bytes();
        self.put_u16_le(bytes.len() as u16);
        self.0.extend_from_slice(&bytes);
    }

    fn put_opt_duration(&mut self, d: &Option<Duration>) {
        match d {
            None => self.put_u8(0),
            Some(d) => {
                self.put_u8(1);
                self.put_u64_le(d.as_secs());
                self.put_u32_le(d.subsec_nanos());
            }
        }
    }

    fn put_bool(&mut self, b: bool) {
        self.put_u8(u8::from(b));
    }

    fn put_stop_reason(&mut self, reason: &ActorStopReason) {
        match reason {
            ActorStopReason::Normal => self.put_u8(0),
            ActorStopReason::Killed => self.put_u8(1),
            ActorStopReason::Panicked(err) => {
                self.put_u8(2);
                self.put_panic_error(err);
            }
            ActorStopReason::LinkDied { id, reason } => {
                self.put_u8(3);
                self.put_actor_id(id);
                self.put_stop_reason(reason);
            }
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => self.put_u8(4),
        }
    }

    fn put_panic_error(&mut self, err: &PanicError) {
        // Encode as Display string + reason tag (matches serde behavior)
        self.put_str(&err.to_string());
        self.put_panic_reason(&err.reason());
    }

    fn put_panic_reason(&mut self, reason: &PanicReason) {
        let tag = match reason {
            PanicReason::HandlerPanic => 0u8,
            PanicReason::OnMessage => 1,
            PanicReason::OnStart => 2,
            PanicReason::OnPanic => 3,
            PanicReason::OnLinkDied => 4,
            PanicReason::OnStop => 5,
        };
        self.put_u8(tag);
    }

    fn put_remote_send_error<E: std::fmt::Display>(&mut self, err: &RemoteSendError<E>) {
        match err {
            RemoteSendError::ActorNotRunning => self.put_u8(0),
            RemoteSendError::ActorStopped => self.put_u8(1),
            RemoteSendError::UnknownActor { actor_remote_id } => {
                self.put_u8(2);
                self.put_str(actor_remote_id);
            }
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => {
                self.put_u8(3);
                self.put_str(actor_remote_id);
                self.put_str(message_remote_id);
            }
            RemoteSendError::BadActorType => self.put_u8(4),
            RemoteSendError::MailboxFull => self.put_u8(5),
            RemoteSendError::ReplyTimeout => self.put_u8(6),
            RemoteSendError::HandlerError(e) => {
                self.put_u8(7);
                // HandlerError payload: for Vec<u8> it's raw bytes, for Infallible it's unreachable
                self.put_str(&e.to_string());
            }
            RemoteSendError::SerializeMessage(s) => {
                self.put_u8(8);
                self.put_str(s);
            }
            RemoteSendError::DeserializeMessage(s) => {
                self.put_u8(9);
                self.put_str(s);
            }
            RemoteSendError::SerializeReply(s) => {
                self.put_u8(10);
                self.put_str(s);
            }
            RemoteSendError::SerializeHandlerError(s) => {
                self.put_u8(11);
                self.put_str(s);
            }
            RemoteSendError::DeserializeHandlerError(s) => {
                self.put_u8(12);
                self.put_str(s);
            }
            RemoteSendError::SwarmNotBootstrapped => self.put_u8(13),
            RemoteSendError::DialFailure => self.put_u8(14),
            RemoteSendError::NetworkTimeout => self.put_u8(15),
            RemoteSendError::ConnectionClosed => self.put_u8(16),
            RemoteSendError::UnsupportedProtocols => self.put_u8(17),
            RemoteSendError::Io(e) => {
                self.put_u8(18);
                // Encode as Display string; on decode we lose the original io::Error
                self.put_str(&match e {
                    Some(e) => e.to_string(),
                    None => String::from("io error"),
                });
            }
        }
    }

    /// Specialized: put `RemoteSendError<Vec<u8>>` preserving the handler error payload.
    fn put_remote_send_error_bytes(&mut self, err: &RemoteSendError<Vec<u8>>) {
        match err {
            RemoteSendError::HandlerError(payload) => {
                self.put_u8(7);
                self.put_bytes(payload);
            }
            // For all other variants, delegate to the generic version
            other => {
                // We need to handle each non-HandlerError variant.
                // Since we can't easily call the generic version without E: Display,
                // match the non-HandlerError variants explicitly.
                match other {
                    RemoteSendError::ActorNotRunning => self.put_u8(0),
                    RemoteSendError::ActorStopped => self.put_u8(1),
                    RemoteSendError::UnknownActor { actor_remote_id } => {
                        self.put_u8(2);
                        self.put_str(actor_remote_id);
                    }
                    RemoteSendError::UnknownMessage {
                        actor_remote_id,
                        message_remote_id,
                    } => {
                        self.put_u8(3);
                        self.put_str(actor_remote_id);
                        self.put_str(message_remote_id);
                    }
                    RemoteSendError::BadActorType => self.put_u8(4),
                    RemoteSendError::MailboxFull => self.put_u8(5),
                    RemoteSendError::ReplyTimeout => self.put_u8(6),
                    RemoteSendError::HandlerError(_) => unreachable!(),
                    RemoteSendError::SerializeMessage(s) => {
                        self.put_u8(8);
                        self.put_str(s);
                    }
                    RemoteSendError::DeserializeMessage(s) => {
                        self.put_u8(9);
                        self.put_str(s);
                    }
                    RemoteSendError::SerializeReply(s) => {
                        self.put_u8(10);
                        self.put_str(s);
                    }
                    RemoteSendError::SerializeHandlerError(s) => {
                        self.put_u8(11);
                        self.put_str(s);
                    }
                    RemoteSendError::DeserializeHandlerError(s) => {
                        self.put_u8(12);
                        self.put_str(s);
                    }
                    RemoteSendError::SwarmNotBootstrapped => self.put_u8(13),
                    RemoteSendError::DialFailure => self.put_u8(14),
                    RemoteSendError::NetworkTimeout => self.put_u8(15),
                    RemoteSendError::ConnectionClosed => self.put_u8(16),
                    RemoteSendError::UnsupportedProtocols => self.put_u8(17),
                    RemoteSendError::Io(e) => {
                        self.put_u8(18);
                        self.put_str(&match e {
                            Some(e) => e.to_string(),
                            None => String::from("io error"),
                        });
                    }
                }
            }
        }
    }

    fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        if self.remaining() < 1 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read_u8"));
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u16_le(&mut self) -> io::Result<u16> {
        if self.remaining() < 2 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read_u16"));
        }
        let v = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_u32_le(&mut self) -> io::Result<u32> {
        if self.remaining() < 4 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read_u32"));
        }
        let v = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_u64_le(&mut self) -> io::Result<u64> {
        if self.remaining() < 8 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read_u64"));
        }
        let v = u64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn read_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_u32_le()? as usize;
        if self.remaining() < len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read_bytes"));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    fn read_string(&mut self) -> io::Result<String> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn read_cow_str(&mut self) -> io::Result<Cow<'static, str>> {
        Ok(Cow::Owned(self.read_string()?))
    }

    fn read_actor_id(&mut self) -> io::Result<ActorId> {
        let len = self.read_u16_le()? as usize;
        if self.remaining() < len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read_actor_id",
            ));
        }
        let bytes = &self.data[self.pos..self.pos + len];
        self.pos += len;
        ActorId::from_bytes(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn read_opt_duration(&mut self) -> io::Result<Option<Duration>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let secs = self.read_u64_le()?;
                let nanos = self.read_u32_le()?;
                Ok(Some(Duration::new(secs, nanos)))
            }
            t => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid Option<Duration> tag: {t}"),
            )),
        }
    }

    fn read_bool(&mut self) -> io::Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    fn read_stop_reason(&mut self) -> io::Result<ActorStopReason> {
        match self.read_u8()? {
            0 => Ok(ActorStopReason::Normal),
            1 => Ok(ActorStopReason::Killed),
            2 => {
                let err = self.read_panic_error()?;
                Ok(ActorStopReason::Panicked(err))
            }
            3 => {
                let id = self.read_actor_id()?;
                let reason = self.read_stop_reason()?;
                Ok(ActorStopReason::LinkDied {
                    id,
                    reason: Box::new(reason),
                })
            }
            #[cfg(feature = "remote")]
            4 => Ok(ActorStopReason::PeerDisconnected),
            t => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid ActorStopReason tag: {t}"),
            )),
        }
    }

    fn read_panic_error(&mut self) -> io::Result<PanicError> {
        let err_string = self.read_string()?;
        let reason = self.read_panic_reason()?;
        Ok(PanicError::from_wire(err_string, reason))
    }

    fn read_panic_reason(&mut self) -> io::Result<PanicReason> {
        match self.read_u8()? {
            0 => Ok(PanicReason::HandlerPanic),
            1 => Ok(PanicReason::OnMessage),
            2 => Ok(PanicReason::OnStart),
            3 => Ok(PanicReason::OnPanic),
            4 => Ok(PanicReason::OnLinkDied),
            5 => Ok(PanicReason::OnStop),
            t => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid PanicReason tag: {t}"),
            )),
        }
    }

    fn read_remote_send_error_infallible(&mut self) -> io::Result<RemoteSendError<Infallible>> {
        self.read_remote_send_error_with(|_r| {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected HandlerError for Infallible",
            ))
        })
    }

    fn read_remote_send_error_bytes(&mut self) -> io::Result<RemoteSendError<Vec<u8>>> {
        self.read_remote_send_error_with(|r| {
            let payload = r.read_bytes()?;
            Ok(RemoteSendError::HandlerError(payload))
        })
    }

    fn read_remote_send_error_with<E>(
        &mut self,
        handler_error: impl FnOnce(&mut Self) -> io::Result<RemoteSendError<E>>,
    ) -> io::Result<RemoteSendError<E>> {
        match self.read_u8()? {
            0 => Ok(RemoteSendError::ActorNotRunning),
            1 => Ok(RemoteSendError::ActorStopped),
            2 => {
                let s = self.read_cow_str()?;
                Ok(RemoteSendError::UnknownActor { actor_remote_id: s })
            }
            3 => {
                let a = self.read_cow_str()?;
                let m = self.read_cow_str()?;
                Ok(RemoteSendError::UnknownMessage {
                    actor_remote_id: a,
                    message_remote_id: m,
                })
            }
            4 => Ok(RemoteSendError::BadActorType),
            5 => Ok(RemoteSendError::MailboxFull),
            6 => Ok(RemoteSendError::ReplyTimeout),
            7 => handler_error(self),
            8 => Ok(RemoteSendError::SerializeMessage(self.read_string()?)),
            9 => Ok(RemoteSendError::DeserializeMessage(self.read_string()?)),
            10 => Ok(RemoteSendError::SerializeReply(self.read_string()?)),
            11 => Ok(RemoteSendError::SerializeHandlerError(self.read_string()?)),
            12 => Ok(RemoteSendError::DeserializeHandlerError(
                self.read_string()?,
            )),
            13 => Ok(RemoteSendError::SwarmNotBootstrapped),
            14 => Ok(RemoteSendError::DialFailure),
            15 => Ok(RemoteSendError::NetworkTimeout),
            16 => Ok(RemoteSendError::ConnectionClosed),
            17 => Ok(RemoteSendError::UnsupportedProtocols),
            18 => {
                let _msg = self.read_string()?;
                // We can't reconstruct the original io::Error, so use None
                Ok(RemoteSendError::Io(None))
            }
            t => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid RemoteSendError tag: {t}"),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// SwarmRequest encode / decode
// ---------------------------------------------------------------------------

fn encode_swarm_request(req: &SwarmRequest) -> Vec<u8> {
    let mut buf = Buf::new();
    match req {
        SwarmRequest::Ask {
            actor_id,
            actor_remote_id,
            message_remote_id,
            payload,
            mailbox_timeout,
            reply_timeout,
            immediate,
        } => {
            buf.put_u8(0);
            buf.put_actor_id(actor_id);
            buf.put_str(actor_remote_id);
            buf.put_str(message_remote_id);
            buf.put_bytes(payload);
            buf.put_opt_duration(mailbox_timeout);
            buf.put_opt_duration(reply_timeout);
            buf.put_bool(*immediate);
        }
        SwarmRequest::Tell {
            actor_id,
            actor_remote_id,
            message_remote_id,
            payload,
            mailbox_timeout,
            immediate,
        } => {
            buf.put_u8(1);
            buf.put_actor_id(actor_id);
            buf.put_str(actor_remote_id);
            buf.put_str(message_remote_id);
            buf.put_bytes(payload);
            buf.put_opt_duration(mailbox_timeout);
            buf.put_bool(*immediate);
        }
        SwarmRequest::Link {
            actor_id,
            actor_remote_id,
            sibling_id,
            sibling_remote_id,
        } => {
            buf.put_u8(2);
            buf.put_actor_id(actor_id);
            buf.put_str(actor_remote_id);
            buf.put_actor_id(sibling_id);
            buf.put_str(sibling_remote_id);
        }
        SwarmRequest::Unlink {
            actor_id,
            actor_remote_id,
            sibling_id,
        } => {
            buf.put_u8(3);
            buf.put_actor_id(actor_id);
            buf.put_str(actor_remote_id);
            buf.put_actor_id(sibling_id);
        }
        SwarmRequest::SignalLinkDied {
            dead_actor_id,
            notified_actor_id,
            notified_actor_remote_id,
            stop_reason,
        } => {
            buf.put_u8(4);
            buf.put_actor_id(dead_actor_id);
            buf.put_actor_id(notified_actor_id);
            buf.put_str(notified_actor_remote_id);
            buf.put_stop_reason(stop_reason);
        }
    }
    buf.into_vec()
}

fn decode_swarm_request(data: &[u8]) -> io::Result<SwarmRequest> {
    let mut r = Reader::new(data);
    match r.read_u8()? {
        0 => {
            let actor_id = r.read_actor_id()?;
            let actor_remote_id = r.read_cow_str()?;
            let message_remote_id = r.read_cow_str()?;
            let payload = r.read_bytes()?;
            let mailbox_timeout = r.read_opt_duration()?;
            let reply_timeout = r.read_opt_duration()?;
            let immediate = r.read_bool()?;
            Ok(SwarmRequest::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            })
        }
        1 => {
            let actor_id = r.read_actor_id()?;
            let actor_remote_id = r.read_cow_str()?;
            let message_remote_id = r.read_cow_str()?;
            let payload = r.read_bytes()?;
            let mailbox_timeout = r.read_opt_duration()?;
            let immediate = r.read_bool()?;
            Ok(SwarmRequest::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            })
        }
        2 => {
            let actor_id = r.read_actor_id()?;
            let actor_remote_id = r.read_cow_str()?;
            let sibling_id = r.read_actor_id()?;
            let sibling_remote_id = r.read_cow_str()?;
            Ok(SwarmRequest::Link {
                actor_id,
                actor_remote_id,
                sibling_id,
                sibling_remote_id,
            })
        }
        3 => {
            let actor_id = r.read_actor_id()?;
            let actor_remote_id = r.read_cow_str()?;
            let sibling_id = r.read_actor_id()?;
            Ok(SwarmRequest::Unlink {
                actor_id,
                actor_remote_id,
                sibling_id,
            })
        }
        4 => {
            let dead_actor_id = r.read_actor_id()?;
            let notified_actor_id = r.read_actor_id()?;
            let notified_actor_remote_id = r.read_cow_str()?;
            let stop_reason = r.read_stop_reason()?;
            Ok(SwarmRequest::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
            })
        }
        t => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid SwarmRequest tag: {t}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// SwarmResponse encode / decode
// ---------------------------------------------------------------------------

fn encode_swarm_response(resp: &SwarmResponse) -> Vec<u8> {
    let mut buf = Buf::new();
    match resp {
        SwarmResponse::Ask(result) => {
            buf.put_u8(0);
            match result {
                Ok(payload) => {
                    buf.put_u8(0); // Ok
                    buf.put_bytes(payload);
                }
                Err(err) => {
                    buf.put_u8(1); // Err
                    buf.put_remote_send_error_bytes(err);
                }
            }
        }
        SwarmResponse::Tell(result) => {
            buf.put_u8(1);
            match result {
                Ok(()) => buf.put_u8(0),
                Err(err) => {
                    buf.put_u8(1);
                    buf.put_remote_send_error(&err);
                }
            }
        }
        SwarmResponse::Link(result) => {
            buf.put_u8(2);
            match result {
                Ok(()) => buf.put_u8(0),
                Err(err) => {
                    buf.put_u8(1);
                    buf.put_remote_send_error(&err);
                }
            }
        }
        SwarmResponse::Unlink(result) => {
            buf.put_u8(3);
            match result {
                Ok(()) => buf.put_u8(0),
                Err(err) => {
                    buf.put_u8(1);
                    buf.put_remote_send_error(&err);
                }
            }
        }
        SwarmResponse::SignalLinkDied(result) => {
            buf.put_u8(4);
            match result {
                Ok(()) => buf.put_u8(0),
                Err(err) => {
                    buf.put_u8(1);
                    buf.put_remote_send_error(&err);
                }
            }
        }
        SwarmResponse::OutboundFailure(err) => {
            buf.put_u8(5);
            buf.put_remote_send_error(&err);
        }
    }
    buf.into_vec()
}

fn decode_swarm_response(data: &[u8]) -> io::Result<SwarmResponse> {
    let mut r = Reader::new(data);
    match r.read_u8()? {
        0 => {
            // Ask
            match r.read_u8()? {
                0 => {
                    let payload = r.read_bytes()?;
                    Ok(SwarmResponse::Ask(Ok(payload)))
                }
                1 => {
                    let err = r.read_remote_send_error_bytes()?;
                    Ok(SwarmResponse::Ask(Err(err)))
                }
                t => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Ask result tag: {t}"),
                )),
            }
        }
        1 => {
            // Tell
            match r.read_u8()? {
                0 => Ok(SwarmResponse::Tell(Ok(()))),
                1 => {
                    let err = r.read_remote_send_error_infallible()?;
                    Ok(SwarmResponse::Tell(Err(err)))
                }
                t => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Tell result tag: {t}"),
                )),
            }
        }
        2 => {
            // Link
            match r.read_u8()? {
                0 => Ok(SwarmResponse::Link(Ok(()))),
                1 => {
                    let err = r.read_remote_send_error_infallible()?;
                    Ok(SwarmResponse::Link(Err(err)))
                }
                t => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Link result tag: {t}"),
                )),
            }
        }
        3 => {
            // Unlink
            match r.read_u8()? {
                0 => Ok(SwarmResponse::Unlink(Ok(()))),
                1 => {
                    let err = r.read_remote_send_error_infallible()?;
                    Ok(SwarmResponse::Unlink(Err(err)))
                }
                t => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Unlink result tag: {t}"),
                )),
            }
        }
        4 => {
            // SignalLinkDied
            match r.read_u8()? {
                0 => Ok(SwarmResponse::SignalLinkDied(Ok(()))),
                1 => {
                    let err = r.read_remote_send_error_infallible()?;
                    Ok(SwarmResponse::SignalLinkDied(Err(err)))
                }
                t => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid SignalLinkDied result tag: {t}"),
                )),
            }
        }
        5 => {
            // OutboundFailure
            let err = r.read_remote_send_error_infallible()?;
            Ok(SwarmResponse::OutboundFailure(err))
        }
        t => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid SwarmResponse tag: {t}"),
        )),
    }
}
