//! Codec for libp2p `request_response::Codec`.
//!
//! When `rkyv-codec` is enabled, uses rkyv-serializable wire types.
//! Otherwise, falls back to a manual binary codec.

use std::{borrow::Cow, io, time::Duration};

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::{StreamProtocol, request_response};

use crate::{
    actor::ActorId,
    error::{ActorStopReason, Infallible, PanicError, PanicReason, RemoteSendError},
};

use super::messaging::{Config, SwarmRequest, SwarmResponse};

/// Codec for `SwarmRequest` / `SwarmResponse`.
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
        let data = encode_swarm_request(&req)?;
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
        let data = encode_swarm_response(&resp)?;
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

// ===========================================================================
// rkyv-based codec (behind `rkyv-codec` feature)
// ===========================================================================

#[cfg(feature = "rkyv-codec")]
mod rkyv_wire {
    use super::*;

    // -----------------------------------------------------------------------
    // Wire types — rkyv-serializable mirrors of runtime types
    // -----------------------------------------------------------------------

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub(super) struct WireActorId {
        pub sequence_id: u64,
        pub peer_id_bytes: Vec<u8>,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub(super) enum WirePanicReason {
        HandlerPanic,
        OnMessage,
        OnStart,
        OnPanic,
        OnLinkDied,
        OnStop,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub(super) struct WirePanicError {
        pub display: String,
        pub reason: WirePanicReason,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(serialize_bounds(
        __S: rkyv::ser::Writer + rkyv::ser::Allocator,
        __S::Error: rkyv::rancor::Source,
    ))]
    #[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
    #[rkyv(bytecheck(
        bounds(
            __C: rkyv::validation::ArchiveContext,
            __C::Error: rkyv::rancor::Source,
        )
    ))]
    pub(super) enum WireActorStopReason {
        Normal,
        Killed,
        Panicked(WirePanicError),
        LinkDied {
            id: WireActorId,
            #[rkyv(omit_bounds)]
            reason: Box<WireActorStopReason>,
        },
        PeerDisconnected,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub(super) enum WireRemoteSendError {
        ActorNotRunning,
        ActorStopped,
        UnknownActor { actor_remote_id: String },
        UnknownMessage { actor_remote_id: String, message_remote_id: String },
        BadActorType,
        MailboxFull,
        ReplyTimeout,
        HandlerError(Vec<u8>),
        SerializeMessage(String),
        DeserializeMessage(String),
        SerializeReply(String),
        SerializeHandlerError(String),
        DeserializeHandlerError(String),
        SwarmNotBootstrapped,
        DialFailure,
        NetworkTimeout,
        ConnectionClosed,
        UnsupportedProtocols,
        Io(Option<String>),
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub(super) enum WireSwarmRequest {
        Ask {
            actor_id: WireActorId,
            actor_remote_id: String,
            message_remote_id: String,
            payload: Vec<u8>,
            mailbox_timeout: Option<Duration>,
            reply_timeout: Option<Duration>,
            immediate: bool,
        },
        Tell {
            actor_id: WireActorId,
            actor_remote_id: String,
            message_remote_id: String,
            payload: Vec<u8>,
            mailbox_timeout: Option<Duration>,
            immediate: bool,
        },
        Link {
            actor_id: WireActorId,
            actor_remote_id: String,
            sibling_id: WireActorId,
            sibling_remote_id: String,
        },
        Unlink {
            actor_id: WireActorId,
            actor_remote_id: String,
            sibling_id: WireActorId,
        },
        SignalLinkDied {
            dead_actor_id: WireActorId,
            notified_actor_id: WireActorId,
            notified_actor_remote_id: String,
            stop_reason: WireActorStopReason,
        },
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub(super) enum WireSwarmResponse {
        Ask(Result<Vec<u8>, WireRemoteSendError>),
        Tell(Result<(), WireRemoteSendError>),
        Link(Result<(), WireRemoteSendError>),
        Unlink(Result<(), WireRemoteSendError>),
        SignalLinkDied(Result<(), WireRemoteSendError>),
        OutboundFailure(WireRemoteSendError),
    }

    // -----------------------------------------------------------------------
    // Conversions: runtime → wire (for encoding)
    // -----------------------------------------------------------------------

    fn actor_id_to_wire(id: &ActorId) -> WireActorId {
        WireActorId {
            sequence_id: id.sequence_id(),
            peer_id_bytes: id.to_bytes()[8..].to_vec(), // skip sequence_id bytes
        }
    }

    fn panic_reason_to_wire(r: &PanicReason) -> WirePanicReason {
        match r {
            PanicReason::HandlerPanic => WirePanicReason::HandlerPanic,
            PanicReason::OnMessage => WirePanicReason::OnMessage,
            PanicReason::OnStart => WirePanicReason::OnStart,
            PanicReason::OnPanic => WirePanicReason::OnPanic,
            PanicReason::OnLinkDied => WirePanicReason::OnLinkDied,
            PanicReason::OnStop => WirePanicReason::OnStop,
        }
    }

    fn panic_error_to_wire(e: &PanicError) -> WirePanicError {
        WirePanicError {
            display: e.to_string(),
            reason: panic_reason_to_wire(&e.reason()),
        }
    }

    fn stop_reason_to_wire(r: &ActorStopReason) -> WireActorStopReason {
        match r {
            ActorStopReason::Normal => WireActorStopReason::Normal,
            ActorStopReason::Killed => WireActorStopReason::Killed,
            ActorStopReason::Panicked(e) => WireActorStopReason::Panicked(panic_error_to_wire(e)),
            ActorStopReason::LinkDied { id, reason } => WireActorStopReason::LinkDied {
                id: actor_id_to_wire(id),
                reason: Box::new(stop_reason_to_wire(reason)),
            },
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => WireActorStopReason::PeerDisconnected,
        }
    }

    fn remote_send_error_infallible_to_wire(e: &RemoteSendError<Infallible>) -> WireRemoteSendError {
        match e {
            RemoteSendError::ActorNotRunning => WireRemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped => WireRemoteSendError::ActorStopped,
            RemoteSendError::UnknownActor { actor_remote_id } => {
                WireRemoteSendError::UnknownActor { actor_remote_id: actor_remote_id.to_string() }
            }
            RemoteSendError::UnknownMessage { actor_remote_id, message_remote_id } => {
                WireRemoteSendError::UnknownMessage {
                    actor_remote_id: actor_remote_id.to_string(),
                    message_remote_id: message_remote_id.to_string(),
                }
            }
            RemoteSendError::BadActorType => WireRemoteSendError::BadActorType,
            RemoteSendError::MailboxFull => WireRemoteSendError::MailboxFull,
            RemoteSendError::ReplyTimeout => WireRemoteSendError::ReplyTimeout,
            RemoteSendError::HandlerError(e) => match *e {},
            RemoteSendError::SerializeMessage(s) => WireRemoteSendError::SerializeMessage(s.clone()),
            RemoteSendError::DeserializeMessage(s) => WireRemoteSendError::DeserializeMessage(s.clone()),
            RemoteSendError::SerializeReply(s) => WireRemoteSendError::SerializeReply(s.clone()),
            RemoteSendError::SerializeHandlerError(s) => WireRemoteSendError::SerializeHandlerError(s.clone()),
            RemoteSendError::DeserializeHandlerError(s) => WireRemoteSendError::DeserializeHandlerError(s.clone()),
            RemoteSendError::SwarmNotBootstrapped => WireRemoteSendError::SwarmNotBootstrapped,
            RemoteSendError::DialFailure => WireRemoteSendError::DialFailure,
            RemoteSendError::NetworkTimeout => WireRemoteSendError::NetworkTimeout,
            RemoteSendError::ConnectionClosed => WireRemoteSendError::ConnectionClosed,
            RemoteSendError::UnsupportedProtocols => WireRemoteSendError::UnsupportedProtocols,
            RemoteSendError::Io(e) => WireRemoteSendError::Io(e.as_ref().map(|e| e.to_string())),
        }
    }

    fn remote_send_error_bytes_to_wire(e: &RemoteSendError<Vec<u8>>) -> WireRemoteSendError {
        match e {
            RemoteSendError::ActorNotRunning => WireRemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped => WireRemoteSendError::ActorStopped,
            RemoteSendError::UnknownActor { actor_remote_id } => {
                WireRemoteSendError::UnknownActor { actor_remote_id: actor_remote_id.to_string() }
            }
            RemoteSendError::UnknownMessage { actor_remote_id, message_remote_id } => {
                WireRemoteSendError::UnknownMessage {
                    actor_remote_id: actor_remote_id.to_string(),
                    message_remote_id: message_remote_id.to_string(),
                }
            }
            RemoteSendError::BadActorType => WireRemoteSendError::BadActorType,
            RemoteSendError::MailboxFull => WireRemoteSendError::MailboxFull,
            RemoteSendError::ReplyTimeout => WireRemoteSendError::ReplyTimeout,
            RemoteSendError::HandlerError(payload) => WireRemoteSendError::HandlerError(payload.clone()),
            RemoteSendError::SerializeMessage(s) => WireRemoteSendError::SerializeMessage(s.clone()),
            RemoteSendError::DeserializeMessage(s) => WireRemoteSendError::DeserializeMessage(s.clone()),
            RemoteSendError::SerializeReply(s) => WireRemoteSendError::SerializeReply(s.clone()),
            RemoteSendError::SerializeHandlerError(s) => WireRemoteSendError::SerializeHandlerError(s.clone()),
            RemoteSendError::DeserializeHandlerError(s) => WireRemoteSendError::DeserializeHandlerError(s.clone()),
            RemoteSendError::SwarmNotBootstrapped => WireRemoteSendError::SwarmNotBootstrapped,
            RemoteSendError::DialFailure => WireRemoteSendError::DialFailure,
            RemoteSendError::NetworkTimeout => WireRemoteSendError::NetworkTimeout,
            RemoteSendError::ConnectionClosed => WireRemoteSendError::ConnectionClosed,
            RemoteSendError::UnsupportedProtocols => WireRemoteSendError::UnsupportedProtocols,
            RemoteSendError::Io(e) => WireRemoteSendError::Io(e.as_ref().map(|e| e.to_string())),
        }
    }

    pub(super) fn request_to_wire(req: &SwarmRequest) -> WireSwarmRequest {
        match req {
            SwarmRequest::Ask { actor_id, actor_remote_id, message_remote_id, payload, mailbox_timeout, reply_timeout, immediate } => {
                WireSwarmRequest::Ask {
                    actor_id: actor_id_to_wire(actor_id),
                    actor_remote_id: actor_remote_id.to_string(),
                    message_remote_id: message_remote_id.to_string(),
                    payload: payload.clone(),
                    mailbox_timeout: *mailbox_timeout,
                    reply_timeout: *reply_timeout,
                    immediate: *immediate,
                }
            }
            SwarmRequest::Tell { actor_id, actor_remote_id, message_remote_id, payload, mailbox_timeout, immediate } => {
                WireSwarmRequest::Tell {
                    actor_id: actor_id_to_wire(actor_id),
                    actor_remote_id: actor_remote_id.to_string(),
                    message_remote_id: message_remote_id.to_string(),
                    payload: payload.clone(),
                    mailbox_timeout: *mailbox_timeout,
                    immediate: *immediate,
                }
            }
            SwarmRequest::Link { actor_id, actor_remote_id, sibling_id, sibling_remote_id } => {
                WireSwarmRequest::Link {
                    actor_id: actor_id_to_wire(actor_id),
                    actor_remote_id: actor_remote_id.to_string(),
                    sibling_id: actor_id_to_wire(sibling_id),
                    sibling_remote_id: sibling_remote_id.to_string(),
                }
            }
            SwarmRequest::Unlink { actor_id, actor_remote_id, sibling_id } => {
                WireSwarmRequest::Unlink {
                    actor_id: actor_id_to_wire(actor_id),
                    actor_remote_id: actor_remote_id.to_string(),
                    sibling_id: actor_id_to_wire(sibling_id),
                }
            }
            SwarmRequest::SignalLinkDied { dead_actor_id, notified_actor_id, notified_actor_remote_id, stop_reason } => {
                WireSwarmRequest::SignalLinkDied {
                    dead_actor_id: actor_id_to_wire(dead_actor_id),
                    notified_actor_id: actor_id_to_wire(notified_actor_id),
                    notified_actor_remote_id: notified_actor_remote_id.to_string(),
                    stop_reason: stop_reason_to_wire(stop_reason),
                }
            }
        }
    }

    pub(super) fn response_to_wire(resp: &SwarmResponse) -> WireSwarmResponse {
        match resp {
            SwarmResponse::Ask(r) => WireSwarmResponse::Ask(match r {
                Ok(payload) => Ok(payload.clone()),
                Err(e) => Err(remote_send_error_bytes_to_wire(e)),
            }),
            SwarmResponse::Tell(r) => WireSwarmResponse::Tell(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(remote_send_error_infallible_to_wire(e)),
            }),
            SwarmResponse::Link(r) => WireSwarmResponse::Link(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(remote_send_error_infallible_to_wire(e)),
            }),
            SwarmResponse::Unlink(r) => WireSwarmResponse::Unlink(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(remote_send_error_infallible_to_wire(e)),
            }),
            SwarmResponse::SignalLinkDied(r) => WireSwarmResponse::SignalLinkDied(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(remote_send_error_infallible_to_wire(e)),
            }),
            SwarmResponse::OutboundFailure(e) => {
                WireSwarmResponse::OutboundFailure(remote_send_error_infallible_to_wire(e))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Conversions: wire → runtime (for decoding)
    // -----------------------------------------------------------------------

    fn wire_to_actor_id(w: WireActorId) -> io::Result<ActorId> {
        let mut bytes = w.sequence_id.to_le_bytes().to_vec();
        bytes.extend_from_slice(&w.peer_id_bytes);
        ActorId::from_bytes(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn wire_to_panic_reason(w: WirePanicReason) -> PanicReason {
        match w {
            WirePanicReason::HandlerPanic => PanicReason::HandlerPanic,
            WirePanicReason::OnMessage => PanicReason::OnMessage,
            WirePanicReason::OnStart => PanicReason::OnStart,
            WirePanicReason::OnPanic => PanicReason::OnPanic,
            WirePanicReason::OnLinkDied => PanicReason::OnLinkDied,
            WirePanicReason::OnStop => PanicReason::OnStop,
        }
    }

    fn wire_to_panic_error(w: WirePanicError) -> PanicError {
        PanicError::from_wire(w.display, wire_to_panic_reason(w.reason))
    }

    fn wire_to_stop_reason(w: WireActorStopReason) -> io::Result<ActorStopReason> {
        Ok(match w {
            WireActorStopReason::Normal => ActorStopReason::Normal,
            WireActorStopReason::Killed => ActorStopReason::Killed,
            WireActorStopReason::Panicked(e) => ActorStopReason::Panicked(wire_to_panic_error(e)),
            WireActorStopReason::LinkDied { id, reason } => ActorStopReason::LinkDied {
                id: wire_to_actor_id(id)?,
                reason: Box::new(wire_to_stop_reason(*reason)?),
            },
            WireActorStopReason::PeerDisconnected => ActorStopReason::PeerDisconnected,
        })
    }

    fn wire_to_remote_send_error_infallible(w: WireRemoteSendError) -> RemoteSendError<Infallible> {
        match w {
            WireRemoteSendError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            WireRemoteSendError::ActorStopped => RemoteSendError::ActorStopped,
            WireRemoteSendError::UnknownActor { actor_remote_id } => {
                RemoteSendError::UnknownActor { actor_remote_id: Cow::Owned(actor_remote_id) }
            }
            WireRemoteSendError::UnknownMessage { actor_remote_id, message_remote_id } => {
                RemoteSendError::UnknownMessage {
                    actor_remote_id: Cow::Owned(actor_remote_id),
                    message_remote_id: Cow::Owned(message_remote_id),
                }
            }
            WireRemoteSendError::BadActorType => RemoteSendError::BadActorType,
            WireRemoteSendError::MailboxFull => RemoteSendError::MailboxFull,
            WireRemoteSendError::ReplyTimeout => RemoteSendError::ReplyTimeout,
            WireRemoteSendError::HandlerError(_) => {
                // Infallible case: should never happen on the wire
                RemoteSendError::ActorNotRunning
            }
            WireRemoteSendError::SerializeMessage(s) => RemoteSendError::SerializeMessage(s),
            WireRemoteSendError::DeserializeMessage(s) => RemoteSendError::DeserializeMessage(s),
            WireRemoteSendError::SerializeReply(s) => RemoteSendError::SerializeReply(s),
            WireRemoteSendError::SerializeHandlerError(s) => RemoteSendError::SerializeHandlerError(s),
            WireRemoteSendError::DeserializeHandlerError(s) => RemoteSendError::DeserializeHandlerError(s),
            WireRemoteSendError::SwarmNotBootstrapped => RemoteSendError::SwarmNotBootstrapped,
            WireRemoteSendError::DialFailure => RemoteSendError::DialFailure,
            WireRemoteSendError::NetworkTimeout => RemoteSendError::NetworkTimeout,
            WireRemoteSendError::ConnectionClosed => RemoteSendError::ConnectionClosed,
            WireRemoteSendError::UnsupportedProtocols => RemoteSendError::UnsupportedProtocols,
            WireRemoteSendError::Io(_) => RemoteSendError::Io(None),
        }
    }

    fn wire_to_remote_send_error_bytes(w: WireRemoteSendError) -> RemoteSendError<Vec<u8>> {
        match w {
            WireRemoteSendError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            WireRemoteSendError::ActorStopped => RemoteSendError::ActorStopped,
            WireRemoteSendError::UnknownActor { actor_remote_id } => {
                RemoteSendError::UnknownActor { actor_remote_id: Cow::Owned(actor_remote_id) }
            }
            WireRemoteSendError::UnknownMessage { actor_remote_id, message_remote_id } => {
                RemoteSendError::UnknownMessage {
                    actor_remote_id: Cow::Owned(actor_remote_id),
                    message_remote_id: Cow::Owned(message_remote_id),
                }
            }
            WireRemoteSendError::BadActorType => RemoteSendError::BadActorType,
            WireRemoteSendError::MailboxFull => RemoteSendError::MailboxFull,
            WireRemoteSendError::ReplyTimeout => RemoteSendError::ReplyTimeout,
            WireRemoteSendError::HandlerError(payload) => RemoteSendError::HandlerError(payload),
            WireRemoteSendError::SerializeMessage(s) => RemoteSendError::SerializeMessage(s),
            WireRemoteSendError::DeserializeMessage(s) => RemoteSendError::DeserializeMessage(s),
            WireRemoteSendError::SerializeReply(s) => RemoteSendError::SerializeReply(s),
            WireRemoteSendError::SerializeHandlerError(s) => RemoteSendError::SerializeHandlerError(s),
            WireRemoteSendError::DeserializeHandlerError(s) => RemoteSendError::DeserializeHandlerError(s),
            WireRemoteSendError::SwarmNotBootstrapped => RemoteSendError::SwarmNotBootstrapped,
            WireRemoteSendError::DialFailure => RemoteSendError::DialFailure,
            WireRemoteSendError::NetworkTimeout => RemoteSendError::NetworkTimeout,
            WireRemoteSendError::ConnectionClosed => RemoteSendError::ConnectionClosed,
            WireRemoteSendError::UnsupportedProtocols => RemoteSendError::UnsupportedProtocols,
            WireRemoteSendError::Io(_) => RemoteSendError::Io(None),
        }
    }

    pub(super) fn wire_to_request(w: WireSwarmRequest) -> io::Result<SwarmRequest> {
        Ok(match w {
            WireSwarmRequest::Ask { actor_id, actor_remote_id, message_remote_id, payload, mailbox_timeout, reply_timeout, immediate } => {
                SwarmRequest::Ask {
                    actor_id: wire_to_actor_id(actor_id)?,
                    actor_remote_id: Cow::Owned(actor_remote_id),
                    message_remote_id: Cow::Owned(message_remote_id),
                    payload,
                    mailbox_timeout,
                    reply_timeout,
                    immediate,
                }
            }
            WireSwarmRequest::Tell { actor_id, actor_remote_id, message_remote_id, payload, mailbox_timeout, immediate } => {
                SwarmRequest::Tell {
                    actor_id: wire_to_actor_id(actor_id)?,
                    actor_remote_id: Cow::Owned(actor_remote_id),
                    message_remote_id: Cow::Owned(message_remote_id),
                    payload,
                    mailbox_timeout,
                    immediate,
                }
            }
            WireSwarmRequest::Link { actor_id, actor_remote_id, sibling_id, sibling_remote_id } => {
                SwarmRequest::Link {
                    actor_id: wire_to_actor_id(actor_id)?,
                    actor_remote_id: Cow::Owned(actor_remote_id),
                    sibling_id: wire_to_actor_id(sibling_id)?,
                    sibling_remote_id: Cow::Owned(sibling_remote_id),
                }
            }
            WireSwarmRequest::Unlink { actor_id, actor_remote_id, sibling_id } => {
                SwarmRequest::Unlink {
                    actor_id: wire_to_actor_id(actor_id)?,
                    actor_remote_id: Cow::Owned(actor_remote_id),
                    sibling_id: wire_to_actor_id(sibling_id)?,
                }
            }
            WireSwarmRequest::SignalLinkDied { dead_actor_id, notified_actor_id, notified_actor_remote_id, stop_reason } => {
                SwarmRequest::SignalLinkDied {
                    dead_actor_id: wire_to_actor_id(dead_actor_id)?,
                    notified_actor_id: wire_to_actor_id(notified_actor_id)?,
                    notified_actor_remote_id: Cow::Owned(notified_actor_remote_id),
                    stop_reason: wire_to_stop_reason(stop_reason)?,
                }
            }
        })
    }

    pub(super) fn wire_to_response(w: WireSwarmResponse) -> SwarmResponse {
        match w {
            WireSwarmResponse::Ask(r) => SwarmResponse::Ask(match r {
                Ok(payload) => Ok(payload),
                Err(e) => Err(wire_to_remote_send_error_bytes(e)),
            }),
            WireSwarmResponse::Tell(r) => SwarmResponse::Tell(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(wire_to_remote_send_error_infallible(e)),
            }),
            WireSwarmResponse::Link(r) => SwarmResponse::Link(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(wire_to_remote_send_error_infallible(e)),
            }),
            WireSwarmResponse::Unlink(r) => SwarmResponse::Unlink(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(wire_to_remote_send_error_infallible(e)),
            }),
            WireSwarmResponse::SignalLinkDied(r) => SwarmResponse::SignalLinkDied(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(wire_to_remote_send_error_infallible(e)),
            }),
            WireSwarmResponse::OutboundFailure(e) => {
                SwarmResponse::OutboundFailure(wire_to_remote_send_error_infallible(e))
            }
        }
    }
}

#[cfg(feature = "rkyv-codec")]
fn encode_swarm_request(req: &SwarmRequest) -> io::Result<Vec<u8>> {
    let wire = rkyv_wire::request_to_wire(req);
    rkyv::to_bytes::<rkyv::rancor::Error>(&wire)
        .map(|v| v.to_vec())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

#[cfg(feature = "rkyv-codec")]
fn decode_swarm_request(data: &[u8]) -> io::Result<SwarmRequest> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(data.len());
    aligned.extend_from_slice(data);
    let wire: rkyv_wire::WireSwarmRequest =
        rkyv::from_bytes::<rkyv_wire::WireSwarmRequest, rkyv::rancor::Error>(&aligned)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    rkyv_wire::wire_to_request(wire)
}

#[cfg(feature = "rkyv-codec")]
fn encode_swarm_response(resp: &SwarmResponse) -> io::Result<Vec<u8>> {
    let wire = rkyv_wire::response_to_wire(resp);
    rkyv::to_bytes::<rkyv::rancor::Error>(&wire)
        .map(|v| v.to_vec())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

#[cfg(feature = "rkyv-codec")]
fn decode_swarm_response(data: &[u8]) -> io::Result<SwarmResponse> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(data.len());
    aligned.extend_from_slice(data);
    let wire: rkyv_wire::WireSwarmResponse =
        rkyv::from_bytes::<rkyv_wire::WireSwarmResponse, rkyv::rancor::Error>(&aligned)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(rkyv_wire::wire_to_response(wire))
}

// ===========================================================================
// Manual binary codec fallback (when `rkyv-codec` is NOT enabled)
// ===========================================================================

#[cfg(not(feature = "rkyv-codec"))]
mod manual_codec {
    use super::*;

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
                RemoteSendError::UnknownMessage { actor_remote_id, message_remote_id } => {
                    self.put_u8(3);
                    self.put_str(actor_remote_id);
                    self.put_str(message_remote_id);
                }
                RemoteSendError::BadActorType => self.put_u8(4),
                RemoteSendError::MailboxFull => self.put_u8(5),
                RemoteSendError::ReplyTimeout => self.put_u8(6),
                RemoteSendError::HandlerError(e) => {
                    self.put_u8(7);
                    self.put_str(&e.to_string());
                }
                RemoteSendError::SerializeMessage(s) => { self.put_u8(8); self.put_str(s); }
                RemoteSendError::DeserializeMessage(s) => { self.put_u8(9); self.put_str(s); }
                RemoteSendError::SerializeReply(s) => { self.put_u8(10); self.put_str(s); }
                RemoteSendError::SerializeHandlerError(s) => { self.put_u8(11); self.put_str(s); }
                RemoteSendError::DeserializeHandlerError(s) => { self.put_u8(12); self.put_str(s); }
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

        fn put_remote_send_error_bytes(&mut self, err: &RemoteSendError<Vec<u8>>) {
            match err {
                RemoteSendError::HandlerError(payload) => {
                    self.put_u8(7);
                    self.put_bytes(payload);
                }
                other => {
                    match other {
                        RemoteSendError::ActorNotRunning => self.put_u8(0),
                        RemoteSendError::ActorStopped => self.put_u8(1),
                        RemoteSendError::UnknownActor { actor_remote_id } => {
                            self.put_u8(2); self.put_str(actor_remote_id);
                        }
                        RemoteSendError::UnknownMessage { actor_remote_id, message_remote_id } => {
                            self.put_u8(3); self.put_str(actor_remote_id); self.put_str(message_remote_id);
                        }
                        RemoteSendError::BadActorType => self.put_u8(4),
                        RemoteSendError::MailboxFull => self.put_u8(5),
                        RemoteSendError::ReplyTimeout => self.put_u8(6),
                        RemoteSendError::HandlerError(_) => unreachable!(),
                        RemoteSendError::SerializeMessage(s) => { self.put_u8(8); self.put_str(s); }
                        RemoteSendError::DeserializeMessage(s) => { self.put_u8(9); self.put_str(s); }
                        RemoteSendError::SerializeReply(s) => { self.put_u8(10); self.put_str(s); }
                        RemoteSendError::SerializeHandlerError(s) => { self.put_u8(11); self.put_str(s); }
                        RemoteSendError::DeserializeHandlerError(s) => { self.put_u8(12); self.put_str(s); }
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
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read_actor_id"));
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
                t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid Option<Duration> tag: {t}"))),
            }
        }

        fn read_bool(&mut self) -> io::Result<bool> {
            Ok(self.read_u8()? != 0)
        }

        fn read_stop_reason(&mut self) -> io::Result<ActorStopReason> {
            match self.read_u8()? {
                0 => Ok(ActorStopReason::Normal),
                1 => Ok(ActorStopReason::Killed),
                2 => Ok(ActorStopReason::Panicked(self.read_panic_error()?)),
                3 => {
                    let id = self.read_actor_id()?;
                    let reason = self.read_stop_reason()?;
                    Ok(ActorStopReason::LinkDied { id, reason: Box::new(reason) })
                }
                #[cfg(feature = "remote")]
                4 => Ok(ActorStopReason::PeerDisconnected),
                t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid ActorStopReason tag: {t}"))),
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
                t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid PanicReason tag: {t}"))),
            }
        }

        fn read_remote_send_error_infallible(&mut self) -> io::Result<RemoteSendError<Infallible>> {
            self.read_remote_send_error_with(|_r| {
                Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected HandlerError for Infallible"))
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
                2 => Ok(RemoteSendError::UnknownActor { actor_remote_id: self.read_cow_str()? }),
                3 => {
                    let a = self.read_cow_str()?;
                    let m = self.read_cow_str()?;
                    Ok(RemoteSendError::UnknownMessage { actor_remote_id: a, message_remote_id: m })
                }
                4 => Ok(RemoteSendError::BadActorType),
                5 => Ok(RemoteSendError::MailboxFull),
                6 => Ok(RemoteSendError::ReplyTimeout),
                7 => handler_error(self),
                8 => Ok(RemoteSendError::SerializeMessage(self.read_string()?)),
                9 => Ok(RemoteSendError::DeserializeMessage(self.read_string()?)),
                10 => Ok(RemoteSendError::SerializeReply(self.read_string()?)),
                11 => Ok(RemoteSendError::SerializeHandlerError(self.read_string()?)),
                12 => Ok(RemoteSendError::DeserializeHandlerError(self.read_string()?)),
                13 => Ok(RemoteSendError::SwarmNotBootstrapped),
                14 => Ok(RemoteSendError::DialFailure),
                15 => Ok(RemoteSendError::NetworkTimeout),
                16 => Ok(RemoteSendError::ConnectionClosed),
                17 => Ok(RemoteSendError::UnsupportedProtocols),
                18 => { let _msg = self.read_string()?; Ok(RemoteSendError::Io(None)) }
                t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid RemoteSendError tag: {t}"))),
            }
        }
    }

    pub(super) fn encode_request(req: &SwarmRequest) -> Vec<u8> {
        let mut buf = Buf::new();
        match req {
            SwarmRequest::Ask { actor_id, actor_remote_id, message_remote_id, payload, mailbox_timeout, reply_timeout, immediate } => {
                buf.put_u8(0);
                buf.put_actor_id(actor_id);
                buf.put_str(actor_remote_id);
                buf.put_str(message_remote_id);
                buf.put_bytes(payload);
                buf.put_opt_duration(mailbox_timeout);
                buf.put_opt_duration(reply_timeout);
                buf.put_bool(*immediate);
            }
            SwarmRequest::Tell { actor_id, actor_remote_id, message_remote_id, payload, mailbox_timeout, immediate } => {
                buf.put_u8(1);
                buf.put_actor_id(actor_id);
                buf.put_str(actor_remote_id);
                buf.put_str(message_remote_id);
                buf.put_bytes(payload);
                buf.put_opt_duration(mailbox_timeout);
                buf.put_bool(*immediate);
            }
            SwarmRequest::Link { actor_id, actor_remote_id, sibling_id, sibling_remote_id } => {
                buf.put_u8(2);
                buf.put_actor_id(actor_id);
                buf.put_str(actor_remote_id);
                buf.put_actor_id(sibling_id);
                buf.put_str(sibling_remote_id);
            }
            SwarmRequest::Unlink { actor_id, actor_remote_id, sibling_id } => {
                buf.put_u8(3);
                buf.put_actor_id(actor_id);
                buf.put_str(actor_remote_id);
                buf.put_actor_id(sibling_id);
            }
            SwarmRequest::SignalLinkDied { dead_actor_id, notified_actor_id, notified_actor_remote_id, stop_reason } => {
                buf.put_u8(4);
                buf.put_actor_id(dead_actor_id);
                buf.put_actor_id(notified_actor_id);
                buf.put_str(notified_actor_remote_id);
                buf.put_stop_reason(stop_reason);
            }
        }
        buf.into_vec()
    }

    pub(super) fn decode_request(data: &[u8]) -> io::Result<SwarmRequest> {
        let mut r = Reader::new(data);
        match r.read_u8()? {
            0 => Ok(SwarmRequest::Ask {
                actor_id: r.read_actor_id()?,
                actor_remote_id: r.read_cow_str()?,
                message_remote_id: r.read_cow_str()?,
                payload: r.read_bytes()?,
                mailbox_timeout: r.read_opt_duration()?,
                reply_timeout: r.read_opt_duration()?,
                immediate: r.read_bool()?,
            }),
            1 => Ok(SwarmRequest::Tell {
                actor_id: r.read_actor_id()?,
                actor_remote_id: r.read_cow_str()?,
                message_remote_id: r.read_cow_str()?,
                payload: r.read_bytes()?,
                mailbox_timeout: r.read_opt_duration()?,
                immediate: r.read_bool()?,
            }),
            2 => Ok(SwarmRequest::Link {
                actor_id: r.read_actor_id()?,
                actor_remote_id: r.read_cow_str()?,
                sibling_id: r.read_actor_id()?,
                sibling_remote_id: r.read_cow_str()?,
            }),
            3 => Ok(SwarmRequest::Unlink {
                actor_id: r.read_actor_id()?,
                actor_remote_id: r.read_cow_str()?,
                sibling_id: r.read_actor_id()?,
            }),
            4 => Ok(SwarmRequest::SignalLinkDied {
                dead_actor_id: r.read_actor_id()?,
                notified_actor_id: r.read_actor_id()?,
                notified_actor_remote_id: r.read_cow_str()?,
                stop_reason: r.read_stop_reason()?,
            }),
            t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid SwarmRequest tag: {t}"))),
        }
    }

    pub(super) fn encode_response(resp: &SwarmResponse) -> Vec<u8> {
        let mut buf = Buf::new();
        match resp {
            SwarmResponse::Ask(result) => {
                buf.put_u8(0);
                match result {
                    Ok(payload) => { buf.put_u8(0); buf.put_bytes(payload); }
                    Err(err) => { buf.put_u8(1); buf.put_remote_send_error_bytes(err); }
                }
            }
            SwarmResponse::Tell(result) => {
                buf.put_u8(1);
                match result {
                    Ok(()) => buf.put_u8(0),
                    Err(err) => { buf.put_u8(1); buf.put_remote_send_error(&err); }
                }
            }
            SwarmResponse::Link(result) => {
                buf.put_u8(2);
                match result {
                    Ok(()) => buf.put_u8(0),
                    Err(err) => { buf.put_u8(1); buf.put_remote_send_error(&err); }
                }
            }
            SwarmResponse::Unlink(result) => {
                buf.put_u8(3);
                match result {
                    Ok(()) => buf.put_u8(0),
                    Err(err) => { buf.put_u8(1); buf.put_remote_send_error(&err); }
                }
            }
            SwarmResponse::SignalLinkDied(result) => {
                buf.put_u8(4);
                match result {
                    Ok(()) => buf.put_u8(0),
                    Err(err) => { buf.put_u8(1); buf.put_remote_send_error(&err); }
                }
            }
            SwarmResponse::OutboundFailure(err) => {
                buf.put_u8(5);
                buf.put_remote_send_error(&err);
            }
        }
        buf.into_vec()
    }

    pub(super) fn decode_response(data: &[u8]) -> io::Result<SwarmResponse> {
        let mut r = Reader::new(data);
        let read_ok_or_err = |r: &mut Reader<'_>, ok: fn() -> SwarmResponse, make_resp: fn(RemoteSendError<Infallible>) -> SwarmResponse| -> io::Result<SwarmResponse> {
            match r.read_u8()? {
                0 => Ok(ok()),
                1 => Ok(make_resp(r.read_remote_send_error_infallible()?)),
                t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid result tag: {t}"))),
            }
        };

        match r.read_u8()? {
            0 => {
                match r.read_u8()? {
                    0 => Ok(SwarmResponse::Ask(Ok(r.read_bytes()?))),
                    1 => Ok(SwarmResponse::Ask(Err(r.read_remote_send_error_bytes()?))),
                    t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid Ask result tag: {t}"))),
                }
            }
            1 => read_ok_or_err(&mut r, || SwarmResponse::Tell(Ok(())), |e| SwarmResponse::Tell(Err(e))),
            2 => read_ok_or_err(&mut r, || SwarmResponse::Link(Ok(())), |e| SwarmResponse::Link(Err(e))),
            3 => read_ok_or_err(&mut r, || SwarmResponse::Unlink(Ok(())), |e| SwarmResponse::Unlink(Err(e))),
            4 => read_ok_or_err(&mut r, || SwarmResponse::SignalLinkDied(Ok(())), |e| SwarmResponse::SignalLinkDied(Err(e))),
            5 => Ok(SwarmResponse::OutboundFailure(r.read_remote_send_error_infallible()?)),
            t => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid SwarmResponse tag: {t}"))),
        }
    }
}

#[cfg(not(feature = "rkyv-codec"))]
fn encode_swarm_request(req: &SwarmRequest) -> io::Result<Vec<u8>> {
    Ok(manual_codec::encode_request(req))
}

#[cfg(not(feature = "rkyv-codec"))]
fn decode_swarm_request(data: &[u8]) -> io::Result<SwarmRequest> {
    manual_codec::decode_request(data)
}

#[cfg(not(feature = "rkyv-codec"))]
fn encode_swarm_response(resp: &SwarmResponse) -> io::Result<Vec<u8>> {
    Ok(manual_codec::encode_response(resp))
}

#[cfg(not(feature = "rkyv-codec"))]
fn decode_swarm_response(data: &[u8]) -> io::Result<SwarmResponse> {
    manual_codec::decode_response(data)
}

#[cfg(test)]
#[cfg(feature = "rkyv-codec")]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn test_peer_id() -> libp2p::PeerId {
        libp2p::PeerId::from_bytes(&[
            0, 32, 77, 249, 14, 119, 133, 11, 205, 96, 61, 232, 63, 206, 126, 234, 204, 60, 241,
            93, 2, 68, 130, 67, 3, 193, 242, 23, 80, 189, 82, 144, 152, 206,
        ])
        .unwrap()
    }

    fn test_actor_id() -> ActorId {
        ActorId::new_with_peer_id(42, test_peer_id())
    }

    // ----- Request round-trips -----

    #[test]
    fn roundtrip_ask_request() {
        let req = SwarmRequest::Ask {
            actor_id: test_actor_id(),
            actor_remote_id: Cow::Borrowed("MyActor"),
            message_remote_id: Cow::Borrowed("Ping"),
            payload: vec![1, 2, 3],
            mailbox_timeout: Some(Duration::from_secs(5)),
            reply_timeout: Some(Duration::from_millis(500)),
            immediate: true,
        };
        let bytes = encode_swarm_request(&req).unwrap();
        let decoded = decode_swarm_request(&bytes).unwrap();

        match decoded {
            SwarmRequest::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            } => {
                assert_eq!(actor_id.sequence_id(), 42);
                assert_eq!(actor_id.peer_id(), Some(&test_peer_id()));
                assert_eq!(actor_remote_id, "MyActor");
                assert_eq!(message_remote_id, "Ping");
                assert_eq!(payload, vec![1, 2, 3]);
                assert_eq!(mailbox_timeout, Some(Duration::from_secs(5)));
                assert_eq!(reply_timeout, Some(Duration::from_millis(500)));
                assert!(immediate);
            }
            other => panic!("expected Ask, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_tell_request() {
        let req = SwarmRequest::Tell {
            actor_id: test_actor_id(),
            actor_remote_id: Cow::Borrowed("Worker"),
            message_remote_id: Cow::Borrowed("DoWork"),
            payload: vec![],
            mailbox_timeout: None,
            immediate: false,
        };
        let bytes = encode_swarm_request(&req).unwrap();
        let decoded = decode_swarm_request(&bytes).unwrap();

        match decoded {
            SwarmRequest::Tell {
                actor_id,
                message_remote_id,
                mailbox_timeout,
                immediate,
                ..
            } => {
                assert_eq!(actor_id.sequence_id(), 42);
                assert_eq!(message_remote_id, "DoWork");
                assert_eq!(mailbox_timeout, None);
                assert!(!immediate);
            }
            other => panic!("expected Tell, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_link_request() {
        let sibling_id = ActorId::new_with_peer_id(99, test_peer_id());
        let req = SwarmRequest::Link {
            actor_id: test_actor_id(),
            actor_remote_id: Cow::Borrowed("A"),
            sibling_id,
            sibling_remote_id: Cow::Borrowed("B"),
        };
        let bytes = encode_swarm_request(&req).unwrap();
        let decoded = decode_swarm_request(&bytes).unwrap();

        match decoded {
            SwarmRequest::Link {
                actor_id,
                sibling_id,
                sibling_remote_id,
                ..
            } => {
                assert_eq!(actor_id.sequence_id(), 42);
                assert_eq!(sibling_id.sequence_id(), 99);
                assert_eq!(sibling_remote_id, "B");
            }
            other => panic!("expected Link, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_unlink_request() {
        let req = SwarmRequest::Unlink {
            actor_id: test_actor_id(),
            actor_remote_id: Cow::Borrowed("A"),
            sibling_id: ActorId::new_with_peer_id(7, test_peer_id()),
        };
        let bytes = encode_swarm_request(&req).unwrap();
        let decoded = decode_swarm_request(&bytes).unwrap();

        match decoded {
            SwarmRequest::Unlink { sibling_id, .. } => {
                assert_eq!(sibling_id.sequence_id(), 7);
            }
            other => panic!("expected Unlink, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_signal_link_died_request() {
        let stop_reason = ActorStopReason::LinkDied {
            id: test_actor_id(),
            reason: Box::new(ActorStopReason::Panicked(PanicError::from_wire(
                "boom".into(),
                PanicReason::HandlerPanic,
            ))),
        };
        let req = SwarmRequest::SignalLinkDied {
            dead_actor_id: test_actor_id(),
            notified_actor_id: ActorId::new_with_peer_id(10, test_peer_id()),
            notified_actor_remote_id: Cow::Borrowed("Watcher"),
            stop_reason,
        };
        let bytes = encode_swarm_request(&req).unwrap();
        let decoded = decode_swarm_request(&bytes).unwrap();

        match decoded {
            SwarmRequest::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                stop_reason,
                ..
            } => {
                assert_eq!(dead_actor_id.sequence_id(), 42);
                assert_eq!(notified_actor_id.sequence_id(), 10);
                match &stop_reason {
                    ActorStopReason::LinkDied { reason, .. } => match reason.as_ref() {
                        ActorStopReason::Panicked(e) => {
                            assert!(e.to_string().contains("boom"));
                        }
                        other => panic!("expected Panicked, got {other:?}"),
                    },
                    other => panic!("expected LinkDied, got {other:?}"),
                }
            }
            other => panic!("expected SignalLinkDied, got {other:?}"),
        }
    }

    // ----- Response round-trips -----

    #[test]
    fn roundtrip_ask_response_ok() {
        let resp = SwarmResponse::Ask(Ok(vec![10, 20, 30]));
        let bytes = encode_swarm_response(&resp).unwrap();
        let decoded = decode_swarm_response(&bytes).unwrap();

        match decoded {
            SwarmResponse::Ask(Ok(payload)) => assert_eq!(payload, vec![10, 20, 30]),
            other => panic!("expected Ask(Ok), got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_ask_response_handler_error() {
        let resp = SwarmResponse::Ask(Err(RemoteSendError::HandlerError(vec![0xDE, 0xAD])));
        let bytes = encode_swarm_response(&resp).unwrap();
        let decoded = decode_swarm_response(&bytes).unwrap();

        match decoded {
            SwarmResponse::Ask(Err(RemoteSendError::HandlerError(payload))) => {
                assert_eq!(payload, vec![0xDE, 0xAD]);
            }
            other => panic!("expected Ask(Err(HandlerError)), got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_tell_response_err() {
        let resp = SwarmResponse::Tell(Err(RemoteSendError::SerializeMessage("bad".into())));
        let bytes = encode_swarm_response(&resp).unwrap();
        let decoded = decode_swarm_response(&bytes).unwrap();

        match decoded {
            SwarmResponse::Tell(Err(RemoteSendError::SerializeMessage(s))) => {
                assert_eq!(s, "bad");
            }
            other => panic!("expected Tell(Err(SerializeMessage)), got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_outbound_failure() {
        let resp = SwarmResponse::OutboundFailure(RemoteSendError::NetworkTimeout);
        let bytes = encode_swarm_response(&resp).unwrap();
        let decoded = decode_swarm_response(&bytes).unwrap();

        assert!(matches!(
            decoded,
            SwarmResponse::OutboundFailure(RemoteSendError::NetworkTimeout)
        ));
    }

    #[test]
    fn roundtrip_all_simple_error_variants() {
        // Test every simple (fieldless) RemoteSendError variant round-trips
        let variants: Vec<RemoteSendError<Infallible>> = vec![
            RemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped,
            RemoteSendError::BadActorType,
            RemoteSendError::MailboxFull,
            RemoteSendError::ReplyTimeout,
            RemoteSendError::SwarmNotBootstrapped,
            RemoteSendError::DialFailure,
            RemoteSendError::NetworkTimeout,
            RemoteSendError::ConnectionClosed,
            RemoteSendError::UnsupportedProtocols,
        ];
        for err in variants {
            let debug_before = format!("{err:?}");
            let resp = SwarmResponse::Tell(Err(err));
            let bytes = encode_swarm_response(&resp).unwrap();
            let decoded = decode_swarm_response(&bytes).unwrap();
            match decoded {
                SwarmResponse::Tell(Err(e)) => {
                    assert_eq!(format!("{e:?}"), debug_before);
                }
                other => panic!("expected Tell(Err(..)), got {other:?}"),
            }
        }
    }

    #[test]
    fn roundtrip_unknown_actor_error() {
        let resp = SwarmResponse::Tell(Err(RemoteSendError::UnknownActor {
            actor_remote_id: Cow::Borrowed("missing"),
        }));
        let bytes = encode_swarm_response(&resp).unwrap();
        let decoded = decode_swarm_response(&bytes).unwrap();

        match decoded {
            SwarmResponse::Tell(Err(RemoteSendError::UnknownActor { actor_remote_id })) => {
                assert_eq!(actor_remote_id, "missing");
            }
            other => panic!("expected UnknownActor, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_unknown_message_error() {
        let resp = SwarmResponse::Tell(Err(RemoteSendError::UnknownMessage {
            actor_remote_id: Cow::Borrowed("actor"),
            message_remote_id: Cow::Borrowed("msg"),
        }));
        let bytes = encode_swarm_response(&resp).unwrap();
        let decoded = decode_swarm_response(&bytes).unwrap();

        match decoded {
            SwarmResponse::Tell(Err(RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            })) => {
                assert_eq!(actor_remote_id, "actor");
                assert_eq!(message_remote_id, "msg");
            }
            other => panic!("expected UnknownMessage, got {other:?}"),
        }
    }
}
