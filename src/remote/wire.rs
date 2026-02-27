#![allow(missing_docs)]

//! Serializable mirror types for [`SwarmRequest`] / [`SwarmResponse`] fields
//! whose runtime representations contain non-serializable data (`PeerId`,
//! `dyn ReplyError`, `CodecError`, `io::Error`).

use std::{borrow::Cow, io};

use crate::{
    actor::ActorId,
    error::{ActorStopReason, Infallible, PanicError, PanicReason, RemoteSendError},
    remote::codec::CodecError,
};

// ---------------------------------------------------------------------------
// WireActorId
// ---------------------------------------------------------------------------

/// Serializable form of [`ActorId`] — stores `PeerId` as raw bytes.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct WireActorId {
    pub sequence_id: u64,
    pub peer_id_bytes: Vec<u8>,
}

impl WireActorId {
    pub fn from_runtime(id: &ActorId) -> Self {
        WireActorId {
            sequence_id: id.sequence_id(),
            peer_id_bytes: id.to_bytes()[8..].to_vec(),
        }
    }

    pub fn into_runtime(self) -> io::Result<ActorId> {
        let mut bytes = self.sequence_id.to_le_bytes().to_vec();
        bytes.extend_from_slice(&self.peer_id_bytes);
        ActorId::from_bytes(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

// ---------------------------------------------------------------------------
// WireActorStopReason
// ---------------------------------------------------------------------------

/// The terminal (non-`LinkDied`) reason an actor stopped.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub enum WireStopReasonLeaf {
    Normal,
    Killed,
    Panicked {
        display: String,
        reason: PanicReason,
    },
    PeerDisconnected,
}

/// Serializable form of [`ActorStopReason`].
///
/// `LinkDied` chains are flattened: `link_chain` holds the actor IDs from
/// outermost to innermost, and `leaf` holds the terminal reason.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct WireActorStopReason {
    pub link_chain: Vec<WireActorId>,
    pub leaf: WireStopReasonLeaf,
}

impl WireActorStopReason {
    pub fn from_runtime(r: &ActorStopReason) -> Self {
        let mut link_chain = Vec::new();
        let mut current = r;
        loop {
            match current {
                ActorStopReason::LinkDied { id, reason } => {
                    link_chain.push(WireActorId::from_runtime(id));
                    current = reason;
                }
                ActorStopReason::Normal => {
                    break WireActorStopReason {
                        link_chain,
                        leaf: WireStopReasonLeaf::Normal,
                    };
                }
                ActorStopReason::Killed => {
                    break WireActorStopReason {
                        link_chain,
                        leaf: WireStopReasonLeaf::Killed,
                    };
                }
                ActorStopReason::Panicked(e) => {
                    break WireActorStopReason {
                        link_chain,
                        leaf: WireStopReasonLeaf::Panicked {
                            display: e.to_string(),
                            reason: e.reason(),
                        },
                    };
                }
                ActorStopReason::PeerDisconnected => {
                    break WireActorStopReason {
                        link_chain,
                        leaf: WireStopReasonLeaf::PeerDisconnected,
                    };
                }
            }
        }
    }

    pub fn into_runtime(self) -> io::Result<ActorStopReason> {
        let mut reason = match self.leaf {
            WireStopReasonLeaf::Normal => ActorStopReason::Normal,
            WireStopReasonLeaf::Killed => ActorStopReason::Killed,
            WireStopReasonLeaf::Panicked { display, reason } => {
                ActorStopReason::Panicked(PanicError::from_wire(display, reason))
            }
            WireStopReasonLeaf::PeerDisconnected => ActorStopReason::PeerDisconnected,
        };
        for wire_id in self.link_chain.into_iter().rev() {
            reason = ActorStopReason::LinkDied {
                id: wire_id.into_runtime()?,
                reason: Box::new(reason),
            };
        }
        Ok(reason)
    }
}

// ---------------------------------------------------------------------------
// WireRemoteSendError
// ---------------------------------------------------------------------------

/// Serializable form of [`RemoteSendError`].
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub enum WireRemoteSendError {
    ActorNotRunning,
    ActorStopped,
    UnknownActor {
        actor_remote_id: String,
    },
    UnknownMessage {
        actor_remote_id: String,
        message_remote_id: String,
    },
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

impl WireRemoteSendError {
    pub fn from_infallible(e: &RemoteSendError<Infallible>) -> Self {
        Self::from_remote_send_error(e, |e| match *e {})
    }

    pub fn from_bytes_error(e: &RemoteSendError<Vec<u8>>) -> Self {
        Self::from_remote_send_error(e, |payload| {
            WireRemoteSendError::HandlerError(payload.clone())
        })
    }

    pub fn into_infallible(self) -> RemoteSendError<Infallible> {
        self.into_remote_send_error(|_| RemoteSendError::ActorNotRunning)
    }

    pub fn into_bytes_error(self) -> RemoteSendError<Vec<u8>> {
        self.into_remote_send_error(RemoteSendError::HandlerError)
    }

    fn from_remote_send_error<E>(e: &RemoteSendError<E>, handler: impl FnOnce(&E) -> Self) -> Self {
        match e {
            RemoteSendError::ActorNotRunning => WireRemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped => WireRemoteSendError::ActorStopped,
            RemoteSendError::UnknownActor { actor_remote_id } => {
                WireRemoteSendError::UnknownActor {
                    actor_remote_id: actor_remote_id.to_string(),
                }
            }
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => WireRemoteSendError::UnknownMessage {
                actor_remote_id: actor_remote_id.to_string(),
                message_remote_id: message_remote_id.to_string(),
            },
            RemoteSendError::BadActorType => WireRemoteSendError::BadActorType,
            RemoteSendError::MailboxFull => WireRemoteSendError::MailboxFull,
            RemoteSendError::ReplyTimeout => WireRemoteSendError::ReplyTimeout,
            RemoteSendError::HandlerError(e) => handler(e),
            RemoteSendError::SerializeMessage(e) => {
                WireRemoteSendError::SerializeMessage(e.to_string())
            }
            RemoteSendError::DeserializeMessage(e) => {
                WireRemoteSendError::DeserializeMessage(e.to_string())
            }
            RemoteSendError::SerializeReply(e) => {
                WireRemoteSendError::SerializeReply(e.to_string())
            }
            RemoteSendError::SerializeHandlerError(e) => {
                WireRemoteSendError::SerializeHandlerError(e.to_string())
            }
            RemoteSendError::DeserializeHandlerError(e) => {
                WireRemoteSendError::DeserializeHandlerError(e.to_string())
            }
            RemoteSendError::SwarmNotBootstrapped => WireRemoteSendError::SwarmNotBootstrapped,
            RemoteSendError::DialFailure => WireRemoteSendError::DialFailure,
            RemoteSendError::NetworkTimeout => WireRemoteSendError::NetworkTimeout,
            RemoteSendError::ConnectionClosed => WireRemoteSendError::ConnectionClosed,
            RemoteSendError::UnsupportedProtocols => WireRemoteSendError::UnsupportedProtocols,
            RemoteSendError::Io(e) => WireRemoteSendError::Io(e.as_ref().map(|e| e.to_string())),
        }
    }

    fn into_remote_send_error<E>(
        self,
        handler: impl FnOnce(Vec<u8>) -> RemoteSendError<E>,
    ) -> RemoteSendError<E> {
        match self {
            WireRemoteSendError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            WireRemoteSendError::ActorStopped => RemoteSendError::ActorStopped,
            WireRemoteSendError::UnknownActor { actor_remote_id } => {
                RemoteSendError::UnknownActor {
                    actor_remote_id: Cow::Owned(actor_remote_id),
                }
            }
            WireRemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => RemoteSendError::UnknownMessage {
                actor_remote_id: Cow::Owned(actor_remote_id),
                message_remote_id: Cow::Owned(message_remote_id),
            },
            WireRemoteSendError::BadActorType => RemoteSendError::BadActorType,
            WireRemoteSendError::MailboxFull => RemoteSendError::MailboxFull,
            WireRemoteSendError::ReplyTimeout => RemoteSendError::ReplyTimeout,
            WireRemoteSendError::HandlerError(payload) => handler(payload),
            WireRemoteSendError::SerializeMessage(s) => {
                RemoteSendError::SerializeMessage(CodecError::new(s))
            }
            WireRemoteSendError::DeserializeMessage(s) => {
                RemoteSendError::DeserializeMessage(CodecError::new(s))
            }
            WireRemoteSendError::SerializeReply(s) => {
                RemoteSendError::SerializeReply(CodecError::new(s))
            }
            WireRemoteSendError::SerializeHandlerError(s) => {
                RemoteSendError::SerializeHandlerError(CodecError::new(s))
            }
            WireRemoteSendError::DeserializeHandlerError(s) => {
                RemoteSendError::DeserializeHandlerError(CodecError::new(s))
            }
            WireRemoteSendError::SwarmNotBootstrapped => RemoteSendError::SwarmNotBootstrapped,
            WireRemoteSendError::DialFailure => RemoteSendError::DialFailure,
            WireRemoteSendError::NetworkTimeout => RemoteSendError::NetworkTimeout,
            WireRemoteSendError::ConnectionClosed => RemoteSendError::ConnectionClosed,
            WireRemoteSendError::UnsupportedProtocols => RemoteSendError::UnsupportedProtocols,
            WireRemoteSendError::Io(_) => RemoteSendError::Io(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "rkyv-codec")]
mod rkyv_tests {
    use super::*;
    use crate::error::PanicReason;
    use crate::remote::codec::{Decode, Encode};

    #[test]
    fn wire_actor_id_roundtrip() {
        let original = WireActorId {
            sequence_id: 42,
            peer_id_bytes: vec![1, 2, 3, 4, 5],
        };
        let bytes = original.encode().unwrap();
        let decoded = WireActorId::decode(&bytes).unwrap();
        assert_eq!(decoded.sequence_id, 42);
        assert_eq!(decoded.peer_id_bytes, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn wire_stop_reason_normal_roundtrip() {
        let original = WireActorStopReason {
            link_chain: vec![],
            leaf: WireStopReasonLeaf::Normal,
        };
        let bytes = original.encode().unwrap();
        let decoded = WireActorStopReason::decode(&bytes).unwrap();
        assert!(decoded.link_chain.is_empty());
        assert!(matches!(decoded.leaf, WireStopReasonLeaf::Normal));
    }

    #[test]
    fn wire_stop_reason_with_link_chain() {
        let original = WireActorStopReason {
            link_chain: vec![
                WireActorId {
                    sequence_id: 1,
                    peer_id_bytes: vec![10],
                },
                WireActorId {
                    sequence_id: 2,
                    peer_id_bytes: vec![20],
                },
            ],
            leaf: WireStopReasonLeaf::Panicked {
                display: "boom".into(),
                reason: PanicReason::HandlerPanic,
            },
        };
        let bytes = original.encode().unwrap();
        let decoded = WireActorStopReason::decode(&bytes).unwrap();
        assert_eq!(decoded.link_chain.len(), 2);
        assert_eq!(decoded.link_chain[0].sequence_id, 1);
        assert_eq!(decoded.link_chain[1].sequence_id, 2);
        assert!(matches!(
            decoded.leaf,
            WireStopReasonLeaf::Panicked { ref display, reason: PanicReason::HandlerPanic }
            if display == "boom"
        ));
    }

    #[test]
    fn wire_remote_send_error_unit_variant() {
        let original = WireRemoteSendError::ActorNotRunning;
        let bytes = original.encode().unwrap();
        let decoded = WireRemoteSendError::decode(&bytes).unwrap();
        assert!(matches!(decoded, WireRemoteSendError::ActorNotRunning));
    }

    #[test]
    fn wire_remote_send_error_handler_error() {
        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let original = WireRemoteSendError::HandlerError(payload.clone());
        let bytes = original.encode().unwrap();
        let decoded = WireRemoteSendError::decode(&bytes).unwrap();
        match decoded {
            WireRemoteSendError::HandlerError(p) => assert_eq!(p, payload),
            other => panic!("expected HandlerError, got {other:?}"),
        }
    }

    #[test]
    fn wire_remote_send_error_unknown_actor() {
        let original = WireRemoteSendError::UnknownActor {
            actor_remote_id: "my_actor".into(),
        };
        let bytes = original.encode().unwrap();
        let decoded = WireRemoteSendError::decode(&bytes).unwrap();
        match decoded {
            WireRemoteSendError::UnknownActor { actor_remote_id } => {
                assert_eq!(actor_remote_id, "my_actor");
            }
            other => panic!("expected UnknownActor, got {other:?}"),
        }
    }
}
