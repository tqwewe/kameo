//! Wire protocol types exchanged between nodes over TCP.
//!
//! Frames are length-delimited (4 byte big-endian prefix) and encoded with MessagePack.

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

/// A single frame on the wire.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Frame {
    Request(RequestFrame),
    Response(ResponseFrame),
}

/// A message sent to an actor on a remote node.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RequestFrame {
    /// `Some(id)` for asks (a response with this id is expected), `None` for tells.
    pub request_id: Option<u64>,
    /// The sequence id of the target actor on the receiving node.
    pub target_sequence_id: u64,
    /// The target actor type's `REMOTE_ID`.
    pub actor_remote_id: String,
    /// The message type's `REMOTE_ID`.
    pub message_remote_id: String,
    /// Reply timeout in milliseconds, asks only.
    pub reply_timeout_ms: Option<u64>,
    /// The MessagePack-encoded message.
    pub payload: ByteBuf,
}

/// A reply to an ask request.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ResponseFrame {
    pub request_id: u64,
    /// The MessagePack-encoded reply, or the error that occurred.
    pub result: Result<ByteBuf, WireError>,
}

/// The serializable error subset that crosses the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum WireError {
    ActorNotRunning,
    ActorStopped,
    BadActorType,
    MailboxFull,
    ReplyTimeout,
    UnknownActor {
        actor_remote_id: String,
    },
    UnknownMessage {
        actor_remote_id: String,
        message_remote_id: String,
    },
    /// The MessagePack-encoded handler error.
    HandlerError(ByteBuf),
    DeserializeMessage(String),
    SerializeReply(String),
    SerializeHandlerError(String),
}

pub(crate) fn encode(frame: &Frame) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(frame)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Frame, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: Frame) -> Frame {
        decode(&encode(&frame).unwrap()).unwrap()
    }

    #[test]
    fn request_frame_round_trip() {
        let frame = round_trip(Frame::Request(RequestFrame {
            request_id: Some(42),
            target_sequence_id: 7,
            actor_remote_id: "my_actor".to_string(),
            message_remote_id: "my_msg".to_string(),
            reply_timeout_ms: Some(30_000),
            payload: ByteBuf::from(vec![1, 2, 3]),
        }));
        let Frame::Request(req) = frame else {
            panic!("expected request frame");
        };
        assert_eq!(req.request_id, Some(42));
        assert_eq!(req.target_sequence_id, 7);
        assert_eq!(req.actor_remote_id, "my_actor");
        assert_eq!(req.message_remote_id, "my_msg");
        assert_eq!(req.reply_timeout_ms, Some(30_000));
        assert_eq!(req.payload.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn tell_frame_round_trip() {
        let frame = round_trip(Frame::Request(RequestFrame {
            request_id: None,
            target_sequence_id: 1,
            actor_remote_id: "a".to_string(),
            message_remote_id: "m".to_string(),
            reply_timeout_ms: None,
            payload: ByteBuf::new(),
        }));
        let Frame::Request(req) = frame else {
            panic!("expected request frame");
        };
        assert_eq!(req.request_id, None);
        assert_eq!(req.reply_timeout_ms, None);
    }

    #[test]
    fn response_ok_round_trip() {
        let frame = round_trip(Frame::Response(ResponseFrame {
            request_id: 9,
            result: Ok(ByteBuf::from(vec![9, 9])),
        }));
        let Frame::Response(res) = frame else {
            panic!("expected response frame");
        };
        assert_eq!(res.request_id, 9);
        assert_eq!(res.result.unwrap().as_ref(), &[9, 9]);
    }

    #[test]
    fn response_errors_round_trip() {
        let errors = vec![
            WireError::ActorNotRunning,
            WireError::ActorStopped,
            WireError::BadActorType,
            WireError::MailboxFull,
            WireError::ReplyTimeout,
            WireError::UnknownActor {
                actor_remote_id: "a".to_string(),
            },
            WireError::UnknownMessage {
                actor_remote_id: "a".to_string(),
                message_remote_id: "m".to_string(),
            },
            WireError::HandlerError(ByteBuf::from(vec![1])),
            WireError::DeserializeMessage("bad".to_string()),
            WireError::SerializeReply("bad".to_string()),
            WireError::SerializeHandlerError("bad".to_string()),
        ];
        for err in errors {
            let frame = round_trip(Frame::Response(ResponseFrame {
                request_id: 1,
                result: Err(err.clone()),
            }));
            let Frame::Response(res) = frame else {
                panic!("expected response frame");
            };
            assert_eq!(res.result.unwrap_err(), err);
        }
    }
}
