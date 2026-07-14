//! Error types for bootstrapping, the registry, and remote messaging.

use std::{convert::Infallible, io};

/// An error which can occur when bootstrapping a [`RemoteNode`](crate::RemoteNode).
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// Failed to bind the gossip or messaging listener.
    #[error("failed to bind listener: {0}")]
    Bind(#[from] io::Error),
    /// The gossip layer failed to start.
    #[error("chitchat: {0}")]
    Chitchat(anyhow::Error),
}

/// An error which can occur when shutting down a [`RemoteNode`](crate::RemoteNode).
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    /// The gossip layer failed to shut down cleanly.
    #[error("chitchat: {0}")]
    Chitchat(anyhow::Error),
}

/// An error which can occur when registering or looking up actors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    /// The actor name is empty or contains a `:`.
    #[error("invalid actor name {0:?}: must be non-empty and must not contain ':'")]
    InvalidName(String),
    /// The node has been shut down; its registry can no longer be used.
    #[error("node has been shut down")]
    NodeShutdown,
    /// The name is registered, but under a different actor type.
    #[error("actor registered as {name:?} is not of type {expected_remote_id:?}")]
    BadActorType {
        /// The name that was looked up.
        name: String,
        /// The `REMOTE_ID` of the actor type the lookup expected.
        expected_remote_id: &'static str,
    },
}

/// An error which can occur when sending a message to a remote actor.
#[derive(Debug, thiserror::Error)]
pub enum RemoteSendError<E = Infallible> {
    /// The remote actor is no longer running.
    #[error("actor not running")]
    ActorNotRunning,
    /// The remote actor stopped before processing the message.
    #[error("actor stopped before reply")]
    ActorStopped,
    /// The remote actor does not accept this message type remotely.
    #[error("actor {actor_remote_id:?} does not handle remote message {message_remote_id:?}")]
    UnknownMessage {
        /// The actor `REMOTE_ID` sent in the request.
        actor_remote_id: String,
        /// The message `REMOTE_ID` sent in the request.
        message_remote_id: String,
    },
    /// The registered actor is of a different type than the request expected.
    #[error("bad actor type")]
    BadActorType,
    /// The remote actor's mailbox is full.
    #[error("mailbox full")]
    MailboxFull,
    /// Timed out waiting for a reply.
    #[error("timed out waiting for reply")]
    ReplyTimeout,
    /// The remote actor's handler returned an error.
    #[error("handler error: {0:?}")]
    HandlerError(E),
    /// Failed to serialize the outgoing message.
    #[error("failed to serialize message: {0}")]
    SerializeMessage(String),
    /// The remote node failed to deserialize the message.
    #[error("failed to deserialize message: {0}")]
    DeserializeMessage(String),
    /// The remote node failed to serialize the reply.
    #[error("failed to serialize reply: {0}")]
    SerializeReply(String),
    /// Failed to deserialize the reply.
    #[error("failed to deserialize reply: {0}")]
    DeserializeReply(String),
    /// The remote node failed to serialize the handler error.
    #[error("failed to serialize handler error: {0}")]
    SerializeHandlerError(String),
    /// Failed to deserialize the handler error.
    #[error("failed to deserialize handler error: {0}")]
    DeserializeHandlerError(String),
    /// Failed to connect to the remote node.
    #[error("failed to connect to remote node: {0}")]
    Connect(#[source] io::Error),
    /// The remote node belongs to a different cluster.
    #[error("cluster id mismatch")]
    ClusterMismatch,
    /// Cluster key authentication with the remote node failed: the keys do not match,
    /// or only one side has a key configured.
    #[error("cluster authentication failed")]
    AuthFailed,
    /// The connection handshake with the remote node failed.
    #[error("handshake failed: {0}")]
    Handshake(String),
    /// The connection closed before a reply was received.
    #[error("connection closed before a reply was received")]
    ConnectionClosed,
    /// The local node has been shut down; its remote refs can no longer send.
    #[error("node has been shut down")]
    NodeShutdown,
}

impl<E> RemoteSendError<E> {
    /// Maps the handler error type.
    pub fn map_err<F>(self, op: impl FnOnce(E) -> F) -> RemoteSendError<F> {
        match self {
            RemoteSendError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped => RemoteSendError::ActorStopped,
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            },
            RemoteSendError::BadActorType => RemoteSendError::BadActorType,
            RemoteSendError::MailboxFull => RemoteSendError::MailboxFull,
            RemoteSendError::ReplyTimeout => RemoteSendError::ReplyTimeout,
            RemoteSendError::HandlerError(err) => RemoteSendError::HandlerError(op(err)),
            RemoteSendError::SerializeMessage(err) => RemoteSendError::SerializeMessage(err),
            RemoteSendError::DeserializeMessage(err) => RemoteSendError::DeserializeMessage(err),
            RemoteSendError::SerializeReply(err) => RemoteSendError::SerializeReply(err),
            RemoteSendError::DeserializeReply(err) => RemoteSendError::DeserializeReply(err),
            RemoteSendError::SerializeHandlerError(err) => {
                RemoteSendError::SerializeHandlerError(err)
            }
            RemoteSendError::DeserializeHandlerError(err) => {
                RemoteSendError::DeserializeHandlerError(err)
            }
            RemoteSendError::Connect(err) => RemoteSendError::Connect(err),
            RemoteSendError::ClusterMismatch => RemoteSendError::ClusterMismatch,
            RemoteSendError::AuthFailed => RemoteSendError::AuthFailed,
            RemoteSendError::Handshake(err) => RemoteSendError::Handshake(err),
            RemoteSendError::ConnectionClosed => RemoteSendError::ConnectionClosed,
            RemoteSendError::NodeShutdown => RemoteSendError::NodeShutdown,
        }
    }
}
