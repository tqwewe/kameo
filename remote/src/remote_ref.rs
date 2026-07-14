//! References to actors registered on other nodes.

use std::{borrow::Cow, fmt, marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use futures::future::BoxFuture;
use kameo::{Reply, message::Message};
use serde::de::DeserializeOwned;
use serde_bytes::ByteBuf;

use crate::{
    dispatch::{DispatchTable, InboundKind},
    error::RemoteSendError,
    id::{NodeId, RemoteActorId},
    messaging::{
        handshake::HandshakeError,
        protocol::{RequestFrame, RequestKind, WireError},
        transport::{ConnectionPool, TransportError},
    },
    remote_actor::{RemoteActor, RemoteMessage},
};

/// A reference to an actor registered on a remote node, obtained via lookup.
///
/// Holding a reference does not affect the actor's lifetime; if the actor stops, sends
/// fail with an error.
///
/// # Ordering
///
/// Messages sent sequentially from one node to one target actor are delivered to the
/// actor's mailbox in send order. Requests to the same target actor are processed one
/// at a time per sending node, so an ask delays subsequent messages to that actor from
/// this node until its reply is sent; requests to different actors or from different
/// nodes are unaffected. Ordering holds within one connection: if the connection fails
/// and is re-established, messages already in flight may interleave with newly sent
/// ones.
pub struct RemoteActorRef<A: RemoteActor> {
    id: RemoteActorId,
    messaging_addr: SocketAddr,
    pool: ConnectionPool,
    /// Set when the actor lives on this node; messages are dispatched in-process
    /// (still serialized, but never touching the network).
    local_dispatch: Option<Arc<DispatchTable>>,
    _marker: PhantomData<fn() -> A>,
}

impl<A: RemoteActor> RemoteActorRef<A> {
    pub(crate) fn new(
        id: RemoteActorId,
        messaging_addr: SocketAddr,
        pool: ConnectionPool,
        local_dispatch: Option<Arc<DispatchTable>>,
    ) -> Self {
        RemoteActorRef {
            id,
            messaging_addr,
            pool,
            local_dispatch,
            _marker: PhantomData,
        }
    }

    /// Returns whether the actor lives on the local node, in which case messages skip
    /// the network and are dispatched in-process.
    pub fn is_local(&self) -> bool {
        self.local_dispatch.is_some()
    }

    /// Returns the remote actor's identity.
    pub fn id(&self) -> &RemoteActorId {
        &self.id
    }

    /// Returns the id of the node the actor lives on.
    pub fn node_id(&self) -> &NodeId {
        &self.id.node_id
    }

    /// Returns the TCP messaging address of the node the actor lives on.
    pub fn messaging_addr(&self) -> SocketAddr {
        self.messaging_addr
    }

    /// Sends a message and waits for a reply.
    ///
    /// The message is taken by reference since it is serialized, never consumed.
    pub fn ask<'a, M>(&'a self, msg: &'a M) -> RemoteAskRequest<'a, A, M>
    where
        A: Message<M>,
        M: RemoteMessage,
    {
        RemoteAskRequest {
            actor_ref: self,
            msg,
            reply_timeout: None,
        }
    }

    /// Sends a message without waiting for a reply.
    ///
    /// The message is taken by reference since it is serialized, never consumed.
    pub fn tell<'a, M>(&'a self, msg: &'a M) -> RemoteTellRequest<'a, A, M>
    where
        A: Message<M>,
        M: RemoteMessage,
    {
        RemoteTellRequest {
            actor_ref: self,
            msg,
        }
    }

    fn request_frame<M: RemoteMessage>(
        &self,
        msg: &M,
        kind: RequestKind,
        reply_timeout_ms: Option<u64>,
    ) -> Result<RequestFrame, rmp_serde::encode::Error> {
        Ok(RequestFrame {
            request_id: None,
            kind,
            target_generation_id: self.id.generation_id,
            target_sequence_id: self.id.sequence_id,
            actor_remote_id: Cow::Borrowed(A::REMOTE_ID),
            message_remote_id: Cow::Borrowed(M::REMOTE_ID),
            reply_timeout_ms,
            payload: ByteBuf::from(rmp_serde::to_vec_named(msg)?),
        })
    }
}

impl<A: RemoteActor> Clone for RemoteActorRef<A> {
    fn clone(&self) -> Self {
        RemoteActorRef {
            id: self.id.clone(),
            messaging_addr: self.messaging_addr,
            pool: self.pool.clone(),
            local_dispatch: self.local_dispatch.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A: RemoteActor> fmt::Debug for RemoteActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteActorRef")
            .field("id", &self.id)
            .field("messaging_addr", &self.messaging_addr)
            .finish()
    }
}

impl<A: RemoteActor> PartialEq for RemoteActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.messaging_addr == other.messaging_addr
    }
}

impl<A: RemoteActor> Eq for RemoteActorRef<A> {}

impl<A: RemoteActor> std::hash::Hash for RemoteActorRef<A> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.messaging_addr.hash(state);
    }
}

/// A pending ask request to a remote actor.
pub struct RemoteAskRequest<'a, A, M>
where
    A: RemoteActor + Message<M>,
    M: RemoteMessage,
{
    actor_ref: &'a RemoteActorRef<A>,
    msg: &'a M,
    reply_timeout: Option<Duration>,
}

impl<'a, A, M> RemoteAskRequest<'a, A, M>
where
    A: RemoteActor + Message<M>,
    M: RemoteMessage,
{
    /// Sets the reply timeout, overriding the node's default.
    pub fn reply_timeout(mut self, duration: Duration) -> Self {
        self.reply_timeout = Some(duration);
        self
    }

    /// Sends the message and waits for the reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>>
    where
        <A::Reply as Reply>::Ok: DeserializeOwned,
        <A::Reply as Reply>::Error: DeserializeOwned,
    {
        let timeout = self
            .reply_timeout
            .unwrap_or_else(|| self.actor_ref.pool.default_reply_timeout());
        let frame = self
            .actor_ref
            .request_frame(self.msg, RequestKind::Ask, Some(timeout.as_millis() as u64))
            .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?;

        if let Some(dispatch) = &self.actor_ref.local_dispatch {
            let kind = InboundKind::Ask {
                reply_timeout: Some(timeout),
            };
            return match dispatch_local(dispatch, frame, kind, Some(timeout)).await {
                Ok(reply) => rmp_serde::from_slice(&reply.unwrap_or_default())
                    .map_err(|err| RemoteSendError::DeserializeReply(err.to_string())),
                Err(err) => Err(map_wire_error(err, decode_handler_error)),
            };
        }

        match self
            .actor_ref
            .pool
            .request(self.actor_ref.messaging_addr, frame, timeout)
            .await
        {
            Ok(bytes) => rmp_serde::from_slice(&bytes)
                .map_err(|err| RemoteSendError::DeserializeReply(err.to_string())),
            Err(err) => Err(map_transport_error(err, decode_handler_error)),
        }
    }
}

impl<'a, A, M> IntoFuture for RemoteAskRequest<'a, A, M>
where
    A: RemoteActor + Message<M>,
    M: RemoteMessage,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    type Output = Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}

/// A pending tell request to a remote actor.
pub struct RemoteTellRequest<'a, A, M>
where
    A: RemoteActor + Message<M>,
    M: RemoteMessage,
{
    actor_ref: &'a RemoteActorRef<A>,
    msg: &'a M,
}

impl<'a, A, M> RemoteTellRequest<'a, A, M>
where
    A: RemoteActor + Message<M>,
    M: RemoteMessage,
{
    /// Sends the message and waits for the receiving node to acknowledge delivery to
    /// the actor's mailbox, matching the semantics of a local `tell(..).send()`.
    ///
    /// The acknowledgement confirms delivery, not processing.
    pub async fn send(self) -> Result<(), RemoteSendError> {
        let timeout = self.actor_ref.pool.default_reply_timeout();
        let frame = self
            .actor_ref
            .request_frame(self.msg, RequestKind::Tell, None)
            .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?;

        if let Some(dispatch) = &self.actor_ref.local_dispatch {
            return dispatch_local(dispatch, frame, InboundKind::Tell, Some(timeout))
                .await
                .map(|_| ())
                .map_err(|err| map_wire_error(err, |_| RemoteSendError::ConnectionClosed));
        }

        self.actor_ref
            .pool
            .request(self.actor_ref.messaging_addr, frame, timeout)
            .await
            .map(|_| ())
            .map_err(|err| {
                // The tell path never produces a handler error.
                map_transport_error(err, |_| RemoteSendError::ConnectionClosed)
            })
    }

    /// Sends the message without waiting for any acknowledgement (at-most-once).
    ///
    /// Returns once the message is queued to the connection (or, for local actors,
    /// delivered to the mailbox); the delivery outcome is only logged, never reported.
    pub async fn send_unacked(self) -> Result<(), RemoteSendError> {
        let frame = self
            .actor_ref
            .request_frame(self.msg, RequestKind::Tell, None)
            .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?;

        if let Some(dispatch) = &self.actor_ref.local_dispatch {
            // Awaited rather than spawned so sequential unacked tells keep their order;
            // no timeout and the outcome only logged, matching the fire-and-forget
            // contract of the remote path.
            if let Err(err) = dispatch_local(dispatch, frame, InboundKind::Tell, None).await {
                tracing::warn!("tell dispatch failed: {err:?}");
            }
            return Ok(());
        }

        self.actor_ref
            .pool
            .enqueue(self.actor_ref.messaging_addr, frame)
            .await
            .map_err(|err| map_transport_error(err, |_| RemoteSendError::ConnectionClosed))
    }
}

impl<'a, A, M> IntoFuture for RemoteTellRequest<'a, A, M>
where
    A: RemoteActor + Message<M>,
    M: RemoteMessage,
{
    type Output = Result<(), RemoteSendError>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}

/// Dispatches a frame to an actor on this node, mirroring what the remote server
/// would do with it.
async fn dispatch_local(
    dispatch: &Arc<DispatchTable>,
    frame: RequestFrame,
    kind: InboundKind,
    timeout: Option<Duration>,
) -> Result<Option<Vec<u8>>, WireError> {
    let handler = dispatch.resolve(&frame)?;
    let fut = handler(frame.payload.into_vec(), kind);
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, fut)
            .await
            .unwrap_or(Err(WireError::ReplyTimeout)),
        None => fut.await,
    }
}

fn decode_handler_error<E: DeserializeOwned>(payload: ByteBuf) -> RemoteSendError<E> {
    match rmp_serde::from_slice(&payload) {
        Ok(err) => RemoteSendError::HandlerError(err),
        Err(err) => RemoteSendError::DeserializeHandlerError(err.to_string()),
    }
}

fn map_transport_error<E>(
    err: TransportError,
    decode_handler_error: impl FnOnce(ByteBuf) -> RemoteSendError<E>,
) -> RemoteSendError<E> {
    match err {
        TransportError::Connect(err) => RemoteSendError::Connect(err),
        TransportError::Handshake(err) => match err {
            HandshakeError::ClusterMismatch => RemoteSendError::ClusterMismatch,
            // Both mean the keys don't match between the two nodes; the rejecting
            // side's log carries the precise reason.
            HandshakeError::AuthRequired | HandshakeError::AuthFailed => {
                RemoteSendError::AuthFailed
            }
            err => RemoteSendError::Handshake(err.to_string()),
        },
        TransportError::ConnectionClosed => RemoteSendError::ConnectionClosed,
        TransportError::NodeShutdown => RemoteSendError::NodeShutdown,
        TransportError::ReplyTimeout => RemoteSendError::ReplyTimeout,
        TransportError::Remote(err) => map_wire_error(err, decode_handler_error),
    }
}

fn map_wire_error<E>(
    err: WireError,
    decode_handler_error: impl FnOnce(ByteBuf) -> RemoteSendError<E>,
) -> RemoteSendError<E> {
    match err {
        WireError::ActorNotRunning => RemoteSendError::ActorNotRunning,
        WireError::ActorStopped => RemoteSendError::ActorStopped,
        WireError::BadActorType => RemoteSendError::BadActorType,
        WireError::MailboxFull => RemoteSendError::MailboxFull,
        WireError::ReplyTimeout => RemoteSendError::ReplyTimeout,
        WireError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        } => RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        },
        WireError::HandlerError(payload) => decode_handler_error(payload),
        WireError::DeserializeMessage(err) => RemoteSendError::DeserializeMessage(err),
        WireError::SerializeReply(err) => RemoteSendError::SerializeReply(err),
        WireError::SerializeHandlerError(err) => RemoteSendError::SerializeHandlerError(err),
    }
}
