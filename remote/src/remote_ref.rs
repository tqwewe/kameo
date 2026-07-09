//! References to actors registered on other nodes.

use std::{fmt, marker::PhantomData, net::SocketAddr, time::Duration};

use futures::future::BoxFuture;
use kameo::{Reply, message::Message};
use serde::de::DeserializeOwned;
use serde_bytes::ByteBuf;

use crate::{
    error::RemoteSendError,
    id::{NodeId, RemoteActorId},
    messaging::{
        protocol::{RequestFrame, WireError},
        transport::{ConnectionPool, TransportError},
    },
    remote_actor::{RemoteActor, RemoteMessage},
};

/// A reference to an actor registered on a remote node, obtained via lookup.
pub struct RemoteActorRef<A: RemoteActor> {
    id: RemoteActorId,
    messaging_addr: SocketAddr,
    pool: ConnectionPool,
    _marker: PhantomData<fn() -> A>,
}

impl<A: RemoteActor> RemoteActorRef<A> {
    pub(crate) fn new(id: RemoteActorId, messaging_addr: SocketAddr, pool: ConnectionPool) -> Self {
        RemoteActorRef {
            id,
            messaging_addr,
            pool,
            _marker: PhantomData,
        }
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
        reply_timeout_ms: Option<u64>,
    ) -> Result<RequestFrame, rmp_serde::encode::Error> {
        Ok(RequestFrame {
            request_id: None,
            target_sequence_id: self.id.sequence_id,
            actor_remote_id: A::REMOTE_ID.to_string(),
            message_remote_id: M::REMOTE_ID.to_string(),
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
            .request_frame(self.msg, Some(timeout.as_millis() as u64))
            .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?;
        match self
            .actor_ref
            .pool
            .ask(self.actor_ref.messaging_addr, frame, timeout)
            .await
        {
            Ok(bytes) => rmp_serde::from_slice(&bytes)
                .map_err(|err| RemoteSendError::DeserializeReply(err.to_string())),
            Err(err) => Err(map_transport_error(err, |payload| {
                match rmp_serde::from_slice(&payload) {
                    Ok(err) => RemoteSendError::HandlerError(err),
                    Err(err) => RemoteSendError::DeserializeHandlerError(err.to_string()),
                }
            })),
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
    /// Sends the message, returning once it is queued to the connection.
    pub async fn send(self) -> Result<(), RemoteSendError> {
        let frame = self
            .actor_ref
            .request_frame(self.msg, None)
            .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?;
        self.actor_ref
            .pool
            .tell(self.actor_ref.messaging_addr, frame)
            .await
            .map_err(|err| {
                // Tells receive no response, so remote errors cannot occur here.
                map_transport_error(err, |_| RemoteSendError::ConnectionClosed)
            })
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

fn map_transport_error<E>(
    err: TransportError,
    decode_handler_error: impl FnOnce(ByteBuf) -> RemoteSendError<E>,
) -> RemoteSendError<E> {
    match err {
        TransportError::Connect(err) => RemoteSendError::Connect(err),
        TransportError::ConnectionClosed => RemoteSendError::ConnectionClosed,
        TransportError::ReplyTimeout => RemoteSendError::ReplyTimeout,
        TransportError::Remote(err) => match err {
            WireError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            WireError::ActorStopped => RemoteSendError::ActorStopped,
            WireError::BadActorType => RemoteSendError::BadActorType,
            WireError::MailboxFull => RemoteSendError::MailboxFull,
            WireError::ReplyTimeout => RemoteSendError::ReplyTimeout,
            WireError::UnknownActor { actor_remote_id } => {
                RemoteSendError::UnknownActor { actor_remote_id }
            }
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
        },
    }
}
