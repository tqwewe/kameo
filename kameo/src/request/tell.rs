use std::{marker::PhantomData, mem, time::Duration};

use futures::TryFutureExt;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{task::JoinHandle, time::timeout};

use crate::{
    actor::{
        remote::{rpc, RemoteActor, RemoteMessage},
        ActorRef, BoundedMailbox, RemoteActorRef, Signal, UnboundedMailbox,
    },
    error::{RemoteSendError, SendError},
    message::Message,
    Actor, Reply,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor without any reply.
///
/// This can be thought of as "fire and forget".
#[allow(missing_debug_implementations)]
pub struct TellRequest<L, Mb, M, T> {
    location: L,
    timeout: T,
    phantom: PhantomData<(Mb, M)>,
}

impl<A, M> TellRequest<LocalTellRequest<A, A::Mailbox>, A::Mailbox, M, WithoutRequestTimeout>
where
    A: Actor,
{
    pub(crate) fn new(actor_ref: &ActorRef<A>, msg: M) -> Self
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest {
            location: LocalTellRequest {
                mailbox: actor_ref.mailbox().clone(),
                signal: Signal::Message {
                    message: Box::new(msg),
                    actor_ref: actor_ref.clone(),
                    reply: None,
                    sent_within_actor: actor_ref.is_current(),
                },
            },
            timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<'a, A, M> TellRequest<RemoteTellRequest<'a, A, M>, A::Mailbox, M, WithoutRequestTimeout>
where
    A: Actor,
{
    pub(crate) fn new_remote(actor_ref: &'a RemoteActorRef<A>, msg: &'a M) -> Self {
        TellRequest {
            location: RemoteTellRequest { actor_ref, msg },
            timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<A, M, T> TellRequest<LocalTellRequest<A, BoundedMailbox<A>>, BoundedMailbox<A>, M, T>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for a reply from the actor.
    pub fn timeout(
        self,
        duration: Duration,
    ) -> TellRequest<LocalTellRequest<A, BoundedMailbox<A>>, BoundedMailbox<A>, M, WithRequestTimeout>
    {
        TellRequest {
            location: self.location,
            timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

impl<'a, A, M, T> TellRequest<RemoteTellRequest<'a, A, M>, BoundedMailbox<A>, M, T>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for a reply from the actor.
    pub fn timeout(
        self,
        duration: Duration,
    ) -> TellRequest<RemoteTellRequest<'a, A, M>, BoundedMailbox<A>, M, WithRequestTimeout> {
        TellRequest {
            location: self.location,
            timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

impl<A, M>
    TellRequest<LocalTellRequest<A, BoundedMailbox<A>>, BoundedMailbox<A>, M, WithoutRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message.
    pub async fn send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.send(self.location.signal).await?;
        Ok(())
    }

    /// Sends the message from outside the async runtime.
    pub fn blocking_send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.location
            .mailbox
            .0
            .blocking_send(self.location.signal)?;
        Ok(())
    }

    /// Tries to send the message if the mailbox is not full.
    pub fn try_send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.try_send(self.location.signal)?;
        Ok(())
    }

    /// Sends the message after the given delay in the background.
    ///
    /// If reserve is true, then a permit will be reserved in
    /// the actors mailbox before waiting for the delay.
    pub fn delayed_send(
        mut self,
        delay: Duration,
        reserve: bool,
    ) -> JoinHandle<Result<(), SendError<M, <A::Reply as Reply>::Error>>>
    where
        A: 'static,
        M: Send,
    {
        tokio::spawn(async move {
            let permit = match reserve {
                true => Some(self.location.mailbox.0.reserve().await.map_err(|_| {
                    SendError::ActorNotRunning(
                        mem::replace(&mut self.location.signal, Signal::Stop) // Replace signal with a dummy value
                            .downcast_message::<M>()
                            .unwrap(),
                    )
                })?),
                false => None,
            };

            tokio::time::sleep(delay).await;

            match permit {
                Some(permit) => permit.send(self.location.signal),
                None => self.location.mailbox.0.send(self.location.signal).await?,
            }

            Ok(())
        })
    }
}

impl<'a, A, M> TellRequest<RemoteTellRequest<'a, A, M>, BoundedMailbox<A>, M, WithoutRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M> + RemoteActor,
    M: Serialize + RemoteMessage,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message.
    pub async fn send(self) -> Result<(), RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_tell(self.location.actor_ref, &self.location.msg, None, false).await
    }

    /// Tries to send the message if the mailbox is not full.
    pub async fn try_send(self) -> Result<(), RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_tell(self.location.actor_ref, &self.location.msg, None, true).await
    }
}

impl<A, M>
    TellRequest<LocalTellRequest<A, BoundedMailbox<A>>, BoundedMailbox<A>, M, WithRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message with the timeout set.
    pub async fn send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.location
            .mailbox
            .0
            .send_timeout(self.location.signal, self.timeout.0)
            .await?;
        Ok(())
    }

    /// Sends the message after the given delay in the background with the timeout set.
    ///
    /// If reserve is true, then a permit will be reserved in
    /// the actors mailbox before waiting for the delay.
    pub fn delayed_send(
        mut self,
        delay: Duration,
        reserve: bool,
    ) -> JoinHandle<Result<(), SendError<M, <A::Reply as Reply>::Error>>>
    where
        A: 'static,
        M: Send,
    {
        tokio::spawn(async move {
            let permit = match reserve {
                true => {
                    let permit = timeout(
                        self.timeout.0,
                        self.location.mailbox.0.reserve().map_err(|_| {
                            SendError::ActorNotRunning(
                                mem::replace(&mut self.location.signal, Signal::Stop) // Replace signal with a dummy value
                                    .downcast_message::<M>()
                                    .unwrap(),
                            )
                        }),
                    )
                    .await??;
                    Some(permit)
                }
                false => None,
            };

            tokio::time::sleep(delay).await;

            match permit {
                Some(permit) => permit.send(self.location.signal),
                None => {
                    self.location
                        .mailbox
                        .0
                        .send_timeout(self.location.signal, self.timeout.0)
                        .await?
                }
            }

            Ok(())
        })
    }
}

impl<'a, A, M> TellRequest<RemoteTellRequest<'a, A, M>, BoundedMailbox<A>, M, WithRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M> + RemoteActor,
    M: Serialize + RemoteMessage,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message with the timeout set.
    pub async fn send(self) -> Result<(), RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_tell(
            self.location.actor_ref,
            &self.location.msg,
            Some(self.timeout.0),
            false,
        )
        .await
    }
}

impl<A, M>
    TellRequest<
        LocalTellRequest<A, UnboundedMailbox<A>>,
        UnboundedMailbox<A>,
        M,
        WithoutRequestTimeout,
    >
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message.
    pub fn send(self) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.send(self.location.signal)?;
        Ok(())
    }

    /// Sends the message after the given delay in the background.
    pub fn delayed_send(
        self,
        delay: Duration,
    ) -> JoinHandle<Result<(), SendError<M, <A::Reply as Reply>::Error>>>
    where
        A: 'static,
        M: Send,
    {
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            self.location.mailbox.0.send(self.location.signal)?;
            Ok(())
        })
    }
}

impl<'a, A, M>
    TellRequest<RemoteTellRequest<'a, A, M>, UnboundedMailbox<A>, M, WithoutRequestTimeout>
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M> + RemoteActor,
    M: Serialize + RemoteMessage,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message.
    pub async fn send(self) -> Result<(), RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_tell(self.location.actor_ref, &self.location.msg, None, false).await
    }
}

/// A request to a local actor.
#[allow(missing_debug_implementations)]
pub struct LocalTellRequest<A, Mb>
where
    A: Actor<Mailbox = Mb>,
{
    mailbox: Mb,
    signal: Signal<A>,
}

/// A request to a remote actor.
#[allow(missing_debug_implementations)]
pub struct RemoteTellRequest<'a, A, M>
where
    A: Actor,
{
    actor_ref: &'a RemoteActorRef<A>,
    msg: &'a M,
}

async fn remote_tell<A, M>(
    actor_ref: &RemoteActorRef<A>,
    msg: &M,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M> + RemoteActor,
    M: RemoteMessage + Serialize,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    let rpc::TellResponse { result } = actor_ref
        .client()
        .tell(rpc::ActorMessage {
            actor_id: actor_ref.id().raw(),
            actor_name: A::REMOTE_ID.to_string(),
            message_name: M::REMOTE_ID.to_string(),
            payload: rmp_serde::to_vec_named(msg)
                .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?,
            mailbox_timeout: mailbox_timeout
                .map(|t| t.as_millis().try_into().unwrap_or(i64::MAX))
                .unwrap_or(-1),
            reply_timeout: -1,
            immediate,
        })
        .await?
        .into_inner();

    match result.unwrap() {
        rpc::tell_response::Result::Ok(()) => Ok(()),
        rpc::tell_response::Result::Error(rpc::RemoteSendError { error }) => {
            Err(match error.unwrap() {
                rpc::remote_send_error::Error::ActorNotRunning(rpc::ActorNotRunning {}) => {
                    RemoteSendError::ActorNotRunning
                }
                rpc::remote_send_error::Error::ActorStopped(rpc::ActorStopped {}) => {
                    RemoteSendError::ActorStopped
                }
                rpc::remote_send_error::Error::UnknownActor(rpc::UnknownActor { actor_name }) => {
                    RemoteSendError::UnknownActor { actor_name }
                }
                rpc::remote_send_error::Error::UnknownMessage(rpc::UnknownMessage {
                    actor_name,
                    message_name,
                }) => RemoteSendError::UnknownMessage {
                    actor_name,
                    message_name,
                },
                rpc::remote_send_error::Error::BadActorType(rpc::BadActorType {}) => {
                    RemoteSendError::BadActorType
                }
                rpc::remote_send_error::Error::MailboxFull(rpc::MailboxFull {}) => {
                    RemoteSendError::MailboxFull
                }
                rpc::remote_send_error::Error::Timeout(rpc::Timeout {}) => RemoteSendError::Timeout,
                rpc::remote_send_error::Error::HandlerError(rpc::HandlerError { payload }) => {
                    RemoteSendError::HandlerError(
                        rmp_serde::decode::from_slice(&payload).map_err(|err| {
                            RemoteSendError::DeserializeHandlerError(err.to_string())
                        })?,
                    )
                }
                rpc::remote_send_error::Error::DeserializeMessage(rpc::DeserializeMessage {
                    err,
                }) => RemoteSendError::DeserializeMessage(err),
                rpc::remote_send_error::Error::SerializeReply(rpc::SerializeReply { err }) => {
                    RemoteSendError::SerializeReply(err)
                }
                rpc::remote_send_error::Error::SerializeHandlerError(
                    rpc::SerializeHandlerError { err },
                ) => RemoteSendError::SerializeHandlerError(err),
            })
        }
    }
}
