use std::{borrow::Cow, marker::PhantomData, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{sync::oneshot, time::timeout};

use crate::{
    actor::{ActorRef, BoundedMailbox, RemoteActorRef, Signal, UnboundedMailbox},
    error::{BoxSendError, RemoteSendError, SendError},
    message::{BoxReply, Message},
    remote::{ActorSwarm, RemoteActor, RemoteMessage, SwarmCommand, SwarmReq, SwarmResp},
    reply::ReplySender,
    Actor, Reply,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor, waiting for a reply.
#[allow(missing_debug_implementations)]
pub struct AskRequest<L, Mb, M, Tm, Tr> {
    location: L,
    mailbox_timeout: Tm,
    reply_timeout: Tr,
    phantom: PhantomData<(Mb, M)>,
}

impl<A, M>
    AskRequest<
        LocalAskRequest<A, A::Mailbox>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor,
{
    pub(crate) fn new(actor_ref: &ActorRef<A>, msg: M) -> Self
    where
        A: Message<M>,
        M: Send + 'static,
    {
        let (reply, rx) = oneshot::channel();

        AskRequest {
            location: LocalAskRequest {
                mailbox: actor_ref.mailbox().clone(),
                signal: Signal::Message {
                    message: Box::new(msg),
                    actor_ref: actor_ref.clone(),
                    reply: Some(reply),
                    sent_within_actor: actor_ref.is_current(),
                },
                rx,
            },
            mailbox_timeout: WithoutRequestTimeout,
            reply_timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor,
{
    pub(crate) fn new_remote(actor_ref: &'a RemoteActorRef<A>, msg: &'a M) -> Self {
        AskRequest {
            location: RemoteAskRequest { actor_ref, msg },
            mailbox_timeout: WithoutRequestTimeout,
            reply_timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<A, M, Tm, Tr> AskRequest<LocalAskRequest<A, BoundedMailbox<A>>, BoundedMailbox<A>, M, Tm, Tr>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<
        LocalAskRequest<A, BoundedMailbox<A>>,
        BoundedMailbox<A>,
        M,
        WithRequestTimeout,
        Tr,
    > {
        AskRequest {
            location: self.location,
            mailbox_timeout: WithRequestTimeout(duration),
            reply_timeout: self.reply_timeout,
            phantom: PhantomData,
        }
    }
}

impl<'a, A, M, Tm, Tr> AskRequest<RemoteAskRequest<'a, A, M>, BoundedMailbox<A>, M, Tm, Tr>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<RemoteAskRequest<'a, A, M>, BoundedMailbox<A>, M, WithRequestTimeout, Tr> {
        AskRequest {
            location: self.location,
            mailbox_timeout: WithRequestTimeout(duration),
            reply_timeout: self.reply_timeout,
            phantom: PhantomData,
        }
    }
}

impl<A, Mb, M, Tm, Tr> AskRequest<LocalAskRequest<A, Mb>, Mb, M, Tm, Tr>
where
    A: Actor<Mailbox = Mb>,
{
    /// Sets the timeout for waiting for a reply from the actor.
    pub fn reply_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<LocalAskRequest<A, Mb>, Mb, M, Tm, WithRequestTimeout> {
        AskRequest {
            location: self.location,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

impl<'a, A, Mb, M, Tm, Tr> AskRequest<RemoteAskRequest<'a, A, M>, Mb, M, Tm, Tr>
where
    A: Actor<Mailbox = Mb>,
{
    /// Sets the timeout for waiting for a reply from the actor.
    pub fn reply_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<RemoteAskRequest<'a, A, M>, Mb, M, Tm, WithRequestTimeout> {
        AskRequest {
            location: self.location,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

// Bounded

impl<A, M>
    AskRequest<
        LocalAskRequest<A, BoundedMailbox<A>>,
        BoundedMailbox<A>,
        M,
        WithRequestTimeout,
        WithRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message with the mailbox and reply timeouts set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location
            .mailbox
            .0
            .send_timeout(self.location.signal, self.mailbox_timeout.0)
            .await?;
        match timeout(self.reply_timeout.0, self.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        BoundedMailbox<A>,
        M,
        WithRequestTimeout,
        WithRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message with the mailbox and reply timeouts set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            Some(self.mailbox_timeout.0),
            Some(self.reply_timeout.0),
            false,
        )
        .await
    }
}

impl<A, M>
    AskRequest<
        LocalAskRequest<A, BoundedMailbox<A>>,
        BoundedMailbox<A>,
        M,
        WithRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message with the mailbox timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location
            .mailbox
            .0
            .send_timeout(self.location.signal, self.mailbox_timeout.0)
            .await?;
        match self.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message with the reply being sent back to `tx`.
    pub async fn forward(
        mut self,
        tx: oneshot::Sender<Result<BoxReply, BoxSendError>>,
    ) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        match &mut self.location.signal {
            Signal::Message { reply, .. } => *reply = Some(tx),
            _ => unreachable!("ask requests only support messages"),
        }

        self.location
            .mailbox
            .0
            .send_timeout(self.location.signal, self.mailbox_timeout.0)
            .await?;

        Ok(())
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        BoundedMailbox<A>,
        M,
        WithRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message with the mailbox timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            Some(self.mailbox_timeout.0),
            None,
            false,
        )
        .await
    }
}

impl<A, M>
    AskRequest<
        LocalAskRequest<A, BoundedMailbox<A>>,
        BoundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message with the reply timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.send(self.location.signal).await?;
        match timeout(self.reply_timeout.0, self.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Tries to send the message if the mailbox is not full, waiting for a reply up to the timeout set.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.try_send(self.location.signal)?;
        match timeout(self.reply_timeout.0, self.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        BoundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message with the reply timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            None,
            Some(self.reply_timeout.0),
            false,
        )
        .await
    }

    /// Tries to send the message if the mailbox is not full, waiting for a reply up to the timeout set.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            None,
            Some(self.reply_timeout.0),
            true,
        )
        .await
    }
}

impl<A, M>
    AskRequest<
        LocalAskRequest<A, BoundedMailbox<A>>,
        BoundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.send(self.location.signal).await?;
        match self.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message from outside the async runtime.
    pub fn blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location
            .mailbox
            .0
            .blocking_send(self.location.signal)?;
        match self.location.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Tries to send the message if the mailbox is not full, waiting for a reply.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.try_send(self.location.signal)?;
        match self.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Tries to send the message if the mailbox is not full from outside the async runtime, waiting for a reply.
    pub fn try_blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.try_send(self.location.signal)?;
        match self.location.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message with the reply being sent back to `tx`.
    pub async fn forward(
        mut self,
        tx: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        match &mut self.location.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.box_sender()),
            _ => unreachable!("ask requests only support messages"),
        }

        self.location
            .mailbox
            .0
            .send(self.location.signal)
            .await
            .map_err(|err| match err.0 {
                Signal::Message {
                    message, mut reply, ..
                } => SendError::ActorNotRunning((
                    message.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                    ReplySender::new(reply.take().unwrap()),
                )),
                _ => unreachable!("ask requests only support messages"),
            })
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        BoundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            None,
            None,
            false,
        )
        .await
    }

    /// Tries to send the message if the mailbox is not full, waiting for a reply.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            None,
            None,
            true,
        )
        .await
    }
}

// Unbounded

impl<A, M>
    AskRequest<
        LocalAskRequest<A, UnboundedMailbox<A>>,
        UnboundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithRequestTimeout,
    >
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message with the reply timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.send(self.location.signal)?;
        match timeout(self.reply_timeout.0, self.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        UnboundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithRequestTimeout,
    >
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message with the reply timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            None,
            Some(self.reply_timeout.0),
            false,
        )
        .await
    }
}

impl<A, M>
    AskRequest<
        LocalAskRequest<A, UnboundedMailbox<A>>,
        UnboundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M>,
    M: 'static,
{
    /// Sends the message, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.send(self.location.signal)?;
        match self.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message from outside the async runtime.
    pub fn blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.location.mailbox.0.send(self.location.signal)?;
        match self.location.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message with the reply being sent back to `tx`.
    pub fn forward(
        mut self,
        tx: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        match &mut self.location.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.box_sender()),
            _ => unreachable!("ask requests only support messages"),
        }

        self.location
            .mailbox
            .0
            .send(self.location.signal)
            .map_err(|err| match err.0 {
                Signal::Message {
                    message, mut reply, ..
                } => SendError::ActorNotRunning((
                    message.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                    ReplySender::new(reply.take().unwrap()),
                )),
                _ => unreachable!("ask requests only support messages"),
            })
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        UnboundedMailbox<A>,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    /// Sends the message, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>> {
        remote_ask(
            self.location.actor_ref,
            &self.location.msg,
            None,
            None,
            false,
        )
        .await
    }
}

/// A request to a local actor.
#[allow(missing_debug_implementations)]
pub struct LocalAskRequest<A, Mb>
where
    A: Actor<Mailbox = Mb>,
{
    mailbox: Mb,
    signal: Signal<A>,
    rx: oneshot::Receiver<Result<BoxReply, BoxSendError>>,
}

/// A request to a remote actor.
#[allow(missing_debug_implementations)]
pub struct RemoteAskRequest<'a, A, M>
where
    A: Actor,
{
    actor_ref: &'a RemoteActorRef<A>,
    msg: &'a M,
}

async fn remote_ask<A, M>(
    actor_ref: &RemoteActorRef<A>,
    msg: &M,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    let actor_id = actor_ref.id();
    let (reply_tx, reply_rx) = oneshot::channel();
    actor_ref
        .send_to_swarm(SwarmCommand::Req {
            peer_id: actor_id
                .peer_id()
                .unwrap_or_else(|| *ActorSwarm::get().unwrap().local_peer_id()),
            req: SwarmReq::Ask {
                actor_id,
                actor_remote_id: Cow::Borrowed(<A as RemoteActor>::REMOTE_ID),
                message_remote_id: Cow::Borrowed(<A as RemoteMessage<M>>::REMOTE_ID),
                payload: rmp_serde::to_vec_named(msg)
                    .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?,
                mailbox_timeout,
                reply_timeout,
                immediate,
            },
            reply: reply_tx,
        })
        .await;

    match reply_rx.await.unwrap() {
        SwarmResp::Ask(res) => match res {
            Ok(payload) => Ok(rmp_serde::decode::from_slice(&payload)
                .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?),
            Err(err) => Err(err
                .map_err(|err| match rmp_serde::decode::from_slice(&err) {
                    Ok(err) => RemoteSendError::HandlerError(err),
                    Err(err) => RemoteSendError::DeserializeHandlerError(err.to_string()),
                })
                .flatten()),
        },
        SwarmResp::OutboundFailure(err) => {
            Err(err.map_err(|_| unreachable!("outbound failure doesn't contain handler errors")))
        }
        _ => panic!("unexpected response"),
    }
}
