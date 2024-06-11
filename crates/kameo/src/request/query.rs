use std::{marker::PhantomData, time::Duration};

use tokio::{sync::oneshot, time::timeout};

use crate::{
    actor::{ActorRef, BoundedMailbox, Signal, UnboundedMailbox},
    error::{BoxSendError, SendError},
    message::{BoxReply, Query},
    reply::ReplySender,
    Actor, Reply,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor, waiting for a reply.
#[allow(missing_debug_implementations)]
pub struct QueryRequest<A, Mb, M, Tm, Tr>
where
    A: Actor<Mailbox = Mb>,
{
    mailbox: Mb,
    signal: Signal<A>,
    rx: oneshot::Receiver<Result<BoxReply, BoxSendError>>,
    mailbox_timeout: Tm,
    reply_timeout: Tr,
    phantom: PhantomData<M>,
}

impl<A, M> QueryRequest<A, A::Mailbox, M, WithoutRequestTimeout, WithoutRequestTimeout>
where
    A: Actor,
{
    pub(crate) fn new(actor_ref: &ActorRef<A>, query: M) -> Self
    where
        A: Query<M>,
        M: Send + 'static,
    {
        let (reply, rx) = oneshot::channel();

        QueryRequest {
            mailbox: actor_ref.mailbox().clone(),
            signal: Signal::Query {
                query: Box::new(query),
                actor_ref: actor_ref.clone(),
                reply: Some(reply),
                sent_within_actor: actor_ref.is_current(),
            },
            rx,
            mailbox_timeout: WithoutRequestTimeout,
            reply_timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<A, M, Tm, Tr> QueryRequest<A, BoundedMailbox<A>, M, Tm, Tr>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> QueryRequest<A, BoundedMailbox<A>, M, WithRequestTimeout, Tr> {
        QueryRequest {
            mailbox: self.mailbox,
            signal: self.signal,
            rx: self.rx,
            mailbox_timeout: WithRequestTimeout(duration),
            reply_timeout: self.reply_timeout,
            phantom: PhantomData,
        }
    }
}

impl<A, Mb, M, Tm, Tr> QueryRequest<A, Mb, M, Tm, Tr>
where
    A: Actor<Mailbox = Mb>,
{
    /// Sets the timeout for waiting for a reply from the actor.
    pub fn reply_timeout(
        self,
        duration: Duration,
    ) -> QueryRequest<A, Mb, M, Tm, WithRequestTimeout> {
        QueryRequest {
            mailbox: self.mailbox,
            signal: self.signal,
            rx: self.rx,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

// Bounded

impl<A, M> QueryRequest<A, BoundedMailbox<A>, M, WithRequestTimeout, WithRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Query<M>,
    M: 'static,
{
    /// Sends the message with the mailbox and reply timeouts set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox
            .0
            .send_timeout(self.signal, self.mailbox_timeout.0)
            .await?;
        match timeout(self.reply_timeout.0, self.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
}

impl<A, M> QueryRequest<A, BoundedMailbox<A>, M, WithRequestTimeout, WithoutRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Query<M>,
    M: 'static,
{
    /// Sends the message with the mailbox timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox
            .0
            .send_timeout(self.signal, self.mailbox_timeout.0)
            .await?;
        match self.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message with the reply being sent back to `tx`.
    pub async fn forward(
        mut self,
        tx: oneshot::Sender<Result<BoxReply, BoxSendError>>,
    ) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        match &mut self.signal {
            Signal::Message { reply, .. } => *reply = Some(tx),
            Signal::Query { reply, .. } => *reply = Some(tx),
            _ => unreachable!("ask requests only support messages and queries"),
        }

        self.mailbox
            .0
            .send_timeout(self.signal, self.mailbox_timeout.0)
            .await?;

        Ok(())
    }
}

impl<A, M> QueryRequest<A, BoundedMailbox<A>, M, WithoutRequestTimeout, WithRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Query<M>,
    M: 'static,
{
    /// Sends the message with the reply timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.send(self.signal).await?;
        match timeout(self.reply_timeout.0, self.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Tries to send the message if the mailbox is not full, waiting for a reply up to the timeout set.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.try_send(self.signal)?;
        match timeout(self.reply_timeout.0, self.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
}

impl<A, M> QueryRequest<A, BoundedMailbox<A>, M, WithoutRequestTimeout, WithoutRequestTimeout>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Query<M>,
    M: 'static,
{
    /// Sends the message, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.send(self.signal).await?;
        match self.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message from outside the async runtime.
    pub fn blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.blocking_send(self.signal)?;
        match self.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Tries to send the message if the mailbox is not full, waiting for a reply.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.try_send(self.signal)?;
        match self.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Tries to send the message if the mailbox is not full from outside the async runtime, waiting for a reply.
    pub fn try_blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.try_send(self.signal)?;
        match self.rx.blocking_recv()? {
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
        match &mut self.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.box_sender()),
            Signal::Query { reply, .. } => *reply = Some(tx.box_sender()),
            _ => unreachable!("ask requests only support messages and queries"),
        }

        self.mailbox
            .0
            .send(self.signal)
            .await
            .map_err(|err| match err.0 {
                Signal::Message {
                    message, mut reply, ..
                } => SendError::ActorNotRunning((
                    message.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                    ReplySender::new(reply.take().unwrap()),
                )),
                Signal::Query {
                    query, mut reply, ..
                } => SendError::ActorNotRunning((
                    query.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                    ReplySender::new(reply.take().unwrap()),
                )),
                _ => unreachable!("ask requests only support messages and queries"),
            })
    }
}

// Unbounded

impl<A, M> QueryRequest<A, UnboundedMailbox<A>, M, WithoutRequestTimeout, WithRequestTimeout>
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Query<M>,
    M: 'static,
{
    /// Sends the message with the reply timeout set, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.send(self.signal)?;
        match timeout(self.reply_timeout.0, self.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
}

impl<A, M> QueryRequest<A, UnboundedMailbox<A>, M, WithoutRequestTimeout, WithoutRequestTimeout>
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Query<M>,
    M: 'static,
{
    /// Sends the message, waiting for a reply.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.send(self.signal)?;
        match self.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends the message from outside the async runtime.
    pub fn blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        self.mailbox.0.send(self.signal)?;
        match self.rx.blocking_recv()? {
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
        match &mut self.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.box_sender()),
            Signal::Query { reply, .. } => *reply = Some(tx.box_sender()),
            _ => unreachable!("ask requests only support messages and queries"),
        }

        self.mailbox.0.send(self.signal).map_err(|err| match err.0 {
            Signal::Message {
                message, mut reply, ..
            } => SendError::ActorNotRunning((
                message.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                ReplySender::new(reply.take().unwrap()),
            )),
            Signal::Query {
                query, mut reply, ..
            } => SendError::ActorNotRunning((
                query.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                ReplySender::new(reply.take().unwrap()),
            )),
            _ => unreachable!("ask requests only support messages and queries"),
        })
    }
}
