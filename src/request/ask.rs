use futures::{future::BoxFuture, FutureExt};
use std::{
    future::{Future, IntoFuture},
    pin, task,
    time::Duration,
};
use tokio::sync::oneshot;

#[cfg(feature = "remote")]
use crate::{actor, remote};

use crate::{
    actor::{ActorRef, ReplyRecipient},
    error::{self, SendError},
    mailbox::{MailboxSender, Signal},
    message::Message,
    reply::{ReplyError, ReplySender},
    Actor, Reply,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor, waiting for a reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct AskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    actor_ref: &'a ActorRef<A>,
    msg: M,
    mailbox_timeout: Tm,
    reply_timeout: Tr,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, A, M, Tm, Tr> AskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    pub(crate) fn new(
        actor_ref: &'a ActorRef<A>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
        Tr: Default,
    {
        AskRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            reply_timeout: Tr::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }

    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<'a, A, M, WithRequestTimeout, Tr> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> AskRequest<'a, A, M, WithRequestTimeout, Tr> {
        AskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            reply_timeout: self.reply_timeout,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sets the timeout for waiting for a reply from the actor.
    pub fn reply_timeout(self, duration: Duration) -> AskRequest<'a, A, M, Tm, WithRequestTimeout> {
        self.reply_timeout_opt(Some(duration))
    }

    pub(crate) fn reply_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> AskRequest<'a, A, M, Tm, WithRequestTimeout> {
        AskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>
    where
        Tm: Into<Option<Duration>>,
        Tr: Into<Option<Duration>>,
    {
        #[cfg(all(debug_assertions, feature = "tracing"))]
        warn_deadlock(self.actor_ref, "An actor is sending an `ask` request to itself, which will likely lead to a deadlock. To avoid this, use a `tell` request instead.", self.called_at);

        let (reply, rx) = oneshot::channel();
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(reply),
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                match self.mailbox_timeout.into() {
                    Some(timeout) => {
                        tx.send_timeout(signal, timeout).await?;
                    }
                    None => {
                        tx.send(signal).await?;
                    }
                }
                let reply = match self.reply_timeout.into() {
                    Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                    None => rx.await?,
                };
                match reply {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                }
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;

                let reply = match self.reply_timeout.into() {
                    Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                    None => rx.await?,
                };
                match reply {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                }
            }
        }
    }

    /// Enqueues the message into the actors mailbox, returning a pending reply which needs to be awaited.
    ///
    /// The actor will not progress until the pending reply has been awaited or dropped.
    /// This may lead to deadlocks if used incorrectly.
    ///
    /// # Example
    ///
    /// ```
    /// # use kameo::Actor;
    /// #
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// let pending = actor_ref.ask(Msg).enqueue().await?;
    /// // Do some other tasks
    /// let reply = pending.await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn enqueue(self) -> Result<PendingReply<M, A::Reply>, SendError>
    where
        Tm: Into<Option<Duration>> + Send + 'static,
        Tr: Into<Option<Duration>> + Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(reply),
            sent_within_actor: self.actor_ref.is_current(),
        };

        let fut = match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                match self.mailbox_timeout.into() {
                    Some(timeout) => {
                        tx.send_timeout(signal, timeout).await?;
                    }
                    None => {
                        tx.send(signal).await?;
                    }
                }

                async move {
                    let reply = match self.reply_timeout.into() {
                        Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                        None => rx.await?,
                    };
                    match reply {
                        Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                        Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                    }
                }
                .boxed()
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;

                async move {
                    let reply = match self.reply_timeout.into() {
                        Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                        None => rx.await?,
                    };
                    match reply {
                        Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                        Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                    }
                }
                .boxed()
            }
        };

        Ok(PendingReply { fut })
    }
}

impl<A, M, Tm> AskRequest<'_, A, M, Tm, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Sends a message with the reply being sent back to a channel.
    pub async fn forward(
        self,
        sender: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    >
    where
        Tm: Into<Option<Duration>>,
    {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(sender.boxed()),
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                match self.mailbox_timeout.into() {
                    Some(timeout) => {
                        tx.send_timeout(signal, timeout).await?;
                    }
                    None => {
                        tx.send(signal).await?;
                    }
                }
                Ok(())
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;
                Ok(())
            }
        }
    }
}

impl<A, M> AskRequest<'_, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send a message without waiting for mailbox capacity,
    /// with the reply being sent back to a channel.
    #[allow(clippy::type_complexity)]
    pub fn try_forward(
        self,
        sender: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(sender.boxed()),
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                tx.try_send(signal)?;
                Ok(())
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;
                Ok(())
            }
        }
    }
}

impl<'a, A, M, Tr> AskRequest<'a, A, M, WithoutRequestTimeout, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub async fn try_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>
    where
        Tr: Into<Option<Duration>>,
    {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(reply),
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                tx.try_send(signal)?;

                let reply = match self.reply_timeout.into() {
                    Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                    None => rx.await?,
                };
                match reply {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                }
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;

                let reply = match self.reply_timeout.into() {
                    Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                    None => rx.await?,
                };
                match reply {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                }
            }
        }
    }

    /// Tries to enqueue the message into the actors mailbox without waiting for mailbox capacity,
    /// returning a pending reply which needs to be awaited.
    ///
    /// The actor will not progress until the pending reply has been awaited or dropped.
    /// This may lead to deadlocks if used incorrectly.
    ///
    /// # Example
    ///
    /// ```
    /// # use kameo::Actor;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// let pending = actor_ref.ask(Msg).try_enqueue()?;
    /// // Do some other tasks
    /// let reply = pending.await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub fn try_enqueue(self) -> Result<PendingReply<M, A::Reply>, SendError>
    where
        Tr: Into<Option<Duration>> + Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(reply),
            sent_within_actor: self.actor_ref.is_current(),
        };

        let fut = match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                tx.try_send(signal)?;

                async move {
                    let reply = match self.reply_timeout.into() {
                        Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                        None => rx.await?,
                    };
                    match reply {
                        Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                        Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                    }
                }
                .boxed()
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;

                async move {
                    let reply = match self.reply_timeout.into() {
                        Some(timeout) => tokio::time::timeout(timeout, rx).await??,
                        None => rx.await?,
                    };
                    match reply {
                        Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                        Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                    }
                }
                .boxed()
            }
        };

        Ok(PendingReply { fut })
    }
}

impl<'a, A, M> AskRequest<'a, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Sends the message in a blocking context.
    #[allow(clippy::type_complexity)]
    pub fn blocking_send(
        self,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(reply),
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                tx.blocking_send(signal)?;

                match rx.blocking_recv()? {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                }
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;

                match rx.blocking_recv()? {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                }
            }
        }
    }

    /// Sends a message in a blocking context with the reply being sent back to a channel.
    #[allow(clippy::type_complexity)]
    pub fn blocking_forward(
        self,
        sender: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(sender.boxed()),
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                tx.blocking_send(signal)?;
                Ok(())
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;
                Ok(())
            }
        }
    }

    /// Enqueues the message into the actors mailbox in a blocking context,
    /// returning a pending reply which needs to be awaited.
    ///
    /// The actor will not progress until the pending reply has been received or dropped.
    /// This may lead to deadlocks if used incorrectly.
    ///
    /// # Example
    ///
    /// ```
    /// # use kameo::Actor;
    /// #
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// # std::thread::spawn(move || {
    /// # let f = move || {
    /// let pending = actor_ref.ask(Msg).blocking_enqueue()?;
    /// // Do some other tasks
    /// let reply = pending.recv()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # };
    /// # f().unwrap();
    /// # });
    /// # });
    /// ```
    pub fn blocking_enqueue(self) -> Result<BlockingPendingReply<'a, M, A::Reply>, SendError> {
        let (reply, rx) = oneshot::channel();
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: Some(reply),
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                tx.blocking_send(signal)?;

                let f = Box::new(move || match rx.blocking_recv()? {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                });
                Ok(BlockingPendingReply { f })
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal)?;

                let f = Box::new(move || match rx.blocking_recv()? {
                    Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
                    Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
                });
                Ok(BlockingPendingReply { f })
            }
        }
    }
}

impl<'a, A, M, Tm, Tr> IntoFuture for AskRequest<'a, A, M, Tm, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
    Tr: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<<A::Reply as Reply>::Ok, error::SendError<M, <A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

/// A pending reply from a previously enqueued ask request.
///
/// The actor will not progress until this has been awaited or dropped.
///
/// This is returned by [`AskRequest::enqueue`] and [`AskRequest::try_enqueue`].
#[allow(missing_debug_implementations)]
#[must_use = "reply wont be received without awaiting"]
pub struct PendingReply<M, R>
where
    R: Reply,
{
    #[allow(clippy::type_complexity)]
    fut: BoxFuture<'static, Result<R::Ok, SendError<M, R::Error>>>,
}

impl<M, R> Future for PendingReply<M, R>
where
    R: Reply,
{
    type Output = Result<R::Ok, SendError<M, R::Error>>;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

/// A pending reply from a previously enqueued ask request.
///
/// The actor will not progress until this has been awaited or dropped.
///
/// This is returned by [`AskRequest::blocking_enqueue`].
#[allow(missing_debug_implementations)]
#[must_use = "reply wont be received without calling .recv()"]
pub struct BlockingPendingReply<'a, M, R>
where
    R: Reply,
{
    #[allow(clippy::type_complexity)]
    f: Box<dyn FnOnce() -> Result<R::Ok, SendError<M, R::Error>> + 'a>,
}

impl<M, R> BlockingPendingReply<'_, M, R>
where
    R: Reply,
{
    /// Receives the reply in a blocking context.
    pub fn recv(self) -> Result<R::Ok, SendError<M, R::Error>> {
        (self.f)()
    }
}


/// A request to send a message to a typed actor with reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct ReplyRecipientAskRequest<'a, M, Ok, Err, Tm>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    actor_ref: &'a ReplyRecipient<M, Ok, Err>,
    msg: M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, M, Ok, Err, Tm> ReplyRecipientAskRequest<'a, M, Ok, Err, Tm>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    pub(crate) fn new(
        actor_ref: &'a ReplyRecipient<M, Ok, Err>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
    {
        ReplyRecipientAskRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }

    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> ReplyRecipientAskRequest<'a, M, Ok, Err, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> ReplyRecipientAskRequest<'a, M, Ok, Err, WithRequestTimeout> {
        ReplyRecipientAskRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(self) -> Result<Ok, SendError<M, Err>>
    where
        Tm: Into<Option<Duration>>,
    {
        self.actor_ref
            .handler
            .ask(self.msg, self.mailbox_timeout.into())
            .await
    }
}

impl<M, Ok, Err> ReplyRecipientAskRequest<'_, M, Ok, Err, WithoutRequestTimeout>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub async fn try_send(self) -> Result<Ok, SendError<M, Err>> {
        self.actor_ref.handler.try_ask(self.msg).await
    }

    /// Sends the message in a blocking context.
    pub fn blocking_send(self) -> Result<Ok, SendError<M, Err>> {
        self.actor_ref.handler.blocking_ask(self.msg)
    }
}

impl<'a, M, Ok, Err, Tm> IntoFuture for ReplyRecipientAskRequest<'a, M, Ok, Err, Tm>
where
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<Ok, SendError<M, Err>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.actor_ref
            .handler
            .ask(self.msg, self.mailbox_timeout.into())
    }
}

#[cfg(all(debug_assertions, feature = "tracing"))]
fn warn_deadlock<A: Actor>(
    actor_ref: &ActorRef<A>,
    msg: &'static str,
    called_at: &'static std::panic::Location<'static>,
) {
    use tracing::warn;

    if actor_ref.is_current() {
        warn!("At {called_at}, {msg}");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        actor::ActorRef,
        error::{Infallible, SendError},
        mailbox,
        message::{Context, Message},
        Actor,
    };

    #[tokio::test]
    async fn bounded_ask_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));

        assert!(actor_ref.ask(Msg).await?); // Should be a regular MessageSend request
        assert!(actor_ref.ask(Msg).send().await?);
        assert!(actor_ref.ask(Msg).try_send().await?);
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await??
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());

        assert!(actor_ref.ask(Msg).await?); // Should be a regular MessageSend request
        assert!(actor_ref.ask(Msg).send().await?);
        assert!(actor_ref.ask(Msg).try_send().await?);
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await??
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.ask(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.ask(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_mailbox_full() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(Duration::from_secs(10)).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1));
        assert_eq!(actor_ref.tell(Msg).try_send(), Ok(()));
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_mailbox_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1));
        // Mailbox empty, this will succeed
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(10))
                .send()
                .await,
            Ok(())
        );
        // Mailbox still empty, this will add one message to it
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(10))
                .send()
                .await,
            Ok(())
        );
        // Mailbox has one item, this will fail
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .send()
                .await,
            Err(SendError::Timeout(Some(Sleep(Duration::from_millis(100)))))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_reply_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(100));
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(120))
                .send()
                .await,
            Ok(true)
        );
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(90))
                .send()
                .await,
            Err(SendError::Timeout(None))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests_reply_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                state: Self::Args,
                _actor_ref: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(120))
                .send()
                .await,
            Ok(true)
        );
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(90))
                .send()
                .await,
            Err(SendError::Timeout(None))
        );
        actor_ref.kill();

        Ok(())
    }
}
