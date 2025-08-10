use std::{fmt, future::{Future, IntoFuture}, time::Duration};
use futures::{future::BoxFuture, FutureExt};

use crate::{
    actor::{ActorRef, Recipient, WeakActorRef},
    error::{self, SendError},
    message::Message,
    Actor,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor without any reply.
///
/// This can be thought of as "fire and forget".
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting send() method"]
pub struct TellRequest<'a, A, M, Tm = WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    pub(crate) actor_ref: &'a ActorRef<A>,
    pub(crate) msg: M,
    pub(crate) mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    pub(crate) called_at: &'static std::panic::Location<'static>,
}

impl<'a, A, M, Tm> TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Default,
{
    pub(crate) fn new(
        actor_ref: &'a ActorRef<A>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))]
        called_at: &'static std::panic::Location<'static>,
    ) -> Self {
        TellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }
}

impl<'a, A, M, Tm> TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> TellRequest<'a, A, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> TellRequest<'a, A, M, WithRequestTimeout> {
        TellRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }
}

impl<A, M, Tm> TellRequest<'_, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Into<Option<Duration>>,
{
    /// Sends the message to the actor, waiting if the actors mailbox is full.
    ///
    /// If the actors mailbox is full, this will wait until the actor is ready to receive a new message
    /// before returning.
    ///
    /// See [`TellRequest::try_send`] for sending without waiting.
    ///
    /// ## Example
    ///
    /// ```
    /// # use kameo::prelude::*;
    /// # use kameo::error::SendError;
    /// #
    /// # #[derive(Actor, Default)]
    /// # struct Counter {
    /// #     count: i64,
    /// # }
    /// #
    /// # impl Message<i64> for Counter {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: i64, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
    /// #         self.count += msg;
    /// #     }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let counter_ref = Counter::default().spawn();
    /// let count = 42;
    /// counter_ref.tell(count).send().await?;
    /// # Ok::<(), SendError>(())
    /// # });
    /// ```
    pub async fn send(self) -> Result<(), SendError<M>> {
        #[cfg(all(debug_assertions, feature = "tracing"))]
        {
            warn_deadlock(self.actor_ref, "telling an actor risks deadlocking if the actor's mailbox is bounded and is sending a message to the current actor that also uses `send()`.", self.called_at);
        }

        self.actor_ref
            .send_fut(self.msg, self.mailbox_timeout.into())
            .await
    }
}

impl<A, M> TellRequest<'_, A, M, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send the message to the actor, without waiting if the actors mailbox is full.
    ///
    /// If the actors mailbox is full, this will return immediately with an error.
    ///
    /// See [`TellRequest::send`] for sending with blocking.
    ///
    /// ## Example
    ///
    /// ```
    /// # use kameo::prelude::*;
    /// # use kameo::error::SendError;
    /// #
    /// # #[derive(Actor, Default)]
    /// # struct Counter {
    /// #     count: i64,
    /// # }
    /// #
    /// # impl Message<i64> for Counter {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: i64, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
    /// #         self.count += msg;
    /// #     }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// # let counter_ref = Counter::default().spawn();
    /// let count = 42;
    /// counter_ref.tell(count).try_send()?;
    /// # Ok::<(), SendError>(())
    /// # });
    /// ```
    pub fn try_send(self) -> Result<(), SendError<M>> {
        self.actor_ref.try_send(self.msg)
    }

    /// Sends the message to the actor using the blocking runtime.
    ///
    /// This is useful when you need to send messages from synchronous code.
    pub fn blocking_send(self) -> Result<(), SendError<M>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                self.send().await
            })
        })
    }
}

impl<'a, A, M, Tm> IntoFuture for TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<(), SendError<M>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

/// A request to send a message to a weak actor reference without any reply.
///
/// This can be thought of as "fire and forget".
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting send() method"]
pub struct WeakTellRequest<'a, A, M, Tm = WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    pub(crate) actor_ref: &'a WeakActorRef<A>,
    pub(crate) msg: M,
    pub(crate) mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    pub(crate) called_at: &'static std::panic::Location<'static>,
}

impl<'a, A, M, Tm> WeakTellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Default,
{
    pub(crate) fn new(
        actor_ref: &'a WeakActorRef<A>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))]
        called_at: &'static std::panic::Location<'static>,
    ) -> Self {
        WeakTellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }
}

impl<'a, A, M, Tm> WeakTellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> WeakTellRequest<'a, A, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> WeakTellRequest<'a, A, M, WithRequestTimeout> {
        WeakTellRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }
}

impl<A, M, Tm> WeakTellRequest<'_, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Into<Option<Duration>>,
{
    /// Sends the message to the actor, waiting if the actors mailbox is full.
    ///
    /// If the actors mailbox is full, this will wait until the actor is ready to receive a new message
    /// before returning.
    ///
    /// See [`WeakTellRequest::try_send`] for sending without waiting.
    pub async fn send(self) -> Result<(), error::ActorNotRunning> {
        #[cfg(all(debug_assertions, feature = "tracing"))]
        {
            if let Some(actor_ref) = self.actor_ref.upgrade() {
                warn_deadlock(&actor_ref, "telling an actor risks deadlocking if the actor's mailbox is bounded and is sending a message to the current actor that also uses `send()`.", self.called_at);
            }
        }

        let actor_ref = self.actor_ref.upgrade().ok_or(error::ActorNotRunning)?;
        actor_ref
            .send_fut(self.msg, self.mailbox_timeout.into())
            .await
            .map_err(|_| error::ActorNotRunning)
    }
}

impl<A, M> WeakTellRequest<'_, A, M, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send the message to the actor, without waiting if the actors mailbox is full.
    ///
    /// If the actors mailbox is full, this will return immediately with an error.
    ///
    /// See [`WeakTellRequest::send`] for sending with blocking.
    pub fn try_send(self) -> Result<(), SendError<M>> {
        let Some(actor_ref) = self.actor_ref.upgrade() else {
            return Err(SendError::ActorNotRunning(self.msg));
        };
        actor_ref.try_send(self.msg)
    }
}

/// A request to send a message to a typed actor without any reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting send() method"]
pub struct RecipientTellRequest<'a, M, Tm = WithoutRequestTimeout>
where
    M: Send + 'static,
{
    actor_ref: &'a Recipient<M>,
    msg: M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, M, Tm> RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
    Tm: Default,
{
    pub(crate) fn new(
        actor_ref: &'a Recipient<M>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))]
        called_at: &'static std::panic::Location<'static>,
    ) -> Self {
        RecipientTellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }
}

impl<'a, M, Tm> RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> RecipientTellRequest<'a, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> RecipientTellRequest<'a, M, WithRequestTimeout> {
        RecipientTellRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }
}

impl<'a, M, Tm> RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
    Tm: Into<Option<Duration>>,
{
    /// Sends the message.
    pub async fn send(self) -> Result<(), SendError<M>> {
        self.actor_ref
            .handler
            .tell(self.msg, self.mailbox_timeout.into())
            .await
    }
}

impl<M> RecipientTellRequest<'_, M, WithoutRequestTimeout>
where
    M: Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub fn try_send(self) -> Result<(), SendError<M>> {
        self.actor_ref.handler.try_tell(self.msg)
    }

    /// Sends the message in a blocking context.
    pub fn blocking_send(self) -> Result<(), SendError<M>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                self.send().await
            })
        })
    }
}

impl<'a, M, Tm> IntoFuture for RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<(), SendError<M>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

/// A trait for abstracting over typed and untyped recipient tell requests.
pub trait RecipientRequest<M>: fmt::Debug
where
    M: Send + 'static,
{
    /// The future returned when sending the message with [`RecipientRequest::send`].
    type Future<'a>: Future<Output = Result<(), SendError<M>>>
    where
        Self: 'a;

    /// Sends the message to the actor.
    fn send(&self, msg: M) -> Self::Future<'_>;

    /// Tries to send the message to the actor without waiting if the actors mailbox is full.
    fn try_send(&self, msg: M) -> Result<(), SendError<M>>;
}

impl<M> RecipientRequest<M> for Recipient<M>
where
    M: Send + 'static,
{
    type Future<'a> = BoxFuture<'a, Result<(), SendError<M>>> where Self: 'a;

    fn send(&self, msg: M) -> Self::Future<'_> {
        Box::pin(async move {
            self.handler.tell(msg, None).await
        })
    }

    fn try_send(&self, msg: M) -> Result<(), SendError<M>> {
        self.handler.try_tell(msg)
    }
}

// TODO: Implement RecipientRequest for BoxActorRef when it's available
// impl<M> RecipientRequest<M> for BoxActorRef
// where
//     M: BoxDebug + Send + 'static,
// {
//     type Future<'a> = BoxFuture<'a, Result<(), SendError>> where Self: 'a;
// 
//     fn send(&self, msg: M) -> Self::Future<'_> {
//         self.send_msg_boxed(Box::new(msg))
//     }
// 
//     fn try_send(&self, msg: M) -> Result<(), SendError> {
//         self.try_send_msg_boxed(Box::new(msg))
//     }
// }


#[cfg(all(debug_assertions, feature = "tracing"))]
fn warn_deadlock<A: Actor>(
    actor_ref: &ActorRef<A>,
    msg: &'static str,
    called_at: &'static std::panic::Location<'static>,
) {
    use tracing::warn;

    use crate::mailbox::MailboxSender;

    match actor_ref.mailbox_sender() {
        MailboxSender::Bounded(_) => {
            if actor_ref.is_current() {
                warn!("At {called_at}, {msg}");
            }
        }
        MailboxSender::Unbounded(_) => {}
    }
}

pub use {
    pub_tell_request_dyn::*,
};





mod pub_tell_request_dyn {
    pub use super::RecipientRequest as DynTellRequest;
}

