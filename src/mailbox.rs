//! A multi-producer, single-consumer queue for sending messages and signals between actors.
//!
//! An actor mailbox is a channel which stores pending messages and signals for an actor to process sequentially.

#[cfg(feature = "async-channel")]
mod async_channel;
#[cfg(feature = "flume")]
mod flume;
mod tokio;

use std::time::Duration;

use dyn_clone::DynClone;
use futures::{future::BoxFuture, FutureExt};

use crate::{
    actor::{ActorId, ActorRef},
    error::{ActorStopReason, SendError},
    message::BoxMessage,
    reply::BoxReplySender,
    Actor,
};

/// Error returned by [`MailboxSender::send`].
#[allow(missing_debug_implementations)]
pub struct MailboxSendError<A: Actor>(pub Signal<A>);

/// Error returned by [`MailboxSender::try_send`].
#[allow(missing_debug_implementations)]
pub enum MailboxTrySendError<A: Actor> {
    /// The data could not be sent on the channel because the channel is currently full and sending would require blocking.
    Full(Signal<A>),
    /// The receive half of the channel was explicitly closed or has been dropped.
    Closed(Signal<A>),
}

/// Error returned by [`MailboxSender::send_timeout`].
#[allow(missing_debug_implementations)]
pub enum MailboxSendTimeoutError<A: Actor> {
    /// The data could not be sent on the channel because the channel is full, and the timeout to send has elapsed.
    Timeout(Option<Signal<A>>),
    /// The receive half of the channel was explicitly closed or has been dropped.
    Closed(Signal<A>),
}

/// Error returned by [`MailboxReceiver::try_recv`].
#[allow(missing_debug_implementations)]
pub enum MailboxTryRecvError {
    /// The channel is currently empty, but the Sender(s) have not yet disconnected, so data may yet become available.
    Empty,
    /// The channel’s sending half has become disconnected, and there will never be any more data received on it.
    Closed,
}

/// Creates a bounded mailbox for communicating between actors with backpressure using tokio's mpsc bounded channel.
///
/// _See tokio's [`mpsc::channel`] docs for more info._
///
/// [`mpsc::channel`]: ::tokio::sync::mpsc::channel
pub fn bounded<A: Actor>(buffer: usize) -> (BoxMailboxSender<A>, BoxMailboxReceiver<A>) {
    let (tx, rx) = ::tokio::sync::mpsc::channel(buffer);
    (BoxMailboxSender::new(tx), BoxMailboxReceiver::new(rx))
}

/// Creates an unbounded mailbox for communicating between actors without backpressure using tokio's mpsc unbounded channel.
///
/// See tokio's [`mpsc::unbounded_channel`] docs for more info.
///
/// [`mpsc::unbounded_channel`]: ::tokio::sync::mpsc::unbounded_channel
pub fn unbounded<A: Actor>() -> (BoxMailboxSender<A>, BoxMailboxReceiver<A>) {
    let (tx, rx) = ::tokio::sync::mpsc::unbounded_channel();
    (BoxMailboxSender::new(tx), BoxMailboxReceiver::new(rx))
}

/// A mailbox sender which sends messages/signals to the associated `MailboxReceiver`.
pub trait MailboxSender<A: Actor>: DynClone + Send + Sync + 'static {
    /// Sends a value, waiting until there is capacity.
    ///
    /// See tokio's [`mpsc::Sender::send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::send`]: ::tokio::sync::mpsc::Sender::send
    /// [`mpsc::UnboundedSender::send`]: ::tokio::sync::mpsc::UnboundedSender::send
    fn send(&self, signal: Signal<A>) -> BoxFuture<'_, Result<(), MailboxSendError<A>>>;

    /// Attempts to immediately send a message on this `Sender`.
    /// Unbounded mailboxes will always have capacity.
    ///
    /// See tokio's [`mpsc::Sender::try_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::try_send`]: ::tokio::sync::mpsc::Sender::try_send
    /// [`mpsc::UnboundedSender::send`]: ::tokio::sync::mpsc::UnboundedSender::send
    #[allow(clippy::result_large_err)]
    fn try_send(&self, signal: Signal<A>) -> Result<(), MailboxTrySendError<A>>;

    /// Sends a value, waiting until there is capacity, but only for a limited time.
    /// Unbounded mailboxes will never need to wait for capacity.
    ///
    /// See tokio's [`mpsc::Sender::try_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::try_send`]: ::tokio::sync::mpsc::Sender::try_send
    /// [`mpsc::UnboundedSender::send`]: ::tokio::sync::mpsc::UnboundedSender::send
    fn send_timeout(
        &self,
        signal: Signal<A>,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<(), MailboxSendTimeoutError<A>>>;

    /// Blocking send to call outside of asynchronous contexts.
    /// Unbounded mailboxes will never block due to unbounded capacity.
    ///
    /// See tokio's [`mpsc::Sender::blocking_send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::blocking_send`]: ::tokio::sync::mpsc::Sender::blocking_send
    /// [`mpsc::UnboundedSender::send`]: ::tokio::sync::mpsc::UnboundedSender::send
    #[allow(clippy::result_large_err)]
    fn blocking_send(&self, signal: Signal<A>) -> Result<(), MailboxSendError<A>>;

    /// Completes when the receiver has dropped.
    ///
    /// See tokio's [`mpsc::Sender::closed`] and [`mpsc::UnboundedSender::closed`] docs for more info.
    ///
    /// [`mpsc::Sender::closed`]: ::tokio::sync::mpsc::Sender::closed
    /// [`mpsc::UnboundedSender::closed`]: ::tokio::sync::mpsc::UnboundedSender::closed
    fn closed(&self) -> BoxFuture<'_, ()>;

    /// Checks if the channel has been closed. This happens when the
    /// [`MailboxReceiver`] is dropped, or when the [`MailboxReceiver::close`] method is
    /// called.
    ///
    /// See tokio's [`mpsc::Sender::is_closed`] and [`mpsc::UnboundedSender::is_closed`] docs for more info.
    ///
    /// [`mpsc::Sender::is_closed`]: ::tokio::sync::mpsc::Sender::is_closed
    /// [`mpsc::UnboundedSender::is_closed`]: ::tokio::sync::mpsc::UnboundedSender::is_closed
    fn is_closed(&self) -> bool;

    /// Returns the current capacity of the channel, if bounded.
    /// Unbounded channels return `None`.
    ///
    /// See tokio's [`mpsc::Sender::capacity`] docs for more info.
    ///
    /// [`mpsc::Sender::capacity`]: ::tokio::sync::mpsc::Sender::capacity
    fn capacity(&self) -> Option<usize>;

    /// Converts the `MailboxSender` to a [`WeakMailboxSender`] that does not count
    /// towards RAII semantics, i.e. if all `Sender` instances of the
    /// channel were dropped and only `WeakMailboxSender` instances remain,
    /// the channel is closed.
    ///
    /// See tokio's [`mpsc::Sender::downgrade`] and [`mpsc::UnboundedSender::downgrade`] docs for more info.
    ///
    /// [`mpsc::Sender::downgrade`]: ::tokio::sync::mpsc::Sender::downgrade
    /// [`mpsc::UnboundedSender::downgrade`]: ::tokio::sync::mpsc::UnboundedSender::downgrade
    fn downgrade(&self) -> BoxWeakMailboxSender<A>;

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Sender::strong_count`] and [`mpsc::UnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::Sender::strong_count`]: ::tokio::sync::mpsc::Sender::strong_count
    /// [`mpsc::UnboundedSender::strong_count`]: ::tokio::sync::mpsc::UnboundedSender::strong_count
    fn strong_count(&self) -> usize;

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Sender::weak_count`] and [`mpsc::UnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::Sender::weak_count`]: ::tokio::sync::mpsc::Sender::weak_count
    /// [`mpsc::UnboundedSender::weak_count`]: ::tokio::sync::mpsc::UnboundedSender::weak_count
    fn weak_count(&self) -> usize;
}

/// A sender which does not prevent the channel from being closed.
pub trait WeakMailboxSender<A: Actor>: DynClone + Send + Sync + 'static {
    /// Tries to convert a `WeakMailboxSender` into a [`MailboxSender`]. This will return `Some`
    /// if there are other `MailboxSender` instances alive and the channel wasn't
    /// previously dropped, otherwise `None` is returned.
    ///
    /// See tokio's [`mpsc::WeakSender::upgrade`] and [`mpsc::WeakUnboundedSender::upgrade`] docs for more info.
    ///
    /// [`mpsc::WeakSender::upgrade`]: ::tokio::sync::mpsc::WeakSender::upgrade
    /// [`mpsc::WeakUnboundedSender::upgrade`]: ::tokio::sync::mpsc::WeakUnboundedSender::upgrade
    fn upgrade(&self) -> Option<BoxMailboxSender<A>>;

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::strong_count`] and [`mpsc::WeakUnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::strong_count`]: ::tokio::sync::mpsc::WeakSender::strong_count
    /// [`mpsc::WeakUnboundedSender::strong_count`]: ::tokio::sync::mpsc::WeakUnboundedSender::strong_count
    fn strong_count(&self) -> usize;

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::weak_count`] and [`mpsc::WeakUnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::weak_count`]: ::tokio::sync::mpsc::WeakSender::weak_count
    /// [`mpsc::WeakUnboundedSender::weak_count`]: ::tokio::sync::mpsc::WeakUnboundedSender::weak_count
    fn weak_count(&self) -> usize;
}

/// A mailbox receiver which receives messages/signals from the associated `MailboxSender`.
pub trait MailboxReceiver<A: Actor>: Send + Sync + 'static {
    /// Receives the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::recv`] and [`mpsc::UnboundedReceiver::recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv`]: ::tokio::sync::mpsc::Receiver::recv
    /// [`mpsc::UnboundedReceiver::recv`]: ::tokio::sync::mpsc::UnboundedReceiver::recv
    fn recv(&mut self) -> BoxFuture<'_, Option<Signal<A>>>;

    /// Tries to receive the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::try_recv`] and [`mpsc::UnboundedReceiver::try_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::try_recv`]: ::tokio::sync::mpsc::Receiver::try_recv
    /// [`mpsc::UnboundedReceiver::try_recv`]: ::tokio::sync::mpsc::UnboundedReceiver::try_recv
    fn try_recv(&mut self) -> Result<Signal<A>, MailboxTryRecvError>;

    /// Blocking receive to call outside of asynchronous contexts.
    ///
    /// See tokio's [`mpsc::Receiver::blocking_recv`] and [`mpsc::UnboundedReceiver::blocking_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::blocking_recv`]: ::tokio::sync::mpsc::Receiver::blocking_recv
    /// [`mpsc::UnboundedReceiver::blocking_recv`]: ::tokio::sync::mpsc::UnboundedReceiver::blocking_recv
    fn blocking_recv(&mut self) -> Option<Signal<A>>;

    /// Closes the receiving half of a channel, without dropping it.
    ///
    /// See tokio's [`mpsc::Receiver::close`] and [`mpsc::UnboundedReceiver::close`] docs for more info.
    ///
    /// [`mpsc::Receiver::close`]: ::tokio::sync::mpsc::Receiver::close
    /// [`mpsc::UnboundedReceiver::close`]: ::tokio::sync::mpsc::UnboundedReceiver::close
    fn close(&mut self);

    /// Checks if a channel is closed.
    ///
    /// See tokio's [`mpsc::Receiver::is_closed`] and [`mpsc::UnboundedReceiver::is_closed`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_closed`]: ::tokio::sync::mpsc::Receiver::is_closed
    /// [`mpsc::UnboundedReceiver::is_closed`]: ::tokio::sync::mpsc::UnboundedReceiver::is_closed
    fn is_closed(&self) -> bool;

    /// Checks if a channel is empty.
    ///
    /// See tokio's [`mpsc::Receiver::is_empty`] and [`mpsc::UnboundedReceiver::is_empty`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_empty`]: ::tokio::sync::mpsc::Receiver::is_empty
    /// [`mpsc::UnboundedReceiver::is_empty`]: ::tokio::sync::mpsc::UnboundedReceiver::is_empty
    fn is_empty(&self) -> bool;

    /// Returns the number of messages in the channel.
    ///
    /// See tokio's [`mpsc::Receiver::len`] and [`mpsc::UnboundedReceiver::len`] docs for more info.
    ///
    /// [`mpsc::Receiver::len`]: ::tokio::sync::mpsc::Receiver::len
    /// [`mpsc::UnboundedReceiver::len`]: ::tokio::sync::mpsc::UnboundedReceiver::len
    fn len(&self) -> usize;

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_strong_count`] and [`mpsc::UnboundedReceiver::sender_strong_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_strong_count`]: ::tokio::sync::mpsc::Receiver::sender_strong_count
    /// [`mpsc::UnboundedReceiver::sender_strong_count`]: ::tokio::sync::mpsc::UnboundedReceiver::sender_strong_count
    fn sender_strong_count(&self) -> usize;

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_weak_count`] and [`mpsc::UnboundedReceiver::sender_weak_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_weak_count`]: ::tokio::sync::mpsc::Receiver::sender_weak_count
    /// [`mpsc::UnboundedReceiver::sender_weak_count`]: ::tokio::sync::mpsc::UnboundedReceiver::sender_weak_count
    fn sender_weak_count(&self) -> usize;
}

/// A boxed [`MailboxSender`].
#[allow(missing_debug_implementations)]
pub struct BoxMailboxSender<A: Actor> {
    inner: Box<dyn MailboxSender<A>>,
}

impl<A: Actor> BoxMailboxSender<A> {
    /// Creates a new mailbox sender.
    pub fn new<T>(tx: T) -> Self
    where
        T: MailboxSender<A>,
    {
        BoxMailboxSender {
            inner: Box::new(tx),
        }
    }
}

impl<A: Actor> MailboxSender<A> for BoxMailboxSender<A> {
    fn send(&self, signal: Signal<A>) -> BoxFuture<'_, Result<(), MailboxSendError<A>>> {
        self.inner.send(signal)
    }

    fn try_send(&self, signal: Signal<A>) -> Result<(), MailboxTrySendError<A>> {
        self.inner.try_send(signal)
    }

    fn send_timeout(
        &self,
        signal: Signal<A>,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<(), MailboxSendTimeoutError<A>>> {
        self.inner.send_timeout(signal, timeout)
    }

    fn blocking_send(&self, signal: Signal<A>) -> Result<(), MailboxSendError<A>> {
        self.inner.blocking_send(signal)
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        self.inner.closed()
    }

    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }

    fn downgrade(&self) -> BoxWeakMailboxSender<A> {
        self.inner.downgrade()
    }

    fn strong_count(&self) -> usize {
        self.inner.strong_count()
    }

    fn weak_count(&self) -> usize {
        self.inner.weak_count()
    }
}

impl<A: Actor> Clone for BoxMailboxSender<A> {
    fn clone(&self) -> Self {
        BoxMailboxSender {
            inner: dyn_clone::clone_box(&*self.inner),
        }
    }
}

/// A boxed [`WeakMailboxSender`].
#[allow(missing_debug_implementations)]
pub struct BoxWeakMailboxSender<A: Actor> {
    inner: Box<dyn WeakMailboxSender<A>>,
}

impl<A: Actor> BoxWeakMailboxSender<A> {
    /// Creates a new weak mailbox sender.
    pub fn new<T>(rx: T) -> Self
    where
        T: WeakMailboxSender<A>,
    {
        BoxWeakMailboxSender {
            inner: Box::new(rx),
        }
    }
}

impl<A: Actor> WeakMailboxSender<A> for BoxWeakMailboxSender<A> {
    fn upgrade(&self) -> Option<BoxMailboxSender<A>> {
        self.inner.upgrade()
    }

    fn strong_count(&self) -> usize {
        self.inner.strong_count()
    }

    fn weak_count(&self) -> usize {
        self.inner.weak_count()
    }
}

impl<A: Actor> Clone for BoxWeakMailboxSender<A> {
    fn clone(&self) -> Self {
        BoxWeakMailboxSender {
            inner: dyn_clone::clone_box(&*self.inner),
        }
    }
}

/// A boxed [`MailboxReceiver`].
#[allow(missing_debug_implementations)]
pub struct BoxMailboxReceiver<A: Actor> {
    inner: Box<dyn MailboxReceiver<A>>,
}

impl<A: Actor> BoxMailboxReceiver<A> {
    /// Creates a new mailbox receiver.
    pub fn new<T>(rx: T) -> Self
    where
        T: MailboxReceiver<A>,
    {
        BoxMailboxReceiver {
            inner: Box::new(rx),
        }
    }
}

impl<A: Actor> MailboxReceiver<A> for BoxMailboxReceiver<A> {
    fn recv(&mut self) -> BoxFuture<'_, Option<Signal<A>>> {
        self.inner.recv()
    }

    fn try_recv(&mut self) -> Result<Signal<A>, MailboxTryRecvError> {
        self.inner.try_recv()
    }

    fn blocking_recv(&mut self) -> Option<Signal<A>> {
        self.inner.blocking_recv()
    }

    fn close(&mut self) {
        self.inner.close()
    }

    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn sender_strong_count(&self) -> usize {
        self.inner.sender_strong_count()
    }

    fn sender_weak_count(&self) -> usize {
        self.inner.sender_weak_count()
    }
}

/// A signal which can be sent to an actors mailbox.
#[allow(missing_debug_implementations)]
pub enum Signal<A: Actor> {
    /// The actor has finished starting up.
    StartupFinished,
    /// A message.
    Message {
        /// The boxed message.
        message: BoxMessage<A>,
        /// The actor ref, to keep the actor from stopping due to RAII semantics.
        actor_ref: ActorRef<A>,
        /// The reply sender.
        reply: Option<BoxReplySender>,
        /// If the message sent from within the actor's tokio task/thread
        sent_within_actor: bool,
    },
    /// A linked actor has died.
    LinkDied {
        /// The dead actor's ID.
        id: ActorId,
        /// The reason the actor stopped.
        reason: ActorStopReason,
    },
    /// Signals the actor to stop.
    Stop,
}

impl<A: Actor> Signal<A> {
    pub(crate) fn downcast_message<M>(self) -> Option<M>
    where
        M: 'static,
    {
        match self {
            Signal::Message { message, .. } => message.as_any().downcast().ok().map(|v| *v),
            _ => None,
        }
    }
}

#[doc(hidden)]
pub trait SignalMailbox: DynClone + Send + Sync {
    fn signal_startup_finished(&self) -> Result<(), SendError>;
    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
}

impl<A> SignalMailbox for BoxMailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        self.inner
            .try_send(Signal::StartupFinished)
            .map_err(|err| match err {
                MailboxTrySendError::Full(_) => SendError::MailboxFull(()),
                MailboxTrySendError::Closed(_) => SendError::ActorNotRunning(()),
            })
    }

    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.inner
                .send(Signal::LinkDied { id, reason })
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.inner
                .send(Signal::Stop)
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }
}

impl<A> SignalMailbox for BoxWeakMailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self.inner.upgrade() {
            Some(tx) => tx.signal_startup_finished(),
            None => Err(SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorId,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.inner.upgrade() {
                Some(tx) => tx.signal_link_died(id, reason).await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.inner.upgrade() {
                Some(tx) => tx.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }
}

dyn_clone::clone_trait_object!(SignalMailbox);
