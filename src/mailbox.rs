//! A multi-producer, single-consumer queue for sending messages and signals between actors.
//!
//! An actor mailbox is a channel which stores pending messages and signals for an actor to process sequentially.

use std::{
    fmt,
    task::{Context, Poll},
};

use dyn_clone::DynClone;
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::{
    actor::{ActorID, ActorRef},
    error::{ActorStopReason, SendError},
    message::BoxMessage,
    reply::BoxReplySender,
    Actor,
};

/// Creates a bounded mailbox for communicating between actors with backpressure.
///
/// _See tokio's [`mpsc::channel`] docs for more info._
///
/// [`mpsc::channel`]: tokio::sync::mpsc::channel
pub fn bounded<A: Actor>(buffer: usize) -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::channel(buffer);
    (MailboxSender::Bounded(tx), MailboxReceiver::Bounded(rx))
}

/// Creates an unbounded mailbox for communicating between actors without backpressure.
///
/// See tokio's [`mpsc::unbounded_channel`] docs for more info.
///
/// [`mpsc::unbounded_channel`]: tokio::sync::mpsc::unbounded_channel
pub fn unbounded<A: Actor>() -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (MailboxSender::Unbounded(tx), MailboxReceiver::Unbounded(rx))
}

/// Sends messages and signals to the associated `MailboxReceiver`.
///
/// Instances are created by the [`bounded`] and [`unbounded`] functions.
pub enum MailboxSender<A: Actor> {
    /// Bounded mailbox sender.
    Bounded(mpsc::Sender<Signal<A>>),
    /// Unbounded mailbox sender.
    Unbounded(mpsc::UnboundedSender<Signal<A>>),
}

impl<A: Actor> MailboxSender<A> {
    /// Sends a value, waiting until there is capacity.
    ///
    /// See tokio's [`mpsc::Sender::send`] and [`mpsc::UnboundedSender::send`] docs for more info.
    ///
    /// [`mpsc::Sender::send`]: tokio::sync::mpsc::Sender::send
    /// [`mpsc::UnboundedSender::send`]: tokio::sync::mpsc::UnboundedSender::send
    pub async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        match self {
            MailboxSender::Bounded(tx) => tx.send(signal).await,
            MailboxSender::Unbounded(tx) => tx.send(signal),
        }
    }

    /// Completes when the receiver has dropped.
    ///
    /// See tokio's [`mpsc::Sender::closed`] and [`mpsc::UnboundedSender::closed`] docs for more info.
    ///
    /// [`mpsc::Sender::closed`]: tokio::sync::mpsc::Sender::closed
    /// [`mpsc::UnboundedSender::closed`]: tokio::sync::mpsc::UnboundedSender::closed
    pub async fn closed(&self) {
        match self {
            MailboxSender::Bounded(tx) => tx.closed().await,
            MailboxSender::Unbounded(tx) => tx.closed().await,
        }
    }

    /// Checks if the channel has been closed. This happens when the
    /// [`MailboxReceiver`] is dropped, or when the [`MailboxReceiver::close`] method is
    /// called.
    ///
    /// See tokio's [`mpsc::Sender::is_closed`] and [`mpsc::UnboundedSender::is_closed`] docs for more info.
    ///
    /// [`mpsc::Sender::is_closed`]: tokio::sync::mpsc::Sender::is_closed
    /// [`mpsc::UnboundedSender::is_closed`]: tokio::sync::mpsc::UnboundedSender::is_closed
    pub fn is_closed(&self) -> bool {
        match self {
            MailboxSender::Bounded(tx) => tx.is_closed(),
            MailboxSender::Unbounded(tx) => tx.is_closed(),
        }
    }

    /// Returns `true` if senders belong to the same channel.
    ///
    /// See tokio's [`mpsc::Sender::same_channel`] and [`mpsc::UnboundedSender::same_channel`] docs for more info.
    ///
    /// [`mpsc::Sender::same_channel`]: tokio::sync::mpsc::Sender::same_channel
    /// [`mpsc::UnboundedSender::same_channel`]: tokio::sync::mpsc::UnboundedSender::same_channel
    pub fn same_channel(&self, other: &MailboxSender<A>) -> bool {
        match (self, other) {
            (MailboxSender::Bounded(a), MailboxSender::Bounded(b)) => a.same_channel(b),
            (MailboxSender::Bounded(_), MailboxSender::Unbounded(_)) => false,
            (MailboxSender::Unbounded(_), MailboxSender::Bounded(_)) => false,
            (MailboxSender::Unbounded(a), MailboxSender::Unbounded(b)) => a.same_channel(b),
        }
    }

    /// Converts the `MailboxSender` to a [`WeakMailboxSender`] that does not count
    /// towards RAII semantics, i.e. if all `Sender` instances of the
    /// channel were dropped and only `WeakMailboxSender` instances remain,
    /// the channel is closed.
    ///
    /// See tokio's [`mpsc::Sender::downgrade`] and [`mpsc::UnboundedSender::downgrade`] docs for more info.
    ///
    /// [`mpsc::Sender::downgrade`]: tokio::sync::mpsc::Sender::downgrade
    /// [`mpsc::UnboundedSender::downgrade`]: tokio::sync::mpsc::UnboundedSender::downgrade
    pub fn downgrade(&self) -> WeakMailboxSender<A> {
        match self {
            MailboxSender::Bounded(tx) => WeakMailboxSender::Bounded(tx.downgrade()),
            MailboxSender::Unbounded(tx) => WeakMailboxSender::Unbounded(tx.downgrade()),
        }
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Sender::strong_count`] and [`mpsc::UnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::Sender::strong_count`]: tokio::sync::mpsc::Sender::strong_count
    /// [`mpsc::UnboundedSender::strong_count`]: tokio::sync::mpsc::UnboundedSender::strong_count
    pub fn strong_count(&self) -> usize {
        match self {
            MailboxSender::Bounded(tx) => tx.strong_count(),
            MailboxSender::Unbounded(tx) => tx.strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Sender::weak_count`] and [`mpsc::UnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::Sender::weak_count`]: tokio::sync::mpsc::Sender::weak_count
    /// [`mpsc::UnboundedSender::weak_count`]: tokio::sync::mpsc::UnboundedSender::weak_count
    pub fn weak_count(&self) -> usize {
        match self {
            MailboxSender::Bounded(tx) => tx.weak_count(),
            MailboxSender::Unbounded(tx) => tx.weak_count(),
        }
    }
}

impl<A: Actor> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        match self {
            MailboxSender::Bounded(tx) => MailboxSender::Bounded(tx.clone()),
            MailboxSender::Unbounded(tx) => MailboxSender::Unbounded(tx.clone()),
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MailboxSender::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            MailboxSender::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

/// A mailbox sender that does not prevent the channel from being closed.
///
/// See tokio's [`mpsc::WeakSender`] and [`mpsc::WeakUnboundedSender`] docs for more info.
///
/// [`mpsc::WeakSender`]: tokio::sync::mpsc::WeakSender
/// [`mpsc::WeakUnboundedSender`]: tokio::sync::mpsc::WeakUnboundedSender
pub enum WeakMailboxSender<A: Actor> {
    /// Bounded weak mailbox sender.
    Bounded(mpsc::WeakSender<Signal<A>>),
    /// Unbounded weak mailbox sender.
    Unbounded(mpsc::WeakUnboundedSender<Signal<A>>),
}

impl<A: Actor> WeakMailboxSender<A> {
    /// Tries to convert a `WeakMailboxSender` into a [`MailboxSender`]. This will return `Some`
    /// if there are other `MailboxSender` instances alive and the channel wasn't
    /// previously dropped, otherwise `None` is returned.
    ///
    /// See tokio's [`mpsc::WeakSender::upgrade`] and [`mpsc::WeakUnboundedSender::upgrade`] docs for more info.
    ///
    /// [`mpsc::WeakSender::upgrade`]: tokio::sync::mpsc::WeakSender::upgrade
    /// [`mpsc::WeakUnboundedSender::upgrade`]: tokio::sync::mpsc::WeakUnboundedSender::upgrade
    pub fn upgrade(&self) -> Option<MailboxSender<A>> {
        match self {
            WeakMailboxSender::Bounded(tx) => tx.upgrade().map(MailboxSender::Bounded),
            WeakMailboxSender::Unbounded(tx) => tx.upgrade().map(MailboxSender::Unbounded),
        }
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::strong_count`] and [`mpsc::WeakUnboundedSender::strong_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::strong_count`]: tokio::sync::mpsc::WeakSender::strong_count
    /// [`mpsc::WeakUnboundedSender::strong_count`]: tokio::sync::mpsc::WeakUnboundedSender::strong_count
    pub fn strong_count(&self) -> usize {
        match self {
            WeakMailboxSender::Bounded(tx) => tx.strong_count(),
            WeakMailboxSender::Unbounded(tx) => tx.strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::WeakSender::weak_count`] and [`mpsc::WeakUnboundedSender::weak_count`] docs for more info.
    ///
    /// [`mpsc::WeakSender::weak_count`]: tokio::sync::mpsc::WeakSender::weak_count
    /// [`mpsc::WeakUnboundedSender::weak_count`]: tokio::sync::mpsc::WeakUnboundedSender::weak_count
    pub fn weak_count(&self) -> usize {
        match self {
            WeakMailboxSender::Bounded(tx) => tx.weak_count(),
            WeakMailboxSender::Unbounded(tx) => tx.weak_count(),
        }
    }
}

impl<A: Actor> Clone for WeakMailboxSender<A> {
    fn clone(&self) -> Self {
        match self {
            WeakMailboxSender::Bounded(tx) => WeakMailboxSender::Bounded(tx.clone()),
            WeakMailboxSender::Unbounded(tx) => WeakMailboxSender::Unbounded(tx.clone()),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakMailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WeakMailboxSender::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            WeakMailboxSender::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

/// Receives values from the associated `MailboxSender`.
///
/// Instances are created by the [`bounded`] and [`unbounded`] functions.
pub enum MailboxReceiver<A: Actor> {
    /// Bounded mailbox receiver.
    Bounded(mpsc::Receiver<Signal<A>>),
    /// Unbounded mailbox receiver.
    Unbounded(mpsc::UnboundedReceiver<Signal<A>>),
}

impl<A: Actor> MailboxReceiver<A> {
    /// Receives the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::recv`] and [`mpsc::UnboundedReceiver::recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv`]: tokio::sync::mpsc::Receiver::recv
    /// [`mpsc::UnboundedReceiver::recv`]: tokio::sync::mpsc::UnboundedReceiver::recv
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.recv().await,
            MailboxReceiver::Unbounded(rx) => rx.recv().await,
        }
    }

    /// Receives the next values for this receiver and extends `buffer`.
    ///
    /// See tokio's [`mpsc::Receiver::recv_many`] and [`mpsc::UnboundedReceiver::recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::recv_many`]: tokio::sync::mpsc::Receiver::recv_many
    /// [`mpsc::UnboundedReceiver::recv_many`]: tokio::sync::mpsc::UnboundedReceiver::recv_many
    pub async fn recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.recv_many(buffer, limit).await,
            MailboxReceiver::Unbounded(rx) => rx.recv_many(buffer, limit).await,
        }
    }

    /// Tries to receive the next value for this receiver.
    ///
    /// See tokio's [`mpsc::Receiver::try_recv`] and [`mpsc::UnboundedReceiver::try_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::try_recv`]: tokio::sync::mpsc::Receiver::try_recv
    /// [`mpsc::UnboundedReceiver::try_recv`]: tokio::sync::mpsc::UnboundedReceiver::try_recv
    pub fn try_recv(&mut self) -> Result<Signal<A>, TryRecvError> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.try_recv(),
            MailboxReceiver::Unbounded(rx) => rx.try_recv(),
        }
    }

    /// Blocking receive to call outside of asynchronous contexts.
    ///
    /// See tokio's [`mpsc::Receiver::blocking_recv`] and [`mpsc::UnboundedReceiver::blocking_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::blocking_recv`]: tokio::sync::mpsc::Receiver::blocking_recv
    /// [`mpsc::UnboundedReceiver::blocking_recv`]: tokio::sync::mpsc::UnboundedReceiver::blocking_recv
    pub fn blocking_recv(&mut self) -> Option<Signal<A>> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.blocking_recv(),
            MailboxReceiver::Unbounded(rx) => rx.blocking_recv(),
        }
    }

    /// Variant of [`Self::recv_many`] for blocking contexts.
    ///
    /// See tokio's [`mpsc::Receiver::blocking_recv_many`] and [`mpsc::UnboundedReceiver::blocking_recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::blocking_recv_many`]: tokio::sync::mpsc::Receiver::blocking_recv_many
    /// [`mpsc::UnboundedReceiver::blocking_recv_many`]: tokio::sync::mpsc::UnboundedReceiver::blocking_recv_many
    pub fn blocking_recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.blocking_recv_many(buffer, limit),
            MailboxReceiver::Unbounded(rx) => rx.blocking_recv_many(buffer, limit),
        }
    }

    /// Closes the receiving half of a channel, without dropping it.
    ///
    /// See tokio's [`mpsc::Receiver::close`] and [`mpsc::UnboundedReceiver::close`] docs for more info.
    ///
    /// [`mpsc::Receiver::close`]: tokio::sync::mpsc::Receiver::close
    /// [`mpsc::UnboundedReceiver::close`]: tokio::sync::mpsc::UnboundedReceiver::close
    pub fn close(&mut self) {
        match self {
            MailboxReceiver::Bounded(rx) => rx.close(),
            MailboxReceiver::Unbounded(rx) => rx.close(),
        }
    }

    /// Checks if a channel is closed.
    ///
    /// See tokio's [`mpsc::Receiver::is_closed`] and [`mpsc::UnboundedReceiver::is_closed`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_closed`]: tokio::sync::mpsc::Receiver::is_closed
    /// [`mpsc::UnboundedReceiver::is_closed`]: tokio::sync::mpsc::UnboundedReceiver::is_closed
    pub fn is_closed(&self) -> bool {
        match self {
            MailboxReceiver::Bounded(rx) => rx.is_closed(),
            MailboxReceiver::Unbounded(rx) => rx.is_closed(),
        }
    }

    /// Checks if a channel is empty.
    ///
    /// See tokio's [`mpsc::Receiver::is_empty`] and [`mpsc::UnboundedReceiver::is_empty`] docs for more info.
    ///
    /// [`mpsc::Receiver::is_empty`]: tokio::sync::mpsc::Receiver::is_empty
    /// [`mpsc::UnboundedReceiver::is_empty`]: tokio::sync::mpsc::UnboundedReceiver::is_empty
    pub fn is_empty(&self) -> bool {
        match self {
            MailboxReceiver::Bounded(rx) => rx.is_empty(),
            MailboxReceiver::Unbounded(rx) => rx.is_empty(),
        }
    }

    /// Returns the number of messages in the channel.
    ///
    /// See tokio's [`mpsc::Receiver::len`] and [`mpsc::UnboundedReceiver::len`] docs for more info.
    ///
    /// [`mpsc::Receiver::len`]: tokio::sync::mpsc::Receiver::len
    /// [`mpsc::UnboundedReceiver::len`]: tokio::sync::mpsc::UnboundedReceiver::len
    pub fn len(&self) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.len(),
            MailboxReceiver::Unbounded(rx) => rx.len(),
        }
    }

    /// Polls to receive the next message on this channel.
    ///
    /// See tokio's [`mpsc::Receiver::poll_recv`] and [`mpsc::UnboundedReceiver::poll_recv`] docs for more info.
    ///
    /// [`mpsc::Receiver::poll_recv`]: tokio::sync::mpsc::Receiver::poll_recv
    /// [`mpsc::UnboundedReceiver::poll_recv`]: tokio::sync::mpsc::UnboundedReceiver::poll_recv
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Signal<A>>> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.poll_recv(cx),
            MailboxReceiver::Unbounded(rx) => rx.poll_recv(cx),
        }
    }

    /// Polls to receive multiple messages on this channel, extending the provided buffer.
    ///
    /// See tokio's [`mpsc::Receiver::poll_recv_many`] and [`mpsc::UnboundedReceiver::poll_recv_many`] docs for more info.
    ///
    /// [`mpsc::Receiver::poll_recv_many`]: tokio::sync::mpsc::Receiver::poll_recv_many
    /// [`mpsc::UnboundedReceiver::poll_recv_many`]: tokio::sync::mpsc::UnboundedReceiver::poll_recv_many
    pub fn poll_recv_many(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut Vec<Signal<A>>,
        limit: usize,
    ) -> Poll<usize> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.poll_recv_many(cx, buffer, limit),
            MailboxReceiver::Unbounded(rx) => rx.poll_recv_many(cx, buffer, limit),
        }
    }

    /// Returns the number of [`MailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_strong_count`] and [`mpsc::UnboundedReceiver::sender_strong_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_strong_count`]: tokio::sync::mpsc::Receiver::sender_strong_count
    /// [`mpsc::UnboundedReceiver::sender_strong_count`]: tokio::sync::mpsc::UnboundedReceiver::sender_strong_count
    pub fn sender_strong_count(&self) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.sender_strong_count(),
            MailboxReceiver::Unbounded(rx) => rx.sender_strong_count(),
        }
    }

    /// Returns the number of [`WeakMailboxSender`] handles.
    ///
    /// See tokio's [`mpsc::Receiver::sender_weak_count`] and [`mpsc::UnboundedReceiver::sender_weak_count`] docs for more info.
    ///
    /// [`mpsc::Receiver::sender_weak_count`]: tokio::sync::mpsc::Receiver::sender_weak_count
    /// [`mpsc::UnboundedReceiver::sender_weak_count`]: tokio::sync::mpsc::UnboundedReceiver::sender_weak_count
    pub fn sender_weak_count(&self) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.sender_weak_count(),
            MailboxReceiver::Unbounded(rx) => rx.sender_weak_count(),
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MailboxReceiver::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            MailboxReceiver::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
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
        id: ActorID,
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
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
}

impl<A> SignalMailbox for MailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self {
            MailboxSender::Bounded(tx) => {
                tx.try_send(Signal::StartupFinished)
                    .map_err(|err| match err {
                        mpsc::error::TrySendError::Full(_) => SendError::MailboxFull(()),
                        mpsc::error::TrySendError::Closed(_) => SendError::ActorNotRunning(()),
                    })
            }
            MailboxSender::Unbounded(tx) => tx
                .send(Signal::StartupFinished)
                .map_err(|_| SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        match self {
            MailboxSender::Bounded(tx) => async move {
                tx.send(Signal::LinkDied { id, reason })
                    .await
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
            MailboxSender::Unbounded(tx) => async move {
                tx.send(Signal::LinkDied { id, reason })
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
        }
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        match self {
            MailboxSender::Bounded(tx) => async move {
                tx.send(Signal::Stop)
                    .await
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
            MailboxSender::Unbounded(tx) => async move {
                tx.send(Signal::Stop)
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
        }
    }
}

impl<A> SignalMailbox for WeakMailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self.upgrade() {
            Some(tx) => tx.signal_startup_finished(),
            None => Err(SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => tx.signal_link_died(id, reason).await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => tx.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }
}

dyn_clone::clone_trait_object!(SignalMailbox);
