//! Bounded mailbox types based on tokio mpsc bounded channels.

use std::fmt;

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc;

use crate::{
    actor::ActorID,
    error::{ActorStopReason, SendError},
    Actor,
};

use super::{Mailbox, MailboxReceiver, Signal, SignalMailbox, WeakMailbox};

/// An unbounded mailbox, where the sending messages to a full mailbox causes backpressure.
pub struct BoundedMailbox<A: Actor>(pub(crate) mpsc::Sender<Signal<A>>);

impl<A: Actor> BoundedMailbox<A> {
    /// Creates a new bounded mailbox with a given capacity.
    #[inline]
    pub fn new(capacity: usize) -> (Self, BoundedMailboxReceiver<A>) {
        let (tx, rx) = mpsc::channel(capacity);
        (BoundedMailbox(tx), BoundedMailboxReceiver(rx))
    }
}

impl<A: Actor> Mailbox<A> for BoundedMailbox<A> {
    type Receiver = BoundedMailboxReceiver<A>;
    type WeakMailbox = WeakBoundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, Self::Receiver) {
        BoundedMailbox::new(1000)
    }

    #[inline]
    async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        self.0.send(signal).await
    }

    #[inline]
    fn try_send(&self, signal: Signal<A>) -> Result<(), mpsc::error::TrySendError<Signal<A>>> {
        self.0.try_send(signal)
    }

    #[inline]
    fn blocking_send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        self.0.blocking_send(signal)
    }

    #[inline]
    async fn closed(&self) {
        self.0.closed().await
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        WeakBoundedMailbox(self.0.downgrade())
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.0.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.0.weak_count()
    }

    #[inline]
    fn capacity(&self) -> Option<usize> {
        Some(self.0.max_capacity())
    }
}

impl<A: Actor> Clone for BoundedMailbox<A> {
    fn clone(&self) -> Self {
        BoundedMailbox(self.0.clone())
    }
}

impl<A: Actor> fmt::Debug for BoundedMailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedMailbox")
            .field("tx", &self.0)
            .finish()
    }
}

/// A bounded mailbox receiver.
pub struct BoundedMailboxReceiver<A: Actor>(mpsc::Receiver<Signal<A>>);

impl<A: Actor> MailboxReceiver<A> for BoundedMailboxReceiver<A> {
    async fn recv(&mut self) -> Option<Signal<A>> {
        self.0.recv().await
    }
}

impl<A: Actor> fmt::Debug for BoundedMailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedMailboxReceiver")
            .field("rx", &self.0)
            .finish()
    }
}

/// A weak bounded mailbox that does not prevent the actor from being stopped.
pub struct WeakBoundedMailbox<A: Actor>(mpsc::WeakSender<Signal<A>>);

impl<A: Actor> WeakMailbox for WeakBoundedMailbox<A> {
    type StrongMailbox = BoundedMailbox<A>;

    #[inline]
    fn upgrade(&self) -> Option<Self::StrongMailbox> {
        self.0.upgrade().map(BoundedMailbox)
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.0.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.0.weak_count()
    }
}

impl<A: Actor> Clone for WeakBoundedMailbox<A> {
    fn clone(&self) -> Self {
        WeakBoundedMailbox(self.0.clone())
    }
}

impl<A: Actor> fmt::Debug for WeakBoundedMailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakBoundedMailbox")
            .field("tx", &self.0)
            .finish()
    }
}

impl<A> SignalMailbox for BoundedMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        self.0
            .try_send(Signal::StartupFinished)
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => SendError::MailboxFull(()),
                mpsc::error::TrySendError::Closed(_) => SendError::ActorNotRunning(()),
            })
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::LinkDied { id, reason })
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::Stop)
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }
}

impl<A> SignalMailbox for WeakBoundedMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self.upgrade() {
            Some(mb) => mb.signal_startup_finished(),
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
                Some(mb) => mb.signal_link_died(id, reason).await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(mb) => mb.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }
}
