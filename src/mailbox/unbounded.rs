//! Unbounded mailbox types based on tokio mpsc unbounded channels.

use std::fmt;

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc;

use crate::{
    actor::ActorID,
    error::{ActorStopReason, SendError},
    Actor,
};

use super::{Mailbox, MailboxReceiver, Signal, SignalMailbox, WeakMailbox};

/// An unbounded mailbox, where the number of messages queued can grow infinitely.
pub struct UnboundedMailbox<A: Actor>(pub(crate) mpsc::UnboundedSender<Signal<A>>);

impl<A: Actor> UnboundedMailbox<A> {
    /// Creates a new unbounded mailbox.
    #[inline]
    pub fn new() -> (Self, UnboundedMailboxReceiver<A>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (UnboundedMailbox(tx), UnboundedMailboxReceiver(rx))
    }
}

impl<A: Actor> Mailbox<A> for UnboundedMailbox<A> {
    type Receiver = UnboundedMailboxReceiver<A>;
    type WeakMailbox = WeakUnboundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, Self::Receiver) {
        UnboundedMailbox::new()
    }

    #[inline]
    async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        self.0.send(signal)
    }

    #[inline]
    fn try_send(&self, signal: Signal<A>) -> Result<(), mpsc::error::TrySendError<Signal<A>>> {
        Ok(self.0.send(signal)?)
    }

    #[inline]
    fn blocking_send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        self.0.send(signal)
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
        WeakUnboundedMailbox(self.0.downgrade())
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
        None
    }
}

impl<A: Actor> Clone for UnboundedMailbox<A> {
    fn clone(&self) -> Self {
        UnboundedMailbox(self.0.clone())
    }
}

impl<A: Actor> fmt::Debug for UnboundedMailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedMailbox")
            .field("tx", &self.0)
            .finish()
    }
}

/// An unbounded mailbox receiver.
pub struct UnboundedMailboxReceiver<A: Actor>(mpsc::UnboundedReceiver<Signal<A>>);

impl<A: Actor> MailboxReceiver<A> for UnboundedMailboxReceiver<A> {
    async fn recv(&mut self) -> Option<Signal<A>> {
        self.0.recv().await
    }
}

impl<A: Actor> fmt::Debug for UnboundedMailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedMailboxReceiver")
            .field("rx", &self.0)
            .finish()
    }
}

/// A weak unbounded mailbox that does not prevent the actor from being stopped.
pub struct WeakUnboundedMailbox<A: Actor>(mpsc::WeakUnboundedSender<Signal<A>>);

impl<A: Actor> WeakMailbox for WeakUnboundedMailbox<A> {
    type StrongMailbox = UnboundedMailbox<A>;

    #[inline]
    fn upgrade(&self) -> Option<Self::StrongMailbox> {
        self.0.upgrade().map(UnboundedMailbox)
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

impl<A: Actor> Clone for WeakUnboundedMailbox<A> {
    fn clone(&self) -> Self {
        WeakUnboundedMailbox(self.0.clone())
    }
}

impl<A: Actor> fmt::Debug for WeakUnboundedMailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakUnboundedMailbox")
            .field("tx", &self.0)
            .finish()
    }
}

impl<A> SignalMailbox for UnboundedMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        self.0
            .send(Signal::StartupFinished)
            .map_err(|_| SendError::ActorNotRunning(()))
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::LinkDied { id, reason })
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::Stop)
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }
}

impl<A> SignalMailbox for WeakUnboundedMailbox<A>
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
