use std::fmt;

use tokio::sync::mpsc;

use crate::{error::SendError, Actor};

use super::{
    Mailbox, MailboxReceiver, MpscMailbox, Signal, SignalMailbox, SignalMessage, WeakMailbox,
};

/// An unbounded mailbox, where the number of messages queued can grow infinitely.
pub struct UnboundedMailbox<A: Actor>(pub(crate) mpsc::UnboundedSender<Signal<SignalMessage<A>>>);

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
    type SignalMessage = SignalMessage<A>;
    type WeakMailbox = WeakUnboundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, Self::Receiver) {
        UnboundedMailbox::new()
    }

    #[inline]
    async fn send<M, E>(&self, signal: Signal<Self::SignalMessage>) -> Result<(), SendError<M, E>>
    where
        M: 'static,
        E: 'static,
    {
        self.0.send(signal)?;
        Ok(())
    }

    #[inline]
    fn signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.downgrade())
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        WeakUnboundedMailbox(self.0.downgrade())
    }
}

impl<A: Actor> MpscMailbox for UnboundedMailbox<A> {
    #[inline]
    async fn closed(&self) {
        self.0.closed().await
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.0.is_closed()
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
pub struct UnboundedMailboxReceiver<A: Actor>(
    pub(crate) mpsc::UnboundedReceiver<Signal<SignalMessage<A>>>,
);

impl<A: Actor> MailboxReceiver for UnboundedMailboxReceiver<A> {
    type SignalMessage = SignalMessage<A>;

    async fn recv(&mut self) -> Option<Signal<Self::SignalMessage>> {
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
pub struct WeakUnboundedMailbox<A: Actor>(
    pub(crate) mpsc::WeakUnboundedSender<Signal<SignalMessage<A>>>,
);

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
