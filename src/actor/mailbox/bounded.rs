use std::fmt;

use tokio::sync::mpsc;

use crate::{error::SendError, Actor};

use super::{
    Mailbox, MailboxReceiver, MpscMailbox, Signal, SignalMailbox, SignalMessage, WeakMailbox,
};

/// A bounded mailbox, where the sending messages to a full mailbox causes backpressure.
pub struct BoundedMailbox<A: Actor>(pub(crate) mpsc::Sender<Signal<SignalMessage<A>>>);

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
    type SignalMessage = SignalMessage<A>;
    type WeakMailbox = WeakBoundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, Self::Receiver) {
        BoundedMailbox::new(1000)
    }

    #[inline]
    async fn send<M, E>(&self, signal: Signal<Self::SignalMessage>) -> Result<(), SendError<M, E>>
    where
        M: 'static,
        E: 'static,
    {
        self.0.send(signal).await?;
        Ok(())
    }

    #[inline]
    fn signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.downgrade())
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        WeakBoundedMailbox(self.0.downgrade())
    }
}

impl<A: Actor> MpscMailbox for BoundedMailbox<A> {
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
pub struct BoundedMailboxReceiver<A: Actor>(pub(crate) mpsc::Receiver<Signal<SignalMessage<A>>>);

impl<A: Actor> fmt::Debug for BoundedMailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedMailboxReceiver")
            .field("rx", &self.0)
            .finish()
    }
}

impl<A: Actor> MailboxReceiver for BoundedMailboxReceiver<A> {
    type SignalMessage = SignalMessage<A>;

    async fn recv(&mut self) -> Option<Signal<Self::SignalMessage>> {
        self.0.recv().await
    }
}

/// A weak bounded mailbox that does not prevent the actor from being stopped.
pub struct WeakBoundedMailbox<A: Actor>(pub(crate) mpsc::WeakSender<Signal<SignalMessage<A>>>);

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
