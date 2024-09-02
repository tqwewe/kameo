use std::fmt;

use futures::stream::AbortHandle;
use tokio::sync::broadcast;

use crate::{
    actor::{
        behaviour::BroadcastActorBehaviour, run_actor_lifecycle, ActorRef, Links, CURRENT_ACTOR_ID,
    },
    error::SendError,
    message::Message,
    Actor,
};

use super::{Mailbox, MailboxReceiver, Signal, SignalBroadcast, SignalMailbox, WeakMailbox};

/// A broadcast mailbox which can be shared between multiple actors.
pub struct BroadcastMailbox<A: Actor> {
    pub(crate) tx: broadcast::Sender<Signal<SignalBroadcast<A>>>,
    pub(crate) rx: broadcast::Receiver<Signal<SignalBroadcast<A>>>,
}

impl<A: Actor> BroadcastMailbox<A> {
    /// Creates a new shared broadcast mailbox with a given capacity.
    pub fn new(capacity: usize) -> BroadcastMailbox<A> {
        let (tx, rx) = broadcast::channel(capacity);
        BroadcastMailbox { tx, rx }
    }

    /// Spawns an actor using the shared broadcast mailbox.
    ///
    /// The actor will only process messages sent to the mailbox after it has been spawned.
    /// Historical messages will not be processed.
    pub fn spawn(&self, actor: A) -> ActorRef<A>
    where
        A: Actor<Mailbox = BroadcastMailbox<A>>,
    {
        let (mailbox, mailbox_rx) = (
            self.clone(),
            BroadcastMailboxReceiver(self.rx.resubscribe()),
        );
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let links = Links::default();
        let actor_ref = ActorRef::new(mailbox, abort_handle, links.clone());
        let id = actor_ref.id();

        tokio::spawn(CURRENT_ACTOR_ID.scope(id, {
            let actor_ref = actor_ref.clone();
            async move {
                run_actor_lifecycle::<A, BroadcastActorBehaviour<A>>(
                    actor_ref,
                    actor,
                    mailbox_rx,
                    abort_registration,
                    links,
                )
                .await
            }
        }));

        actor_ref
    }

    /// Sends a message to multiple actors in a shared mailbox.
    #[inline]
    pub fn send<M>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: Message<M>,
        M: Clone + Send + 'static,
    {
        self.tx.send(Signal::Message(SignalBroadcast {
            message: Box::new(msg),
            sent_from_actor: CURRENT_ACTOR_ID.try_with(Clone::clone).ok(),
        }))?;
        Ok(())
    }
}

impl<A: Actor> Mailbox<A> for BroadcastMailbox<A> {
    type Receiver = BroadcastMailboxReceiver<A>;
    type SignalMessage = SignalBroadcast<A>;
    type WeakMailbox = Self;

    fn default_mailbox() -> (Self, Self::Receiver) {
        let (tx, rx) = broadcast::channel(1000);
        (
            BroadcastMailbox {
                tx,
                rx: rx.resubscribe(),
            },
            BroadcastMailboxReceiver(rx),
        )
    }

    #[inline]
    async fn send<M, E>(&self, signal: Signal<Self::SignalMessage>) -> Result<(), SendError<M, E>>
    where
        M: 'static,
        E: 'static,
    {
        self.tx.send(signal)?;
        Ok(())
    }

    #[inline]
    fn signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.clone())
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        self.clone()
    }
}

impl<A: Actor> WeakMailbox for BroadcastMailbox<A> {
    type StrongMailbox = Self;

    fn upgrade(&self) -> Option<Self::StrongMailbox> {
        Some(self.clone())
    }

    fn strong_count(&self) -> usize {
        self.tx.receiver_count()
    }

    fn weak_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl<A: Actor> Clone for BroadcastMailbox<A> {
    fn clone(&self) -> Self {
        BroadcastMailbox {
            tx: self.tx.clone(),
            rx: self.rx.resubscribe(),
        }
    }
}

impl<A: Actor> fmt::Debug for BroadcastMailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastMailbox")
            .field("tx", &self.tx)
            .field("rx", &self.rx)
            .finish()
    }
}

/// A broadcast mailbox receiver.
pub struct BroadcastMailboxReceiver<A: Actor>(
    pub(crate) broadcast::Receiver<Signal<SignalBroadcast<A>>>,
);

impl<A: Actor> MailboxReceiver for BroadcastMailboxReceiver<A> {
    type SignalMessage = SignalBroadcast<A>;

    async fn recv(&mut self) -> Option<Signal<Self::SignalMessage>> {
        self.0.recv().await.ok()
    }
}

impl<A: Actor> fmt::Debug for BroadcastMailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastMailboxReceiver")
            .field("rx", &self.0)
            .finish()
    }
}
