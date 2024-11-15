//! Behaviour for actor mailboxes.
//!
//! An actor mailbox is a channel which stores pending messages and signals for an actor to process sequentially.

pub mod bounded;
pub mod unbounded;

use dyn_clone::DynClone;
use futures::{future::BoxFuture, Future};

use crate::{
    actor::{ActorID, ActorRef},
    error::{ActorStopReason, SendError},
    message::DynMessage,
    reply::BoxReplySender,
    Actor,
};

/// A trait defining the behaviour and functionality of a mailbox.
pub trait Mailbox<A: Actor>: SignalMailbox + Clone + Send + Sync {
    /// Mailbox receiver type.
    type Receiver: MailboxReceiver<A>;
    /// Mailbox weak type.
    type WeakMailbox: WeakMailbox<StrongMailbox = Self>;

    /// Creates a default mailbox and receiver.
    fn default_mailbox() -> (Self, Self::Receiver);
    /// Sends a signal to the mailbox.
    fn send<E: 'static>(
        &self,
        signal: Signal<A>,
    ) -> impl Future<Output = Result<(), SendError<Signal<A>, E>>> + Send + '_;
    /// Tries to send a signal to the mailbox, failing if the mailbox is full.
    fn try_send<E: 'static>(&self, signal: Signal<A>) -> Result<(), SendError<Signal<A>, E>>;
    /// Sends a signal to the mailbox, blocking the current thread.
    fn blocking_send<E: 'static>(&self, signal: Signal<A>) -> Result<(), SendError<Signal<A>, E>>;
    /// Waits for the mailbox to be closed.
    fn closed(&self) -> impl Future<Output = ()> + Send + '_;
    /// Checks if the mailbox is closed.
    fn is_closed(&self) -> bool;
    /// Downgrades the mailbox to a weak mailbox.
    fn downgrade(&self) -> Self::WeakMailbox;
    /// Returns the number of strong mailboxes.
    fn strong_count(&self) -> usize;
    /// Returns the number of weak mailboxes.
    fn weak_count(&self) -> usize;
}

/// A mailbox receiver.
pub trait MailboxReceiver<A: Actor>: Send + 'static {
    /// Receives a value from the mailbox.
    fn recv(&mut self) -> impl Future<Output = Option<Signal<A>>> + Send + '_;
}

/// A weak mailbox which can be upraded.
pub trait WeakMailbox: SignalMailbox + Clone + Send + Sync {
    /// Strong mailbox type.
    type StrongMailbox;

    /// Upgrades the mailbox to a strong mailbox, or returns None if all strong mailboxes have been dropped.
    fn upgrade(&self) -> Option<Self::StrongMailbox>;
    /// Returns the number of strong mailboxes.
    fn strong_count(&self) -> usize;
    /// Returns the number of weak mailboxes.
    fn weak_count(&self) -> usize;
}

#[allow(missing_debug_implementations)]
#[doc(hidden)]
pub enum Signal<A: Actor> {
    StartupFinished,
    Message {
        message: Box<dyn DynMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<BoxReplySender>,
        sent_within_actor: bool,
    },
    LinkDied {
        id: ActorID,
        reason: ActorStopReason,
    },
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
pub trait SignalMailbox: DynClone + Send {
    fn signal_startup_finished(&self) -> Result<(), SendError>;
    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
}

dyn_clone::clone_trait_object!(SignalMailbox);
