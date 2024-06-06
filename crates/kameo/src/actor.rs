//! Actor abstractions and utilities for building concurrent, asynchronous systems.
//!
//! This module provides the core abstractions for spawning and managing actors in an
//! asynchronous, concurrent application. Actors in kameo are independent units of
//! computation that communicate through message passing, encapsulating state and behavior.
//!
//! Central to this module is the [Actor] trait, which defines the lifecycle hooks and
//! functionalities every actor must implement. These hooks include initialization ([`on_start`](Actor::on_start)),
//! cleanup ([`on_stop`](Actor::on_stop)), error handling ([`on_panic`](Actor::on_panic)), and managing relationships with other
//! actors ([`on_link_died`](Actor::on_link_died)). Additionally, the [`name`](Actor::name) method provides a means to identify
//! actors, facilitating debugging and tracing.
//!
//! To interact with and manage actors, this module introduces two key structures:
//! - [ActorRef]: A strong reference to an actor, containing all necessary information for
//!   sending messages, stopping the actor, and managing actor links. It serves as the primary
//!   interface for external interactions with an actor.
//! - [WeakActorRef]: Similar to `ActorRef`, but does not prevent the actor from being stopped.
//!
//! The design of this module emphasizes loose coupling and high cohesion among actors, promoting
//! a scalable and maintainable architecture. By leveraging asynchronous message passing and
//! lifecycle management, developers can create complex, responsive systems with high degrees
//! of concurrency and parallelism.

mod actor_ref;
mod kind;
mod pool;
mod spawn;

use std::any;

use futures::Future;
use tokio::sync::mpsc;

use crate::error::{ActorStopReason, BoxError, PanicError};

pub use actor_ref::*;
pub use pool::*;
pub use spawn::*;

/// Functionality for an actor including lifecycle hooks.
///
/// Methods in this trait that return [`BoxError`] will stop the actor with the reason
/// [`ActorStopReason::Panicked`] containing the error.
///
/// # Example
///
/// ```
/// use kameo::Actor;
/// use kameo::error::{ActorStopReason, BoxError, PanicError};
///
/// struct MyActor;
///
/// impl Actor for MyActor {
///     async fn on_start(&mut self) -> Result<(), BoxError> {
///         println!("actor started");
///         Ok(())
///     }
///
///     async fn on_panic(&mut self, err: PanicError) -> Result<Option<ActorStopReason>, BoxError> {
///         println!("actor panicked");
///         Ok(Some(ActorStopReason::Panicked(err))) // Return `Some` to stop the actor
///     }
///
///     async fn on_stop(&mut self, reason: ActorStopReason) -> Result<(), BoxError> {
///         println!("actor stopped");
///         Ok(())
///     }
/// }
/// ```
pub trait Actor: Sized {
    /// The mailbox used for the actor.
    ///
    /// This can either be `BoundedMailbox<Self>` or `UnboundedMailbox<Self>.`
    ///
    /// Bounded mailboxes enable backpressure to prevent the queued messages from growing indefinitely,
    /// whilst unbounded mailboxes have no backpressure and can grow infinitely until the system runs out of memory.
    type Mailbox: MailboxBehaviour<Self> + SignalMailbox + Clone + Send + Sync;

    /// Actor name, useful for logging.
    fn name() -> &'static str {
        any::type_name::<Self>()
    }

    /// The capacity of the actors mailbox.
    fn new_mailbox(&self) -> (Self::Mailbox, MailboxReceiver<Self>) {
        Self::Mailbox::default_mailbox()
    }

    /// The maximum number of concurrent queries to handle at a time.
    ///
    /// This defaults to the number of cpus on the system.
    fn max_concurrent_queries() -> usize {
        num_cpus::get()
    }

    /// Hook that is called before the actor starts processing messages.
    ///
    /// # Guarantees
    /// Messages sent internally by the actor during `on_start` are prioritized and processed before any externally
    /// sent messages, even if external messages are received before `on_start` completes.
    /// This is ensured by an internal buffering mechanism that holds external messages until after `on_start` has
    /// finished executing.
    ///
    /// # Returns
    /// A result indicating successful initialization or an error if initialization fails.
    #[allow(unused_variables)]
    fn on_start(
        &mut self,
        actor_ref: ActorRef<Self>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Hook that is called when an actor panicked or returns an error during an async message.
    ///
    /// This method provides an opportunity to clean up or reset state.
    /// It can also determine whether the actor should be killed or if it should continue processing messages by returning `None`.
    ///
    /// # Parameters
    /// - `err`: The error that occurred.
    ///
    /// # Returns
    /// Whether the actor should continue processing, or be stopped by returning a stop reason.
    #[allow(unused_variables)]
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl Future<Output = Result<Option<ActorStopReason>, BoxError>> + Send {
        async move { Ok(Some(ActorStopReason::Panicked(err))) }
    }

    /// Hook that is called when a linked actor dies.
    ///
    /// By default, the current actor will be stopped if the reason is anything other than normal.
    ///
    /// # Returns
    /// Whether the actor should continue processing, or be stopped by returning a stop reason.
    #[allow(unused_variables)]
    fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: u64,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<Option<ActorStopReason>, BoxError>> + Send {
        async move {
            match &reason {
                ActorStopReason::Normal => Ok(None),
                ActorStopReason::Killed
                | ActorStopReason::Panicked(_)
                | ActorStopReason::LinkDied { .. } => Ok(Some(ActorStopReason::LinkDied {
                    id,
                    reason: Box::new(reason),
                })),
            }
        }
    }

    /// Hook that is called before the actor is stopped.
    ///
    /// This method allows for cleanup and finalization tasks to be performed before the
    /// actor is fully stopped. It can be used to release resources, notify other actors,
    /// or complete any final tasks.
    ///
    /// # Parameters
    /// - `reason`: The reason why the actor is being stopped.
    #[allow(unused_variables)]
    fn on_stop(
        self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }
}

#[doc(hidden)]
pub trait MailboxBehaviour<A: Actor>: Sized {
    type WeakMailbox: WeakMailboxBehaviour<StrongMailbox = Self> + Clone + Send;

    fn default_mailbox() -> (Self, MailboxReceiver<A>);
    fn send(
        &self,
        signal: Signal<A>,
    ) -> impl Future<Output = Result<(), mpsc::error::SendError<Signal<A>>>> + Send + '_;
    fn closed(&self) -> impl Future<Output = ()> + '_;
    fn is_closed(&self) -> bool;
    fn downgrade(&self) -> Self::WeakMailbox;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

#[doc(hidden)]
pub trait WeakMailboxBehaviour {
    type StrongMailbox;

    fn upgrade(&self) -> Option<Self::StrongMailbox>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

/// An unbounded mailbox, where the sending messages to a full mailbox causes backpressure.
#[allow(missing_debug_implementations)]
pub struct BoundedMailbox<A: Actor>(pub(crate) mpsc::Sender<Signal<A>>);

impl<A: Actor> MailboxBehaviour<A> for BoundedMailbox<A> {
    type WeakMailbox = WeakBoundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, MailboxReceiver<A>) {
        let (tx, rx) = mpsc::channel(1000);
        (BoundedMailbox(tx), MailboxReceiver::Bounded(rx))
    }

    #[inline]
    async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        self.0.send(signal).await
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
}

impl<A: Actor> Clone for BoundedMailbox<A> {
    fn clone(&self) -> Self {
        BoundedMailbox(self.0.clone())
    }
}

/// A weak bounded mailbox that does not prevent the actor from being stopped.
#[allow(missing_debug_implementations)]
pub struct WeakBoundedMailbox<A: Actor>(pub(crate) mpsc::WeakSender<Signal<A>>);

impl<A: Actor> WeakMailboxBehaviour for WeakBoundedMailbox<A> {
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

/// An unbounded mailbox, where the number of messages queued can grow infinitely.
#[allow(missing_debug_implementations)]
pub struct UnboundedMailbox<A: Actor>(pub(crate) mpsc::UnboundedSender<Signal<A>>);

impl<A: Actor> MailboxBehaviour<A> for UnboundedMailbox<A> {
    type WeakMailbox = WeakUnboundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, MailboxReceiver<A>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (UnboundedMailbox(tx), MailboxReceiver::Unbounded(rx))
    }

    #[inline]
    async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
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
}

impl<A: Actor> Clone for UnboundedMailbox<A> {
    fn clone(&self) -> Self {
        UnboundedMailbox(self.0.clone())
    }
}

/// A weak unbounded mailbox that does not prevent the actor from being stopped.
#[allow(missing_debug_implementations)]
pub struct WeakUnboundedMailbox<A: Actor>(pub(crate) mpsc::WeakUnboundedSender<Signal<A>>);

impl<A: Actor> WeakMailboxBehaviour for WeakUnboundedMailbox<A> {
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
