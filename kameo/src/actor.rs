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
mod mailbox;
mod pool;
mod pubsub;
mod spawn;

use std::any;

use futures::Future;

use crate::error::{ActorStopReason, BoxError, PanicError};

pub use actor_ref::*;
pub use mailbox::*;
pub use pool::*;
pub use pubsub::*;
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
    type Mailbox: Mailbox<Self>;

    /// Actor name, useful for logging.
    fn name() -> &'static str {
        any::type_name::<Self>()
    }

    /// Creates a new mailbox.
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
