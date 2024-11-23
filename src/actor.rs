//! Core functionality for defining and managing actors in Kameo.
//!
//! Actors are independent units of computation that run asynchronously, sending and receiving messages.
//! Each actor operates within its own task, and its lifecycle is managed by hooks such as `on_start`, `on_panic`, and `on_stop`.
//!
//! The actor trait is designed to support fault tolerance, recovery from panics, and clean termination.
//! Lifecycle hooks allow customization of actor behavior when starting, encountering errors, or shutting down.
//!
//! This module contains the primary `Actor` trait, which all actors must implement,
//! as well as types for managing message queues (mailboxes) and actor references ([`ActorRef`]).
//!
//! # Features
//! - **Asynchronous Message Handling**: Each actor processes messages asynchronously within its own task.
//! - **Lifecycle Hooks**: Customizable hooks ([`on_start`], [`on_stop`], [`on_panic`]) for managing the actor's lifecycle.
//! - **Backpressure**: Mailboxes can be bounded or unbounded, controlling the flow of messages.
//! - **Supervision**: Actors can be linked, enabling robust supervision and error recovery systems.
//!
//! This module allows building resilient, fault-tolerant, distributed systems with flexible control over the actor lifecycle.
//!
//! [`on_start`]: Actor::on_start
//! [`on_stop`]: Actor::on_stop
//! [`on_panic`]: Actor::on_panic

mod actor_ref;
mod id;
mod kind;
pub mod pool;
pub mod pubsub;
mod spawn;

use std::any;

use futures::Future;

use crate::{
    error::{ActorStopReason, BoxError, PanicError},
    mailbox::Mailbox,
    message::{BoxDebug, BoxMessage},
    reply::BoxReplySender,
};

pub use actor_ref::*;
pub use id::*;
pub use spawn::*;

/// Core behavior of an actor, including its lifecycle events and how it processes messages.
///
/// Every actor must implement this trait, which provides hooks
/// for the actor's initialization ([`on_start`]), handling errors ([`on_panic`]), and cleanup ([`on_stop`]).
///
/// The actor runs within its own task and processes messages asynchronously from a mailbox.
/// Each actor can be linked to others, allowing for robust supervision and failure recovery mechanisms.
///
/// Methods in this trait that return [`BoxError`] will cause the actor to stop with the reason
/// [`ActorStopReason::Panicked`] if an error occurs. This enables graceful handling of actor panics
/// or errors.
///
/// # Example with Derive
///
/// ```
/// use kameo::Actor;
///
/// #[derive(Actor)]
/// struct MyActor;
/// ```
///
/// # Example Override Behaviour
///
/// ```
/// use kameo::actor::{Actor, ActorRef, WeakActorRef};
/// use kameo::error::{ActorStopReason, BoxError};
/// use kameo::mailbox::unbounded::UnboundedMailbox;
///
/// struct MyActor;
///
/// impl Actor for MyActor {
///     type Mailbox = UnboundedMailbox<Self>;
///
///     async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
///         println!("actor started");
///         Ok(())
///     }
///
///     async fn on_stop(
///         &mut self,
///         actor_ref: WeakActorRef<Self>,
///         reason: ActorStopReason,
///     ) -> Result<(), BoxError> {
///         println!("actor stopped");
///         Ok(())
///     }
/// }
/// ```
///
/// # Lifecycle Hooks
/// - `on_start`: Called when the actor starts. This is where initialization happens.
/// - `on_message`: Called when the actor receives a message to be processed.
/// - `on_panic`: Called when the actor encounters a panic or an error while processing a "tell" message.
/// - `on_stop`: Called before the actor is stopped. This allows for cleanup tasks.
/// - `on_link_died`: Hook that is invoked when a linked actor dies.
///
/// # Mailboxes
/// Actors use a mailbox to queue incoming messages. You can choose between:
/// - **Bounded Mailbox**: Limits the number of messages that can be queued, providing backpressure.
/// - **Unbounded Mailbox**: Allows an infinite number of messages, but can lead to high memory usage.
///
/// Mailboxes enable efficient asynchronous message passing with support for both backpressure and
/// unbounded queueing depending on system requirements.
///
/// [`on_start`]: Actor::on_start
/// [`on_stop`]: Actor::on_stop
/// [`on_panic`]: Actor::on_panic
pub trait Actor: Sized + Send + 'static {
    /// The mailbox type used for the actor.
    ///
    /// This can either be a `BoundedMailbox<Self>` or an `UnboundedMailbox<Self>`, depending on
    /// whether you want to enforce backpressure or allow infinite message queueing.
    ///
    /// - **Bounded Mailbox**: Prevents unlimited message growth, enforcing backpressure.
    /// - **Unbounded Mailbox**: Allows an infinite number of messages, but can consume large amounts of memory.
    type Mailbox: Mailbox<Self>;

    /// The name of the actor, which can be useful for logging or debugging.
    ///
    /// # Default Implementation
    /// By default, this returns the type name of the actor.
    #[inline]
    fn name() -> &'static str {
        any::type_name::<Self>()
    }

    /// Creates a new mailbox for the actor. This sets up the message queue and receiver for the actor.
    ///
    /// # Returns
    /// A tuple containing:
    /// - The created mailbox for sending messages.
    /// - The receiver for processing messages.
    #[inline]
    fn new_mailbox() -> (Self::Mailbox, <Self::Mailbox as Mailbox<Self>>::Receiver) {
        Self::Mailbox::default_mailbox()
    }

    /// Called when the actor starts, before it processes any messages.
    ///
    /// Messages sent internally by the actor during `on_start` are prioritized and processed
    /// before any externally sent messages, even if external messages are received first.
    ///
    /// This ensures that the actor can properly initialize before handling external messages.
    #[allow(unused_variables)]
    #[inline]
    fn on_start(
        &mut self,
        actor_ref: ActorRef<Self>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Called when the actor receives a message to be processed.
    ///
    /// By default, this method handles the incoming message immediately using the actor's standard message handling logic.
    ///
    /// Advanced use cases can override this method to customize how messages are processed, such as buffering messages for later processing or implementing custom scheduling.
    ///
    /// # Parameters
    /// - `msg`: The incoming message, wrapped in a `BoxMessage<Self>`.
    /// - `actor_ref`: A reference to the actor itself.
    /// - `tx`: An optional reply sender, used when the message expects a response.
    ///
    /// # Returns
    /// A future that resolves to `Result<(), BoxDebug>`. An `Ok(())` indicates successful processing, while an `Err` indicates an error occurred during message handling.
    ///
    /// # Default Implementation
    /// The default implementation handles the message immediately by calling `msg.handle_dyn(self, actor_ref, tx).await`.
    ///
    /// # Notes
    /// - Overriding this method allows you to intercept and manipulate messages before they are processed.
    /// - Be cautious when buffering messages, as unbounded buffering can lead to increased memory usage.
    /// - Custom implementations should ensure that messages are eventually handled or appropriately discarded to prevent message loss.
    /// - The `tx` (reply sender) is tied to the specific `BoxMessage` it corresponds to, and passing an incorrect or mismatched `tx` can lead to a panic.
    #[inline]
    fn on_message(
        &mut self,
        msg: BoxMessage<Self>,
        actor_ref: ActorRef<Self>,
        tx: Option<BoxReplySender>,
    ) -> impl Future<Output = Result<(), BoxDebug>> + Send {
        async move { msg.handle_dyn(self, actor_ref, tx).await }
    }

    /// Called when the actor encounters a panic or an error during "tell" message handling.
    ///
    /// This method gives the actor an opportunity to clean up or reset its state and determine
    /// whether it should be stopped or continue processing messages.
    ///
    /// # Parameters
    /// - `err`: The panic or error that occurred.
    ///
    /// # Returns
    /// - `Some(ActorStopReason)`: Stops the actor.
    /// - `None`: Allows the actor to continue processing messages.
    #[allow(unused_variables)]
    #[inline]
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl Future<Output = Result<Option<ActorStopReason>, BoxError>> + Send {
        async move { Ok(Some(ActorStopReason::Panicked(err))) }
    }

    /// Called when a linked actor dies.
    ///
    /// By default, the actor will stop if the reason for the linked actor's death is anything other
    /// than `Normal`. You can customize this behavior in the implementation.
    ///
    /// # Returns
    /// Whether the actor should stop or continue processing messages.
    #[allow(unused_variables)]
    #[inline]
    fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorID,
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

    /// Called before the actor stops.
    ///
    /// This allows the actor to perform any necessary cleanup or release resources before being fully stopped.
    ///
    /// # Parameters
    /// - `reason`: The reason why the actor is being stopped.
    #[allow(unused_variables)]
    #[inline]
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }
}
