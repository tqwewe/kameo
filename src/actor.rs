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
mod spawn;

use std::{any, ops::ControlFlow};

use futures::Future;

use crate::{
    error::{ActorStopReason, PanicError},
    mailbox::{MailboxReceiver, Signal},
    reply::ReplyError,
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
/// Methods in this trait that return [`Self::Error`] will cause the actor to stop with the reason
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
/// use kameo::error::{ActorStopReason, Infallible};
///
/// struct MyActor;
///
/// impl Actor for MyActor {
///     type Error = Infallible;
///
///     async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), Self::Error> {
///         println!("actor started");
///         Ok(())
///     }
///
///     async fn on_stop(
///         &mut self,
///         actor_ref: WeakActorRef<Self>,
///         reason: ActorStopReason,
///     ) -> Result<(), Self::Error> {
///         println!("actor stopped");
///         Ok(())
///     }
/// }
/// ```
///
/// # Lifecycle Hooks
/// - [`on_start`]: Called when the actor starts. This is where initialization happens.
/// - [`on_panic`]: Called when the actor encounters a panic or an error while processing a "tell" message.
/// - [`on_stop`]: Called before the actor is stopped. This allows for cleanup tasks.
/// - [`on_link_died`]: Hook that is invoked when a linked actor dies.
///
/// # Mailboxes
/// Actors use a mailbox to queue incoming messages. You can choose between:
/// - [**Bounded Mailbox**]: Limits the number of messages that can be queued, providing backpressure.
/// - [**Unbounded Mailbox**]: Allows an infinite number of messages, but can lead to high memory usage.
///
/// Mailboxes enable efficient asynchronous message passing with support for both backpressure and
/// unbounded queueing depending on system requirements.
///
/// [`on_start`]: Actor::on_start
/// [`on_panic`]: Actor::on_panic
/// [`on_stop`]: Actor::on_stop
/// [`on_link_died`]: Actor::on_link_died
pub trait Actor: Sized + Send + 'static {
    /// Actor error type.
    ///
    /// This error is used as the error returned by lifecycle hooks in this actor.
    type Error: ReplyError;

    /// The name of the actor, which can be useful for logging or debugging.
    ///
    /// # Default Implementation
    /// By default, this returns the type name of the actor.
    fn name() -> &'static str {
        any::type_name::<Self>()
    }

    /// Called when the actor starts, before it processes any messages.
    ///
    /// Messages sent internally by the actor during `on_start` are prioritized and processed
    /// before any externally sent messages, even if external messages are received first.
    ///
    /// This ensures that the actor can properly initialize before handling external messages.
    #[allow(unused_variables)]
    fn on_start(
        &mut self,
        actor_ref: ActorRef<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Called when the actor encounters a panic or an error during "tell" message handling.
    ///
    /// This method gives the actor an opportunity to clean up or reset its state and determine
    /// whether it should be stopped or continue processing messages.
    ///
    /// The `PanicError` parameter holds the error that occurred during.
    /// This error can typically be downcasted into one of a few types:
    /// - [`Self::Error`] type, which is the error type when one of the actor's lifecycle functions returns an error.
    /// - An error type returned when handling a message.
    /// - A string type, which can be accessed with `PanicError::with_str`.
    ///   String types are the result of regular panics, typically raised using the [`std::panic!`] macro.
    /// - Any other panic types. Typically uncommon, though possible with [`std::panic::panic_any`].
    ///
    /// # Returns
    /// Whether the actor should stop or continue processing messages.
    #[allow(unused_variables)]
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        async move { Ok(ControlFlow::Break(ActorStopReason::Panicked(err))) }
    }

    /// Called when a linked actor dies.
    ///
    /// By default, the actor will stop if the reason for the linked actor's death is anything other
    /// than `Normal`. You can customize this behavior in the implementation.
    ///
    /// # Returns
    /// Whether the actor should stop or continue processing messages.
    #[allow(unused_variables)]
    fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorID,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        async move {
            match &reason {
                ActorStopReason::Normal => Ok(ControlFlow::Continue(())),
                ActorStopReason::Killed
                | ActorStopReason::Panicked(_)
                | ActorStopReason::LinkDied { .. } => {
                    Ok(ControlFlow::Break(ActorStopReason::LinkDied {
                        id,
                        reason: Box::new(reason),
                    }))
                }
                #[cfg(feature = "remote")]
                ActorStopReason::PeerDisconnected => {
                    Ok(ControlFlow::Break(ActorStopReason::PeerDisconnected))
                }
            }
        }
    }

    /// Called before the actor stops.
    ///
    /// This allows the actor to perform any necessary cleanup or release resources before being fully stopped.
    ///
    /// The error returned by this method will be unwraped by kameo, causing a panic in the tokio task or
    /// thread running the actor.
    #[allow(unused_variables)]
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Awaits the next signal typically from the mailbox.
    ///
    /// This can be overwritten for more advanced actor behaviour, such as awaiting multiple channels, etc.
    /// The return value is a signal which will be handled by the actor.
    #[allow(unused_variables)]
    fn next(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> impl Future<Output = Option<Signal<Self>>> + Send {
        async move { mailbox_rx.recv().await }
    }
}
