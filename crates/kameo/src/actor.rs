use std::{any, borrow::Cow};

use futures::Future;

use crate::error::{ActorStopReason, BoxError, PanicError};

/// Functionality for an actor including lifecycle hooks.
///
/// Methods in this trait that return `BoxError` will stop the actor with the reason
/// `ActorReason::Panicked` containing the error.
///
/// # Example
///
/// ```
/// struct MyActor;
///
/// impl Actor for MyActor {
///     async fn on_start(&mut self) -> Result<(), BoxError> {
///         println!("actor started");
///         Ok(())
///     }
///
///     async fn on_panic(&mut self, err: PanicErr) -> Result<Option<ActorStopReason>, BoxError> {
///         println!("actor panicked");
///         Ok(Some(ActorStopReason::Panicked(err))) // Return some to stop the actor
///     }
///
///     async fn on_stop(&mut self, reason: ActorStopReason) -> Result<(), BoxError> {
///         println!("actor stopped");
///         Ok(())
///     }
/// }
/// ```
pub trait Actor: Sized {
    /// Actor name, useful for logging.
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(any::type_name::<Self>())
    }

    /// The maximum number of concurrent queries to handle at a time.
    ///
    /// This defaults to the number of cpus on the system.
    fn max_concurrent_queries() -> usize {
        num_cpus::get()
    }

    /// Hook that is called before the actor starts processing messages.
    ///
    /// # Returns
    /// A result indicating successful initialization or an error if initialization fails.
    fn on_start(&mut self) -> impl Future<Output = Result<(), BoxError>> + Send {
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
    fn on_panic(
        &mut self,
        err: PanicError,
    ) -> impl Future<Output = Result<Option<ActorStopReason>, BoxError>> + Send {
        async move { Ok(Some(ActorStopReason::Panicked(err))) }
    }

    /// Hook that is called before the actor is stopped.
    ///
    /// This method allows for cleanup and finalization tasks to be performed before the
    /// actor is fully stopped. It can be used to release resources, notify other actors,
    /// or complete any final tasks.
    ///
    /// # Parameters
    /// - `reason`: The reason why the actor is being stopped.
    fn on_stop(
        self,
        _reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
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
}
