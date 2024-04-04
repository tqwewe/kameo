use std::any;

use futures::Future;

use crate::{
    actor_ref::ActorRef,
    error::{ActorStopReason, BoxError, PanicError},
    message::{Message, Reply},
};

/// Functionality for an actor including lifecycle hooks.
///
/// Methods in this trait that return `BoxError` will stop the actor with the reason
/// `ActorReason::Panicked` containing the error.
///
/// # Example
///
/// ```
/// use kameo::{Actor, ActorStopReason, BoxError, PanicError};
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
    fn name() -> &'static str {
        any::type_name::<Self>()
    }

    /// The maximum number of concurrent queries to handle at a time.
    ///
    /// This defaults to the number of cpus on the system.
    fn max_concurrent_queries() -> usize {
        num_cpus::get()
    }

    /// Retrieves a reference to the current actor.
    ///
    /// # Panics
    ///
    /// This function will panic if called outside the scope of an actor.
    ///
    /// # Returns
    /// A reference to the actor of type `Self::Ref`.
    fn actor_ref(&self) -> ActorRef<Self>
    where
        Self: 'static,
    {
        match Self::try_actor_ref() {
            Some(actor_ref) => actor_ref,
            None => panic!("actor_ref called outside the scope of an actor"),
        }
    }

    /// Retrieves a reference to the current actor, if available.
    ///
    /// # Returns
    /// An `Option` containing a reference to the actor of type `Self::Ref` if available,
    /// or `None` if the actor reference is not available.
    fn try_actor_ref() -> Option<ActorRef<Self>>
    where
        Self: 'static,
    {
        ActorRef::current()
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

impl<M, R> Actor for fn(M) -> R {}

impl<M, Fu, R> Message<M> for fn(M) -> Fu
where
    M: Send + 'static,
    Fu: Future<Output = R> + Send + 'static,
    R: Reply + Send + 'static,
{
    type Reply = R;

    async fn handle(&mut self, msg: M) -> Self::Reply {
        self(msg).await
    }
}
