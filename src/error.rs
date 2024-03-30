use std::{
    any::Any,
    error, fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::sync::{mpsc, oneshot};

use crate::{actor_ref::Signal, message::BoxDebug};

/// A dyn boxed error.
pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;

/// Error that can occur when sending a message to an actor.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendError<E = ()> {
    /// The actor isn't running.
    ActorNotRunning(E),
    /// The actor panicked or was stopped.
    ActorStopped,
}

impl<E> SendError<E> {
    /// Clears in inner data back to `()`.
    pub fn reset(self) -> SendError<()> {
        match self {
            SendError::ActorNotRunning(_) => SendError::ActorNotRunning(()),
            SendError::ActorStopped => SendError::ActorStopped,
        }
    }
}

impl<E> fmt::Debug for SendError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::ActorNotRunning(_) => {
                write!(f, "ActorNotRunning")
            }
            SendError::ActorStopped => write!(f, "ActorStopped"),
        }
    }
}

impl<E> fmt::Display for SendError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::ActorNotRunning(_) => write!(f, "actor not running"),
            SendError::ActorStopped => write!(f, "actor stopped"),
        }
    }
}

impl<A, M> From<mpsc::error::SendError<Signal<A>>> for SendError<M>
where
    M: 'static,
{
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self {
        SendError::ActorNotRunning(err.0.downcast_message::<M>().unwrap())
    }
}

impl<M> From<oneshot::error::RecvError> for SendError<M> {
    fn from(_err: oneshot::error::RecvError) -> Self {
        SendError::ActorStopped
    }
}

impl<E> error::Error for SendError<E> {}

/// A shared error that occurs when an actor panics or returns an error from a hook in the [Actor](crate::Actor) trait.
#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct PanicError(Arc<Mutex<Box<dyn Any + Send>>>);

impl PanicError {
    /// Creates a new PanicError from a generic error.
    pub fn new<E>(err: E) -> Self
    where
        E: Send + 'static,
    {
        PanicError(Arc::new(Mutex::new(Box::new(err))))
    }

    /// Creates a new PanicError from a generic boxed error.
    pub fn new_boxed(err: Box<dyn Any + Send>) -> Self {
        PanicError(Arc::new(Mutex::new(err)))
    }

    /// Calls the passed closure `f` with an option containing the boxed any type downcasted into a `Cow<'static, str>`,
    /// or `None` if it's not a string type.
    pub fn with_str<F, R>(
        &self,
        f: F,
    ) -> Result<Option<R>, PoisonError<MutexGuard<'_, Box<dyn Any + Send>>>>
    where
        F: FnOnce(&str) -> R,
    {
        self.with(|any| {
            any.downcast_ref::<&'static str>()
                .copied()
                .or_else(|| any.downcast_ref::<String>().map(String::as_str))
                .map(|s| f(s))
        })
    }

    /// Calls the passed closure `f` with the inner type downcasted into `T`, otherwise returns `None`.
    pub fn with_downcast_ref<T, F, R>(
        &self,
        f: F,
    ) -> Result<Option<R>, PoisonError<MutexGuard<'_, Box<dyn Any + Send>>>>
    where
        T: 'static,
        F: FnOnce(&T) -> R,
    {
        let lock = self.0.lock()?;
        Ok(lock.downcast_ref().map(f))
    }

    /// Returns a reference to the error as a `Box<dyn Any + Send>`.
    pub fn with<F, R>(&self, f: F) -> Result<R, PoisonError<MutexGuard<'_, Box<dyn Any + Send>>>>
    where
        F: FnOnce(&Box<dyn Any + Send>) -> R,
    {
        let lock = self.0.lock()?;
        Ok(f(&lock))
    }
}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.with(|any| {
            // Types are strings if panicked with the `std::panic!` macro
            let s = any
                .downcast_ref::<&'static str>()
                .copied()
                .or_else(|| any.downcast_ref::<String>().map(String::as_str));
            if let Some(s) = s {
                return write!(f, "panicked: {s}");
            }

            // Types are `BoxError` if the panic occured because of an actor hook returning an error
            let box_err = any.downcast_ref::<BoxError>();
            if let Some(err) = box_err {
                return write!(f, "panicked: {err}");
            }

            // Types are `BoxDebug` if the panic occured as a result of a `send_async` message returning an error
            let box_err = any.downcast_ref::<BoxDebug>();
            if let Some(err) = box_err {
                return write!(f, "panicked: {:?}", Err::<(), _>(err));
            }

            write!(f, "panicked")
        })
        .ok()
        .unwrap_or_else(|| write!(f, "panicked"))
    }
}
