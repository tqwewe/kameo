//! Defines error handling constructs for kameo.
//!
//! This module centralizes error types used throughout kameo, encapsulating common failure scenarios encountered
//! in actor lifecycle management, message passing, and actor interaction. It simplifies error handling by providing
//! a consistent set of errors that can occur in the operation of actors and their communications.

use std::{
    any::{self, Any},
    convert::Infallible,
    error, fmt,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{self, Poll},
};

use futures::{Future, FutureExt};
use tokio::sync::{mpsc, oneshot};

use crate::{
    actor::{MessageReceiver, Signal},
    message::{BoxDebug, BoxReply},
};

/// A dyn boxed error.
pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;
/// A dyn boxed send error.
pub type BoxSendError = SendError<Box<dyn any::Any + Send>, Box<dyn any::Any + Send>>;

/// A result returned when sending a message to an actor, allowing it to be awaited asyncronously, or received
/// in a blocking context with `SendResult::blocking_recv()`.
#[derive(Debug)]
pub struct SendResult<T, M, E> {
    inner: SendResultInner<T, M, E>,
}

#[derive(Debug)]
enum SendResultInner<T, M, E> {
    Ok(MessageReceiver<T, M, E>),
    Err(Option<SendError<M, E>>),
}

impl<T, M, E> SendResult<T, M, E>
where
    T: 'static,
    M: 'static,
    E: 'static,
{
    pub(crate) fn ok(rx: oneshot::Receiver<Result<BoxReply, BoxSendError>>) -> SendResult<T, M, E> {
        SendResult {
            inner: SendResultInner::Ok(MessageReceiver::new(rx)),
        }
    }

    pub(crate) fn err(err: SendError<M, E>) -> SendResult<T, M, E> {
        SendResult {
            inner: SendResultInner::Err(Some(err)),
        }
    }

    /// Receives the message response outside an async context.
    pub fn blocking_recv(self) -> Result<T, SendError<M, E>> {
        match self.inner {
            SendResultInner::Ok(rx) => rx.blocking_recv(),
            SendResultInner::Err(mut err) => Err(err.take().unwrap()),
        }
    }

    /// Converts the `SendResult` into a std `Result`.
    pub fn into_result(self) -> Result<MessageReceiver<T, M, E>, SendError<M, E>> {
        match self.inner {
            SendResultInner::Ok(rx) => Ok(rx),
            SendResultInner::Err(mut err) => Err(err.take().unwrap()),
        }
    }
}

impl<T, M, E> Future for SendResult<T, M, E>
where
    T: Unpin + 'static,
    M: Unpin + 'static,
    E: Unpin + 'static,
{
    type Output = Result<T, SendError<M, E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().inner {
            SendResultInner::Ok(rx) => rx.poll_unpin(cx),
            SendResultInner::Err(err) => Poll::Ready(Err(err.take().unwrap())),
        }
    }
}

/// Error that can occur when sending a message to an actor.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendError<M = (), E = Infallible> {
    /// The actor isn't running.
    ActorNotRunning(M),
    /// The actor panicked or was stopped before a reply could be received.
    ActorStopped,
    /// An error returned by the actor's message handler.
    HandlerError(E),
    /// The actor was spawned as `!Sync`, which doesn't support blocking messages.
    BlockingMessagesNotSupported,
    /// The actor was spawned as `!Sync`, which doesn't support queries.
    QueriesNotSupported,
}

impl<M, E> SendError<M, E> {
    /// Clears in inner data back to `()`.
    pub fn reset(self) -> SendError<(), ()> {
        match self {
            SendError::ActorNotRunning(_) => SendError::ActorNotRunning(()),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::HandlerError(_) => SendError::HandlerError(()),
            SendError::BlockingMessagesNotSupported => SendError::BlockingMessagesNotSupported,
            SendError::QueriesNotSupported => SendError::QueriesNotSupported,
        }
    }

    /// Maps the inner message to another type if the variant is [`ActorNotRunning`](SendError::ActorNotRunning).
    pub fn map_msg<N, F>(self, mut f: F) -> SendError<N, E>
    where
        F: FnMut(M) -> N,
    {
        match self {
            SendError::ActorNotRunning(msg) => SendError::ActorNotRunning(f(msg)),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::HandlerError(err) => SendError::HandlerError(err),
            SendError::BlockingMessagesNotSupported => SendError::BlockingMessagesNotSupported,
            SendError::QueriesNotSupported => SendError::QueriesNotSupported,
        }
    }

    /// Maps the inner error to another type if the variant is [`HandlerError`](SendError::HandlerError).
    pub fn map_err<F, O>(self, mut op: O) -> SendError<M, F>
    where
        O: FnMut(E) -> F,
    {
        match self {
            SendError::ActorNotRunning(msg) => SendError::ActorNotRunning(msg),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::HandlerError(err) => SendError::HandlerError(op(err)),
            SendError::BlockingMessagesNotSupported => SendError::BlockingMessagesNotSupported,
            SendError::QueriesNotSupported => SendError::QueriesNotSupported,
        }
    }

    /// Converts the inner error types to `Box<dyn Any + Send>`.
    pub fn boxed(self) -> BoxSendError
    where
        M: Send + 'static,
        E: Send + 'static,
    {
        match self {
            SendError::HandlerError(err) => SendError::HandlerError(Box::new(err)),
            SendError::ActorNotRunning(err) => SendError::ActorNotRunning(Box::new(err)),
            SendError::ActorStopped => SendError::QueriesNotSupported,
            SendError::BlockingMessagesNotSupported => SendError::BlockingMessagesNotSupported,
            SendError::QueriesNotSupported => SendError::QueriesNotSupported,
        }
    }
}

impl<M, E> SendError<M, SendError<M, E>> {
    /// Flattens a nested SendError.
    pub fn flatten(self) -> SendError<M, E> {
        match self {
            SendError::ActorNotRunning(msg)
            | SendError::HandlerError(SendError::ActorNotRunning(msg)) => {
                SendError::ActorNotRunning(msg)
            }
            SendError::ActorStopped | SendError::HandlerError(SendError::ActorStopped) => {
                SendError::ActorStopped
            }
            SendError::HandlerError(SendError::HandlerError(err)) => SendError::HandlerError(err),
            SendError::BlockingMessagesNotSupported
            | SendError::HandlerError(SendError::BlockingMessagesNotSupported) => {
                SendError::BlockingMessagesNotSupported
            }
            SendError::QueriesNotSupported
            | SendError::HandlerError(SendError::QueriesNotSupported) => {
                SendError::QueriesNotSupported
            }
        }
    }
}

impl BoxSendError {
    /// Downcasts the inner error types to a concrete type.
    pub fn downcast<M, E>(self) -> SendError<M, E>
    where
        M: 'static,
        E: 'static,
    {
        match self {
            SendError::HandlerError(err) => SendError::HandlerError(*err.downcast().unwrap()),
            SendError::ActorNotRunning(err) => SendError::ActorNotRunning(*err.downcast().unwrap()),
            SendError::ActorStopped => SendError::QueriesNotSupported,
            SendError::BlockingMessagesNotSupported => SendError::BlockingMessagesNotSupported,
            SendError::QueriesNotSupported => SendError::QueriesNotSupported,
        }
    }
}

impl<M, E> fmt::Debug for SendError<M, E>
where
    E: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::ActorNotRunning(_) => write!(f, "ActorNotRunning"),
            SendError::ActorStopped => write!(f, "ActorStopped"),
            SendError::HandlerError(err) => err.fmt(f),
            SendError::BlockingMessagesNotSupported => write!(f, "BlockingMessagesNotSupported"),
            SendError::QueriesNotSupported => write!(f, "QueriesNotSupported"),
        }
    }
}

impl<M, E> fmt::Display for SendError<M, E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::ActorNotRunning(_) => write!(f, "actor not running"),
            SendError::ActorStopped => write!(f, "actor stopped"),
            SendError::HandlerError(err) => err.fmt(f),
            SendError::BlockingMessagesNotSupported => {
                write!(f, "actor spawned as !Sync cannot handle blocking messages")
            }
            SendError::QueriesNotSupported => {
                write!(f, "actor spawned as !Sync cannot handle queries")
            }
        }
    }
}

impl<A, M, E> From<mpsc::error::SendError<Signal<A>>> for SendError<M, E>
where
    M: 'static,
{
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self {
        SendError::ActorNotRunning(err.0.downcast_message::<M>().unwrap())
    }
}

impl<M, E> From<oneshot::error::RecvError> for SendError<M, E> {
    fn from(_err: oneshot::error::RecvError) -> Self {
        SendError::ActorStopped
    }
}

impl<M, E> error::Error for SendError<M, E> where E: fmt::Debug + fmt::Display {}

/// Reason for an actor being stopped.
#[derive(Clone)]
pub enum ActorStopReason {
    /// Actor stopped normally.
    Normal,
    /// Actor was killed.
    Killed,
    /// Actor panicked.
    Panicked(PanicError),
    /// Link died.
    LinkDied {
        /// Actor ID.
        id: u64,
        /// Actor died reason.
        reason: Box<ActorStopReason>,
    },
}

impl fmt::Debug for ActorStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorStopReason::Normal => write!(f, "Normal"),
            ActorStopReason::Killed => write!(f, "Killed"),
            ActorStopReason::Panicked(_) => write!(f, "Panicked"),
            ActorStopReason::LinkDied { id, reason } => f
                .debug_struct("LinkDied")
                .field("id", id)
                .field("reason", &reason)
                .finish(),
        }
    }
}

impl fmt::Display for ActorStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorStopReason::Normal => write!(f, "actor stopped normally"),
            ActorStopReason::Killed => write!(f, "actor was killed"),
            ActorStopReason::Panicked(err) => err.fmt(f),
            ActorStopReason::LinkDied { id, reason: _ } => {
                write!(f, "link {id} died")
            }
        }
    }
}

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
                .map(f)
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
