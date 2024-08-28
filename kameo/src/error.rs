//! Defines error handling constructs for kameo.
//!
//! This module centralizes error types used throughout kameo, encapsulating common failure scenarios encountered
//! in actor lifecycle management, message passing, and actor interaction. It simplifies error handling by providing
//! a consistent set of errors that can occur in the operation of actors and their communications.

use std::{
    any::{self, Any},
    cmp, error, fmt,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::error::Elapsed,
};

use crate::{
    actor::{ActorID, Signal},
    message::BoxDebug,
    Actor,
};

/// A dyn boxed error.
pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;
/// A dyn boxed send error.
pub type BoxSendError = SendError<Box<dyn any::Any + Send>, Box<dyn any::Any + Send>>;

/// Error that can occur when sending a message to an actor.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendError<M = (), E = Infallible> {
    /// The actor isn't running.
    ActorNotRunning(M),
    /// The actor panicked or was stopped before a reply could be received.
    ActorStopped,
    /// The actors mailbox is full.
    MailboxFull(M),
    /// An error returned by the actor's message handler.
    HandlerError(E),
    /// Timed out waiting for a reply.
    Timeout(Option<M>),
    /// The actor was spawned as `!Sync`, which doesn't support queries.
    QueriesNotSupported,
}

impl<M, E> SendError<M, E> {
    /// Clears in inner data back to `()`.
    pub fn reset(self) -> SendError<(), ()> {
        match self {
            SendError::ActorNotRunning(_) => SendError::ActorNotRunning(()),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::MailboxFull(_) => SendError::MailboxFull(()),
            SendError::HandlerError(_) => SendError::HandlerError(()),
            SendError::Timeout(_) => SendError::Timeout(None),
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
            SendError::MailboxFull(msg) => SendError::MailboxFull(f(msg)),
            SendError::HandlerError(err) => SendError::HandlerError(err),
            SendError::Timeout(msg) => SendError::Timeout(msg.map(f)),
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
            SendError::MailboxFull(msg) => SendError::MailboxFull(msg),
            SendError::HandlerError(err) => SendError::HandlerError(op(err)),
            SendError::Timeout(msg) => SendError::Timeout(msg),
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
            SendError::ActorNotRunning(err) => SendError::ActorNotRunning(Box::new(err)),
            SendError::ActorStopped => SendError::QueriesNotSupported,
            SendError::MailboxFull(msg) => SendError::MailboxFull(Box::new(msg)),
            SendError::HandlerError(err) => SendError::HandlerError(Box::new(err)),
            SendError::Timeout(msg) => {
                SendError::Timeout(msg.map(|msg| Box::new(msg) as Box<dyn any::Any + Send>))
            }
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
            SendError::MailboxFull(msg) | SendError::HandlerError(SendError::MailboxFull(msg)) => {
                SendError::MailboxFull(msg)
            }
            SendError::HandlerError(SendError::HandlerError(err)) => SendError::HandlerError(err),
            SendError::Timeout(msg) | SendError::HandlerError(SendError::Timeout(msg)) => {
                SendError::Timeout(msg)
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
            SendError::ActorNotRunning(err) => SendError::ActorNotRunning(*err.downcast().unwrap()),
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::MailboxFull(err) => SendError::MailboxFull(*err.downcast().unwrap()),
            SendError::HandlerError(err) => SendError::HandlerError(*err.downcast().unwrap()),
            SendError::Timeout(err) => SendError::Timeout(err.map(|err| *err.downcast().unwrap())),
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
            SendError::MailboxFull(_) => write!(f, "MailboxFull"),
            SendError::HandlerError(err) => err.fmt(f),
            SendError::Timeout(_) => write!(f, "Timeout"),
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
            SendError::MailboxFull(_) => write!(f, "mailbox full"),
            SendError::HandlerError(err) => err.fmt(f),
            SendError::Timeout(_) => write!(f, "timeout"),
            SendError::QueriesNotSupported => {
                write!(f, "actor spawned as !Sync cannot handle queries")
            }
        }
    }
}

impl<A, M, E> From<mpsc::error::SendError<Signal<A>>> for SendError<M, E>
where
    A: Actor,
    M: 'static,
{
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self {
        SendError::ActorNotRunning(err.0.downcast_message::<M>().unwrap())
    }
}

impl<A, M, E> From<mpsc::error::TrySendError<Signal<A>>> for SendError<M, E>
where
    A: Actor,
    M: 'static,
{
    fn from(err: mpsc::error::TrySendError<Signal<A>>) -> Self {
        match err {
            mpsc::error::TrySendError::Full(signal) => {
                SendError::MailboxFull(signal.downcast_message::<M>().unwrap())
            }
            mpsc::error::TrySendError::Closed(signal) => {
                SendError::ActorNotRunning(signal.downcast_message::<M>().unwrap())
            }
        }
    }
}

impl<M, E> From<oneshot::error::RecvError> for SendError<M, E> {
    fn from(_err: oneshot::error::RecvError) -> Self {
        SendError::ActorStopped
    }
}

impl<A, M, E> From<mpsc::error::SendTimeoutError<Signal<A>>> for SendError<M, E>
where
    A: Actor,
    M: 'static,
{
    fn from(err: mpsc::error::SendTimeoutError<Signal<A>>) -> Self {
        match err {
            mpsc::error::SendTimeoutError::Timeout(msg) => {
                SendError::Timeout(Some(msg.downcast_message::<M>().unwrap()))
            }
            mpsc::error::SendTimeoutError::Closed(msg) => {
                SendError::ActorNotRunning(msg.downcast_message::<M>().unwrap())
            }
        }
    }
}

impl<M, E> From<Elapsed> for SendError<M, E> {
    fn from(_: Elapsed) -> Self {
        SendError::Timeout(None)
    }
}

impl<M, E> error::Error for SendError<M, E> where E: fmt::Debug + fmt::Display {}

/// Error that can occur when sending a message to an actor.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RemoteSendError<E> {
    /// The actor isn't running.
    ActorNotRunning,
    /// The actor panicked or was stopped before a reply could be received.
    ActorStopped,
    /// The actor's remote ID was not found.
    UnknownActor {
        /// The remote ID of the actor.
        actor_name: String,
    },
    /// The message remote ID was not found for the actor.
    UnknownMessage {
        /// The remote ID of the actor.
        actor_name: String,
        /// The remote ID of the message.
        message_name: String,
    },
    /// The remote actor was found given the ID, but was not the correct type.
    BadActorType,
    /// The actors mailbox is full.
    MailboxFull,
    /// Timed out waiting for a reply.
    Timeout,
    /// An error returned by the actor's message handler.
    HandlerError(E),
    /// Failed to serialize the message.
    SerializeMessage(String),
    /// Failed to deserialize the incoming message.
    DeserializeMessage(String),
    /// Failed to serialize the reply.
    SerializeReply(String),
    /// Failed to serialize the handler error.
    SerializeHandlerError(String),
    /// Failed to deserialize the handler error.
    DeserializeHandlerError(String),
    /// Rpc error.
    #[serde(skip)]
    Rpc(tonic::Status),
}

impl<E> RemoteSendError<E> {
    /// Maps the inner error to another type if the variant is [`HandlerError`](RemoteSendError::HandlerError).
    pub fn map_err<F, O>(self, mut op: O) -> RemoteSendError<F>
    where
        O: FnMut(E) -> F,
    {
        match self {
            RemoteSendError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped => RemoteSendError::ActorStopped,
            RemoteSendError::UnknownActor { actor_name } => {
                RemoteSendError::UnknownActor { actor_name }
            }
            RemoteSendError::UnknownMessage {
                actor_name,
                message_name,
            } => RemoteSendError::UnknownMessage {
                actor_name,
                message_name,
            },
            RemoteSendError::BadActorType => RemoteSendError::BadActorType,
            RemoteSendError::MailboxFull => RemoteSendError::MailboxFull,
            RemoteSendError::Timeout => RemoteSendError::Timeout,
            RemoteSendError::HandlerError(err) => RemoteSendError::HandlerError(op(err)),
            RemoteSendError::SerializeMessage(err) => RemoteSendError::SerializeMessage(err),
            RemoteSendError::DeserializeMessage(err) => RemoteSendError::DeserializeMessage(err),
            RemoteSendError::SerializeReply(err) => RemoteSendError::SerializeReply(err),
            RemoteSendError::SerializeHandlerError(err) => {
                RemoteSendError::SerializeHandlerError(err)
            }
            RemoteSendError::DeserializeHandlerError(err) => {
                RemoteSendError::DeserializeHandlerError(err)
            }
            RemoteSendError::Rpc(err) => RemoteSendError::Rpc(err),
        }
    }
}

impl<E> RemoteSendError<RemoteSendError<E>> {
    /// Flattens a nested SendError.
    pub fn flatten(self) -> RemoteSendError<E> {
        use RemoteSendError::*;
        match self {
            ActorNotRunning | HandlerError(ActorNotRunning) => ActorNotRunning,
            ActorStopped | HandlerError(ActorStopped) => ActorStopped,
            UnknownActor { actor_name } | HandlerError(UnknownActor { actor_name }) => {
                UnknownActor { actor_name }
            }
            UnknownMessage {
                actor_name,
                message_name,
            }
            | HandlerError(UnknownMessage {
                actor_name,
                message_name,
            }) => UnknownMessage {
                actor_name,
                message_name,
            },
            BadActorType | HandlerError(BadActorType) => BadActorType,
            MailboxFull | HandlerError(MailboxFull) => MailboxFull,
            Timeout | HandlerError(Timeout) => Timeout,
            HandlerError(HandlerError(err)) => HandlerError(err),
            SerializeMessage(err) | HandlerError(SerializeMessage(err)) => SerializeMessage(err),
            DeserializeMessage(err) | HandlerError(DeserializeMessage(err)) => {
                DeserializeMessage(err)
            }
            SerializeReply(err) | HandlerError(SerializeReply(err)) => SerializeReply(err),
            SerializeHandlerError(err) | HandlerError(SerializeHandlerError(err)) => {
                SerializeHandlerError(err)
            }
            DeserializeHandlerError(err) | HandlerError(DeserializeHandlerError(err)) => {
                RemoteSendError::DeserializeHandlerError(err)
            }
            Rpc(err) | HandlerError(Rpc(err)) => Rpc(err),
        }
    }
}

impl<M, E> From<SendError<M, E>> for RemoteSendError<E> {
    fn from(err: SendError<M, E>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => RemoteSendError::ActorNotRunning,
            SendError::ActorStopped => RemoteSendError::ActorStopped,
            SendError::MailboxFull(_) => RemoteSendError::MailboxFull,
            SendError::HandlerError(err) => RemoteSendError::HandlerError(err),
            SendError::Timeout(_) => RemoteSendError::Timeout,
            SendError::QueriesNotSupported => unimplemented!(),
        }
    }
}

impl<E> From<tonic::Status> for RemoteSendError<E> {
    fn from(err: tonic::Status) -> Self {
        RemoteSendError::Rpc(err)
    }
}

impl<E> fmt::Display for RemoteSendError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteSendError::ActorNotRunning => write!(f, "actor not running"),
            RemoteSendError::ActorStopped => write!(f, "actor stopped"),
            RemoteSendError::UnknownActor { actor_name } => {
                write!(f, "unknown actor '{actor_name}'")
            }
            RemoteSendError::UnknownMessage {
                actor_name,
                message_name,
            } => write!(
                f,
                "unknown message '{message_name}' for actor '{actor_name}'"
            ),
            RemoteSendError::BadActorType => write!(f, "bad actor type"),
            RemoteSendError::MailboxFull => write!(f, "mailbox full"),
            RemoteSendError::Timeout => write!(f, "timeout"),
            RemoteSendError::HandlerError(err) => err.fmt(f),
            RemoteSendError::SerializeMessage(err) => {
                write!(f, "failed to serialize message: {err}")
            }
            RemoteSendError::DeserializeMessage(err) => {
                write!(f, "failed to deserialize message: {err}")
            }
            RemoteSendError::SerializeReply(err) => {
                write!(f, "failed to serialize reply: {err}")
            }
            RemoteSendError::SerializeHandlerError(err) => {
                write!(f, "failed to serialize handler error: {err}")
            }
            RemoteSendError::DeserializeHandlerError(err) => {
                write!(f, "failed to deserialize handler error: {err}")
            }
            RemoteSendError::Rpc(err) => err.fmt(f),
        }
    }
}

impl<E> error::Error for RemoteSendError<E> where E: fmt::Debug + fmt::Display {}

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
        id: ActorID,
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

/// A shared error that occurs when an actor panics or returns an error from a hook in the [Actor] trait.
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

            // Types are `BoxDebug` if the panic occured as a result of a `tell` message returning an error
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

/// Errors that can occur when spawning a remote actor.
#[derive(Debug, Serialize, Deserialize)]
pub enum RemoteSpawnError {
    /// Failed to serialize actor state.
    SerializeActor(String),
    /// Failed to deserialize actor state.
    DeserializeActor(String),
    /// Unknown actor.
    UnknownActor(String),
}

impl fmt::Display for RemoteSpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteSpawnError::SerializeActor(err) => write!(f, "failed to serialize actor: {err}"),
            RemoteSpawnError::DeserializeActor(err) => {
                write!(f, "failed to deserialize actor: {err}")
            }
            RemoteSpawnError::UnknownActor(actor_name) => write!(f, "unknown actor '{actor_name}'"),
        }
    }
}

impl error::Error for RemoteSpawnError {}

/// An infallible error type, similar to std::convert::Infallible.
///
/// Kameo provides its own Infallible type in order to implement Serialize/Deserialize for it.
#[derive(Copy, Serialize, Deserialize)]
pub enum Infallible {}

impl Clone for Infallible {
    fn clone(&self) -> Infallible {
        match *self {}
    }
}

impl fmt::Debug for Infallible {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl fmt::Display for Infallible {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl error::Error for Infallible {
    fn description(&self) -> &str {
        match *self {}
    }
}

impl PartialEq for Infallible {
    fn eq(&self, _: &Infallible) -> bool {
        match *self {}
    }
}

impl Eq for Infallible {}

impl PartialOrd for Infallible {
    fn partial_cmp(&self, _other: &Self) -> Option<cmp::Ordering> {
        match *self {}
    }
}

impl Ord for Infallible {
    fn cmp(&self, _other: &Self) -> cmp::Ordering {
        match *self {}
    }
}

impl Hash for Infallible {
    fn hash<H: Hasher>(&self, _: &mut H) {
        match *self {}
    }
}
