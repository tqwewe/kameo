//! Defines error handling constructs for kameo.
//!
//! This module centralizes error types used throughout kameo, encapsulating common failure scenarios encountered
//! in actor lifecycle management, message passing, and actor interaction. It simplifies error handling by providing
//! a consistent set of errors that can occur in the operation of actors and their communications.

use std::{
    any::{self},
    cmp, error, fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex,
        atomic::{AtomicPtr, Ordering},
    },
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize, ser::SerializeStruct};
use tokio::{
    sync::{mpsc, oneshot},
    time::error::Elapsed,
};

use crate::{Actor, actor::ActorId, mailbox::Signal, reply::ReplyError};

type ErrorHookFn = fn(&PanicError);

static PANIC_HOOK: AtomicPtr<()> = AtomicPtr::new(default_panic_hook as *mut ());

#[allow(unused_variables)]
fn default_panic_hook(err: &PanicError) {
    #[cfg(feature = "tracing")]
    tracing::error!("actor panicked: {err:?}");
}

/// Sets a custom error hook function that's called when an actor's lifecycle hooks return an error.
///
/// This function allows you to define custom error handling behavior when an actor's
/// `on_start` or `on_stop` method returns an error. The hook will be called immediately
/// when such errors occur, regardless of whether the error is explicitly handled elsewhere.
///
/// By default, the actor system uses a hook that simply logs the error. Setting a custom
/// hook allows for more sophisticated error handling, such as metrics collection,
/// alerting, or custom logging formats.
///
/// # Parameters
///
/// * `hook`: A function that takes a reference to the error information and performs
///   custom error handling.
///
/// # Example
///
/// ```
/// use kameo::error::{set_actor_error_hook, PanicError};
///
/// // Define a custom error hook
/// fn my_custom_hook(err: &PanicError) {
///     // log the error or something...
/// }
///
/// // Install the custom hook
/// set_actor_error_hook(my_custom_hook);
/// ```
///
/// # Notes
///
/// * This hook is global and will affect all actors in the system.
/// * Setting a new hook replaces any previously set hook.
/// * The hook is called even if the error is also being explicitly handled via
///   `wait_for_startup_result` or `wait_for_shutdown_result`.
pub fn set_actor_error_hook(hook: ErrorHookFn) {
    let fn_ptr = hook as *mut ();
    PANIC_HOOK.store(fn_ptr, Ordering::SeqCst);
}

pub(crate) fn invoke_actor_error_hook(err: &PanicError) {
    // Load the function pointer atomically
    let fn_ptr = PANIC_HOOK.load(Ordering::SeqCst);

    // Cast back to function type and call it
    let hook = unsafe { std::mem::transmute::<*mut (), ErrorHookFn>(fn_ptr) };
    hook(err);
}

/// A dyn boxed send error.
pub type BoxSendError = SendError<Box<dyn any::Any + Send>, Box<dyn any::Any + Send>>;

/// Error that can occur when sending a message to an actor.
#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
}

impl<M, E> SendError<M, E> {
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
            SendError::ActorStopped => SendError::ActorStopped,
            SendError::MailboxFull(msg) => SendError::MailboxFull(Box::new(msg)),
            SendError::HandlerError(err) => SendError::HandlerError(Box::new(err)),
            SendError::Timeout(msg) => {
                SendError::Timeout(msg.map(|msg| Box::new(msg) as Box<dyn any::Any + Send>))
            }
        }
    }

    /// Returns the inner message if available.
    pub fn msg(self) -> Option<M> {
        match self {
            SendError::ActorNotRunning(msg) => Some(msg),
            SendError::MailboxFull(msg) => Some(msg),
            SendError::Timeout(msg) => msg,
            _ => None,
        }
    }

    /// Returns the inner error if available.
    pub fn err(self) -> Option<E> {
        match self {
            SendError::HandlerError(err) => Some(err),
            _ => None,
        }
    }

    /// Unwraps the inner message, consuming the `self` value.
    ///
    /// # Panics
    ///
    /// Panics if the error does not contain the inner message.
    pub fn unwrap_msg(self) -> M {
        match self.msg() {
            Some(msg) => msg,
            None => panic!("called `SendError::unwrap_msg()` on a non message error"),
        }
    }

    /// Unwraps the inner handler error, consuming the `self` value.
    ///
    /// # Panics
    ///
    /// Panics if the error does not contain a handler error.
    pub fn unwrap_err(self) -> E {
        match self.err() {
            Some(err) => err,
            None => panic!("called `SendError::unwrap_err()` on a non error"),
        }
    }

    pub(crate) fn reset_err_infallible<F>(self) -> SendError<M, F> {
        self.map_err(|_| panic!("reset err infallible called on a `SendError::HandlerError`"))
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
        self.try_downcast().unwrap()
    }

    /// Downcasts the inner error types to a concrete type, returning an error if its the wrong type.
    pub fn try_downcast<M, E>(self) -> Result<SendError<M, E>, Self>
    where
        M: 'static,
        E: 'static,
    {
        match self {
            SendError::ActorNotRunning(err) => Ok(SendError::ActorNotRunning(
                *err.downcast::<M>().map_err(SendError::ActorNotRunning)?,
            )),
            SendError::ActorStopped => Ok(SendError::ActorStopped),
            SendError::MailboxFull(err) => Ok(SendError::MailboxFull(
                *err.downcast().map_err(SendError::MailboxFull)?,
            )),
            SendError::HandlerError(err) => Ok(SendError::HandlerError(
                *err.downcast().map_err(SendError::HandlerError)?,
            )),
            SendError::Timeout(err) => Ok(SendError::Timeout(
                err.map(|err| {
                    err.downcast()
                        .map(|v| *v)
                        .map_err(|err| SendError::Timeout(Some(err)))
                })
                .transpose()?,
            )),
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

/// Reason for an actor being stopped.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
        id: ActorId,
        /// Actor died reason.
        reason: Box<ActorStopReason>,
    },
    /// The peer was disconnected.
    #[cfg(feature = "remote")]
    PeerDisconnected,
}

impl fmt::Debug for ActorStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorStopReason::Normal => write!(f, "Normal"),
            ActorStopReason::Killed => write!(f, "Killed"),
            ActorStopReason::Panicked(err) => {
                let mut dbg_struct = f.debug_struct("Panicked");
                err.with_debug_inner(|err| {
                    dbg_struct.field("err", err);
                });
                dbg_struct.finish()
            }
            ActorStopReason::LinkDied { id, reason } => f
                .debug_struct("LinkDied")
                .field("id", id)
                .field("reason", &reason)
                .finish(),
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => write!(f, "PeerDisconnected"),
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
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => write!(f, "peer disconnected"),
        }
    }
}

/// An error type returned from actor startup/shutdown results.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum HookError<E> {
    /// The hook panic error.
    Panicked(PanicError),
    /// The returned hook error.
    Error(E),
}

impl<E> fmt::Display for HookError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HookError::Panicked(err) => err.fmt(f),
            HookError::Error(err) => err.fmt(f),
        }
    }
}

impl<E> error::Error for HookError<E> where E: error::Error {}

/// A shared error that occurs when an actor panics or returns an error from a hook in the [Actor] trait.
#[derive(Clone)]
pub struct PanicError {
    kind: PanicErrorKind,
    reason: PanicReason,
}

#[derive(Clone)]
enum PanicErrorKind {
    /// Local actor error — preserves concrete type for downcasting.
    Dynamic(Arc<Mutex<Box<dyn ReplyError>>>),
    /// Deserialized from network — just the Display string, no Box.
    Message(String),
}

impl PanicError {
    /// Creates a new PanicError from a generic boxed reply error.
    pub fn new(err: Box<dyn ReplyError>, reason: PanicReason) -> Self {
        PanicError {
            kind: PanicErrorKind::Dynamic(Arc::new(Mutex::new(err))),
            reason,
        }
    }

    /// Creates a [`PanicError`] from a pre-formatted error string.
    ///
    /// Useful when deserializing a remote error where the original concrete
    /// type is unavailable — only the `Display` output was transmitted.
    pub fn from_wire(err: String, reason: PanicReason) -> Self {
        PanicError {
            kind: PanicErrorKind::Message(err),
            reason,
        }
    }

    pub(crate) fn new_from_panic_any(err: Box<dyn any::Any + Send>, reason: PanicReason) -> Self {
        err.downcast::<&'static str>()
            .map(|s| PanicError::new(Box::new(*s), reason))
            .or_else(|err| {
                err.downcast::<String>()
                    .map(|s| PanicError::new(Box::new(*s), reason))
            })
            .unwrap_or_else(|err| PanicError::new(Box::new(err), reason))
    }

    /// Returns the reason for the panic.
    pub fn reason(&self) -> PanicReason {
        self.reason
    }

    /// Returns the error message as a string, if available.
    pub fn with_str<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&str) -> R,
    {
        match &self.kind {
            PanicErrorKind::Message(s) => Some(f(s)),
            PanicErrorKind::Dynamic(err) => {
                let lock = match err.lock() {
                    Ok(lock) => lock,
                    Err(poisoned) => poisoned.into_inner(),
                };
                lock.downcast_ref::<&str>()
                    .copied()
                    .or_else(|| lock.downcast_ref::<String>().map(String::as_str))
                    .map(f)
            }
        }
    }

    /// Downcasts and clones the inner error, returning `Some` if the panic error matches the type `T`.
    pub fn downcast<T>(&self) -> Option<T>
    where
        T: ReplyError + Clone,
    {
        self.with_downcast_ref(|err: &T| err.clone())
    }

    /// Calls the passed closure `f` with the inner type downcast into `T`, otherwise returns `None`.
    pub fn with_downcast_ref<T, F, R>(&self, f: F) -> Option<R>
    where
        T: ReplyError,
        F: FnOnce(&T) -> R,
    {
        match &self.kind {
            PanicErrorKind::Message(_) => None,
            PanicErrorKind::Dynamic(err) => match err.lock() {
                Ok(lock) => lock.downcast_ref().map(f),
                Err(err) => err.get_ref().downcast_ref().map(f),
            },
        }
    }

    /// Returns a reference to the error as a `&Box<dyn ReplyError>`.
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Box<dyn ReplyError>) -> R,
    {
        match &self.kind {
            PanicErrorKind::Dynamic(err) => match err.lock() {
                Ok(lock) => f(&lock),
                Err(err) => f(err.get_ref()),
            },
            PanicErrorKind::Message(s) => {
                let boxed: Box<dyn ReplyError> = Box::new(s.clone());
                f(&boxed)
            }
        }
    }

    fn with_debug_inner<F>(&self, mut f: F)
    where
        F: FnMut(&dyn fmt::Debug),
    {
        match &self.kind {
            PanicErrorKind::Message(s) => f(&s),
            PanicErrorKind::Dynamic(_) => {
                self.with_str(|s| f(&s))
                    .or_else(|| self.with_downcast_ref::<Box<dyn ReplyError>, _, _>(|err| f(err)))
                    .unwrap_or_else(|| self.with(|any| f(any)));
            }
        }
    }
}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.with_str(|s| write!(f, "{}: {s}", self.reason))
            .unwrap_or_else(|| write!(f, "{}", self.reason))
    }
}

impl fmt::Debug for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg_struct = f.debug_struct("PanicError");

        self.with_debug_inner(|err| {
            dbg_struct.field("err", err);
            dbg_struct.field("reason", &self.reason);
        });

        dbg_struct.finish()
    }
}

impl error::Error for PanicError {}

#[cfg(feature = "serde")]
impl Serialize for PanicError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut ser = serializer.serialize_struct("PanicError", 2)?;
        ser.serialize_field("err", &self.to_string())?;
        ser.serialize_field("reason", &self.reason)?;
        ser.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for PanicError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            Err,
            Reason,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> serde::de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str("`err` or `reason`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "err" => Ok(Field::Err),
                            "reason" => Ok(Field::Reason),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct PanicErrorVisitor;

        impl<'de> serde::de::Visitor<'de> for PanicErrorVisitor {
            type Value = PanicError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct PanicError")
            }

            fn visit_map<V>(self, mut map: V) -> Result<PanicError, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut err: Option<String> = None;
                let mut reason = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Err => {
                            if err.is_some() {
                                return Err(serde::de::Error::duplicate_field("err"));
                            }
                            err = Some(map.next_value()?);
                        }
                        Field::Reason => {
                            if reason.is_some() {
                                return Err(serde::de::Error::duplicate_field("reason"));
                            }
                            reason = Some(map.next_value()?);
                        }
                    }
                }

                let err = err.ok_or_else(|| serde::de::Error::missing_field("err"))?;
                let reason = reason.ok_or_else(|| serde::de::Error::missing_field("reason"))?;

                Ok(PanicError::from_wire(err, reason))
            }
        }

        const FIELDS: &[&str] = &["err", "reason"];
        deserializer.deserialize_struct("PanicError", FIELDS, PanicErrorVisitor)
    }
}

/// Describes the cause of an actor panic or fatal error.
///
/// In kameo, several error conditions are treated as panics, triggering the
/// [`on_panic`](crate::actor::Actor::on_panic) lifecycle hook and potentially
/// stopping the actor.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PanicReason {
    /// A message handler panicked during execution.
    ///
    /// This occurs when an actor's [`Message::handle`](crate::message::Message::handle)
    /// implementation panics or unwinds.
    HandlerPanic,
    /// The [`on_message`] hook returned an error.
    ///
    /// In the default implementation, this occurs when a message handler returns
    /// an error during a [`tell`](crate::actor::ActorRef::tell) operation, where
    /// there's no mechanism to return the error to the caller. However, if
    /// [`on_message`] is overridden with
    /// custom logic, this variant indicates that the custom implementation
    /// returned an error.
    ///
    /// [`on_message`]: Actor::on_message
    OnMessage,
    /// The [`on_start`](Actor::on_start) lifecycle hook returned an error.
    OnStart,
    /// The [`on_panic`](Actor::on_panic) lifecycle hook returned an error.
    OnPanic,
    /// The [`on_link_died`](Actor::on_link_died) lifecycle hook returned an error.
    OnLinkDied,
    /// The [`on_stop`](Actor::on_stop) lifecycle hook returned an error.
    OnStop,
    /// The [`next`](Actor::next) lifecycle hook returned an error.
    Next,
}

impl PanicReason {
    /// Returns `true` if the panic occurred in a lifecycle hook.
    ///
    /// Lifecycle hooks include `on_start`, `on_panic`, `on_link_died`, and `on_stop`.
    /// This can be useful for distinguishing between initialization/cleanup errors
    /// and runtime message handling errors.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::error::PanicReason;
    ///
    /// assert!(PanicReason::OnStart.is_lifecycle_hook());
    /// assert!(!PanicReason::HandlerPanic.is_lifecycle_hook());
    /// ```
    pub fn is_lifecycle_hook(&self) -> bool {
        matches!(
            self,
            PanicReason::OnStart
                | PanicReason::OnPanic
                | PanicReason::OnLinkDied
                | PanicReason::OnStop
        )
    }

    /// Returns `true` if the panic occurred while processing a message.
    ///
    /// This includes both panics during message handler execution and errors
    /// returned by `on_message`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::error::PanicReason;
    ///
    /// assert!(PanicReason::HandlerPanic.is_message_processing());
    /// assert!(PanicReason::OnMessage.is_message_processing());
    /// assert!(!PanicReason::OnStart.is_message_processing());
    /// ```
    pub fn is_message_processing(&self) -> bool {
        matches!(self, PanicReason::HandlerPanic | PanicReason::OnMessage)
    }
}

impl fmt::Display for PanicReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PanicReason::HandlerPanic => write!(f, "message handler panicked"),
            PanicReason::OnMessage => write!(f, "on_message returned error"),
            PanicReason::OnStart => write!(f, "on_start returned error"),
            PanicReason::OnPanic => write!(f, "on_panic returned error"),
            PanicReason::OnLinkDied => write!(f, "on_link_died returned error"),
            PanicReason::OnStop => write!(f, "on_stop returned error"),
            PanicReason::Next => write!(f, "next returned error"),
        }
    }
}

/// An infallible error type, similar to [std::convert::Infallible].
///
/// Kameo provides its own Infallible type in order to implement Serialize/Deserialize for it.
#[derive(Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Infallible {}

impl Clone for Infallible {
    #[allow(clippy::non_canonical_clone_impl)]
    fn clone(&self) -> Infallible {
        *self
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
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, _other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(_other))
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

#[cfg(all(feature = "remote", not(feature = "serde-codec"),))]
impl crate::remote::codec::Encode for Infallible {
    fn encode(&self) -> Result<Vec<u8>, crate::remote::codec::CodecError> {
        match *self {}
    }
}

#[cfg(all(feature = "remote", not(feature = "serde-codec"),))]
impl crate::remote::codec::Decode for Infallible {
    fn decode(_bytes: &[u8]) -> Result<Self, crate::remote::codec::CodecError> {
        Err(crate::remote::codec::CodecError::new(
            "cannot decode Infallible",
        ))
    }
}

/// An error that can occur when registering & looking up actors by name.
#[derive(Debug)]
pub enum RegistryError {
    /// The actor swarm has not been bootstrapped.
    #[cfg(feature = "remote")]
    SwarmNotBootstrapped,
    /// The remote actor was found given the ID, but was not the correct type.
    BadActorType,
    /// An actor has already been registered under the name.
    NameAlreadyRegistered,
    /// Quorum failed.
    #[cfg(feature = "remote")]
    QuorumFailed {
        /// Required quorum.
        quorum: std::num::NonZero<usize>,
    },
    /// Timeout.
    #[cfg(feature = "remote")]
    Timeout,
    /// Storing the record failed.
    #[cfg(feature = "remote")]
    Store(libp2p::kad::store::Error),
    /// Invalid actor registration.
    #[cfg(feature = "remote")]
    InvalidActorRegistration(crate::remote::registry::InvalidActorRegistration),
    /// Get providers error.
    #[cfg(feature = "remote")]
    GetProviders(libp2p::kad::GetProvidersError),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "remote")]
            RegistryError::SwarmNotBootstrapped => write!(f, "actor swarm not bootstrapped"),
            RegistryError::NameAlreadyRegistered => write!(f, "name already registered"),
            RegistryError::BadActorType => write!(f, "bad actor type"),
            #[cfg(feature = "remote")]
            RegistryError::QuorumFailed { quorum } => {
                write!(f, "the quorum failed; needed {quorum} peers")
            }
            #[cfg(feature = "remote")]
            RegistryError::Timeout => write!(f, "the request timed out"),
            #[cfg(feature = "remote")]
            RegistryError::Store(err) => err.fmt(f),
            #[cfg(feature = "remote")]
            RegistryError::InvalidActorRegistration(err) => err.fmt(f),
            #[cfg(feature = "remote")]
            RegistryError::GetProviders(err) => err.fmt(f),
        }
    }
}

impl error::Error for RegistryError {}

#[cfg(feature = "remote")]
impl From<crate::remote::registry::InvalidActorRegistration> for RegistryError {
    fn from(err: crate::remote::registry::InvalidActorRegistration) -> Self {
        RegistryError::InvalidActorRegistration(err)
    }
}

#[cfg(feature = "remote")]
impl From<libp2p::kad::store::Error> for RegistryError {
    fn from(err: libp2p::kad::store::Error) -> Self {
        RegistryError::Store(err)
    }
}

#[cfg(feature = "remote")]
impl From<libp2p::kad::AddProviderError> for RegistryError {
    fn from(err: libp2p::kad::AddProviderError) -> Self {
        match err {
            libp2p::kad::AddProviderError::Timeout { .. } => RegistryError::Timeout,
        }
    }
}

#[cfg(feature = "remote")]
impl From<libp2p::kad::PutRecordError> for RegistryError {
    fn from(err: libp2p::kad::PutRecordError) -> Self {
        match err {
            libp2p::kad::PutRecordError::QuorumFailed { quorum, .. } => {
                RegistryError::QuorumFailed { quorum }
            }
            libp2p::kad::PutRecordError::Timeout { .. } => RegistryError::Timeout,
        }
    }
}

#[cfg(feature = "remote")]
impl From<libp2p::kad::GetProvidersError> for RegistryError {
    fn from(err: libp2p::kad::GetProvidersError) -> Self {
        RegistryError::GetProviders(err)
    }
}

/// Error that can occur when sending a message to an actor.
#[cfg(feature = "remote")]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RemoteSendError<E = Infallible> {
    /// The actor isn't running.
    ActorNotRunning,
    /// The actor panicked or was stopped before a reply could be received.
    ActorStopped,
    /// The actor's remote ID was not found.
    UnknownActor {
        /// The remote ID of the actor.
        actor_remote_id: std::borrow::Cow<'static, str>,
    },
    /// The message remote ID was not found for the actor.
    UnknownMessage {
        /// The remote ID of the actor.
        actor_remote_id: std::borrow::Cow<'static, str>,
        /// The remote ID of the message.
        message_remote_id: std::borrow::Cow<'static, str>,
    },
    /// The remote actor was found given the ID, but was not the correct type.
    BadActorType,
    /// The actors mailbox is full.
    MailboxFull,
    /// Timed out waiting for a reply.
    ReplyTimeout,
    /// An error returned by the actor's message handler.
    HandlerError(E),
    /// Failed to serialize the message.
    SerializeMessage(crate::remote::codec::CodecError),
    /// Failed to deserialize the incoming message.
    DeserializeMessage(crate::remote::codec::CodecError),
    /// Failed to serialize the reply.
    SerializeReply(crate::remote::codec::CodecError),
    /// Failed to serialize the handler error.
    SerializeHandlerError(crate::remote::codec::CodecError),
    /// Failed to deserialize the handler error.
    DeserializeHandlerError(crate::remote::codec::CodecError),

    /// The actor swarm has not been bootstrapped.
    SwarmNotBootstrapped,
    /// The request could not be sent because a dialing attempt failed.
    DialFailure,
    /// The request timed out before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    NetworkTimeout,
    /// The connection closed before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    ConnectionClosed,
    /// The remote supports none of the requested protocols.
    UnsupportedProtocols,
    /// An IO failure happened on an outbound stream.
    #[cfg_attr(feature = "serde", serde(skip))]
    Io(Option<std::io::Error>),
}

#[cfg(feature = "remote")]
impl<E> RemoteSendError<E> {
    /// Maps the inner error to another type if the variant is [`HandlerError`](RemoteSendError::HandlerError).
    pub fn map_err<F, O>(self, mut op: O) -> RemoteSendError<F>
    where
        O: FnMut(E) -> F,
    {
        match self {
            RemoteSendError::ActorNotRunning => RemoteSendError::ActorNotRunning,
            RemoteSendError::ActorStopped => RemoteSendError::ActorStopped,
            RemoteSendError::UnknownActor { actor_remote_id } => {
                RemoteSendError::UnknownActor { actor_remote_id }
            }
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            },
            RemoteSendError::BadActorType => RemoteSendError::BadActorType,
            RemoteSendError::MailboxFull => RemoteSendError::MailboxFull,
            RemoteSendError::ReplyTimeout => RemoteSendError::ReplyTimeout,
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
            RemoteSendError::SwarmNotBootstrapped => RemoteSendError::SwarmNotBootstrapped,
            RemoteSendError::DialFailure => RemoteSendError::DialFailure,
            RemoteSendError::NetworkTimeout => RemoteSendError::NetworkTimeout,
            RemoteSendError::ConnectionClosed => RemoteSendError::ConnectionClosed,
            RemoteSendError::UnsupportedProtocols => RemoteSendError::UnsupportedProtocols,
            RemoteSendError::Io(err) => RemoteSendError::Io(err),
        }
    }
}

#[cfg(feature = "remote")]
impl<E> RemoteSendError<RemoteSendError<E>> {
    /// Flattens a nested SendError.
    pub fn flatten(self) -> RemoteSendError<E> {
        use RemoteSendError::*;
        match self {
            ActorNotRunning | HandlerError(ActorNotRunning) => ActorNotRunning,
            ActorStopped | HandlerError(ActorStopped) => ActorStopped,
            UnknownActor { actor_remote_id } | HandlerError(UnknownActor { actor_remote_id }) => {
                UnknownActor { actor_remote_id }
            }
            UnknownMessage {
                actor_remote_id,
                message_remote_id,
            }
            | HandlerError(UnknownMessage {
                actor_remote_id,
                message_remote_id,
            }) => UnknownMessage {
                actor_remote_id,
                message_remote_id,
            },
            BadActorType | HandlerError(BadActorType) => BadActorType,
            MailboxFull | HandlerError(MailboxFull) => MailboxFull,
            ReplyTimeout | HandlerError(ReplyTimeout) => ReplyTimeout,
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
            SwarmNotBootstrapped | HandlerError(SwarmNotBootstrapped) => SwarmNotBootstrapped,
            DialFailure | HandlerError(DialFailure) => DialFailure,
            NetworkTimeout | HandlerError(NetworkTimeout) => NetworkTimeout,
            ConnectionClosed | HandlerError(ConnectionClosed) => ConnectionClosed,
            UnsupportedProtocols | HandlerError(UnsupportedProtocols) => UnsupportedProtocols,
            Io(err) | HandlerError(Io(err)) => Io(err),
        }
    }
}

#[cfg(feature = "remote")]
impl<M, E> From<SendError<M, E>> for RemoteSendError<E> {
    fn from(err: SendError<M, E>) -> Self {
        match err {
            SendError::ActorNotRunning(_) => RemoteSendError::ActorNotRunning,
            SendError::ActorStopped => RemoteSendError::ActorStopped,
            SendError::MailboxFull(_) => RemoteSendError::MailboxFull,
            SendError::HandlerError(err) => RemoteSendError::HandlerError(err),
            SendError::Timeout(_) => RemoteSendError::ReplyTimeout,
        }
    }
}

#[cfg(feature = "remote")]
impl<E> From<libp2p::request_response::OutboundFailure> for RemoteSendError<E> {
    fn from(err: libp2p::request_response::OutboundFailure) -> Self {
        match err {
            libp2p::request_response::OutboundFailure::DialFailure => RemoteSendError::DialFailure,
            libp2p::request_response::OutboundFailure::Timeout => RemoteSendError::NetworkTimeout,
            libp2p::request_response::OutboundFailure::ConnectionClosed => {
                RemoteSendError::ConnectionClosed
            }
            libp2p::request_response::OutboundFailure::UnsupportedProtocols => {
                RemoteSendError::UnsupportedProtocols
            }
            libp2p::request_response::OutboundFailure::Io(err) => RemoteSendError::Io(Some(err)),
        }
    }
}

#[cfg(feature = "remote")]
impl<E> fmt::Display for RemoteSendError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteSendError::ActorNotRunning => write!(f, "actor not running"),
            RemoteSendError::ActorStopped => write!(f, "actor stopped"),
            RemoteSendError::UnknownActor { actor_remote_id } => {
                write!(f, "unknown actor '{actor_remote_id}'")
            }
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => write!(
                f,
                "unknown message '{message_remote_id}' for actor '{actor_remote_id}'"
            ),
            RemoteSendError::BadActorType => write!(f, "bad actor type"),
            RemoteSendError::MailboxFull => write!(f, "mailbox full"),
            RemoteSendError::ReplyTimeout => write!(f, "timeout"),
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
            RemoteSendError::SwarmNotBootstrapped => write!(f, "swarm not bootstrapped"),
            RemoteSendError::DialFailure => write!(f, "dial failure"),
            RemoteSendError::NetworkTimeout => write!(f, "network timeout"),
            RemoteSendError::ConnectionClosed => write!(f, "connection closed"),
            RemoteSendError::UnsupportedProtocols => write!(f, "unsupported protocols"),
            RemoteSendError::Io(Some(err)) => err.fmt(f),
            RemoteSendError::Io(None) => write!(f, "io error"),
        }
    }
}

#[cfg(feature = "remote")]
impl<E> error::Error for RemoteSendError<E> where E: fmt::Debug + fmt::Display {}

/// An error returned when the remote system has already been bootstrapped.
#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug)]
pub struct SwarmAlreadyBootstrappedError;

#[cfg(feature = "remote")]
impl fmt::Display for SwarmAlreadyBootstrappedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "swarm already bootstrapped")
    }
}

#[cfg(feature = "remote")]
impl error::Error for SwarmAlreadyBootstrappedError {}
