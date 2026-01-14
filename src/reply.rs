//! Constructs for handling replies and errors in Kameo's actor communication.
//!
//! This module provides the [`Reply`] trait and associated structures for managing message replies within the actor
//! system. It enables actors to communicate effectively, handling both successful outcomes and errors through a
//! unified interface.
//!
//! **Reply Trait Overview**
//!
//! The `Reply` trait plays a crucial role in Kameo by defining how actors respond to messages.
//! It is implemented for a variety of common types, facilitating easy adoption and use.
//! Special attention is given to the `Result` and [`DelegatedReply`] types:
//! - Implementations for `Result` allow errors returned by actor handlers to be communicated back as
//!   [`SendError::HandlerError`], integrating closely with Rust’s error handling patterns.
//! - The `DelegatedReply` type signifies that the actual reply will be managed by another part of the system,
//!   supporting asynchronous and decoupled communication workflows.
//! - Importantly, when messages are sent asynchronously with [`tell`](crate::actor::ActorRef::tell) and an error is returned by the actor
//!   without a direct means for the caller to handle it (due to the absence of a reply expectation), the error is treated
//!   as a panic within the actor. This behavior will trigger the actor's [`on_panic`](crate::actor::Actor::on_panic) hook, which may result in the actor
//!   being restarted or stopped based on the [Actor] implementation (which stops the actor by default).
//!
//! The `Reply` trait, by encompassing a broad range of types and defining specific behaviors for error handling,
//! ensures that actors can manage their communication responsibilities efficiently and effectively.

use std::{
    any,
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque},
    fmt,
    marker::PhantomData,
    num::{
        NonZeroI128, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroIsize, NonZeroU128,
        NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU8, NonZeroUsize,
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicUsize},
        Arc, Mutex, Once, RwLock,
    },
    thread::Thread,
};

#[cfg(target_has_atomic = "16")]
use std::sync::atomic::{AtomicI16, AtomicU16};
#[cfg(target_has_atomic = "32")]
use std::sync::atomic::{AtomicI32, AtomicU32};
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::{AtomicI64, AtomicU64};
#[cfg(target_has_atomic = "8")]
use std::sync::atomic::{AtomicI8, AtomicU8};

use downcast_rs::{impl_downcast, DowncastSend};
use futures::Future;
use tokio::sync::oneshot;

use crate::{
    actor::{
        ActorId, ActorRef, PreparedActor, Recipient, ReplyRecipient, WeakActorRef, WeakRecipient,
        WeakReplyRecipient,
    },
    error::{ActorStopReason, BoxSendError, Infallible, PanicError, SendError},
    mailbox::{MailboxReceiver, MailboxSender},
    message::{BoxReply, Context},
    Actor,
};

/// A boxed reply sender which will be downcast to the correct type when receiving a reply.
///
/// This is reserved for advanced use cases, and misuse of this can result in panics.
pub type BoxReplySender = oneshot::Sender<Result<BoxReply, BoxSendError>>;

/// A reply value.
///
/// If an Err is returned by a handler, and is unhandled by the caller (ie, the message was sent asynchronously with `tell`),
/// then the error is treated as a panic in the actor.
///
/// This is implemented for all std lib types, and can be implemented on custom types manually or with the derive
/// macro.
///
/// # Example
///
/// ```
/// use kameo::Reply;
///
/// #[derive(Reply)]
/// pub struct Foo { }
/// ```
pub trait Reply: Send + 'static {
    /// The success type in the reply.
    type Ok: Send + 'static;
    /// The error type in the reply.
    type Error: ReplyError;
    /// The type sent back to the receiver.
    ///
    /// In almost all cases this will be `Self`. The only exception is the `DelegatedReply` type.
    type Value: Reply;

    /// Converts a reply to a `Result`.
    fn to_result(self) -> Result<Self::Ok, Self::Error>;

    /// Converts the reply into a `Box<any::Any + Send>` if it's an Err, otherwise `None`.
    fn into_any_err(self) -> Option<Box<dyn ReplyError>>;

    /// Converts the type to Self::Reply.
    ///
    /// In almost all cases, this will simply return itself.
    fn into_value(self) -> Self::Value;

    /// Downcasts a `Box<dyn Any>` into the `Self::Ok` type.
    fn downcast_ok(ok: Box<dyn any::Any>) -> Self::Ok {
        *ok.downcast().unwrap()
    }

    /// Downcasts a `Box<dyn Any>` into a `Self::Error` type.
    fn downcast_err<M: 'static>(err: BoxSendError) -> SendError<M, Self::Error> {
        err.downcast()
    }
}

/// A mechanism for sending replies back to the original requester in a message exchange.
///
/// `ReplySender` encapsulates the functionality to send a response back to wherever
/// a request was initiated. It is typically used in scenarios where the
/// processing of a request is delegated to another actor within the system.
/// Upon completion of the request handling, `ReplySender` is used to send the result back,
/// ensuring that the flow of communication is maintained and the requester receives the
/// necessary response.
///
/// This type is designed to be used once per message received; it consumes itself upon sending
/// a reply to enforce a single use and prevent multiple replies to a single message.
///
/// # Usage
///
/// A `ReplySender` is obtained as part of the delegation process when handling a message. It should
/// be used to send a reply once the requested data is available or the operation is complete.
///
/// The `ReplySender` provides a clear and straightforward interface for completing the message handling cycle,
/// facilitating efficient and organized communication within the system.
#[must_use = "the receiver expects a reply to be sent"]
pub struct ReplySender<R: ?Sized> {
    tx: BoxReplySender,
    phantom: PhantomData<R>,
}

impl<R> ReplySender<R> {
    pub(crate) fn new(tx: BoxReplySender) -> Self {
        ReplySender {
            tx,
            phantom: PhantomData,
        }
    }

    /// Converts the reply sender to a generic `BoxReplySender`.
    pub fn boxed(self) -> BoxReplySender {
        self.tx
    }

    /// Sends a reply using the current `ReplySender`.
    ///
    /// Consumes the `ReplySender`, sending the specified reply to the original
    /// requester. This method is the final step in the response process for
    /// delegated replies, ensuring that the message's intended recipient receives
    /// the necessary data or acknowledgment.
    ///
    /// The method takes ownership of the `ReplySender` to prevent multiple uses,
    /// aligning with the one-time use pattern typical in actor-based messaging for
    /// reply mechanisms. Once called, the `ReplySender` cannot be used again,
    /// enforcing a single-reply guarantee for each message received.
    ///
    /// # Note
    ///
    /// It is crucial to send a reply for every received message to avoid leaving the
    /// requester in a state of indefinite waiting. Failure to do so can lead to deadlocks
    /// or wasted resources in waiting for a response that will never arrive.
    pub fn send(self, reply: R)
    where
        R: Reply,
    {
        let _ = self.tx.send(
            reply
                .to_result()
                .map(|value| Box::new(value) as BoxReply)
                .map_err(|err| BoxSendError::HandlerError(Box::new(err))),
        );
    }

    pub(crate) fn cast<R2>(self) -> ReplySender<R2> {
        ReplySender {
            tx: self.tx,
            phantom: PhantomData,
        }
    }
}

impl<R: ?Sized> fmt::Debug for ReplySender<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplySender")
            .field("tx", &self.tx)
            .field("phantom", &self.phantom)
            .finish()
    }
}

/// An error type which can be used in replies.
///
/// This is implemented for all types which are `Debug + Send + 'static`.
pub trait ReplyError: DowncastSend + fmt::Debug + 'static {}
impl<T> ReplyError for T where T: fmt::Debug + Send + 'static {}
impl_downcast!(ReplyError);

/// A marker type indicating that the reply to a message will be handled elsewhere.
///
/// This structure is created by the [`reply_sender`] method on [`Context`].
///
/// [`reply_sender`]: method@crate::message::Context::reply_sender
/// [`Context`]: struct@crate::message::Context
#[must_use = "the deligated reply should be returned by the handler"]
#[derive(Clone, Copy, Debug)]
pub struct DelegatedReply<R> {
    phantom: PhantomData<fn() -> R>,
}

impl<R> DelegatedReply<R> {
    pub(crate) fn new() -> Self {
        DelegatedReply {
            phantom: PhantomData,
        }
    }
}

impl<R> Reply for DelegatedReply<R>
where
    R: Reply,
{
    type Ok = R::Ok;
    type Error = R::Error;
    type Value = R::Value;

    fn to_result(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("a DeligatedReply cannot be converted to a result and is only a marker type")
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        unimplemented!("a DeligatedReply cannot be converted to a value and is only a marker type")
    }
}

/// A delegated reply that has been forwarded to another actor or contains a direct response.
///
/// This type is returned by [`Context::forward`] and its variants, but can also be created
/// directly to respond to the original sender when forwarding is not possible or desired.
/// This allows actors to gracefully handle scenarios where forwarding would otherwise
/// require a panic or cause unexpected termination.
///
/// # Examples
///
/// ## Basic forwarding
///
/// ```
/// use kameo::{prelude::*, reply::ForwardedReply};
/// use std::collections::HashMap;
///
/// #[derive(Actor)]
/// struct RouterActor {
///     routes: HashMap<u32, ActorRef<TargetActor>>,
/// }
///
/// #[derive(Actor, Default)]
/// struct TargetActor;
///
/// struct RouteMessage {
///     target_id: u32,
///     data: String,
/// }
///
/// #[derive(Clone)]
/// struct ProcessData(String);
///
/// impl Message<ProcessData> for TargetActor {
///     type Reply = String;
///
///     async fn handle(&mut self, msg: ProcessData, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
///         format!("Processed: {}", msg.0)
///     }
/// }
///
/// impl Message<RouteMessage> for RouterActor {
///     type Reply = ForwardedReply<ProcessData, String>;
///
///     async fn handle(&mut self, msg: RouteMessage, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
///         if let Some(target_ref) = self.routes.get(&msg.target_id) {
///             // Forward to the target actor
///             ctx.forward(target_ref, ProcessData(msg.data)).await
///         } else {
///             // Target not found - respond directly instead of panicking
///             ForwardedReply::from_ok("Default response".to_string())
///         }
///     }
/// }
/// ```
///
/// ## Error handling without forwarding
///
/// ```
/// use kameo::{prelude::*, reply::ForwardedReply};
///
/// #[derive(Debug)]
/// enum RouterError {
///     TargetNotFound,
///     InvalidData,
/// }
///
/// impl std::fmt::Display for RouterError {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         match self {
///             RouterError::TargetNotFound => write!(f, "Target not found"),
///             RouterError::InvalidData => write!(f, "Invalid data"),
///         }
///     }
/// }
///
/// impl std::error::Error for RouterError {}
///
/// #[derive(Actor)]
/// struct SafeRouter;
///
/// struct SafeRouteMessage {
///     target_id: u32,
///     data: String,
/// }
///
/// impl Message<SafeRouteMessage> for SafeRouter {
///     type Reply = ForwardedReply<SafeRouteMessage, Result<String, RouterError>>;
///
///     async fn handle(&mut self, msg: SafeRouteMessage, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
///         if msg.data.is_empty() {
///             // Return error directly without forwarding
///             ForwardedReply::from_err(RouterError::InvalidData)
///         } else if msg.target_id == 999 {
///             // Another error case
///             ForwardedReply::from_err(RouterError::TargetNotFound)
///         } else {
///             // Success case
///             ForwardedReply::from_ok(format!("Routed: {}", msg.data))
///         }
///     }
/// }
/// ```
///
/// [`Context::forward`]: crate::message::Context::forward
pub struct ForwardedReply<M, R>
where
    R: Reply,
{
    inner: ForwardedReplyInner<M, R>,
}

enum ForwardedReplyInner<M, R>
where
    R: Reply,
{
    /// The message was successfully forwarded or failed to be forwarded
    Forwarded(Result<(), SendError<M, R::Error>>),
    /// A direct response without forwarding
    Direct(Result<R::Ok, R::Error>),
}

// Manual Debug implementation to avoid requiring Debug on R::Value
impl<M, R> fmt::Debug for ForwardedReply<M, R>
where
    M: fmt::Debug,
    R: Reply,
    R::Ok: fmt::Debug,
    R::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForwardedReply")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<M, R> fmt::Debug for ForwardedReplyInner<M, R>
where
    M: fmt::Debug,
    R: Reply,
    R::Ok: fmt::Debug,
    R::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forwarded(res) => f.debug_tuple("Forwarded").field(res).finish(),
            Self::Direct(res) => f.debug_tuple("Direct").field(res).finish(),
        }
    }
}

impl<M, R> ForwardedReply<M, R>
where
    R: Reply,
{
    pub(crate) fn new(res: Result<(), SendError<M, R::Error>>) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Forwarded(res),
        }
    }

    /// Creates a ForwardedReply with a direct successful response.
    ///
    /// This allows an actor to respond directly to the original sender when forwarding
    /// is not possible or desired, avoiding the need to panic or cause actor termination.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::reply::ForwardedReply;
    ///
    /// // Create a direct successful response
    /// let reply: ForwardedReply<(), String> = ForwardedReply::from_ok("Success!".to_string());
    /// ```
    pub fn from_ok(value: R::Ok) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Direct(Ok(value)),
        }
    }

    /// Creates a ForwardedReply with a direct error response.
    ///
    /// This allows an actor to respond directly with an error when forwarding
    /// is not possible or desired, maintaining the error semantics of the system.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::reply::ForwardedReply;
    /// use std::{error, fmt};
    ///
    /// #[derive(Debug)]
    /// enum MyError {
    ///     InvalidParameters,
    /// }
    ///
    /// impl fmt::Display for MyError {
    ///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ///         write!(f, "Invalid parameters")
    ///     }
    /// }
    ///
    /// impl error::Error for MyError {}
    ///
    /// // Create a direct error response
    /// let reply: ForwardedReply<(), Result<String, MyError>> =
    ///     ForwardedReply::from_err(MyError::InvalidParameters);
    /// ```
    pub fn from_err(error: R::Error) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Direct(Err(error)),
        }
    }

    /// Creates a ForwardedReply from a Result.
    ///
    /// This is a convenience method that accepts either an Ok or Err value
    /// and creates the appropriate direct response.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::reply::ForwardedReply;
    /// use std::{error, fmt};
    ///
    /// #[derive(Debug)]
    /// enum ComputeError {
    ///     InvalidInput,
    /// }
    ///
    /// impl fmt::Display for ComputeError {
    ///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    ///         write!(f, "Invalid input")
    ///     }
    /// }
    ///
    /// impl error::Error for ComputeError {}
    ///
    /// // Create from a successful result
    /// let success_result = Ok(42);
    /// let reply_ok: ForwardedReply<(), Result<i32, ComputeError>> =
    ///     ForwardedReply::from_result(success_result);
    ///
    /// // Create from an error result
    /// let error_result = Err(ComputeError::InvalidInput);
    /// let reply_err: ForwardedReply<(), Result<i32, ComputeError>> =
    ///     ForwardedReply::from_result(error_result);
    /// ```
    pub fn from_result(result: Result<R::Ok, R::Error>) -> Self {
        ForwardedReply {
            inner: ForwardedReplyInner::Direct(result),
        }
    }
}

impl<M, R> Reply for ForwardedReply<M, R>
where
    R: Reply,
    M: Send + 'static,
{
    type Ok = R::Ok;
    type Error = SendError<M, R::Error>;
    type Value = Result<Self::Ok, Self::Error>;

    fn to_result(self) -> Result<Self::Ok, Self::Error> {
        match self.inner {
            ForwardedReplyInner::Forwarded(res) => res.map(|_| {
                unreachable!("forwarded reply is only converted to a result if its an error")
            }),
            ForwardedReplyInner::Direct(result) => {
                result.map_err(SendError::<M, R::Error>::HandlerError)
            }
        }
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        match self.inner {
            ForwardedReplyInner::Forwarded(res) => {
                res.err().map(|err| Box::new(err) as Box<dyn ReplyError>)
            }
            ForwardedReplyInner::Direct(result) => result.err().map(|err| {
                Box::new(SendError::<M, R::Error>::HandlerError(err)) as Box<dyn ReplyError>
            }),
        }
    }

    fn into_value(self) -> Self::Value {
        match self.inner {
            ForwardedReplyInner::Forwarded(res) => res.map(|_| {
                unreachable!("forwarded reply is only an error if it failed to forward the message")
            }),
            ForwardedReplyInner::Direct(result) => {
                result.map_err(SendError::<M, R::Error>::HandlerError)
            }
        }
    }

    /// If the forwarded reply succeeded, the we can safely assume
    /// the `Box<dyn Any>` we have here is the ok value of the inner `R`.
    fn downcast_ok(ok: Box<dyn any::Any>) -> Self::Ok {
        *ok.downcast().unwrap()
    }

    /// The error is either from the inner `R`, or our outer `SendError`.
    /// We'll try both.
    fn downcast_err<N: 'static>(err: BoxSendError) -> SendError<N, Self::Error> {
        err.try_downcast::<N, R::Error>()
            .map(|err| err.map_err(SendError::HandlerError))
            .unwrap_or_else(|err| {
                err.downcast::<M, SendError<M, R::Error>>().map_msg(|_| {
                    unreachable!(
                        "forwarded reply is only an error if it failed to forward the message"
                    )
                })
            })
    }
}

impl<T, E> Reply for Result<T, E>
where
    T: Send + 'static,
    E: ReplyError,
{
    type Ok = T;
    type Error = E;
    type Value = Self;

    fn to_result(self) -> Result<T, E> {
        self
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        self.map_err(|err| Box::new(err) as Box<dyn ReplyError>)
            .err()
    }

    #[inline]
    fn into_value(self) -> Self::Value {
        self
    }
}

macro_rules! impl_infallible_reply {
    ([
        $(
            $( {
                $( $generics:tt )*
             } )?
            $ty:ty
        ),* $(,)?
    ]) => {
        $(
            impl_infallible_reply!(
                $( {
                    $( $generics )*
                 } )?
                $ty
            );
        )*
    };
    (
        $( {
            $( $generics:tt )*
         } )?
        $ty:ty
    ) => {
        impl $( < $($generics)* > )? Reply for $ty {
            type Ok = Self;
            type Error = $crate::error::Infallible;
            type Value = Self;

            fn to_result(self) -> Result<Self, $crate::error::Infallible> {
                Ok(self)
            }

            fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
                None
            }

            #[inline]
            fn into_value(self) -> Self::Value {
                self
            }
        }
    };
}

impl_infallible_reply!([
    ActorId,
    {A: Actor} ActorRef<A>,
    {A: Actor} PreparedActor<A>,
    {M: Send} Recipient<M>,
    {M: Send, Ok: Send, Err: ReplyError} ReplyRecipient<M, Ok, Err>,
    {A: Actor} WeakActorRef<A>,
    {M: Send} WeakRecipient<M>,
    {M: Send, Ok: Send, Err: ReplyError} WeakReplyRecipient<M, Ok, Err>,
    ActorStopReason,
    PanicError,
    SendError,
    {A: Actor} MailboxReceiver<A>,
    {A: Actor} MailboxSender<A>,
    {A: Actor, R: Reply} Context<A, R>,
    {R: Reply} ReplySender<R>,
    Infallible,
    (),
    usize,
    u8,
    u16,
    u32,
    u64,
    u128,
    isize,
    i8,
    i16,
    i32,
    i64,
    i128,
    f32,
    f64,
    char,
    bool,
    &'static str,
    String,
    &'static Path,
    PathBuf,
    {T: 'static + Send} Option<T>,
    {T: Clone + Send + Sync} Cow<'static, T>,
    {T: 'static + Send + Sync} Arc<T>,
    {T: 'static + Send} Mutex<T>,
    {T: 'static + Send} RwLock<T>,
    {const N: usize, T: 'static + Send + Sync} &'static [T; N],
    {const N: usize, T: 'static + Send} [T; N],
    {T: 'static + Send + Sync} &'static [T],
    {T: 'static + Send} &'static mut T,
    {T: 'static + Send} Box<T>,
    {T: 'static + Send} Vec<T>,
    {T: 'static + Send} VecDeque<T>,
    {T: 'static + Send} LinkedList<T>,
    {K: 'static + Send, V: 'static + Send} HashMap<K, V>,
    {K: 'static + Send, V: 'static + Send} BTreeMap<K, V>,
    {T: 'static + Send} HashSet<T>,
    {T: 'static + Send} BTreeSet<T>,
    {T: 'static + Send} BinaryHeap<T>,
    NonZeroI8,
    NonZeroI16,
    NonZeroI32,
    NonZeroI64,
    NonZeroI128,
    NonZeroIsize,
    NonZeroU8,
    NonZeroU16,
    NonZeroU32,
    NonZeroU64,
    NonZeroU128,
    NonZeroUsize,
    AtomicBool,
    AtomicIsize,
    {T: 'static + Send} AtomicPtr<T>,
    AtomicUsize,
    Once,
    Thread,
    {T: 'static + Send} std::cell::OnceCell<T>,
    {T: 'static + Send} std::sync::mpsc::Sender<T>,
    {T: 'static + Send} std::sync::mpsc::Receiver<T>,
    {T: 'static + Send + Future<Output = O>, O: Send} futures::stream::FuturesOrdered<T>,
    {T: 'static + Send} futures::stream::FuturesUnordered<T>,
    {T: 'static + Send} tokio::sync::OnceCell<T>,
    tokio::sync::Semaphore,
    tokio::sync::Notify,
    {T: 'static + Send} tokio::sync::mpsc::Sender<T>,
    {T: 'static + Send} tokio::sync::mpsc::Receiver<T>,
    {T: 'static + Send} tokio::sync::mpsc::UnboundedSender<T>,
    {T: 'static + Send} tokio::sync::mpsc::UnboundedReceiver<T>,
    {T: 'static + Send + Sync} tokio::sync::watch::Sender<T>,
    {T: 'static + Send + Sync} tokio::sync::watch::Receiver<T>,
    {T: 'static + Send} tokio::sync::broadcast::Sender<T>,
    {T: 'static + Send} tokio::sync::broadcast::Receiver<T>,
    {T: 'static + Send} tokio::sync::oneshot::Sender<T>,
    {T: 'static + Send} tokio::sync::oneshot::Receiver<T>,
    {T: 'static + Send} tokio::sync::Mutex<T>,
    {T: 'static + Send} tokio::sync::RwLock<T>,
    {A: 'static + Send} (A,),
    {A: 'static + Send, B: 'static + Send} (A, B),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send} (A, B, C),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send} (A, B, C, D),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send} (A, B, C, D, E),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send} (A, B, C, D, E, F),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send} (A, B, C, D, E, F, G),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send} (A, B, C, D, E, F, G, H),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send} (A, B, C, D, E, F, G, H, I),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send} (A, B, C, D, E, F, G, H, I, J),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send, X: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send, X: 'static + Send, Y: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y),
    {A: 'static + Send, B: 'static + Send, C: 'static + Send, D: 'static + Send, E: 'static + Send, F: 'static + Send, G: 'static + Send, H: 'static + Send, I: 'static + Send, J: 'static + Send, K: 'static + Send, L: 'static + Send, M: 'static + Send, N: 'static + Send, O: 'static + Send, P: 'static + Send, Q: 'static + Send, R: 'static + Send, S: 'static + Send, T: 'static + Send, U: 'static + Send, V: 'static + Send, W: 'static + Send, X: 'static + Send, Y: 'static + Send, Z: 'static + Send} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z),
]);

#[cfg(feature = "remote")]
impl_infallible_reply!([
    {E: 'static + Send} crate::error::RemoteSendError<E>,
]);

#[cfg(target_has_atomic = "8")]
impl_infallible_reply!([AtomicI8, AtomicU8]);
#[cfg(target_has_atomic = "16")]
impl_infallible_reply!([AtomicI16, AtomicU16]);
#[cfg(target_has_atomic = "32")]
impl_infallible_reply!([AtomicI32, AtomicU32]);
#[cfg(target_has_atomic = "64")]
impl_infallible_reply!([AtomicI64, AtomicU64]);

#[cfg(test)]
mod tests {
    use std::{error, fmt};

    use crate::error::Infallible;
    use crate::{
        actor::Actor,
        message::{Context, Message},
    };

    use super::ForwardedReply;

    #[tokio::test]
    async fn test_forwarded_reply_from_ok() {
        #[derive(Default)]
        struct TestActor;

        impl Actor for TestActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                args: Self::Args,
                _actor_ref: crate::actor::ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(args)
            }
        }

        #[derive(Debug)]
        struct TestMessage;

        impl Message<TestMessage> for TestActor {
            type Reply = ForwardedReply<TestMessage, String>;

            async fn handle(
                &mut self,
                _msg: TestMessage,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                // Instead of forwarding, respond directly with a success value
                ForwardedReply::from_ok("Direct response".to_string())
            }
        }

        let actor_ref = TestActor::spawn(TestActor);
        let response = actor_ref.ask(TestMessage).await.unwrap();
        assert_eq!(response, "Direct response");
    }

    #[tokio::test]
    async fn test_forwarded_reply_from_err() {
        #[derive(Default)]
        struct TestActor;

        impl Actor for TestActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                args: Self::Args,
                _actor_ref: crate::actor::ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(args)
            }
        }

        #[derive(Debug)]
        struct TestMessage;

        #[derive(Debug, Clone, PartialEq)]
        struct TestError {
            message: String,
        }

        impl fmt::Display for TestError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.message)
            }
        }

        impl error::Error for TestError {}

        impl Message<TestMessage> for TestActor {
            type Reply = ForwardedReply<TestMessage, Result<String, TestError>>;

            async fn handle(
                &mut self,
                _msg: TestMessage,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                // Instead of forwarding, respond directly with an error
                ForwardedReply::from_err(TestError {
                    message: "Something went wrong".to_string(),
                })
            }
        }

        let actor_ref = TestActor::spawn(TestActor);
        let response = actor_ref.ask(TestMessage).await;

        match response {
            Err(error) => {
                match error.err() {
                    Some(handler_error) => {
                        // handler_error is a SendError<TestMessage, TestError>
                        match handler_error.err() {
                            Some(test_error) => {
                                assert_eq!(test_error.message, "Something went wrong");
                            }
                            None => panic!("Expected inner TestError"),
                        }
                    }
                    None => panic!("Expected handler error"),
                }
            }
            Ok(_) => panic!("Expected error response"),
        }
    }

    #[tokio::test]
    async fn test_forwarded_reply_from_result() {
        #[derive(Default)]
        struct TestActor;

        impl Actor for TestActor {
            type Args = Self;
            type Error = Infallible;

            async fn on_start(
                args: Self::Args,
                _actor_ref: crate::actor::ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(args)
            }
        }

        #[derive(Debug)]
        struct TestMessage {
            should_succeed: bool,
        }

        #[derive(Debug, Clone, PartialEq)]
        struct TestError;

        impl fmt::Display for TestError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "Test error")
            }
        }

        impl error::Error for TestError {}

        impl Message<TestMessage> for TestActor {
            type Reply = ForwardedReply<TestMessage, Result<i32, TestError>>;

            async fn handle(
                &mut self,
                msg: TestMessage,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                let result = if msg.should_succeed {
                    Ok(42)
                } else {
                    Err(TestError)
                };

                // Use from_result to create a ForwardedReply from a Result
                ForwardedReply::from_result(result)
            }
        }

        let actor_ref = TestActor::spawn(TestActor);

        // Test success case
        let response = actor_ref
            .ask(TestMessage {
                should_succeed: true,
            })
            .await
            .unwrap();
        assert_eq!(response, 42);

        // Test error case
        let response = actor_ref
            .ask(TestMessage {
                should_succeed: false,
            })
            .await;
        assert!(response.is_err());
    }
}
