//! Constructs for handling replies and errors in kameo's actor communication.
//!
//! This module provides the [`Reply`] trait and associated structures for managing message replies within the actor
//! system. It enables actors to communicate effectively, handling both successful outcomes and errors through a
//! unified interface.
//!
//! **Reply Trait Overview**
//!
//! The `Reply` trait plays a crucial role in kameo by defining how actors respond to messages.
//! It is implemented for a variety of common types, facilitating easy adoption and use.
//! Special attention is given to the `Result` and [`DelegatedReply`] types:
//! - Implementations for `Result` allow errors returned by actor handlers to be communicated back as
//! [`SendError::HandlerError`], integrating closely with Rust’s error handling patterns.
//! - The `DelegatedReply` type signifies that the actual reply will be managed by another part of the system,
//! supporting asynchronous and decoupled communication workflows.
//! - Importantly, when messages are sent asynchronously with [`tell`](crate::actor::ActorRef::tell) and an error is returned by the actor
//! without a direct means for the caller to handle it (due to the absence of a reply expectation), the error is treated
//! as a panic within the actor. This behavior will trigger the actor's [`on_panic`](crate::actor::Actor::on_panic) hook, which may result in the actor
//! being restarted or stopped based on the [Actor](crate::Actor) implementation (which stops the actor by default).
//!
//! The `Reply` trait, by encompassing a broad range of types and defining specific behaviors for error handling,
//! ensures that actors can manage their communication responsibilities efficiently and effectively.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    marker::PhantomData,
    num::{
        NonZeroI128, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroIsize, NonZeroU128,
        NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU8, NonZeroUsize,
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{
            AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicPtr,
            AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
        },
        Arc, Mutex, Once, RwLock,
    },
    thread::Thread,
};

use futures::Future;
use tokio::sync::oneshot;

use crate::{
    error::{BoxSendError, SendError},
    message::{BoxDebug, BoxReply},
};

/// A boxed reply sender which will be downcasted to the correct type when receiving a reply.
///
/// This is reserved for advanced use cases, and misuse of this can result in panics.
pub type BoxReplySender = oneshot::Sender<Result<BoxReply, BoxSendError>>;

/// A deligated reply that has been forwarded to another actor.
pub type ForwardedReply<T, M, E = ()> = DelegatedReply<Result<T, SendError<M, E>>>;

/// A reply value.
///
/// If an Err is returned by a handler, and is unhandled by the caller (ie, the message was sent asyncronously with `tell`),
/// then the error is treated as a panic in the actor.
///
/// This is implemented for all many std lib types, and can be implemented on custom types manually or with the derive
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
    type Error: Send + 'static;
    /// The type sent back to the receiver.
    ///
    /// In almost all cases this will be `Self`. The only exception is the `DelegatedReply` type.
    type Value: Reply;

    /// Converts a reply to a `Result`.
    fn to_result(self) -> Result<Self::Ok, Self::Error>;

    /// Converts the reply into a `Box<fmt::Debug + Send + Sync + 'static>` if it's an Err, otherwise `None`.
    fn into_boxed_err(self) -> Option<BoxDebug>;

    /// Converts the type to Self::Reply.
    ///
    /// In almost all cases, this will simply return itself.
    fn into_value(self) -> Self::Value;
}

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

    fn into_boxed_err(self) -> Option<BoxDebug> {
        None
    }

    fn into_value(self) -> Self::Value {
        unimplemented!("a DeligatedReply cannot be converted to a value and is only a marker type")
    }
}

/// A mechanism for sending replies back to the original requester in a message exchange.
///
/// `ReplySender` encapsulates the functionality to send a response back to whereever
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
}

impl<R: ?Sized> fmt::Debug for ReplySender<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplySender")
            .field("tx", &self.tx)
            .field("phantom", &self.phantom)
            .finish()
    }
}

impl<T, E> Reply for Result<T, E>
where
    T: Send + 'static,
    E: fmt::Debug + Send + Sync + 'static,
{
    type Ok = T;
    type Error = E;
    type Value = Self;

    fn to_result(self) -> Result<T, E> {
        self
    }

    fn into_boxed_err(self) -> Option<BoxDebug> {
        self.map_err(|err| Box::new(err) as BoxDebug).err()
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

            fn into_boxed_err(self) -> Option<BoxDebug> {
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
    {T: 'static + Send} Vec<T>,
    {T: 'static + Send} Box<T>,
    {K: 'static + Send, V: 'static + Send} HashMap<K, V>,
    {T: 'static + Send} HashSet<T>,
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
    AtomicI8,
    AtomicI16,
    AtomicI32,
    AtomicI64,
    AtomicIsize,
    {T: 'static + Send} AtomicPtr<T>,
    AtomicU8,
    AtomicU16,
    AtomicU32,
    AtomicU64,
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
    {T: 'static + Send} tokio_stream::wrappers::ReceiverStream<T>,
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
