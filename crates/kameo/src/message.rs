use std::{
    any,
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    num::{
        NonZeroI128, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroIsize, NonZeroU128,
        NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU8, NonZeroUsize,
    },
    sync::{
        atomic::{
            AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicPtr,
            AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
        },
        Arc, Mutex, Once, RwLock,
    },
    thread::Thread,
};

use futures::{future::BoxFuture, Future, FutureExt};

use crate::error::SendError;

pub(crate) type BoxDebug = Box<dyn fmt::Debug + Send + Sync + 'static>;
pub(crate) type BoxReply = Box<dyn any::Any + Send>;

/// A message that can modify an actors state.
///
/// Messages are processed sequentially one at a time, with exclusive mutable access to the actors state.
///
/// The reply type must implement [Reply].
pub trait Message<T>: Send + 'static {
    /// The reply sent back to the message caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this message.
    fn handle(&mut self, msg: T) -> impl Future<Output = Self::Reply> + Send;
}

/// Queries the actor for some data.
///
/// Unlike regular messages, queries can be processed by the actor in parallel
/// if multiple queries are sent in sequence. This means queries only have read access
/// to the actors state.
///
/// The reply type must implement [Reply].
pub trait Query<T>: Send + 'static {
    /// The reply sent back to the query caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this query.
    fn handle(&self, query: T) -> impl Future<Output = Self::Reply> + Send;
}

/// A reply value.
///
/// If an Err is returned by a hadler, and is unhandled by the caller (ie, the message was sent async),
/// then the error is treated as a panic in the actor.
///
/// This is implemented for all many std lib types, and can be implemented on custom types manually or with the derive
/// macro.
pub trait Reply {
    /// The success type in the reply.
    type Ok;
    /// The error type in the reply.
    type Error;

    /// Converts a reply to a `SendError`, containing the `HandleError` if the reply is an error.
    fn to_send_error<M>(self) -> Result<Self::Ok, SendError<M, Self::Error>>;

    /// Converts the reply into a `Box<fmt::Debug + Send + Sync + 'static>` if it's an Err, otherwise `None`.
    fn into_boxed_err(self) -> Option<BoxDebug>;
}

impl<T, E> Reply for Result<T, E>
where
    E: fmt::Debug + Send + Sync + 'static,
{
    type Ok = T;
    type Error = E;

    fn to_send_error<M>(self) -> Result<T, SendError<M, E>> {
        self.map_err(SendError::HandlerError)
    }

    fn into_boxed_err(self) -> Option<BoxDebug> {
        self.map_err(|err| Box::new(err) as BoxDebug).err()
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
            type Error = ();

            fn to_send_error<Msg>(self) -> Result<Self, SendError<Msg, ()>> {
                Ok(self)
            }

            fn into_boxed_err(self) -> Option<BoxDebug> {
                None
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
    {T} Option<T>,
    {'a, T: Clone} Cow<'a, T>,
    {T} Arc<T>,
    {T} Mutex<T>,
    {T} RwLock<T>,
    {'a, const N: usize, T} &'a [T; N],
    {const N: usize, T} [T; N],
    {'a, T} &'a [T],
    {'a, T} &'a mut T,
    {T} Vec<T>,
    {T} Box<T>,
    {K, V} HashMap<K, V>,
    {T} HashSet<T>,
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
    {T} AtomicPtr<T>,
    AtomicU8,
    AtomicU16,
    AtomicU32,
    AtomicU64,
    AtomicUsize,
    Once,
    Thread,
    {T} std::cell::OnceCell<T>,
    {T} std::sync::mpsc::Sender<T>,
    {T} std::sync::mpsc::Receiver<T>,
    {T} tokio::sync::OnceCell<T>,
    tokio::sync::Semaphore,
    tokio::sync::Notify,
    {T} tokio::sync::mpsc::Sender<T>,
    {T} tokio::sync::mpsc::Receiver<T>,
    {T} tokio::sync::mpsc::UnboundedSender<T>,
    {T} tokio::sync::mpsc::UnboundedReceiver<T>,
    {T} tokio::sync::watch::Sender<T>,
    {T} tokio::sync::watch::Receiver<T>,
    {T} tokio::sync::broadcast::Sender<T>,
    {T} tokio::sync::broadcast::Receiver<T>,
    {T} tokio::sync::oneshot::Sender<T>,
    {T} tokio::sync::oneshot::Receiver<T>,
    {T} tokio::sync::Mutex<T>,
    {T} tokio::sync::RwLock<T>,
    {A} (A,),
    {A, B} (A, B),
    {A, B, C} (A, B, C),
    {A, B, C, D} (A, B, C, D),
    {A, B, C, D, E} (A, B, C, D, E),
    {A, B, C, D, E, F} (A, B, C, D, E, F),
    {A, B, C, D, E, F, G} (A, B, C, D, E, F, G),
    {A, B, C, D, E, F, G, H} (A, B, C, D, E, F, G, H),
    {A, B, C, D, E, F, G, H, I} (A, B, C, D, E, F, G, H, I),
    {A, B, C, D, E, F, G, H, I, J} (A, B, C, D, E, F, G, H, I, J),
    {A, B, C, D, E, F, G, H, I, J, K} (A, B, C, D, E, F, G, H, I, J, K),
    {A, B, C, D, E, F, G, H, I, J, K, L} (A, B, C, D, E, F, G, H, I, J, K, L),
    {A, B, C, D, E, F, G, H, I, J, K, L, M} (A, B, C, D, E, F, G, H, I, J, K, L, M),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N} (A, B, C, D, E, F, G, H, I, J, K, L, M, N),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y),
    {A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z} (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z),
]);

pub(crate) trait DynMessage<A>: Send {
    fn handle_dyn(self: Box<Self>, state: &mut A) -> BoxFuture<'_, BoxReply>
    where
        A: Send;
    fn handle_dyn_async(self: Box<Self>, state: &mut A) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, T> DynMessage<A> for T
where
    A: Message<T>,
    T: Send + 'static,
{
    fn handle_dyn(self: Box<Self>, state: &mut A) -> BoxFuture<'_, BoxReply>
    where
        A: Send,
    {
        async move { Box::new(state.handle(*self).await) as BoxReply }.boxed()
    }

    fn handle_dyn_async(self: Box<Self>, state: &mut A) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send,
    {
        async move { state.handle(*self).await.into_boxed_err() }.boxed()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}

pub(crate) trait DynQuery<A>: Send {
    fn handle_dyn(self: Box<Self>, state: &A) -> BoxFuture<'_, BoxReply>
    where
        A: Send + Sync;
    fn handle_dyn_async(self: Box<Self>, state: &A) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send + Sync;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, T> DynQuery<A> for T
where
    A: Query<T>,
    T: Send + 'static,
{
    fn handle_dyn(self: Box<Self>, state: &A) -> BoxFuture<'_, BoxReply>
    where
        A: Send + Sync,
    {
        async move { Box::new(state.handle(*self).await) as BoxReply }.boxed()
    }

    fn handle_dyn_async(self: Box<Self>, state: &A) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send + Sync,
    {
        async move { state.handle(*self).await.into_boxed_err() }.boxed()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}
