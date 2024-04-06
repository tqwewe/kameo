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
use tokio::sync::oneshot;

use crate::{
    actor_ref::ActorRef,
    error::{BoxSendError, SendError},
};

pub(crate) type BoxDebug = Box<dyn fmt::Debug + Send + 'static>;
pub(crate) type BoxReply = Box<dyn any::Any + Send>;

/// A message that can modify an actors state.
///
/// Messages are processed sequentially one at a time, with exclusive mutable access to the actors state.
///
/// The reply type must implement [Reply].
pub trait Message<A>: Send + 'static {
    /// The reply sent back to the message caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this message.
    fn handle(state: &mut A, msg: Self) -> impl Future<Output = Self::Reply> + Send;
}

/// Queries the actor for some data.
///
/// Unlike regular messages, queries can be processed by the actor in parallel
/// if multiple queries are sent in sequence. This means queries only have read access
/// to the actors state.
///
/// The reply type must implement [Reply].
pub trait Query<A>: Send + 'static {
    /// The reply sent back to the query caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this query.
    fn handle(state: &A, query: Self) -> impl Future<Output = Self::Reply> + Send;
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
    /// The type returned to the oneshot channel.
    ///
    /// This is useful in cases where the type being returned differs from self.
    /// Though in most cases, this will be `Self`.
    type Return;

    /// Converts a reply to a `SendError`, containing the `HandleError` if the reply is an error.
    fn to_send_error<M>(ret: Self::Return) -> Result<Self::Ok, SendError<M, Self::Error>>;

    /// Converts the reply into a `Box<fmt::Debug + Send + Sync + 'static>` if it's an Err, otherwise `None`.
    fn into_boxed_err(self) -> Option<BoxDebug>;

    /// Sends the reply to the oneshot channel.
    ///
    /// It may be useful to overwrite this function in cases such as a reply is sent after awaiting a future,
    /// or deligating the reply to another location.
    fn reply(
        self,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> Result<(), BoxDebug>
    where
        Self: Send + Sized + 'static,
    {
        match tx {
            Some(tx) => {
                let _ = tx.send(Ok(Box::new(self) as BoxReply));
                Ok(())
            }
            None => match self.into_boxed_err() {
                Some(err) => Err(err),
                None => Ok(()),
            },
        }
    }
}

/// A future returned by an actor.
///
/// The value returned by [ActorRef::send] will be the value returned by the future `R`.
///
/// If the message was called with [ActorRef::send_async] or [ActorRef::send_after] and the future resolves to an error,
/// it will be signalled to the actor internally where [Actor::on_panic](crate::Actor::on_panic) will be called,
/// possibly stopping the actor.
///
/// # Example
///
/// ```
/// use std::io;
/// use kameo::{Actor, Message, ReplyFuture};
/// use tokio::fs::File;
///
/// #[derive(Actor, Default)]
/// pub struct FileActor {
///     files_seen: HashSet<PathBuf>,
/// }
///
/// struct OpenFile {
///     path: PathBuf,
/// }
///
/// impl Message<FileActor> for OpenFile {
///     type Reply = ReplyFuture<FileActor, io::Result<File>>;
///
///     async fn handle(state: &mut FileActor, msg: OpenFile) -> Self::Reply {
///         state.files_seen.insert(msg.path.clone());
///         
///         ReplyFuture::new(state.actor_ref(), async move {
///             File::open(msg.path).await
///         })
///     }
/// }
///
/// let file_actor = FileActor::default();
/// let file = file_actor.send(OpenFile { path: "./foo.txt".into() }).await?;
/// // We have access to the file... and `FileActor` never blocked while opening it!
/// ```
#[allow(missing_debug_implementations)]
pub struct ReplyFuture<A, R> {
    actor_ref: ActorRef<A>,
    fut: BoxFuture<'static, R>,
    finally: Option<Box<dyn for<'a> FnOnce(&'a mut A, &'a R) -> BoxFuture<'a, ()> + Send>>,
}

impl<A, R> ReplyFuture<A, R> {
    /// Constructs a new future to be returned as a reply by an actor.
    pub fn new<F>(actor_ref: ActorRef<A>, fut: F) -> Self
    where
        F: Future<Output = R> + Send + 'static,
    {
        ReplyFuture {
            actor_ref,
            fut: fut.boxed(),
            finally: None,
        }
    }

    pub fn then(
        mut self,
        f: impl for<'a> FnOnce(&'a mut A, &'a R) -> BoxFuture<'a, ()> + Send + 'static,
    ) -> Self {
        self.finally = Some(Box::new(f));
        self
    }
}

impl<A, R> Reply for ReplyFuture<A, R>
where
    A: Send,
    R: Reply<Return = R>,
    R::Ok: Send,
    R::Error: Send,
{
    type Ok = R::Ok;
    type Error = R::Error;
    type Return = R::Return;

    fn to_send_error<M>(ret: Self::Return) -> Result<Self::Ok, SendError<M, Self::Error>> {
        R::to_send_error(ret)
    }

    fn into_boxed_err(self) -> Option<BoxDebug> {
        None
    }

    fn reply(
        self,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> Result<(), BoxDebug>
    where
        Self: Send + Sized + 'static,
    {
        tokio::spawn(async move {
            let reply = self.fut.await;
            match tx {
                Some(tx) => {
                    let res = R::to_send_error::<Box<dyn any::Any + Send>>(reply)
                        .map(|val| Box::new(val) as BoxReply)
                        .map_err(SendError::boxed);
                    let _ = tx.send(res);
                }
                None => {
                    if let Some(err) = reply.into_boxed_err() {
                        let _ = self.actor_ref.send_async(ReplyFutureFailed { err });
                    }
                }
            }
        });

        Ok(())
    }
}

/// Forwards the error to the actor to be handled in an async way.
///
/// By default, this causes the actor to panic and stop.
struct ReplyFutureFinished<R> {
    res: R,
}

impl<A, R> Message<A> for ReplyFutureFinished<A, R>
where
    A: Send,
    R: Reply + Send,
    R::Error: fmt::Debug + Send + 'static,
{
    type Reply = Result<(), BoxDebug>;

    async fn handle(
        state: &mut A,
        ReplyFutureFinished { res, cb }: ReplyFutureFinished<A, R>,
    ) -> Self::Reply {
        if let Some(cb) = cb {
            cb(state, &res).await;
        }
        match res.into_boxed_err() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl<T, E> Reply for Result<T, E>
where
    E: fmt::Debug + Send + 'static,
{
    type Ok = T;
    type Error = E;
    type Return = Self;

    fn to_send_error<M>(ret: Self::Return) -> Result<T, SendError<M, E>> {
        ret.map_err(SendError::HandlerError)
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
            type Return = Self;

            fn to_send_error<Msg>(ret: Self::Return) -> Result<Self, SendError<Msg, ()>> {
                Ok(ret)
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
    fn handle_dyn(
        self: Box<Self>,
        state: &mut A,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Result<(), BoxDebug>>
    where
        A: Send;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, M> DynMessage<A> for M
where
    M: Message<A>,
{
    fn handle_dyn(
        self: Box<Self>,
        state: &mut A,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Result<(), BoxDebug>>
    where
        A: Send,
    {
        async move {
            let reply = Message::handle(state, *self).await;
            reply.reply(tx)
        }
        .boxed()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}

pub(crate) trait DynQuery<A>: Send {
    fn handle_dyn(
        self: Box<Self>,
        state: &A,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Result<(), BoxDebug>>
    where
        A: Send + Sync;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, M> DynQuery<A> for M
where
    M: Query<A>,
{
    fn handle_dyn(
        self: Box<Self>,
        state: &A,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Result<(), BoxDebug>>
    where
        A: Send + Sync,
    {
        async move {
            let reply = Query::handle(state, *self).await;
            reply.reply(tx)
        }
        .boxed()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}
