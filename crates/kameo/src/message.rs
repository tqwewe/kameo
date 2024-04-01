use std::{any, fmt};

use futures::{future::BoxFuture, Future, FutureExt};

pub(crate) type BoxDebug = Box<dyn fmt::Debug + Send + Sync + 'static>;
pub(crate) type BoxReply = Box<dyn any::Any + Send>;

/// A message that can modify an actors state.
///
/// Messages are processed sequentially one at a time, with exclusive mutable access to the actors state.
///
/// The reply type must implement [Reply], which has different implementations based on the `nightly` feature flag.
/// See the Reply docs for more information on this.
pub trait Message<A>: Send + 'static {
    /// The reply sent back to the message caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this message.
    fn handle(self, state: &mut A) -> impl Future<Output = Self::Reply> + Send;
}

/// Queries the actor for some data.
///
/// Unlike regular messages, queries can be processed by the actor in parallel
/// if multiple queries are sent in sequence. This means queries only have read access
/// to the actors state.
///
/// The reply type must implement [Reply], which has different implementations based on the `nightly` feature flag.
/// See the Reply docs for more information on this.
pub trait Query<A>: Send + 'static {
    /// The reply sent back to the query caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this query.
    fn handle(self, state: &A) -> impl Future<Output = Self::Reply> + Send;
}

/// A reply value.
///
/// If an Err is returned by a handler, and is unhandled by the caller (ie, the message was sent async),
/// then the error is treated as a panic in the actor.
///
/// ### On Stable
///
/// This is implemented for all `Result<T, E>` types, where `E: Debug + Send + Sync + 'static`.
///
/// ### On Nightly
///
/// This is implemented for all types, and uses specialization to recognize errors, which are any `Result` types.
pub trait Reply {
    /// Converts the reply into a `Box<fmt::Debug + Send + Sync + 'static>` if it's an Err, otherwise `None`.
    fn into_boxed_err(self) -> Option<BoxDebug>;
}

#[cfg(feature = "nightly")]
impl<T> Reply for T {
    default fn into_boxed_err(self) -> Option<BoxDebug> {
        None
    }
}

#[cfg(feature = "nightly")]
#[derive(Debug)]
struct UnknownError(&'static str);

#[cfg(feature = "nightly")]
impl fmt::Display for UnknownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "nightly")]
impl std::error::Error for UnknownError {}

#[cfg(feature = "nightly")]
impl<T, E> Reply for Result<T, E> {
    default fn into_boxed_err(self) -> Option<BoxDebug> {
        self.map_err(|err| Box::new(UnknownError(any::type_name_of_val(&err))) as BoxDebug)
            .err()
    }
}

impl<T, E> Reply for Result<T, E>
where
    E: fmt::Debug + Send + Sync + 'static,
{
    fn into_boxed_err(self) -> Option<BoxDebug> {
        self.map_err(|err| Box::new(err) as BoxDebug).err()
    }
}

pub(crate) trait DynMessage<A>: Send {
    fn handle_dyn(self: Box<Self>, state: &mut A) -> BoxFuture<'_, BoxReply>
    where
        A: Send;
    fn handle_dyn_async(self: Box<Self>, state: &mut A) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, M> DynMessage<A> for M
where
    M: Message<A>,
{
    fn handle_dyn(self: Box<Self>, state: &mut A) -> BoxFuture<'_, BoxReply>
    where
        A: Send,
    {
        async move { Box::new((*self).handle(state).await) as BoxReply }.boxed()
    }

    fn handle_dyn_async(self: Box<Self>, state: &mut A) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send,
    {
        async move { (*self).handle(state).await.into_boxed_err() }.boxed()
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

impl<A, M> DynQuery<A> for M
where
    M: Query<A>,
{
    fn handle_dyn(self: Box<Self>, state: &A) -> BoxFuture<'_, BoxReply>
    where
        A: Send + Sync,
    {
        async move { Box::new((*self).handle(state).await) as BoxReply }.boxed()
    }

    fn handle_dyn_async(self: Box<Self>, state: &A) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send + Sync,
    {
        async move { (*self).handle(state).await.into_boxed_err() }.boxed()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}
