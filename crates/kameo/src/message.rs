use std::{any, fmt};

use futures::{future::BoxFuture, Future, FutureExt};

use crate::SendError;

pub(crate) type BoxDebug = Box<dyn fmt::Debug + Send + Sync + 'static>;
pub(crate) type BoxReply = Box<dyn any::Any + Send>;

/// A message that can modify an actors state.
///
/// Messages are processed sequentially one at a time, with exclusive mutable access to the actors state.
///
/// The reply type must implement [Reply], which has different implementations based on the `nightly` feature flag.
/// See the Reply docs for more information on this.
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
/// The reply type must implement [Reply], which has different implementations based on the `nightly` feature flag.
/// See the Reply docs for more information on this.
pub trait Query<T>: Send + 'static {
    /// The reply sent back to the query caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this query.
    fn handle(&self, query: T) -> impl Future<Output = Self::Reply> + Send;
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
