use std::{any, error, fmt};

use async_trait::async_trait;

pub(crate) type BoxDebug = Box<dyn fmt::Debug + Send + Sync + 'static>;
pub(crate) type BoxReply = Box<dyn any::Any + Send>;

/// A message that can modify an actors state.
///
/// Messages are processed sequentially one at a time.
#[async_trait]
pub trait Message<A>: Send + 'static {
    /// The reply sent back to the message caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this message.
    async fn handle(self, state: &mut A) -> Self::Reply;
}

/// Queries the actor for some data.
///
/// Unlike regular messages, queries can be processed by the actor in parallel
/// if multiple queries are sent in sequence. This means we only have read access
/// to the actors state.
#[async_trait]
pub trait Query<A>: Send + 'static {
    /// The reply sent back to the query caller.
    type Reply: Reply + Send + 'static;

    /// Handler for this query.
    async fn handle(self, state: &A) -> Self::Reply;
}

#[doc(hidden)]
pub trait Reply {
    fn into_boxed_err(self) -> Option<BoxDebug>;
}

impl<T> Reply for T {
    default fn into_boxed_err(self) -> Option<BoxDebug> {
        None
    }
}

#[derive(Debug)]
struct UnknownError(&'static str);

impl fmt::Display for UnknownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl error::Error for UnknownError {}

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

#[async_trait]
pub(crate) trait DynMessage<A>: Send {
    async fn handle_dyn(self: Box<Self>, state: &mut A) -> BoxReply;
    async fn handle_dyn_async(self: Box<Self>, state: &mut A) -> Option<BoxDebug>;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

#[async_trait]
impl<A, M> DynMessage<A> for M
where
    A: Send,
    M: Message<A>,
{
    async fn handle_dyn(self: Box<Self>, state: &mut A) -> BoxReply {
        Box::new((*self).handle(state).await)
    }

    async fn handle_dyn_async(self: Box<Self>, state: &mut A) -> Option<BoxDebug> {
        (*self).handle(state).await.into_boxed_err()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}

#[async_trait]
pub(crate) trait DynQuery<A>: Send {
    async fn handle_dyn(self: Box<Self>, state: &A) -> BoxReply;
    async fn handle_dyn_async(self: Box<Self>, state: &A) -> Option<BoxDebug>;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

#[async_trait]
impl<A, M> DynQuery<A> for M
where
    A: Send + Sync,
    M: Query<A>,
{
    async fn handle_dyn(self: Box<Self>, state: &A) -> BoxReply {
        Box::new((*self).handle(state).await)
    }

    async fn handle_dyn_async(self: Box<Self>, state: &A) -> Option<BoxDebug> {
        (*self).handle(state).await.into_boxed_err()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}
