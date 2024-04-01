use futures::{
    future::{BoxFuture, LocalBoxFuture},
    Future,
};

use crate::message::{BoxDebug, BoxReply, DynMessage, DynQuery};

pub(crate) trait MessageActorKind<A> {
    type MessageFuture<'a>: Future<Output = BoxReply> + 'a
    where
        A: 'a;
    type MessageAsyncFuture<'a>: Future<Output = Option<BoxDebug>> + 'a
    where
        A: 'a;

    fn handle_message<'a>(msg: Box<dyn DynMessage<A>>, state: &'a mut A)
        -> Self::MessageFuture<'a>;
    fn handle_message_async<'a>(
        msg: Box<dyn DynMessage<A>>,
        state: &'a mut A,
    ) -> Self::MessageAsyncFuture<'a>;
}

pub(crate) trait QueryActorKind<A> {
    type QueryFuture<'a>: Future<Output = BoxReply> + 'a
    where
        A: 'a;
    type QueryAsyncFuture<'a>: Future<Output = Option<BoxDebug>> + 'a
    where
        A: 'a;

    fn handle_query<'a>(msg: Box<dyn DynQuery<A>>, state: &'a A) -> Self::QueryFuture<'a>;
    fn handle_query_async<'a>(
        msg: Box<dyn DynQuery<A>>,
        state: &'a A,
    ) -> Self::QueryAsyncFuture<'a>;
}

pub(crate) struct SendActor;

impl<A> MessageActorKind<A> for SendActor
where
    A: Send,
{
    type MessageFuture<'a> = BoxFuture<'a, BoxReply> where A: 'a;
    type MessageAsyncFuture<'a> = BoxFuture<'a, Option<BoxDebug>> where A: 'a;

    fn handle_message<'a>(
        msg: Box<dyn DynMessage<A>>,
        state: &'a mut A,
    ) -> Self::MessageFuture<'a> {
        msg.handle_dyn(state)
    }

    fn handle_message_async<'a>(
        msg: Box<dyn DynMessage<A>>,
        state: &'a mut A,
    ) -> Self::MessageAsyncFuture<'a> {
        msg.handle_dyn_async(state)
    }
}

impl<A> QueryActorKind<A> for SendActor
where
    A: Send + Sync,
{
    type QueryFuture<'a> = BoxFuture<'a, BoxReply> where A: 'a;
    type QueryAsyncFuture<'a> = BoxFuture<'a, Option<BoxDebug>> where A: 'a;

    fn handle_query<'a>(msg: Box<dyn DynQuery<A>>, state: &'a A) -> Self::QueryFuture<'a> {
        msg.handle_dyn(state)
    }

    fn handle_query_async<'a>(
        msg: Box<dyn DynQuery<A>>,
        state: &'a A,
    ) -> Self::QueryAsyncFuture<'a> {
        msg.handle_dyn_async(state)
    }
}

pub(crate) struct LocalActor;

impl<A> MessageActorKind<A> for LocalActor {
    type MessageFuture<'a> = LocalBoxFuture<'a, BoxReply> where A: 'a;
    type MessageAsyncFuture<'a> = LocalBoxFuture<'a, Option<BoxDebug>> where A: 'a;

    fn handle_message<'a>(
        msg: Box<dyn DynMessage<A>>,
        state: &'a mut A,
    ) -> Self::MessageFuture<'a> {
        msg.handle_dyn_local(state)
    }

    fn handle_message_async<'a>(
        msg: Box<dyn DynMessage<A>>,
        state: &'a mut A,
    ) -> Self::MessageAsyncFuture<'a> {
        msg.handle_dyn_async_local(state)
    }
}

impl<A> QueryActorKind<A> for LocalActor {
    type QueryFuture<'a> = LocalBoxFuture<'a, BoxReply> where A: 'a;
    type QueryAsyncFuture<'a> = LocalBoxFuture<'a, Option<BoxDebug>> where A: 'a;

    fn handle_query<'a>(msg: Box<dyn DynQuery<A>>, state: &'a A) -> Self::QueryFuture<'a> {
        msg.handle_dyn_local(state)
    }

    fn handle_query_async<'a>(
        msg: Box<dyn DynQuery<A>>,
        state: &'a A,
    ) -> Self::QueryAsyncFuture<'a> {
        msg.handle_dyn_async_local(state)
    }
}
