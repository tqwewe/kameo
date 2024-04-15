//! Messaging infrastructure for actor communication in kameo.
//!
//! This module provides the constructs necessary for handling messages and queries within kameo,
//! defining how actors communicate and interact. It equips actors with the ability to receive and respond
//! to both commands that might change their internal state and requests for information which do not alter their state.
//!
//! A key component of this module is the [`Context`], which is passed to message and query handlers, offering them a
//! reference to the current actor and a way to reply to messages. This enables actors to perform a wide range of
//! actions in response to received messages, from altering their own state to querying other actors.
//!
//! The module distinguishes between two kinds of communication: messages, which are intended to modify an actor's
//! state and might lead to side effects, and queries, which are read-only requests for information from an actor.
//! This distinction helps in clearly separating commands from queries, aligning with the CQRS
//! (Command Query Responsibility Segregation) principle and enhancing the clarity and maintainability of actor
//! interactions. It also provides some performance benefits in that sequential queries can be processed concurrently.

use std::{any, fmt};

use futures::{future::BoxFuture, Future, FutureExt};
use tokio::sync::oneshot;

use crate::{
    actor::ActorRef,
    error::{BoxSendError, SendError},
    reply::{DelegatedReply, ForwardedReply, Reply, ReplySender},
};

pub(crate) type BoxDebug = Box<dyn fmt::Debug + Send + 'static>;
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
    fn handle(
        &mut self,
        msg: T,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send;
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
    fn handle(
        &self,
        query: T,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send;
}

/// A trait for handling messages streamed to an actor.
///
/// Implementors of this trait can receive and process messages from a stream attached to the actor.
/// This trait is designed to facilitate the integration of streaming data sources with actors, allowing
/// actors to react and process each message as it arrives from the stream.
///
/// The `S` and `F` generics allows additional data to be provided when attaching a stream.
///
/// # Examples
///
/// ```
/// impl StreamMessage<MyMessageType, (), ()> for MyActor {
///     async fn handle(&mut self, msg: MyMessageType, ctx: Context<'_, Self, ()>) {
///         // Process each message here
///     }
///
///     async fn on_start(&mut self, msg: (), ctx: Context<'_, Self, ()>) {
///         // Handle the start of the stream
///     }
///
///     async fn on_finish(&mut self, msg: (), ctx: Context<'_, Self, ()>) {
///         // Handle the completion of the stream
///     }
/// }
/// ```
pub trait StreamMessage<T, S, F> {
    /// Handles an individual message received from the stream.
    ///
    /// This method is called for each message that arrives from the attached stream, providing a way for the actor
    /// to process streamed data. The method receives the message and a context allowing interaction with the actor system.
    fn handle(&mut self, msg: T, ctx: Context<'_, Self, ()>) -> impl Future<Output = ()> + Send;

    /// Called when a stream has been attached to the actor.
    ///
    /// This optional method can be implemented to perfom initialization for handling the stream. Additional data can be
    /// provided through the `S` generic, which is received from the [`ActorRef::attach_stream`] method.
    /// By default, it does nothing, but implementors can override it to add behavior specific to their actor's needs.
    #[allow(unused_variables)]
    fn on_start(&mut self, msg: S, ctx: Context<'_, Self, ()>) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Called when the message stream attached to the actor is finished.
    ///
    /// This optional method can be implemented to perform cleanup or final actions once the stream has ended.
    /// Additional data can be provided through the `F` generic, which is received from the [`ActorRef::attach_stream`] method.
    /// By default, it does nothing, but implementors can override it to add behavior specific to their actor's needs.
    #[allow(unused_variables)]
    fn on_finish(&mut self, msg: F, ctx: Context<'_, Self, ()>) -> impl Future<Output = ()> + Send {
        async {}
    }
}

impl<A, T, S, F> Message<StreamMessageEnvelope<T, S, F>> for A
where
    A: StreamMessage<T, S, F> + Send + 'static,
    T: Send,
    S: Send,
    F: Send,
{
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StreamMessageEnvelope<T, S, F>,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        match msg {
            StreamMessageEnvelope::Next(msg) => StreamMessage::handle(self, msg, ctx).await,
            StreamMessageEnvelope::Started(value) => {
                StreamMessage::on_start(self, value, ctx).await
            }
            StreamMessageEnvelope::Finished(value) => {
                StreamMessage::on_finish(self, value, ctx).await
            }
        }
    }
}

pub(crate) enum StreamMessageEnvelope<T, S, F> {
    Next(T),
    Started(S),
    Finished(F),
}

impl<T, S, F> StreamMessageEnvelope<T, S, F> {
    pub(crate) fn unwrap(self) -> T {
        match self {
            StreamMessageEnvelope::Next(msg) => msg,
            _ => panic!("unwrap panicked during StreamMessageEnvelope::unwrap"),
        }
    }
}

/// A context provided to message and query handlers providing access
/// to the current actor ref, and reply channel.
#[derive(Debug)]
pub struct Context<'r, A: ?Sized, R: ?Sized>
where
    R: Reply,
{
    actor_ref: ActorRef<A>,
    reply: &'r mut Option<ReplySender<R::Value>>,
}

impl<'r, A, R> Context<'r, A, R>
where
    R: Reply,
{
    pub(crate) fn new(
        actor_ref: ActorRef<A>,
        reply: &'r mut Option<ReplySender<R::Value>>,
    ) -> Self {
        Context { actor_ref, reply }
    }

    /// Returns the current actor's ref, allowing messages to be sent to itself.
    pub fn actor_ref(&self) -> ActorRef<A> {
        self.actor_ref.clone()
    }

    /// Extracts the reply sender, providing a mechanism for delegated responses and an optional reply sender.
    ///
    /// This method is designed for scenarios where the response to a message is not immediate and needs to be
    /// handled by another actor or elsewhere. Upon calling this method, if the reply sender exists (is `Some`),
    /// it must be utilized through [ReplySender::send] to send the response back to the original requester.
    ///
    /// This method returns a tuple consisting of [DelegatedReply] and an optional [ReplySender]. The `DelegatedReply`
    /// is a marker type indicating that the message handler will delegate the task of replying to another part of the
    /// system. It should be returned by the message handler to signify this intention. The `ReplySender`, if present,
    /// should be used to actually send the response back to the caller. The `ReplySender` will not be present if the
    /// message was sent as async (no repsonse is needed by the caller).
    ///
    /// # Usage
    ///
    /// - The [DelegatedReply] marker should be returned by the handler to indicate that the response will be delegated.
    /// - The [ReplySender], if not `None`, should be used by the delegated responder to send the actual reply.
    ///
    /// ```
    /// use kameo::message::{Context, DelegatedReply, Message};
    ///
    /// impl Message<MyMsg> for MyActor {
    ///     type Reply = DelegatedReply<String>;
    ///
    ///     async fn handle(&mut self, msg: MyMsg, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
    ///         let (delegated_reply, reply_sender) = ctx.reply_sender();
    ///
    ///         if let Some(tx) = reply_sender {
    ///             tokio::spawn(async move {
    ///                 tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    ///                 tx.send("done!".to_string());
    ///             });
    ///         }
    ///
    ///         delegated_reply
    ///     }
    /// }
    /// ```
    ///
    /// It is important to ensure that [ReplySender::send] is called to complete the transaction and send the response
    /// back to the requester. Failure to do so could result in the requester waiting indefinitely for a response.
    pub fn reply_sender(&mut self) -> (DelegatedReply<R::Value>, Option<ReplySender<R::Value>>) {
        (DelegatedReply::new(), self.reply.take())
    }

    /// Forwards the message to another actor, returning a [ForwardedReply].
    ///
    /// The message will be sent handled by another actor without blocking the current actor.
    pub fn forward<B, M, R2, E>(
        &mut self,
        actor_ref: ActorRef<B>,
        message: M,
    ) -> ForwardedReply<R::Ok, M, E>
    where
        B: Message<M, Reply = R2>,
        M: Send + Sync + 'static,
        R: Reply<Error = SendError<M, E>, Value = Result<<R as Reply>::Ok, SendError<M, E>>>,
        R2: Reply<Ok = R::Ok, Error = E, Value = Result<R::Ok, E>>,
        E: fmt::Debug + Send + Sync + 'static,
    {
        let (delegated_reply, reply_sender) = self.reply_sender();
        tokio::spawn(async move {
            let reply = actor_ref.send(message).await;
            if let Some(reply_sender) = reply_sender {
                reply_sender.send(reply);
            }
        });

        delegated_reply
    }
}

pub(crate) trait DynMessage<A>
where
    Self: Send,
{
    fn handle_dyn(
        self: Box<Self>,
        state: &mut A,
        actor_ref: ActorRef<A>,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, T> DynMessage<A> for T
where
    A: Message<T>,
    T: Send + 'static,
{
    fn handle_dyn(
        self: Box<Self>,
        state: &mut A,
        actor_ref: ActorRef<A>,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send,
    {
        async move {
            let mut reply_sender = tx.map(ReplySender::new);
            let ctx: Context<'_, A, <A as Message<T>>::Reply> =
                Context::new(actor_ref, &mut reply_sender);
            let reply = Message::handle(state, *self, ctx).await;
            if let Some(tx) = reply_sender.take() {
                tx.send(reply.into_value());
                None
            } else {
                reply.into_boxed_err()
            }
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
        actor_ref: ActorRef<A>,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send + Sync;
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, T> DynQuery<A> for T
where
    A: Query<T>,
    T: Send + 'static,
{
    fn handle_dyn(
        self: Box<Self>,
        state: &A,
        actor_ref: ActorRef<A>,
        tx: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    ) -> BoxFuture<'_, Option<BoxDebug>>
    where
        A: Send + Sync,
    {
        async move {
            let mut reply_sender = tx.map(ReplySender::new);
            let ctx: Context<'_, A, <A as Query<T>>::Reply> =
                Context::new(actor_ref, &mut reply_sender);
            let reply = Query::handle(state, *self, ctx).await;
            if let Some(tx) = reply_sender.take() {
                tx.send(reply.into_value());
                None
            } else {
                reply.into_boxed_err()
            }
        }
        .boxed()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}
