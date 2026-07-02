//! Messaging infrastructure for actor communication in Kameo.
//!
//! This module provides the constructs necessary for handling messages within Kameo,
//! defining how actors communicate and interact. It equips actors with the ability to receive and respond
//! to both commands that might change their internal state and requests for information which do not alter their state.
//!
//! A key component of this module is the [`Context`], which is passed to message handlers, offering them a
//! reference to the current actor and a way to reply to messages. This enables actors to perform a wide range of
//! actions in response to received messages, from altering their own state to querying other actors.
//!
//! The module distinguishes between two kinds of communication: messages, which are intended to modify an actor's
//! state and might lead to side effects, and queries, which are read-only requests for information from an actor.
//! This distinction helps in clearly separating commands from queries, aligning with the CQRS
//! (Command Query Responsibility Segregation) principle and enhancing the clarity and maintainability of actor
//! interactions. It also provides some performance benefits in that sequential queries can be processed concurrently.

use std::{any, fmt};

use futures::{Future, FutureExt, future::BoxFuture};

use crate::{
    Actor,
    actor::ActorRef,
    error::{self, PanicError, PanicReason, SendError},
    mailbox::Signal,
    reply::{BoxReplySender, DelegatedReply, ForwardedReply, Reply, ReplyError, ReplySender},
};

/// A boxed dynamic message type for the actor `A`.
pub type BoxMessage<A> = Box<dyn DynMessage<A>>;

/// A boxed continuation run against an actor's state when a [`Context::pipe_with`] future resolves.
///
/// The continuation returns a future that borrows `&mut A`, so it is boxed behind a `for<'a>`
/// bound rather than a plain `FnOnce`.
pub(crate) type CallbackFn<A> =
    Box<dyn for<'a> FnOnce(&'a mut A, &'a mut Context<A, ()>) -> BoxFuture<'a, ()> + Send>;

/// A boxed dynamic type used for message replies.
pub type BoxReply = Box<dyn any::Any + Send>;

/// A message that can modify an actors state.
///
/// Messages are processed sequentially one at a time, with exclusive mutable access to the actors state.
///
/// The reply type must implement [Reply].
pub trait Message<T: Send + 'static>: Actor {
    /// The reply sent back to the message caller.
    type Reply: Reply;

    /// The name of the message, which can be useful for logging or debugging.
    ///
    /// # Default Implementation
    /// By default, this returns the type name of the message.
    #[inline]
    fn name() -> &'static str {
        any::type_name::<T>()
    }

    /// Handler for this message.
    fn handle(
        &mut self,
        msg: T,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send;
}

/// A type for handling streams attached to an actor.
///
/// Actors which implement handling messages of this type can receive and process messages from a stream attached to the actor.
/// This type is designed to facilitate the integration of streaming data sources with actors,
/// allowing actors to react and process each message as it arrives from the stream.
///
/// It's typically used with [ActorRef::attach_stream] to attach a stream to an actor.
#[derive(Clone, Debug)]
pub enum StreamMessage<T, S, F> {
    /// The next item in a stream.
    Next(T),
    /// The stream has just been attached.
    Started(S),
    /// The stream has finished, and no more items will be sent.
    Finished(F),
}

/// A context provided to message handlers providing access
/// to the current actor ref, and reply channel.
pub struct Context<A, R>
where
    A: Actor,
    R: Reply + ?Sized,
{
    actor_ref: ActorRef<A>,
    reply: Option<ReplySender<R::Value>>,
    stop: bool,
}

impl<A, R> Context<A, R>
where
    A: Actor,
    R: Reply + ?Sized,
{
    pub(crate) fn new(
        actor_ref: ActorRef<A>,
        reply: Option<ReplySender<R::Value>>,
        stop: bool,
    ) -> Self {
        Context {
            actor_ref,
            reply,
            stop,
        }
    }

    /// Returns the current actor's ref, allowing messages to be sent to itself.
    pub fn actor_ref(&self) -> &ActorRef<A> {
        &self.actor_ref
    }

    /// Stops the actor normally after processing the current message.
    pub fn stop(&mut self) {
        self.stop = true;
    }

    /// Whether [`Context::stop`] was called on this context.
    pub(crate) fn should_stop(&self) -> bool {
        self.stop
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
    /// message was sent as a "tell" request (no response is needed by the caller).
    ///
    /// # Usage
    ///
    /// - The [DelegatedReply] marker should be returned by the handler to indicate that the response will be delegated.
    /// - The [ReplySender], if not `None`, should be used by the delegated responder to send the actual reply.
    ///
    /// ```
    /// use kameo::message::{Context, Message};
    /// use kameo::reply::DelegatedReply;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// struct Msg;
    ///
    /// impl Message<Msg> for MyActor {
    ///     type Reply = DelegatedReply<String>;
    ///
    ///     async fn handle(&mut self, msg: Msg, mut ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
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
    #[must_use = "the reply must be sent to the ReplySender"]
    pub fn reply_sender(&mut self) -> (DelegatedReply<R::Value>, Option<ReplySender<R::Value>>) {
        (DelegatedReply::new(), self.reply.take())
    }

    /// Sends a reply to the caller early, returning a `DelegatedReply`.
    ///
    /// This is a shortcut for creating a `DelegatedReply` in cases where you didn't need access to the `ReplySender`.
    pub fn reply(&mut self, reply: R::Value) -> DelegatedReply<R::Value> {
        if let Some(reply_sender) = self.reply.take() {
            reply_sender.send(reply);
        }
        DelegatedReply::new()
    }

    /// Spawns a detached task to handle the current message asynchronously.
    ///
    /// This method allows an actor to delegate message processing to a separate task,
    /// returning immediately with a [`DelegatedReply`]. The spawned task will complete
    /// independently of the actor's lifecycle and send the result back to the original
    /// message sender.
    ///
    /// # Error Handling
    ///
    /// - **Ask requests** (with reply expected): Errors are sent back to the caller
    /// - **Tell requests** (no reply expected): Errors are handled by the global error hook.
    ///
    /// The actor's [`on_panic`] hook is NOT called since the task is detached from the actor's message processing loop.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::prelude::*;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// struct ProcessData {
    ///     data: Vec<u8>,
    /// }
    ///
    /// impl Message<ProcessData> for MyActor {
    ///     type Reply = DelegatedReply<Result<String, std::io::Error>>;
    ///
    ///     async fn handle(&mut self, msg: ProcessData, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
    ///         // Spawn intensive processing in a separate task
    ///         ctx.spawn(async move {
    ///             // This runs independently of the actor
    ///             tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    ///             
    ///             // Process the data...
    ///             if msg.data.is_empty() {
    ///                 Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Empty data"))
    ///             } else {
    ///                 Ok(String::from_utf8_lossy(&msg.data).to_string())
    ///             }
    ///         })
    ///     }
    /// }
    /// ```
    ///
    /// # Important Notes
    ///
    /// - The spawned task continues running even if the actor stops
    /// - The task runs on the Tokio runtime's thread pool, not the actor's task
    ///
    /// [`on_panic`]: Actor::on_panic
    pub fn spawn<F>(&mut self, future: F) -> DelegatedReply<R::Value>
    where
        F: Future<Output = R::Value> + Send + 'static,
    {
        let (delegated_reply, reply_sender) = self.reply_sender();
        tokio::spawn(async move {
            let reply = future.await;
            match reply_sender {
                Some(tx) => {
                    tx.send(reply);
                }
                None => {
                    if let Some(err) = reply.into_any_err() {
                        error::invoke_actor_error_hook(&PanicError::new(
                            err,
                            PanicReason::OnMessage,
                        ));
                    }
                }
            }
        });

        delegated_reply
    }

    /// Runs a future off the actor's message loop, then sends its result back to the actor as a
    /// message.
    ///
    /// This is the "pipe to self" pattern: `future` runs on a separate task so the actor keeps
    /// processing other messages while it is in flight. When `future` resolves, its output is
    /// delivered to the actor with [`tell`] semantics, so it runs through the normal message
    /// pipeline (handler, ordering, tracing) and its reply is discarded.
    ///
    /// Use [`pipe_with`] instead when the completion logic is a one-off that should mutate the
    /// actor's state inline rather than go through a dedicated [`Message`] handler.
    ///
    /// The actor is kept alive until `future` resolves. If the actor has stopped by the time it
    /// resolves, the message is dropped. The future itself is not cancelled when the actor stops.
    ///
    /// [`tell`]: crate::actor::ActorRef::tell
    /// [`pipe_with`]: Context::pipe_with
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::prelude::*;
    ///
    /// #[derive(Actor, Default)]
    /// struct MyActor {
    ///     last: u32,
    /// }
    ///
    /// struct Fetch;
    /// struct Fetched(u32);
    ///
    /// impl Message<Fetch> for MyActor {
    ///     type Reply = ();
    ///
    ///     async fn handle(&mut self, _: Fetch, ctx: &mut Context<Self, Self::Reply>) {
    ///         ctx.pipe(async { Fetched(40 + 2) });
    ///     }
    /// }
    ///
    /// impl Message<Fetched> for MyActor {
    ///     type Reply = ();
    ///
    ///     async fn handle(&mut self, Fetched(value): Fetched, _: &mut Context<Self, Self::Reply>) {
    ///         self.last = value;
    ///     }
    /// }
    /// ```
    pub fn pipe<F, M>(&self, future: F)
    where
        A: Message<M>,
        F: Future<Output = M> + Send + 'static,
        M: Send + 'static,
    {
        let actor_ref = self.actor_ref.clone();
        tokio::spawn(async move {
            let msg = future.await;
            let _ = actor_ref.tell(msg).send().await;
        });
    }

    /// Runs a future off the actor's message loop, then applies its result back to the actor with
    /// a custom continuation.
    ///
    /// Like [`pipe`], but instead of delivering the result as a message, `on_complete` runs with
    /// `&mut self` (and its own [`Context`]) between messages, so it can mutate the actor's state
    /// directly, and it may `.await`. Prefer [`pipe`] when the completion should go through an
    /// existing [`Message`] handler.
    ///
    /// The actor is kept alive until `future` resolves. If the actor has stopped or is
    /// restarting by the time it resolves, `on_complete` is not run. The future itself is not
    /// cancelled when the actor stops.
    ///
    /// Because `on_complete` returns a future that borrows `&mut self`, it is written as a
    /// closure returning a boxed future (`Box::pin(async move { .. })`).
    ///
    /// [`pipe`]: Context::pipe
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::prelude::*;
    ///
    /// #[derive(Actor, Default)]
    /// struct MyActor {
    ///     last: u32,
    /// }
    ///
    /// struct Fetch;
    ///
    /// impl Message<Fetch> for MyActor {
    ///     type Reply = ();
    ///
    ///     async fn handle(&mut self, _: Fetch, ctx: &mut Context<Self, Self::Reply>) {
    ///         ctx.pipe_with(async { 40 + 2 }, |actor, _ctx, result| {
    ///             Box::pin(async move {
    ///                 actor.last = result;
    ///             })
    ///         });
    ///     }
    /// }
    /// ```
    pub fn pipe_with<F, Fun>(&self, future: F, on_complete: Fun)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        Fun: for<'a> FnOnce(&'a mut A, &'a mut Context<A, ()>, F::Output) -> BoxFuture<'a, ()>
            + Send
            + 'static,
    {
        let actor_ref = self.actor_ref.clone();
        tokio::spawn(async move {
            let output = future.await;
            let callback: CallbackFn<A> =
                Box::new(move |actor, ctx| on_complete(actor, ctx, output));
            // The signal carries a strong `ActorRef` so the actor stays alive until the
            // callback is processed, even after this task drops its own ref.
            let signal = Signal::Callback {
                actor_ref: actor_ref.clone(),
                callback,
            };
            let _ = actor_ref.mailbox_sender().send(signal).await;
        });
    }

    /// Forwards the message to another actor, returning a [ForwardedReply].
    pub async fn forward<B, M>(
        &mut self,
        actor_ref: &ActorRef<B>,
        message: M,
    ) -> ForwardedReply<M, <B as Message<M>>::Reply>
    where
        B: Message<M>,
        M: Send + 'static,
    {
        match self.reply.take() {
            Some(tx) => {
                let res = actor_ref
                    .ask(message)
                    .forward(tx.cast())
                    .await
                    .map_err(|err| {
                        err.map_msg(|(msg, tx)| {
                            self.reply = Some(tx.cast());
                            msg
                        })
                    });
                ForwardedReply::new(res)
            }
            None => {
                let res = actor_ref
                    .tell(message)
                    .send()
                    .await
                    .map_err(SendError::reset_err_infallible);
                ForwardedReply::new(res)
            }
        }
    }

    /// Tries to forward the message to another actor, returning a [ForwardedReply],
    /// or an error if the mailbox is full.
    pub fn try_forward<B, M>(
        &mut self,
        actor_ref: &ActorRef<B>,
        message: M,
    ) -> ForwardedReply<M, <B as Message<M>>::Reply>
    where
        B: Message<M>,
        M: Send + 'static,
    {
        match self.reply.take() {
            Some(tx) => {
                let res = actor_ref
                    .ask(message)
                    .try_forward(tx.cast())
                    .map_err(|err| {
                        err.map_msg(|(msg, tx)| {
                            self.reply = Some(tx.cast());
                            msg
                        })
                    });
                ForwardedReply::new(res)
            }
            None => {
                let res = actor_ref
                    .tell(message)
                    .try_send()
                    .map_err(SendError::reset_err_infallible);
                ForwardedReply::new(res)
            }
        }
    }

    /// Forwards the message to another actor, returning a [ForwardedReply].
    ///
    /// This method blocks the current thread while waiting for mailbox capacity.
    pub fn blocking_forward<B, M>(
        &mut self,
        actor_ref: &ActorRef<B>,
        message: M,
    ) -> ForwardedReply<M, <B as Message<M>>::Reply>
    where
        B: Message<M>,
        M: Send + 'static,
    {
        match self.reply.take() {
            Some(tx) => {
                let res = actor_ref
                    .ask(message)
                    .blocking_forward(tx.cast())
                    .map_err(|err| {
                        err.map_msg(|(msg, tx)| {
                            self.reply = Some(tx.cast());
                            msg
                        })
                    });
                ForwardedReply::new(res)
            }
            None => {
                let res = actor_ref
                    .tell(message)
                    .blocking_send()
                    .map_err(SendError::reset_err_infallible);
                ForwardedReply::new(res)
            }
        }
    }
}

impl<A, R> fmt::Debug for Context<A, R>
where
    A: Actor,
    R: Reply + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("actor_ref", &self.actor_ref)
            .field("reply", &self.reply)
            .finish()
    }
}

/// An object safe message which can be handled by an actor `A`.
///
/// This trait is implemented for all types which implement [`Message`], and is typically used for advanced cases such
/// as buffering actor messages.
pub trait DynMessage<A>
where
    Self: Send,
    A: Actor,
{
    /// Handles the dyn message with the provided actor state, ref, and reply sender.
    fn handle_dyn<'a>(
        self: Box<Self>,
        state: &'a mut A,
        actor_ref: ActorRef<A>,
        tx: Option<BoxReplySender>,
        stop: &'a mut bool,
    ) -> BoxFuture<'a, Result<(), Box<dyn ReplyError>>>;

    /// Casts the type to a `Box<dyn Any>`.
    fn as_any(self: Box<Self>) -> Box<dyn any::Any>;
}

impl<A, T> DynMessage<A> for T
where
    A: Actor + Message<T>,
    T: Send + 'static,
{
    fn handle_dyn<'a>(
        self: Box<Self>,
        state: &'a mut A,
        actor_ref: ActorRef<A>,
        tx: Option<BoxReplySender>,
        stop: &'a mut bool,
    ) -> BoxFuture<'a, Result<(), Box<dyn ReplyError>>> {
        async move {
            let reply_sender = tx.map(ReplySender::new);
            let mut ctx: Context<A, <A as Message<T>>::Reply> =
                Context::new(actor_ref, reply_sender, *stop);
            let reply = Message::handle(state, *self, &mut ctx).await;
            *stop = ctx.stop;
            if let Some(tx) = ctx.reply.take() {
                tx.send(reply.into_value());
                Ok(())
            } else {
                match reply.into_any_err() {
                    Some(err) => Err(err),
                    None => Ok(()),
                }
            }
        }
        .boxed()
    }

    fn as_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
}
