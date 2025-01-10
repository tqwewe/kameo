use std::{cell::Cell, collections::HashMap, fmt, ops, sync::Arc};

use futures::{stream::AbortHandle, Stream, StreamExt};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
    task_local,
};

#[cfg(feature = "remote")]
use crate::remote;
#[cfg(feature = "remote")]
use std::marker::PhantomData;

use crate::{
    error::{self, SendError},
    mailbox::{bounded::BoundedMailbox, Mailbox, SignalMailbox, WeakMailbox},
    message::{Message, StreamMessage},
    reply::Reply,
    request::{
        self, AskRequest, LocalAskRequest, LocalTellRequest, MessageSend, TellRequest,
        WithoutRequestTimeout,
    },
    Actor,
};

use super::id::ActorID;

task_local! {
    pub(crate) static CURRENT_ACTOR_ID: ActorID;
}
thread_local! {
    pub(crate) static CURRENT_THREAD_ACTOR_ID: Cell<Option<ActorID>> = const { Cell::new(None) };
}

/// A reference to an actor, used for sending messages and managing its lifecycle.
///
/// An `ActorRef` allows interaction with an actor through message passing, both for asking (waiting for a reply)
/// and telling (without waiting for a reply). It also provides utilities for managing the actor's state,
/// such as checking if the actor is alive, registering the actor under a name, and stopping the actor gracefully.
pub struct ActorRef<A: Actor> {
    id: ActorID,
    mailbox: A::Mailbox,
    abort_handle: AbortHandle,
    pub(crate) links: Links,
    pub(crate) startup_semaphore: Arc<Semaphore>,
}

impl<A> ActorRef<A>
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new(
        mailbox: A::Mailbox,
        abort_handle: AbortHandle,
        links: Links,
        startup_semaphore: Arc<Semaphore>,
    ) -> Self {
        ActorRef {
            id: ActorID::generate(),
            mailbox,
            abort_handle,
            links,
            startup_semaphore,
        }
    }

    /// Returns the unique identifier of the actor.
    #[inline]
    pub fn id(&self) -> ActorID {
        self.id
    }

    /// Returns whether the actor is currently alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        !self.mailbox.is_closed()
    }

    /// Registers the actor under a given name within the actor swarm.
    ///
    /// This makes the actor discoverable by other nodes in the distributed system.
    #[cfg(feature = "remote")]
    pub async fn register(&self, name: &str) -> Result<(), error::RegistrationError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistrationError::SwarmNotBootstrapped)?
            .register(self.clone(), name.to_string())
            .await
    }

    /// Looks up an actor registered locally by its name.
    ///
    /// Returns `Some` if the actor exists, or `None` if no actor with the given name is registered.
    #[cfg(feature = "remote")]
    pub async fn lookup(name: &str) -> Result<Option<Self>, error::RegistrationError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistrationError::SwarmNotBootstrapped)?
            .lookup_local(name.to_string())
            .await
    }

    /// Converts the `ActorRef` to a [`WeakActorRef`] that does not count
    /// towards RAII semantics, i.e. if all `ActorRef` instances of the
    /// actor were dropped and only `WeakActorRef` instances remain,
    /// the actor is stopped.
    #[must_use = "Downgrade creates a WeakActorRef without destroying the original non-weak actor ref."]
    #[inline]
    pub fn downgrade(&self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.downgrade(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_notify: self.startup_semaphore.clone(),
        }
    }

    /// Returns the number of [`ActorRef`] handles.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.mailbox.strong_count()
    }

    /// Returns the number of [`WeakActorRef`] handles.
    #[inline]
    pub fn weak_count(&self) -> usize {
        self.mailbox.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// This is useful when checking if certain code is being executed from within the actor's own context.
    #[inline]
    pub fn is_current(&self) -> bool {
        CURRENT_ACTOR_ID
            .try_with(Clone::clone)
            .map(|current_actor_id| current_actor_id == self.id())
            .unwrap_or(false)
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// This method ensures that the actor finishes processing any messages that were already in the queue
    /// before it shuts down. Any new messages sent after the stop signal will be ignored.
    #[inline]
    pub async fn stop_gracefully(&self) -> Result<(), error::SendError> {
        self.mailbox.signal_stop().await
    }

    /// Kills the actor immediately.
    ///
    /// This method aborts the actor immediately. Messages in the mailbox will be ignored and dropped.
    ///
    /// The actors on_stop hook will still be called.
    ///
    /// Note: If the actor is in the middle of processing a message, it will abort processing of that message.
    #[inline]
    pub fn kill(&self) {
        self.abort_handle.abort()
    }

    /// Waits for the actor to finish startup and become ready to process messages.
    ///
    /// This method ensures the actors on_start lifecycle hook has been fully processed.
    /// If `wait_startup` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use kameo::actor::{Actor, ActorRef};
    /// use kameo::error::BoxError;
    /// use kameo::mailbox::unbounded::UnboundedMailbox;
    /// use tokio::time::sleep;
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Mailbox = UnboundedMailbox<Self>;
    ///
    ///     async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
    ///         sleep(Duration::from_secs(2)).await; // Some io operation
    ///         Ok(())
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// actor_ref.wait_startup().await;
    /// println!("Actor ready to handle messages!");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn wait_startup(&self) {
        let _ = self.startup_semaphore.acquire().await;
    }

    /// Waits for the actor to finish processing and stop.
    ///
    /// This method suspends execution until the actor has stopped, ensuring that any ongoing
    /// processing is completed and the actor has fully terminated. This is particularly useful
    /// in scenarios where it's necessary to wait for an actor to clean up its resources or
    /// complete its final tasks before proceeding.
    ///
    /// Note: This method does not initiate the stop process; it only waits for the actor to
    /// stop. You should signal the actor to stop using [`stop_gracefully`](ActorRef::stop_gracefully) or [`kill`](ActorRef::kill)
    /// before calling this method.
    #[inline]
    pub async fn wait_for_stop(&self) {
        self.mailbox.closed().await
    }

    /// Sends a message to the actor and waits for a reply.
    ///
    /// The `ask` pattern is used when you expect a response from the actor. This method returns
    /// an `AskRequest`, which can be awaited asynchronously, or sent in a blocking manner using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::actor::ActorRef;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// # let msg = Msg;
    /// let reply = actor_ref.ask(msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    pub fn ask<M>(
        &self,
        msg: M,
    ) -> AskRequest<
        LocalAskRequest<'_, A, A::Mailbox>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
    where
        A: Message<M>,
        M: Send + 'static,
    {
        AskRequest::new(
            self,
            msg,
            #[cfg(debug_assertions)]
            std::panic::Location::caller(),
        )
    }

    /// Sends a message to the actor without waiting for a reply.
    ///
    /// The `tell` pattern is used for one-way communication, where no response is expected from the actor. This method
    /// returns a `TellRequest`, which can be awaited asynchronously, or configured using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::actor::ActorRef;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// # let msg = Msg;
    /// actor_ref.tell(msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    pub fn tell<M>(
        &self,
        msg: M,
    ) -> TellRequest<LocalTellRequest<'_, A, A::Mailbox>, A::Mailbox, M, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest::new(
            self,
            msg,
            #[cfg(debug_assertions)]
            std::panic::Location::caller(),
        )
    }

    /// Links two actors as siblings, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// let sibbling_ref = kameo::spawn(MyActor);
    ///
    /// actor_ref.link(&sibbling_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn link<B>(&self, sibbling_ref: &ActorRef<B>)
    where
        B: Actor,
    {
        if self.id == sibbling_ref.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling_ref.links.lock());
        this_links.insert(sibbling_ref.id(), sibbling_ref.weak_signal_mailbox());
        sibbling_links.insert(self.id, self.weak_signal_mailbox());
    }

    /// Unlinks two previously linked sibling actors.
    ///
    /// # Example
    ///
    /// ```
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// let sibbling_ref = kameo::spawn(MyActor);
    ///
    /// actor_ref.link(&sibbling_ref).await;
    /// actor_ref.unlink(&sibbling_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn unlink<B>(&self, sibbling: &ActorRef<B>)
    where
        B: Actor,
    {
        if self.id == sibbling.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling.links.lock());
        this_links.remove(&sibbling.id());
        sibbling_links.remove(&self.id);
    }

    /// Links this actor with a child actor, establishing a parent-child relationship.
    ///
    /// If the parent dies, the child actor will be notified with a "link died" signal.
    ///
    /// # Example
    ///
    /// ```
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// let child_ref = kameo::spawn(MyActor);
    /// actor_ref.link_child(&child_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[deprecated(
        since = "0.13.0",
        note = "child linking is being phased out in favor of bidirectional links – use the `ActorRef::link` method instead"
    )]
    #[inline]
    pub async fn link_child<B>(&self, child: &ActorRef<B>)
    where
        B: Actor,
    {
        if self.id == child.id() {
            return;
        }

        let child_id = child.id();
        let child: Box<dyn SignalMailbox> = child.weak_signal_mailbox();
        self.links.lock().await.insert(child_id, child);
    }

    /// Unlinks a previously linked child actor.
    ///
    /// # Example
    ///
    /// ```
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// let child_ref = kameo::spawn(MyActor);
    /// actor_ref.link_child(&child_ref).await;
    /// actor_ref.unlink_child(&child_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[deprecated(
        since = "0.13.0",
        note = "child linking is being phased out in favor of bidirectional links – use the `ActorRef::unlink` method instead"
    )]
    #[inline]
    pub async fn unlink_child<B>(&self, child: &ActorRef<B>)
    where
        B: Actor,
    {
        if self.id == child.id() {
            return;
        }

        self.links.lock().await.remove(&child.id());
    }

    /// Links two actors as siblings, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// let sibbling_ref = kameo::spawn(MyActor);
    /// actor_ref.link_together(&sibbling_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[deprecated(
        since = "0.13.0",
        note = "this method has been renamed to `ActorRef::link`"
    )]
    #[inline]
    pub async fn link_together<B>(&self, sibbling_ref: &ActorRef<B>)
    where
        B: Actor,
    {
        if self.id == sibbling_ref.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling_ref.links.lock());
        this_links.insert(sibbling_ref.id(), sibbling_ref.weak_signal_mailbox());
        sibbling_links.insert(self.id, self.weak_signal_mailbox());
    }

    /// Unlinks two previously linked sibling actors.
    ///
    /// # Example
    ///
    /// ```
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor);
    /// let sibbling_ref = kameo::spawn(MyActor);
    /// actor_ref.link_together(&sibbling_ref).await;
    /// actor_ref.unlink_together(&sibbling_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[deprecated(
        since = "0.13.0",
        note = "this method has been renamed to `ActorRef::unlink`"
    )]
    #[inline]
    pub async fn unlink_together<B>(&self, sibbling: &ActorRef<B>)
    where
        B: Actor,
    {
        if self.id == sibbling.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling.links.lock());
        this_links.remove(&sibbling.id());
        sibbling_links.remove(&self.id);
    }

    /// Attaches a stream of messages to the actor, forwarding each item in the stream.
    ///
    /// The stream will continue until it is completed or the actor is stopped. A `JoinHandle` is returned,
    /// which can be used to cancel the stream. The `start_value` and `finish_value` can provide additional
    /// context for the stream but are optional.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::message::{Context, Message, StreamMessage};
    ///
    /// #[derive(kameo::Actor)]
    /// struct MyActor;
    ///
    /// impl Message<StreamMessage<u32, (), ()>> for MyActor {
    ///     type Reply = ();
    ///
    ///     async fn handle(&mut self, msg: StreamMessage<u32, (), ()>, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
    ///         match msg {
    ///             StreamMessage::Next(num) => {
    ///                 println!("Received item: {num}");
    ///             }
    ///             StreamMessage::Started(()) => {
    ///                 println!("Stream attached!");
    ///             }
    ///             StreamMessage::Finished(()) => {
    ///                 println!("Stream finished!");
    ///             }
    ///         }
    ///     }
    /// }
    /// #
    /// # tokio_test::block_on(async {
    /// let stream = futures::stream::iter(vec![17, 19, 24]);
    ///
    /// let actor_ref = kameo::spawn(MyActor);
    /// actor_ref.attach_stream(stream, (), ()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn attach_stream<M, S, T, F>(
        &self,
        mut stream: S,
        start_value: T,
        finish_value: F,
    ) -> JoinHandle<Result<S, SendError<StreamMessage<M, T, F>, <A::Reply as Reply>::Error>>>
    where
        A: Message<StreamMessage<M, T, F>>,
        S: Stream<Item = M> + Send + Unpin + 'static,
        M: Send + 'static,
        T: Send + 'static,
        F: Send + 'static,
        for<'a> TellRequest<
            LocalTellRequest<'a, A, A::Mailbox>,
            A::Mailbox,
            StreamMessage<M, T, F>,
            WithoutRequestTimeout,
        >: request::MessageSend<
            Ok = (),
            Error = SendError<StreamMessage<M, T, F>, <A::Reply as Reply>::Error>,
        >,
    {
        let actor_ref = self.clone();
        tokio::spawn(async move {
            actor_ref
                .tell(StreamMessage::Started(start_value))
                .send()
                .await?;

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        match msg {
                            Some(msg) => {
                                actor_ref.tell(StreamMessage::Next(msg)).send().await?;
                            }
                            None => break,
                        }
                    }
                    _ = actor_ref.wait_for_stop() => {
                        return Ok(stream);
                    }
                }
            }

            actor_ref
                .tell(StreamMessage::Finished(finish_value))
                .send()
                .await?;

            Ok(stream)
        })
    }

    #[inline]
    pub(crate) fn mailbox(&self) -> &A::Mailbox {
        &self.mailbox
    }

    #[inline]
    pub(crate) fn weak_signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.mailbox.downgrade())
    }
}

impl<A> ActorRef<A>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Returns the current capacity of the mailbox.
    ///
    /// The capacity goes down when sending a message to the actor.
    /// The capacity goes up when messages are handled by the actor.
    /// This is distinct from [`max_capacity`], which always returns mailbox capacity initially specified when spawning the actor.
    ///
    /// [`max_capacity`]: ActorRef::max_capacity
    pub fn capacity(&self) -> usize {
        self.mailbox.0.capacity()
    }

    /// Returns the maximum buffer capacity of the mailbox.
    ///
    /// The maximum capacity is the buffer capacity initially specified when spawning the actor.
    /// This is distinct from [`capacity`], which returns the current available buffer capacity:
    /// as messages are sent and received, the value returned by [`capacity`] will go up or down,
    /// whereas the value returned by [`max_capacity`] will remain constant.
    ///
    /// [`capacity`]: ActorRef::capacity
    /// [`max_capacity`]: ActorRef::max_capacity
    pub fn max_capacity(&self) -> usize {
        self.mailbox.0.max_capacity()
    }
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_semaphore: self.startup_semaphore.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("ActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(_) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

impl<A: Actor> AsRef<Links> for ActorRef<A> {
    fn as_ref(&self) -> &Links {
        &self.links
    }
}

/// A reference to an actor running remotely.
///
/// `RemoteActorRef` allows sending messages to actors on different nodes in a distributed system.
/// It supports the same messaging patterns as `ActorRef` for local actors, including `ask` and `tell` messaging.
#[cfg(feature = "remote")]
pub struct RemoteActorRef<A: Actor> {
    id: ActorID,
    swarm_tx: remote::SwarmSender,
    phantom: PhantomData<A::Mailbox>,
}

#[cfg(feature = "remote")]
impl<A: Actor> RemoteActorRef<A> {
    pub(crate) fn new(id: ActorID, swarm_tx: remote::SwarmSender) -> Self {
        RemoteActorRef {
            id,
            swarm_tx,
            phantom: PhantomData,
        }
    }

    /// Returns the unique identifier of the remote actor.
    pub fn id(&self) -> ActorID {
        self.id
    }

    /// Looks up an actor registered by name across the distributed network.
    ///
    /// Returns `Some` if the actor is found, or `None` if no actor with the given name is registered.
    pub async fn lookup(name: &str) -> Result<Option<Self>, error::RegistrationError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistrationError::SwarmNotBootstrapped)?
            .lookup(name.to_string())
            .await
    }

    /// Sends a message to the remote actor and waits for a reply.
    ///
    /// The `ask` pattern is used when a response is expected from the remote actor. This method
    /// returns an `AskRequest`, which can be awaited asynchronously, or sent in a blocking manner using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kameo::actor::RemoteActorRef;
    ///
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # #[actor(mailbox = bounded)]
    /// # struct MyActor;
    /// #
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Msg;
    /// #
    /// # #[kameo::remote_message("id")]
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let remote_actor_ref = RemoteActorRef::<MyActor>::lookup("my_actor").await?.unwrap();
    /// # let msg = Msg;
    /// let reply = remote_actor_ref.ask(&msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    pub fn ask<'a, M>(
        &'a self,
        msg: &'a M,
    ) -> AskRequest<
        request::RemoteAskRequest<'a, A, M>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
    where
        A: remote::RemoteActor + Message<M> + remote::RemoteMessage<M>,
        M: serde::Serialize + Send + 'static,
        <A::Reply as Reply>::Ok: for<'de> serde::Deserialize<'de>,
    {
        AskRequest::new_remote(
            self,
            msg,
            #[cfg(debug_assertions)]
            std::panic::Location::caller(),
        )
    }

    /// Sends a message to the remote actor without waiting for a reply.
    ///
    /// The `tell` pattern is used when no response is expected from the remote actor. This method
    /// returns a `TellRequest`, which can be awaited asynchronously, or configured using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kameo::actor::RemoteActorRef;
    ///
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # #[actor(mailbox = bounded)]
    /// # struct MyActor;
    /// #
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Msg;
    /// #
    /// # #[kameo::remote_message("id")]
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: kameo::message::Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let remote_actor_ref = RemoteActorRef::<MyActor>::lookup("my_actor").await?.unwrap();
    /// # let msg = Msg;
    /// remote_actor_ref.tell(&msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    pub fn tell<'a, M>(
        &'a self,
        msg: &'a M,
    ) -> TellRequest<request::RemoteTellRequest<'a, A, M>, A::Mailbox, M, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest::new_remote(
            self,
            msg,
            #[cfg(debug_assertions)]
            std::panic::Location::caller(),
        )
    }

    pub(crate) fn send_to_swarm(&self, msg: remote::SwarmCommand) {
        self.swarm_tx.send(msg)
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> Clone for RemoteActorRef<A> {
    fn clone(&self) -> Self {
        RemoteActorRef {
            id: self.id,
            swarm_tx: self.swarm_tx.clone(),
            phantom: PhantomData,
        }
    }
}

#[cfg(feature = "remote")]
impl<A: Actor> fmt::Debug for RemoteActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("RemoteActorRef");
        d.field("id", &self.id);
        d.finish()
    }
}

/// A actor ref that does not prevent the actor from being stopped.
///
/// If all [`ActorRef`] instances of an actor were dropped and only
/// `WeakActorRef` instances remain, the actor is stopped.
///
/// In order to send messages to an actor, the `WeakActorRef` needs to be upgraded using
/// [`WeakActorRef::upgrade`], which returns `Option<ActorRef>`. It returns `None`
/// if all `ActorRef`s have been dropped, and otherwise it returns an `ActorRef`.
pub struct WeakActorRef<A: Actor> {
    id: ActorID,
    mailbox: <A::Mailbox as Mailbox<A>>::WeakMailbox,
    abort_handle: AbortHandle,
    links: Links,
    startup_notify: Arc<Semaphore>,
}

impl<A: Actor> WeakActorRef<A> {
    /// Returns the actor identifier.
    pub fn id(&self) -> ActorID {
        self.id
    }

    /// Tries to convert a `WeakActorRef` into a [`ActorRef`]. This will return `Some`
    /// if there are other `ActorRef` instances alive, otherwise `None` is returned.
    pub fn upgrade(&self) -> Option<ActorRef<A>> {
        self.mailbox.upgrade().map(|mailbox| ActorRef {
            id: self.id,
            mailbox,
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_semaphore: self.startup_notify.clone(),
        })
    }

    /// Returns the number of [`ActorRef`] handles.
    pub fn strong_count(&self) -> usize {
        self.mailbox.strong_count()
    }

    /// Returns the number of [`WeakActorRef`] handles.
    pub fn weak_count(&self) -> usize {
        self.mailbox.weak_count()
    }
}

impl<A: Actor> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_notify: self.startup_notify.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(_) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

/// A collection of links to other actors that are notified when the actor dies.
///
/// Links are used for parent-child or sibling relationships, allowing actors to observe each other's lifecycle.
#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
pub(crate) struct Links(Arc<Mutex<HashMap<ActorID, Box<dyn SignalMailbox>>>>);

impl ops::Deref for Links {
    type Target = Mutex<HashMap<ActorID, Box<dyn SignalMailbox>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
