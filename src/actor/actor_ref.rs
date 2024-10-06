use std::{cell::Cell, collections::HashMap, fmt, marker::PhantomData, ops, sync::Arc};

use futures::{stream::AbortHandle, Stream, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{sync::Mutex, task::JoinHandle, task_local};

use crate::{
    error::{RegistrationError, SendError},
    mailbox::{Mailbox, SignalMailbox, WeakMailbox},
    message::{Message, StreamMessage},
    remote::{ActorSwarm, RemoteActor, RemoteMessage, SwarmCommand, SwarmSender},
    reply::Reply,
    request::{
        AskRequest, LocalAskRequest, LocalTellRequest, MessageSend, RemoteAskRequest,
        RemoteTellRequest, TellRequest, WithoutRequestTimeout,
    },
    Actor,
};

use super::id::ActorID;

task_local! {
    pub(crate) static CURRENT_ACTOR_ID: ActorID;
}
thread_local! {
    pub(crate) static CURRENT_THREAD_ACTOR_ID: Cell<Option<ActorID>> = Cell::new(None);
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
    links: Links,
}

impl<A> ActorRef<A>
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new(mailbox: A::Mailbox, abort_handle: AbortHandle, links: Links) -> Self {
        ActorRef {
            id: ActorID::generate(),
            mailbox,
            abort_handle,
            links,
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
    pub async fn register(&self, name: &str) -> Result<(), RegistrationError>
    where
        A: RemoteActor + 'static,
    {
        ActorSwarm::get()
            .ok_or(RegistrationError::SwarmNotBootstrapped)?
            .register(self.clone(), name.to_string())
            .await
    }

    /// Looks up an actor registered locally by its name.
    ///
    /// Returns `Some` if the actor exists, or `None` if no actor with the given name is registered.
    pub async fn lookup(name: &str) -> Result<Option<Self>, RegistrationError>
    where
        A: RemoteActor + 'static,
    {
        ActorSwarm::get()
            .ok_or(RegistrationError::SwarmNotBootstrapped)?
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
    pub async fn stop_gracefully(&self) -> Result<(), SendError> {
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
    /// use kameo::request::MessageSend;
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
    /// let reply = actor_ref.ask(msg).send().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
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
        AskRequest::new(self, msg)
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
    /// use kameo::request::MessageSend;
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
    /// actor_ref.tell(msg).send().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub fn tell<M>(
        &self,
        msg: M,
    ) -> TellRequest<LocalTellRequest<'_, A, A::Mailbox>, A::Mailbox, M, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest::new(self, msg)
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
        >: MessageSend<
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

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
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
pub struct RemoteActorRef<A: Actor> {
    id: ActorID,
    swarm_tx: SwarmSender,
    phantom: PhantomData<A::Mailbox>,
}

impl<A: Actor> RemoteActorRef<A> {
    pub(crate) fn new(id: ActorID, swarm_tx: SwarmSender) -> Self {
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
    pub async fn lookup(name: &str) -> Result<Option<Self>, RegistrationError>
    where
        A: RemoteActor + 'static,
    {
        ActorSwarm::get()
            .ok_or(RegistrationError::SwarmNotBootstrapped)?
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
    /// use kameo::request::MessageSend;
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
    /// let reply = remote_actor_ref.ask(&msg).send().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub fn ask<'a, M>(
        &'a self,
        msg: &'a M,
    ) -> AskRequest<
        RemoteAskRequest<'a, A, M>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
    where
        A: RemoteActor + Message<M> + RemoteMessage<M>,
        M: Serialize,
        <A::Reply as Reply>::Ok: DeserializeOwned,
    {
        AskRequest::new_remote(self, msg)
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
    /// use kameo::request::MessageSend;
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
    /// remote_actor_ref.tell(&msg).send().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub fn tell<'a, M>(
        &'a self,
        msg: &'a M,
    ) -> TellRequest<RemoteTellRequest<'a, A, M>, A::Mailbox, M, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest::new_remote(self, msg)
    }

    pub(crate) fn send_to_swarm(&self, msg: SwarmCommand) {
        self.swarm_tx.send(msg)
    }
}

impl<A: Actor> Clone for RemoteActorRef<A> {
    fn clone(&self) -> Self {
        RemoteActorRef {
            id: self.id.clone(),
            swarm_tx: self.swarm_tx.clone(),
            phantom: PhantomData,
        }
    }
}

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
}

impl<A: Actor> WeakActorRef<A> {
    /// Returns the actor identifier.
    pub fn id(&self) -> ActorID {
        self.id
    }

    /// Tries to convert a `WeakActorRef` into a [`ActorRef`]. This will return `Some`
    /// if there are other `ActorRef` instances alive, otherwise `None` is returned.
    pub fn upgrade(&self) -> Option<ActorRef<A>> {
        self.mailbox.upgrade().and_then(|mailbox| {
            Some(ActorRef {
                id: self.id,
                mailbox,
                abort_handle: self.abort_handle.clone(),
                links: self.links.clone(),
            })
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
