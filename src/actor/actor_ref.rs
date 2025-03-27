use std::{
    cell::Cell,
    collections::HashMap,
    fmt, ops,
    sync::{Arc, OnceLock},
};

use futures::{stream::AbortHandle, Stream, StreamExt};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
    task_local,
};

#[cfg(feature = "remote")]
use std::marker::PhantomData;

#[cfg(feature = "remote")]
use crate::remote;
#[cfg(feature = "remote")]
use crate::request;

use crate::{
    error::{self, PanicError, SendError},
    mailbox::{MailboxSender, Signal, SignalMailbox, WeakMailboxSender},
    message::{Message, StreamMessage},
    reply::Reply,
    request::{AskRequest, TellRequest, WithoutRequestTimeout},
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
    mailbox_sender: MailboxSender<A>,
    abort_handle: AbortHandle,
    pub(crate) links: Links,
    pub(crate) startup_semaphore: Arc<Semaphore>,
    pub(crate) startup_error: Arc<OnceLock<Option<PanicError>>>,
}

impl<A> ActorRef<A>
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new(
        mailbox: MailboxSender<A>,
        abort_handle: AbortHandle,
        links: Links,
        startup_semaphore: Arc<Semaphore>,
        startup_error: Arc<OnceLock<Option<PanicError>>>,
    ) -> Self {
        ActorRef {
            id: ActorID::generate(),
            mailbox_sender: mailbox,
            abort_handle,
            links,
            startup_semaphore,
            startup_error,
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
        !self.mailbox_sender.is_closed()
    }

    /// Registers the actor under a given name in the actor registry.
    ///
    /// This makes the actor discoverable by parts of the app by name.
    #[cfg(not(feature = "remote"))]
    pub fn register(
        &self,
        name: impl Into<std::borrow::Cow<'static, str>>,
    ) -> Result<(), error::RegistryError> {
        crate::registry::ACTOR_REGISTRY
            .lock()
            .unwrap()
            .insert(name, self.clone());
        Ok(())
    }

    /// Registers the actor under a given name within the actor swarm.
    ///
    /// This makes the actor discoverable by other nodes in the distributed system.
    #[cfg(feature = "remote")]
    pub async fn register(&self, name: &str) -> Result<(), error::RegistryError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistryError::SwarmNotBootstrapped)?
            .register(self.clone(), name.to_string())
            .await
    }

    /// Looks up an actor registered locally by its name.
    ///
    /// Returns `Some` if the actor exists, or `None` if no actor with the given name is registered.
    #[cfg(not(feature = "remote"))]
    pub fn lookup<Q>(name: &Q) -> Result<Option<Self>, error::RegistryError>
    where
        Q: std::hash::Hash + Eq + ?Sized,
        std::borrow::Cow<'static, str>: std::borrow::Borrow<Q>,
    {
        crate::registry::ACTOR_REGISTRY.lock().unwrap().get(name)
    }

    /// Looks up an actor registered locally by its name.
    ///
    /// Returns `Some` if the actor exists, or `None` if no actor with the given name is registered.
    #[cfg(feature = "remote")]
    pub async fn lookup(name: &str) -> Result<Option<Self>, error::RegistryError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistryError::SwarmNotBootstrapped)?
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
            mailbox: self.mailbox_sender.downgrade(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_notify: self.startup_semaphore.clone(),
            startup_error: self.startup_error.clone(),
        }
    }

    /// Returns the number of [`ActorRef`] handles.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.mailbox_sender.strong_count()
    }

    /// Returns the number of [`WeakActorRef`] handles.
    #[inline]
    pub fn weak_count(&self) -> usize {
        self.mailbox_sender.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// This is useful when checking if certain code is being executed from within the actor's own context.
    #[inline]
    pub fn is_current(&self) -> bool {
        CURRENT_ACTOR_ID
            .try_with(Clone::clone)
            .map(|current_actor_id| current_actor_id == self.id)
            .unwrap_or(false)
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// This method ensures that the actor finishes processing any messages that were already in the queue
    /// before it shuts down. Any new messages sent after the stop signal will be ignored.
    #[inline]
    pub async fn stop_gracefully(&self) -> Result<(), SendError> {
        Ok(self.mailbox_sender.send(Signal::Stop).await?)
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
    /// use kameo::error::Infallible;
    /// use kameo::mailbox;
    /// use tokio::time::sleep;
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Error = Infallible;
    ///
    ///     async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), Self::Error> {
    ///         sleep(Duration::from_secs(2)).await; // Some io operation
    ///         Ok(())
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// actor_ref.wait_startup().await;
    /// println!("Actor ready to handle messages!");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn wait_startup(&self) {
        let _ = self.startup_semaphore.acquire().await;
    }

    /// Waits for the actor to finish startup, returning the startup result with a clone of the error.
    ///
    /// This method ensures the actors on_start lifecycle hook has been fully processed.
    /// If `wait_startup_result` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::ParseIntError;
    /// use std::time::Duration;
    ///
    /// use kameo::actor::{Actor, ActorRef};
    /// use kameo::mailbox;
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Error = ParseIntError;
    ///
    ///     async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), Self::Error> {
    ///         "invalid int".parse().map(|_: i32| ()) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// let startup_result = actor_ref.wait_startup_result().await;
    /// assert!(startup_result.is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn wait_startup_result(&self) -> Result<(), A::Error>
    where
        A::Error: Clone,
    {
        let _ = self.startup_semaphore.acquire().await;
        match self
            .startup_error
            .get()
            .expect("startup error should be set")
        {
            Some(err) => Err(err
                .with_downcast_ref(|err: &A::Error| err.clone())
                .expect("panic error type should be the Actor's error type")),
            None => Ok(()),
        }
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
        self.mailbox_sender.closed().await
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
    /// use kameo::mailbox;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
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
    ) -> AskRequest<'_, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        AskRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
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
    /// use kameo::mailbox;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # struct Msg;
    /// #
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// # let msg = Msg;
    /// actor_ref.tell(msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    pub fn tell<M>(&self, msg: M) -> TellRequest<'_, A, M, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }

    /// Links two actors as siblings, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::mailbox;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// let sibbling_ref = kameo::spawn(MyActor, mailbox::unbounded());
    ///
    /// actor_ref.link(&sibbling_ref).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn link<B: Actor>(&self, sibbling_ref: &ActorRef<B>) {
        if self.id == sibbling_ref.id {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling_ref.links.lock());
        this_links.insert(
            sibbling_ref.id,
            Link::Local(sibbling_ref.weak_signal_mailbox()),
        );
        sibbling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
    }

    /// Blockingly links two actors as siblings, ensuring they notify each other if either one dies.
    ///
    /// This method is intended for use cases where you need to link actors in synchronous code.
    /// For async contexts, [`link`] is preferred.
    ///
    /// # Example
    ///
    /// ```
    /// use std::thread;
    /// use kameo::mailbox;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// let sibbling_ref = kameo::spawn(MyActor, mailbox::unbounded());
    ///
    /// thread::spawn(move || {
    ///     actor_ref.blocking_link(&sibbling_ref);
    /// });
    /// # });
    /// ```
    ///
    /// [`link`]: ActorRef::link
    #[inline]
    pub fn blocking_link<B: Actor>(&self, sibbling_ref: &ActorRef<B>) {
        if self.id == sibbling_ref.id {
            return;
        }

        let mut this_links = self.links.blocking_lock();
        let mut sibbling_links = sibbling_ref.links.blocking_lock();
        this_links.insert(
            sibbling_ref.id,
            Link::Local(sibbling_ref.weak_signal_mailbox()),
        );
        sibbling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
    }

    /// Links the local actor with a remote actor, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kameo::actor::RemoteActorRef;
    /// use kameo::mailbox;
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct MyActor;
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct OtherActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// let sibbling_ref = RemoteActorRef::<OtherActor>::lookup("other_actor").await?.unwrap();
    ///
    /// actor_ref.link_remote(&sibbling_ref).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "remote")]
    pub async fn link_remote<B>(
        &self,
        sibbling_ref: &RemoteActorRef<B>,
    ) -> Result<(), error::RemoteSendError<error::Infallible>>
    where
        A: remote::RemoteActor,
        B: Actor + remote::RemoteActor,
    {
        if self.id == sibbling_ref.id {
            return Ok(());
        }

        remote::REMOTE_REGISTRY.lock().await.insert(
            self.id,
            remote::RemoteRegistryActorRef {
                actor_ref: Box::new(self.clone()),
                signal_mailbox: self.weak_signal_mailbox(),
                links: self.links.clone(),
            },
        );

        self.links.lock().await.insert(
            sibbling_ref.id,
            Link::Remote(std::borrow::Cow::Borrowed(B::REMOTE_ID)),
        );
        remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .link::<A, B>(self.id, sibbling_ref.id)
            .await
    }

    /// Unlinks two previously linked sibling actors.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::mailbox;
    ///
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// let sibbling_ref = kameo::spawn(MyActor, mailbox::unbounded());
    ///
    /// actor_ref.link(&sibbling_ref).await;
    /// actor_ref.unlink(&sibbling_ref).await;
    /// # });
    /// ```
    #[inline]
    pub async fn unlink<B: Actor>(&self, sibbling_ref: &ActorRef<B>) {
        if self.id == sibbling_ref.id {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling_ref.links.lock());
        this_links.remove(&sibbling_ref.id);
        sibbling_links.remove(&self.id);
    }

    /// Blockingly unlinks two previously linked sibling actors.
    ///
    /// This method is intended for use cases where you need to link actors in synchronous code.
    /// For async contexts, [`unlink`] is preferred.
    ///
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::mailbox;
    ///
    /// # use std::thread;
    /// #
    /// # #[derive(kameo::Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// let sibbling_ref = kameo::spawn(MyActor, mailbox::unbounded());
    ///
    /// thread::spawn(move || {
    ///     actor_ref.blocking_link(&sibbling_ref);
    ///     actor_ref.blocking_unlink(&sibbling_ref);
    /// });
    /// # });
    /// ```
    ///
    /// [`unlink`]: ActorRef::unlink
    #[inline]
    pub fn blocking_unlink<B: Actor>(&self, sibbling_ref: &ActorRef<B>) {
        if self.id == sibbling_ref.id {
            return;
        }

        let mut this_links = self.links.blocking_lock();
        let mut sibbling_links = sibbling_ref.links.blocking_lock();
        this_links.remove(&sibbling_ref.id);
        sibbling_links.remove(&self.id);
    }

    /// Unlinks the local actor with a previously linked remote actor.
    ///
    /// # Example
    ///
    /// ```
    /// # use kameo::actor::RemoteActorRef;
    /// use kameo::mailbox;
    ///
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct MyActor;
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct OtherActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    /// let sibbling_ref = RemoteActorRef::<OtherActor>::lookup("other_actor").await?.unwrap();
    ///
    /// actor_ref.unlink_remote(&sibbling_ref).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "remote")]
    pub async fn unlink_remote<B>(
        &self,
        sibbling_ref: &RemoteActorRef<B>,
    ) -> Result<(), error::RemoteSendError<error::Infallible>>
    where
        A: remote::RemoteActor,
        B: Actor + remote::RemoteActor,
    {
        if self.id == sibbling_ref.id {
            return Ok(());
        }

        self.links.lock().await.remove(&sibbling_ref.id);
        remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .unlink::<B>(self.id, sibbling_ref.id)
            .await
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
    /// use kameo::mailbox;
    /// use kameo::message::{Context, Message, StreamMessage};
    ///
    /// #[derive(kameo::Actor)]
    /// struct MyActor;
    ///
    /// impl Message<StreamMessage<u32, (), ()>> for MyActor {
    ///     type Reply = ();
    ///
    ///     async fn handle(&mut self, msg: StreamMessage<u32, (), ()>, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
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
    /// let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
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

    /// Returns a reference to the mailbox sender.
    pub fn mailbox_sender(&self) -> &MailboxSender<A> {
        &self.mailbox_sender
    }

    #[inline]
    pub(crate) fn weak_signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.mailbox_sender.downgrade())
    }
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id,
            mailbox_sender: self.mailbox_sender.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_semaphore: self.startup_semaphore.clone(),
            startup_error: self.startup_error.clone(),
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

/// A reference to an actor running remotely.
///
/// `RemoteActorRef` allows sending messages to actors on different nodes in a distributed system.
/// It supports the same messaging patterns as `ActorRef` for local actors, including `ask` and `tell` messaging.
#[cfg(feature = "remote")]
pub struct RemoteActorRef<A: Actor> {
    id: ActorID,
    swarm_tx: remote::SwarmSender,
    phantom: PhantomData<fn(&mut A)>,
}

#[cfg(feature = "remote")]
impl<A> RemoteActorRef<A>
where
    A: Actor + remote::RemoteActor,
{
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
    pub async fn lookup(name: &str) -> Result<Option<Self>, error::RegistryError>
    where
        A: remote::RemoteActor + 'static,
    {
        remote::ActorSwarm::get()
            .ok_or(error::RegistryError::SwarmNotBootstrapped)?
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
    /// # struct MyActor;
    /// #
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Msg;
    /// #
    /// # #[kameo::remote_message("id")]
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply { }
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
    ) -> request::RemoteAskRequest<'a, A, M, WithoutRequestTimeout, WithoutRequestTimeout>
    where
        A: remote::RemoteActor + Message<M> + remote::RemoteMessage<M>,
        M: serde::Serialize + Send + 'static,
    {
        request::RemoteAskRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
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
    /// # struct MyActor;
    /// #
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Msg;
    /// #
    /// # #[kameo::remote_message("id")]
    /// # impl kameo::message::Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: &mut kameo::message::Context<Self, Self::Reply>) -> Self::Reply { }
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
    ) -> request::RemoteTellRequest<'a, A, M, WithoutRequestTimeout>
    where
        A: Message<M> + remote::RemoteMessage<M>,
        M: Send + 'static,
    {
        request::RemoteTellRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }

    /// Links two remote actors, ensuring they notify each other if either one dies.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kameo::actor::RemoteActorRef;
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct ActorA;
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct ActorB;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_a = RemoteActorRef::<ActorA>::lookup("actor_a").await?.unwrap();
    /// let actor_b = RemoteActorRef::<ActorB>::lookup("actor_b").await?.unwrap();
    ///
    /// actor_a.unlink_remote(&actor_b).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn link_remote<B>(
        &self,
        sibbling_ref: &RemoteActorRef<B>,
    ) -> Result<(), error::RemoteSendError<error::Infallible>>
    where
        A: remote::RemoteActor,
        B: Actor + remote::RemoteActor,
    {
        if self.id == sibbling_ref.id {
            return Ok(());
        }

        let fut_a = remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .link::<A, B>(self.id, sibbling_ref.id);
        let fut_b = remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .link::<B, A>(sibbling_ref.id, self.id);

        tokio::try_join!(fut_a, fut_b)?;

        Ok(())
    }

    /// Unlinks two previously linked remote actors.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kameo::actor::RemoteActorRef;
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct ActorA;
    /// #
    /// # #[derive(kameo::Actor, kameo::RemoteActor)]
    /// # struct ActorB;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_a = RemoteActorRef::<ActorA>::lookup("actor_a").await?.unwrap();
    /// let actor_b = RemoteActorRef::<ActorB>::lookup("actor_b").await?.unwrap();
    ///
    /// actor_a.unlink_remote(&actor_b).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn unlink_remote<B>(
        &self,
        sibbling_ref: &RemoteActorRef<B>,
    ) -> Result<(), error::RemoteSendError<error::Infallible>>
    where
        A: remote::RemoteActor,
        B: Actor + remote::RemoteActor,
    {
        if self.id == sibbling_ref.id {
            return Ok(());
        }

        let fut_a = remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .unlink::<B>(self.id, sibbling_ref.id);
        let fut_b = remote::ActorSwarm::get()
            .ok_or(error::RemoteSendError::SwarmNotBootstrapped)?
            .unlink::<A>(sibbling_ref.id, self.id);

        tokio::try_join!(fut_a, fut_b)?;

        Ok(())
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
    mailbox: WeakMailboxSender<A>,
    abort_handle: AbortHandle,
    pub(crate) links: Links,
    startup_notify: Arc<Semaphore>,
    startup_error: Arc<OnceLock<Option<PanicError>>>,
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
            mailbox_sender: mailbox,
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_semaphore: self.startup_notify.clone(),
            startup_error: self.startup_error.clone(),
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
            startup_error: self.startup_error.clone(),
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
pub(crate) struct Links(Arc<Mutex<HashMap<ActorID, Link>>>);

impl ops::Deref for Links {
    type Target = Mutex<HashMap<ActorID, Link>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub(crate) enum Link {
    Local(Box<dyn SignalMailbox>),
    #[cfg(feature = "remote")]
    Remote(std::borrow::Cow<'static, str>),
}
