use std::{
    cell::Cell,
    cmp,
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    ops,
    sync::Arc,
    time::Duration,
};

use dyn_clone::DynClone;
use futures::{future::BoxFuture, stream::AbortHandle, FutureExt, Stream, StreamExt};
use tokio::{
    sync::{Mutex, SetOnce},
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
    error::{self, HookError, Infallible, PanicError, SendError},
    mailbox::{MailboxSender, Signal, SignalMailbox, WeakMailboxSender},
    message::{Message, StreamMessage},
    reply::ReplyError,
    request::{
        AskRequest, ReplyRecipientAskRequest,
        TellRequest, WithoutRequestTimeout,
    },
    Actor, Reply,
};

use super::id::ActorId;

task_local! {
    pub(crate) static CURRENT_ACTOR_ID: ActorId;
}
thread_local! {
    pub(crate) static CURRENT_THREAD_ACTOR_ID: Cell<Option<ActorId>> = const { Cell::new(None) };
}

/// A reference to an actor, used for sending messages and managing its lifecycle.
///
/// An `ActorRef` allows interaction with an actor through message passing, both for asking (waiting for a reply)
/// and telling (without waiting for a reply). It also provides utilities for managing the actor's state,
/// such as checking if the actor is alive, registering the actor under a name, and stopping the actor gracefully.
pub struct ActorRef<A: Actor> {
    id: ActorId,
    mailbox_sender: MailboxSender<A>,
    abort_handle: AbortHandle,
    pub(crate) links: Links,
    pub(crate) startup_result: Arc<SetOnce<Result<(), PanicError>>>,
    pub(crate) shutdown_result: Arc<SetOnce<Result<(), PanicError>>>,
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
        startup_result: Arc<SetOnce<Result<(), PanicError>>>,
        shutdown_result: Arc<SetOnce<Result<(), PanicError>>>,
    ) -> Self {
        ActorRef {
            id: ActorId::generate(),
            mailbox_sender: mailbox,
            abort_handle,
            links,
            startup_result,
            shutdown_result,
        }
    }

    /// Returns the unique identifier of the actor.
    #[inline]
    pub fn id(&self) -> ActorId {
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
        let was_inserted = crate::registry::ACTOR_REGISTRY
            .lock()
            .unwrap()
            .insert(name, self.clone());
        if !was_inserted {
            Err(error::RegistryError::NameAlreadyRegistered)
        } else {
            Ok(())
        }
    }

    /// Registers the actor under a given name.
    ///
    /// Registration should be done through the transport/registry, not through the actor system itself.
    /// This method is removed in favor of external registration.
    #[cfg(feature = "remote")]
    #[deprecated(since = "0.18.0", note = "Registration should be done through the transport/registry")]
    pub async fn register(&self, _name: &str) -> Result<(), error::RegistryError>
    {
        Err(error::RegistryError::SwarmNotBootstrapped)
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
    {
        // Local lookup should be done through the local registry
        // TODO: Implement local registry lookup
        Err(error::RegistryError::SwarmNotBootstrapped)
    }

    /// Creates a message-specific recipient for this actor.
    ///
    /// This allows creating a more specific reference that hides the concrete
    /// actor type while preserving the ability to send messages via `tell`.
    ///
    /// The recipient maintains the same message handling behavior as the
    /// original actor reference, but with a more focused API.
    ///
    /// For bidirectional communication that supports `ask` requests,
    /// see [`ActorRef::reply_recipient`].
    pub fn recipient<M>(self) -> Recipient<M>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        Recipient::new(self)
    }

    /// Creates a message-specific recipient for this actor with bidirectional communication.
    ///
    /// This allows creating a more specific reference that hides the concrete
    /// actor type while preserving the ability to send messages via both `tell` and `ask`.
    ///
    /// The recipient maintains the same message handling behavior as the
    /// original actor reference, but with a more focused API. The `Ok` and `Err`
    /// types are determined by the message's `Reply` implementation.
    ///
    /// For unidirectional communication that only supports `tell`,
    /// see [`ActorRef::recipient`].
    pub fn reply_recipient<M>(
        self,
    ) -> ReplyRecipient<M, <A::Reply as Reply>::Ok, <A::Reply as Reply>::Error>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        ReplyRecipient::new(self)
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
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
        }
    }

    pub(crate) fn into_downgrade(self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox_sender.downgrade(),
            abort_handle: self.abort_handle,
            links: self.links,
            startup_result: self.startup_result,
            shutdown_result: self.shutdown_result,
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
        self.mailbox_sender
            .send(Signal::Stop)
            .await
            .map_err(|_| SendError::ActorNotRunning(()))
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
    /// If `wait_for_startup` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use kameo::actor::{Actor, ActorRef};
    /// use kameo::error::Infallible;
    /// use tokio::time::sleep;
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = Infallible;
    ///
    ///     async fn on_start(
    ///         state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         sleep(Duration::from_secs(2)).await; // Some io operation
    ///         Ok(state)
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.wait_for_startup().await;
    /// println!("Actor ready to handle messages!");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    pub async fn wait_for_startup(&self) {
        self.startup_result.wait().await;
    }

    /// Waits for the actor to finish startup, returning the startup result with a clone of the error.
    ///
    /// This method ensures the actors on_start lifecycle hook has been fully processed.
    /// If `wait_for_startup_result` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::ParseIntError;
    ///
    /// use kameo::actor::{Actor, ActorRef};
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = ParseIntError;
    ///
    ///     async fn on_start(
    ///         _state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         "invalid int".parse().map(|_: i32| MyActor) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let startup_result = actor_ref.wait_for_startup_result().await;
    /// assert!(startup_result.is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn wait_for_startup_result(&self) -> Result<(), HookError<A::Error>>
    where
        A::Error: Clone,
    {
        match self.startup_result.wait().await {
            Ok(()) => Ok(()),
            Err(err) => Err(err
                .with_downcast_ref(|err: &A::Error| HookError::Error(err.clone()))
                .unwrap_or_else(|| HookError::Panicked(err.clone()))),
        }
    }

    /// Waits for the actor to finish startup, returning the startup result with a clousre containing the error.
    ///
    /// This method ensures the actors on_start lifecycle hook has been fully processed.
    /// If `wait_for_startup_with_result` is called after the actor has already started up, this will return immediately.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::actor::{Actor, ActorRef};
    ///
    /// struct MyActor;
    ///
    /// #[derive(Debug)]
    /// struct NonCloneError;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = NonCloneError;
    ///
    ///     async fn on_start(
    ///         _state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         Err(NonCloneError) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.wait_for_startup_with_result(|res| {
    ///     assert!(res.is_err());
    /// }).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn wait_for_startup_with_result<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Result<(), HookError<&A::Error>>) -> R,
    {
        match self.startup_result.wait().await {
            Ok(()) => f(Ok(())),
            Err(err) => match err.0.lock() {
                Ok(lock) => match lock.downcast_ref() {
                    Some(err) => f(Err(HookError::Error(err))),
                    None => f(Err(HookError::Panicked(err.clone()))),
                },
                Err(poison_err) => match poison_err.get_ref().downcast_ref() {
                    Some(err) => f(Err(HookError::Error(err))),
                    None => f(Err(HookError::Panicked(err.clone()))),
                },
            },
        }
    }

    /// Waits for the actor to finish processing and stop running.
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
    pub async fn wait_for_shutdown(&self) {
        self.mailbox_sender.closed().await
    }

    /// Waits for the actor to finish shutdown, returning the shutdown result with a clone of the error.
    ///
    /// This method ensures the actor's on_stop lifecycle hook has been fully processed.
    /// If `wait_for_shutdown_result` is called after the actor has already shut down, this will return immediately.
    ///
    /// Note: This method does not initiate the stop process; it only waits for the actor to
    /// stop and returns the result. You should signal the actor to stop using [`stop_gracefully`](ActorRef::stop_gracefully) or [`kill`](ActorRef::kill)
    /// before calling this method.
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::ParseIntError;
    ///
    /// use kameo::actor::{Actor, ActorRef, WeakActorRef};
    /// use kameo::error::ActorStopReason;
    ///
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = ParseIntError;
    ///
    ///     async fn on_start(
    ///         state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         Ok(state)
    ///     }
    ///
    ///     async fn on_stop(&mut self, actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), Self::Error> {
    ///         "invalid int".parse().map(|_: i32| ()) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.stop_gracefully().await;
    /// let shutdown_result = actor_ref.wait_for_shutdown_result().await;
    /// assert!(shutdown_result.is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn wait_for_shutdown_result(&self) -> Result<(), HookError<A::Error>>
    where
        A::Error: Clone,
    {
        self.mailbox_sender.closed().await;
        match self.shutdown_result.wait().await {
            Ok(()) => Ok(()),
            Err(err) => Err(err
                .with_downcast_ref(|err: &A::Error| HookError::Error(err.clone()))
                .unwrap_or_else(|| HookError::Panicked(err.clone()))),
        }
    }

    /// Waits for the actor to finish shutdown, returning the shutdown result with a clone of the error.
    ///
    /// This method ensures the actor's on_stop lifecycle hook has been fully processed.
    /// If `wait_for_shutdown_result` is called after the actor has already shut down, this will return immediately.
    ///
    /// Note: This method does not initiate the stop process; it only waits for the actor to
    /// stop and returns the result. You should signal the actor to stop using [`stop_gracefully`](ActorRef::stop_gracefully) or [`kill`](ActorRef::kill)
    /// before calling this method.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::actor::{Actor, ActorRef, WeakActorRef};
    /// use kameo::error::ActorStopReason;
    ///
    /// struct MyActor;
    ///
    /// #[derive(Debug)]
    /// struct NonCloneError;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = NonCloneError;
    ///
    ///     async fn on_start(
    ///         state: Self::Args,
    ///         _actor_ref: ActorRef<Self>,
    ///     ) -> Result<Self, Self::Error> {
    ///         Ok(state)
    ///     }
    ///
    ///     async fn on_stop(&mut self, actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), Self::Error> {
    ///         Err(NonCloneError) // Will always error
    ///     }
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// actor_ref.stop_gracefully().await;
    /// actor_ref.wait_for_shutdown_with_result(|res| {
    ///     assert!(res.is_err());
    /// }).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn wait_for_shutdown_with_result<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Result<(), HookError<&A::Error>>) -> R,
    {
        self.mailbox_sender.closed().await;
        match self.shutdown_result.wait().await {
            Ok(()) => f(Ok(())),
            Err(err) => match err.0.lock() {
                Ok(lock) => match lock.downcast_ref() {
                    Some(err) => f(Err(HookError::Error(err))),
                    None => f(Err(HookError::Panicked(err.clone()))),
                },
                Err(poison_err) => match poison_err.get_ref().downcast_ref() {
                    Some(err) => f(Err(HookError::Error(err))),
                    None => f(Err(HookError::Panicked(err.clone()))),
                },
            },
        }
    }

    /// Sends a message to the actor and waits for a reply.
    ///
    /// The `ask` pattern is used when you expect a response from the actor. This method returns
    /// an `AskRequest`, which can be awaited asynchronously, or sent in a blocking manner using one of the [`request`](crate::request) traits.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::actor::{Actor, ActorRef};
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
    /// let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// let reply = actor_ref.ask(msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    #[doc(alias = "send")]
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
    /// use kameo::actor::{Actor, ActorRef};
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
    /// let actor_ref = MyActor::spawn(MyActor);
    /// # let msg = Msg;
    /// actor_ref.tell(msg).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[inline]
    #[track_caller]
    #[doc(alias = "send_async")]
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
    /// # use kameo::Actor;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibbling_ref = MyActor::spawn(MyActor);
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

        if self.id < sibbling_ref.id {
            let mut this_links = self.links.lock().await;
            let mut sibbling_links = sibbling_ref.links.lock().await;

            this_links.insert(
                sibbling_ref.id,
                Link::Local(sibbling_ref.weak_signal_mailbox()),
            );
            sibbling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        } else {
            let mut sibbling_links = sibbling_ref.links.lock().await;
            let mut this_links = self.links.lock().await;

            this_links.insert(
                sibbling_ref.id,
                Link::Local(sibbling_ref.weak_signal_mailbox()),
            );
            sibbling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        }
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
    ///
    /// # use kameo::Actor;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibbling_ref = MyActor::spawn(MyActor);
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

        if self.id < sibbling_ref.id {
            let mut this_links = self.links.blocking_lock();
            let mut sibbling_links = sibbling_ref.links.blocking_lock();

            this_links.insert(
                sibbling_ref.id,
                Link::Local(sibbling_ref.weak_signal_mailbox()),
            );
            sibbling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        } else {
            let mut sibbling_links = sibbling_ref.links.blocking_lock();
            let mut this_links = self.links.blocking_lock();

            this_links.insert(
                sibbling_ref.id,
                Link::Local(sibbling_ref.weak_signal_mailbox()),
            );
            sibbling_links.insert(self.id, Link::Local(self.weak_signal_mailbox()));
        }
    }


    /// Unlinks two previously linked sibling actors.
    ///
    /// # Example
    ///
    /// ```
    /// # use kameo::Actor;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibbling_ref = MyActor::spawn(MyActor);
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

        if self.id < sibbling_ref.id {
            let mut this_links = self.links.lock().await;
            let mut sibbling_links = sibbling_ref.links.lock().await;

            this_links.remove(&sibbling_ref.id);
            sibbling_links.remove(&self.id);
        } else {
            let mut sibbling_links = sibbling_ref.links.lock().await;
            let mut this_links = self.links.lock().await;

            this_links.remove(&sibbling_ref.id);
            sibbling_links.remove(&self.id);
        }
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
    /// # use std::thread;
    /// #
    /// # use kameo::Actor;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// let actor_ref = MyActor::spawn(MyActor);
    /// let sibbling_ref = MyActor::spawn(MyActor);
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

        if self.id < sibbling_ref.id {
            let mut this_links = self.links.blocking_lock();
            let mut sibbling_links = sibbling_ref.links.blocking_lock();

            this_links.remove(&sibbling_ref.id);
            sibbling_links.remove(&self.id);
        } else {
            let mut sibbling_links = sibbling_ref.links.blocking_lock();
            let mut this_links = self.links.blocking_lock();

            this_links.remove(&sibbling_ref.id);
            sibbling_links.remove(&self.id);
        }
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
    /// let actor_ref = MyActor::spawn(MyActor);
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
    ) -> JoinHandle<Result<S, SendError<StreamMessage<M, T, F>>>>
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
                .await
                .map_err(|_| SendError::ActorStopped)?;

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        match msg {
                            Some(msg) => {
                                actor_ref.tell(StreamMessage::Next(msg)).send().await.map_err(|_| SendError::ActorStopped)?;
                            }
                            None => break,
                        }
                    }
                    _ = actor_ref.wait_for_shutdown() => {
                        return Ok(stream);
                    }
                }
            }

            actor_ref
                .tell(StreamMessage::Finished(finish_value))
                .send()
                .await
                .map_err(|_| SendError::ActorStopped)?;

            Ok(stream)
        })
    }

    /// Returns a reference to the mailbox sender.
    pub fn mailbox_sender(&self) -> &MailboxSender<A> {
        &self.mailbox_sender
    }

    /// Sends a message to the actor without waiting for a reply.
    /// This is an internal method used by TellRequest.
    pub(crate) async fn send_fut<M>(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> Result<(), SendError<M>>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        let signal = Signal::Message {
            message: Box::new(msg),
            actor_ref: self.clone(),
            reply: None,
            sent_within_actor: self.is_current(),
        };

        match &self.mailbox_sender {
            MailboxSender::Bounded(tx) => {
                match mailbox_timeout {
                    Some(timeout) => {
                        match tokio::time::timeout(timeout, tx.send(signal)).await {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(err)) => Err(err.into()),
                            Err(_) => Err(SendError::Timeout(None)),
                        }
                    }
                    None => tx.send(signal).await.map_err(Into::into),
                }
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal).map_err(Into::into)
            }
        }
    }

    /// Tries to send a message to the actor without waiting.
    /// This is an internal method used by TellRequest.
    pub(crate) fn try_send<M>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        let signal = Signal::Message {
            message: Box::new(msg),
            actor_ref: self.clone(),
            reply: None,
            sent_within_actor: self.is_current(),
        };

        match &self.mailbox_sender {
            MailboxSender::Bounded(tx) => {
                tx.try_send(signal).map_err(Into::into)
            }
            MailboxSender::Unbounded(tx) => {
                tx.send(signal).map_err(Into::into)
            }
        }
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
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
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

impl<A: Actor> PartialEq for ActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<A: Actor> Eq for ActorRef<A> {}

impl<A: Actor> PartialOrd for ActorRef<A> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Actor> Ord for ActorRef<A> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<A: Actor> Hash for ActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// A type-erased actor reference for bidirectional communication with a single message type.
///
/// Supports both `tell` and `ask` operations, with response types determined by the
/// message's `Reply` implementation. This provides a focused API while hiding the
/// concrete actor type.
///
/// Created by [`ActorRef::reply_recipient`].
pub struct ReplyRecipient<M: Send + 'static, Ok: Send + 'static, Err: ReplyError = Infallible> {
    pub(crate) handler: Box<dyn ReplyMessageHandler<M, Ok, Err>>,
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> ReplyRecipient<M, Ok, Err> {
    fn new<A, AR>(actor_ref: ActorRef<A>) -> Self
    where
        AR: Reply<Ok = Ok, Error = Err>,
        A: Actor + Message<M, Reply = AR>,
    {
        ReplyRecipient {
            handler: Box::new(actor_ref),
        }
    }

    /// Converts this reply recipient into a regular recipient, losing `ask` capability.
    ///
    /// Returns a [`Recipient<M>`] that only supports `tell` operations. This is useful
    /// when you need to pass the recipient to code that doesn't require bidirectional
    /// communication.
    pub fn erase_reply(self) -> Recipient<M> {
        Recipient {
            handler: self.handler.upcast(),
        }
    }

    /// Returns the unique identifier of the actor.
    #[inline]
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Returns whether the actor is currently alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        self.handler.is_alive()
    }

    /// Converts the `ReplyRecipient` to a [`WeakReplyRecipient`] that does not count
    /// towards RAII semantics, i.e. if all `ActorRef`/`ReplyRecipient` instances of the
    /// actor were dropped and only `WeakActorRef`/`WeakReplyRecipient` instances remain,
    /// the actor is stopped.
    #[must_use = "Downgrade creates a WeakReplyRecipient without destroying the original non-weak recipient."]
    #[inline]
    pub fn downgrade(&self) -> WeakReplyRecipient<M, Ok, Err> {
        self.handler.reply_downgrade()
    }

    /// Returns the number of [`ActorRef`]/[`ReplyRecipient`] handles.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakReplyRecipient`] handles.
    #[inline]
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// See [`ActorRef::is_current`].
    #[inline]
    pub fn is_current(&self) -> bool {
        self.handler.is_current()
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// See [`ActorRef::stop_gracefully`].
    #[inline]
    pub async fn stop_gracefully(&self) -> Result<(), SendError> {
        self.handler.stop_gracefully().await
    }

    /// Kills the actor immediately.
    ///
    /// See [`ActorRef::kill`].
    #[inline]
    pub fn kill(&self) {
        self.handler.kill()
    }

    /// Waits for the actor to finish startup and become ready to process messages.
    ///
    /// See [`ActorRef::wait_for_startup`].
    #[inline]
    pub async fn wait_for_startup(&self) {
        self.handler.wait_for_startup().await
    }

    /// Waits for the actor to finish processing and stop.
    ///
    /// See [`ActorRef::wait_for_shutdown`].
    #[inline]
    pub async fn wait_for_shutdown(&self) {
        self.handler.wait_for_shutdown().await
    }

    /// Sends a message to the actor without waiting for a reply.
    ///
    /// See [`ActorRef::tell`].
    pub async fn tell(&self, msg: M) -> Result<(), SendError> {
        self.handler.tell(msg, None).await.map_err(|_| SendError::ActorStopped)
    }

    /// Sends a message to the actor waits for a reply.
    ///
    /// See [`ActorRef::ask`].
    #[track_caller]
    pub fn ask(&self, msg: M) -> ReplyRecipientAskRequest<'_, M, Ok, Err, WithoutRequestTimeout> {
        ReplyRecipientAskRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Clone for ReplyRecipient<M, Ok, Err> {
    fn clone(&self) -> Self {
        ReplyRecipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> fmt::Debug
    for ReplyRecipient<M, Ok, Err>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("ReplyRecipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialEq
    for ReplyRecipient<M, Ok, Err>
{
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Eq for ReplyRecipient<M, Ok, Err> {}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialOrd
    for ReplyRecipient<M, Ok, Err>
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Ord for ReplyRecipient<M, Ok, Err> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Hash for ReplyRecipient<M, Ok, Err> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
    }
}

/// A type erased actor ref, accepting only a single message type.
///
/// This is returned by [ActorRef::recipient].
pub struct Recipient<M: Send + 'static> {
    pub(crate) handler: Box<dyn MessageHandler<M>>,
}

impl<M: Send + 'static> Recipient<M> {
    fn new<A>(actor_ref: ActorRef<A>) -> Self
    where
        A: Actor + Message<M>,
    {
        Recipient {
            handler: Box::new(actor_ref),
        }
    }

    /// Returns the unique identifier of the actor.
    #[inline]
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Returns whether the actor is currently alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        self.handler.is_alive()
    }

    /// Converts the `Recipient` to a [`WeakRecipient`] that does not count
    /// towards RAII semantics, i.e. if all `ActorRef`/`Recipient` instances of the
    /// actor were dropped and only `WeakActorRef`/`WeakRecipient` instances remain,
    /// the actor is stopped.
    #[must_use = "Downgrade creates a WeakRecipient without destroying the original non-weak recipient."]
    #[inline]
    pub fn downgrade(&self) -> WeakRecipient<M> {
        self.handler.downgrade()
    }

    /// Returns the number of [`ActorRef`]/[`Recipient`] handles.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakRecipient`] handles.
    #[inline]
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }

    /// Returns `true` if the current task is the actor itself.
    ///
    /// See [`ActorRef::is_current`].
    #[inline]
    pub fn is_current(&self) -> bool {
        self.handler.is_current()
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// See [`ActorRef::stop_gracefully`].
    #[inline]
    pub async fn stop_gracefully(&self) -> Result<(), SendError> {
        self.handler.stop_gracefully().await
    }

    /// Kills the actor immediately.
    ///
    /// See [`ActorRef::kill`].
    #[inline]
    pub fn kill(&self) {
        self.handler.kill()
    }

    /// Waits for the actor to finish startup and become ready to process messages.
    ///
    /// See [`ActorRef::wait_for_startup`].
    #[inline]
    pub async fn wait_for_startup(&self) {
        self.handler.wait_for_startup().await
    }

    /// Waits for the actor to finish processing and stop.
    ///
    /// See [`ActorRef::wait_for_shutdown`].
    #[inline]
    pub async fn wait_for_shutdown(&self) {
        self.handler.wait_for_shutdown().await
    }

    /// Sends a message to the actor without waiting for a reply.
    ///
    /// See [`ActorRef::tell`].
    #[track_caller]
    pub fn tell(&self, msg: M) -> crate::request::RecipientTellRequest<'_, M> {
        crate::request::RecipientTellRequest::new(
            self,
            msg,
            #[cfg(all(debug_assertions, feature = "tracing"))]
            std::panic::Location::caller(),
        )
    }
}

impl<M: Send + 'static> Clone for Recipient<M> {
    fn clone(&self) -> Self {
        Recipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static> fmt::Debug for Recipient<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Recipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static> PartialEq for Recipient<M> {
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static> Eq for Recipient<M> {}

impl<M: Send + 'static> PartialOrd for Recipient<M> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static> Ord for Recipient<M> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static> Hash for Recipient<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
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
    id: ActorId,
    mailbox: WeakMailboxSender<A>,
    abort_handle: AbortHandle,
    pub(crate) links: Links,
    pub(crate) startup_result: Arc<SetOnce<Result<(), PanicError>>>,
    pub(crate) shutdown_result: Arc<SetOnce<Result<(), PanicError>>>,
}

impl<A: Actor> WeakActorRef<A> {
    /// Returns the actor identifier.
    pub fn id(&self) -> ActorId {
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
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
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

    #[cfg(feature = "remote")]
    #[inline]
    pub(crate) fn weak_signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.mailbox.clone())
    }
}

impl<A: Actor> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
            startup_result: self.startup_result.clone(),
            shutdown_result: self.shutdown_result.clone(),
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

impl<A: Actor> PartialEq for WeakActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<A: Actor> Eq for WeakActorRef<A> {}

impl<A: Actor> PartialOrd for WeakActorRef<A> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Actor> Ord for WeakActorRef<A> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<A: Actor> Hash for WeakActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// A weak recipient that does not prevent the actor from being stopped.
pub struct WeakRecipient<M: Send + 'static> {
    handler: Box<dyn WeakMessageHandler<M>>,
}

impl<M: Send + 'static> WeakRecipient<M> {
    fn new<A>(weak_actor_ref: WeakActorRef<A>) -> Self
    where
        A: Actor + Message<M>,
    {
        WeakRecipient {
            handler: Box::new(weak_actor_ref),
        }
    }

    /// Returns the actor identifier.
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Tries to convert a `WeakRecipient` into a [`Recipient`]. This will return `Some`
    /// if there are other `ActorRef`/`Recipient` instances alive, otherwise `None` is returned.
    pub fn upgrade(&self) -> Option<Recipient<M>> {
        self.handler.upgrade()
    }

    /// Returns the number of [`ActorRef`]/[`Recipient`] handles.
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakRecipient`] handles.
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }
}

impl<M: Send + 'static> Clone for WeakRecipient<M> {
    fn clone(&self) -> Self {
        WeakRecipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static> fmt::Debug for WeakRecipient<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakRecipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static> PartialEq for WeakRecipient<M> {
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static> Eq for WeakRecipient<M> {}

impl<M: Send + 'static> PartialOrd for WeakRecipient<M> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static> Ord for WeakRecipient<M> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static> Hash for WeakRecipient<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
    }
}

/// A weak recipient that does not prevent the actor from being stopped.
pub struct WeakReplyRecipient<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> {
    handler: Box<dyn WeakReplyMessageHandler<M, Ok, Err>>,
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> WeakReplyRecipient<M, Ok, Err> {
    fn new<A, AR>(weak_actor_ref: WeakActorRef<A>) -> Self
    where
        AR: Reply<Ok = Ok, Error = Err>,
        A: Actor + Message<M, Reply = AR>,
    {
        WeakReplyRecipient {
            handler: Box::new(weak_actor_ref),
        }
    }

    /// Returns the actor identifier.
    pub fn id(&self) -> ActorId {
        self.handler.id()
    }

    /// Tries to convert a `WeakReplyRecipient` into a [`ReplyRecipient`]. This will return `Some`
    /// if there are other `ActorRef`/`ReplyRecipient` instances alive, otherwise `None` is returned.
    pub fn upgrade(&self) -> Option<ReplyRecipient<M, Ok, Err>> {
        self.handler.reply_upgrade()
    }

    /// Returns the number of [`ActorRef`]/[`ReplyRecipient`] handles.
    pub fn strong_count(&self) -> usize {
        self.handler.strong_count()
    }

    /// Returns the number of [`WeakActorRef`]/[`WeakReplyRecipient`] handles.
    pub fn weak_count(&self) -> usize {
        self.handler.weak_count()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Clone
    for WeakReplyRecipient<M, Ok, Err>
{
    fn clone(&self) -> Self {
        WeakReplyRecipient {
            handler: dyn_clone::clone_box(&*self.handler),
        }
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> fmt::Debug
    for WeakReplyRecipient<M, Ok, Err>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakReplyRecipient");
        d.field("id", &self.handler.id());
        d.finish()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialEq
    for WeakReplyRecipient<M, Ok, Err>
{
    fn eq(&self, other: &Self) -> bool {
        self.handler.id() == other.handler.id()
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Eq for WeakReplyRecipient<M, Ok, Err> {}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> PartialOrd
    for WeakReplyRecipient<M, Ok, Err>
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Ord
    for WeakReplyRecipient<M, Ok, Err>
{
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.handler.id().cmp(&other.handler.id())
    }
}

impl<M: Send + 'static, Ok: Send + 'static, Err: ReplyError> Hash
    for WeakReplyRecipient<M, Ok, Err>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.id().hash(state);
    }
}

/// A collection of links to other actors that are notified when the actor dies.
///
/// Links are used for parent-child or sibling relationships, allowing actors to observe each other's lifecycle.
#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
pub(crate) struct Links(Arc<Mutex<HashMap<ActorId, Link>>>);

impl ops::Deref for Links {
    type Target = Mutex<HashMap<ActorId, Link>>;

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

pub(crate) trait MessageHandler<M: Send + 'static>:
    DynClone + Send + Sync + 'static
{
    fn id(&self) -> ActorId;
    fn is_alive(&self) -> bool;
    fn downgrade(&self) -> WeakRecipient<M>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
    fn is_current(&self) -> bool;
    fn stop_gracefully(&self) -> BoxFuture<'_, Result<(), SendError>>;
    fn kill(&self);
    fn wait_for_startup(&self) -> BoxFuture<'_, ()>;
    fn wait_for_shutdown(&self) -> BoxFuture<'_, ()>;

    #[allow(clippy::type_complexity)]
    fn tell(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<(), SendError<M>>>;
    fn try_tell(&self, msg: M) -> Result<(), SendError<M>>;
}

impl<A, M> MessageHandler<M> for ActorRef<A>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    #[inline]
    fn id(&self) -> ActorId {
        self.id
    }

    #[inline]
    fn is_alive(&self) -> bool {
        self.is_alive()
    }

    #[inline]
    fn downgrade(&self) -> WeakRecipient<M> {
        WeakRecipient::new(self.downgrade())
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.weak_count()
    }

    #[inline]
    fn is_current(&self) -> bool {
        self.is_current()
    }

    #[inline]
    fn stop_gracefully(&self) -> BoxFuture<'_, Result<(), SendError>> {
        self.stop_gracefully().boxed()
    }

    #[inline]
    fn kill(&self) {
        self.kill()
    }

    #[inline]
    fn wait_for_startup(&self) -> BoxFuture<'_, ()> {
        self.wait_for_startup().boxed()
    }

    #[inline]
    fn wait_for_shutdown(&self) -> BoxFuture<'_, ()> {
        self.wait_for_shutdown().boxed()
    }

    fn tell(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<(), SendError<M>>> {
        async move {
            self.send_fut(msg, mailbox_timeout).await
        }.boxed()
    }

    fn try_tell(&self, msg: M) -> Result<(), SendError<M>> {
        self.try_send(msg)
    }

}

pub(crate) trait ReplyMessageHandler<M: Send + 'static, Ok: Send + 'static, Err: ReplyError>:
    MessageHandler<M>
{
    #[allow(clippy::type_complexity)]
    fn ask(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>>;
    fn try_ask(&self, msg: M) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>>;
    fn blocking_ask(&self, msg: M) -> Result<Ok, SendError<M, Err>>;

    fn reply_downgrade(&self) -> WeakReplyRecipient<M, Ok, Err>;
    fn upcast(self: Box<Self>) -> Box<dyn MessageHandler<M>>;
}

impl<A, M, AR, Ok, Err> ReplyMessageHandler<M, Ok, Err> for ActorRef<A>
where
    AR: Reply<Ok = Ok, Error = Err>,
    A: Actor + Message<M, Reply = AR>,
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    #[allow(clippy::type_complexity)]
    fn ask(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>> {
        self.ask(msg)
            .mailbox_timeout_opt(mailbox_timeout)
            .send()
            .boxed()
    }
    fn try_ask(&self, msg: M) -> BoxFuture<'_, Result<Ok, SendError<M, Err>>> {
        Box::pin(self.ask(msg).try_send())
    }
    fn blocking_ask(&self, msg: M) -> Result<Ok, SendError<M, Err>> {
        self.ask(msg).blocking_send()
    }

    #[inline]
    fn reply_downgrade(&self) -> WeakReplyRecipient<M, Ok, Err> {
        WeakReplyRecipient::new(self.downgrade())
    }

    fn upcast(self: Box<Self>) -> Box<dyn MessageHandler<M>> {
        self
    }
}

trait WeakMessageHandler<M: Send + 'static>: DynClone + Send + Sync + 'static {
    fn id(&self) -> ActorId;
    fn upgrade(&self) -> Option<Recipient<M>>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

impl<A, M> WeakMessageHandler<M> for WeakActorRef<A>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    #[inline]
    fn id(&self) -> ActorId {
        self.id
    }

    #[inline]
    fn upgrade(&self) -> Option<Recipient<M>> {
        self.upgrade().map(Recipient::new)
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.weak_count()
    }
}

trait WeakReplyMessageHandler<M: Send + 'static, Ok: Send + 'static, Err: ReplyError>:
    WeakMessageHandler<M>
{
    fn reply_upgrade(&self) -> Option<ReplyRecipient<M, Ok, Err>>;
}

impl<A, M, AR, Ok, Err> WeakReplyMessageHandler<M, Ok, Err> for WeakActorRef<A>
where
    AR: Reply<Ok = Ok, Error = Err>,
    A: Actor + Message<M, Reply = AR>,
    M: Send + 'static,
    Ok: Send + 'static,
    Err: ReplyError,
{
    #[inline]
    fn reply_upgrade(&self) -> Option<ReplyRecipient<M, Ok, Err>> {
        self.upgrade().map(ReplyRecipient::new)
    }
}
