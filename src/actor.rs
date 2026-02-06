//! Core functionality for defining and managing actors in Kameo.
//!
//! Actors are independent units of computation that run asynchronously, sending and receiving messages.
//! Each actor operates within its own task, and its lifecycle is managed by hooks such as `on_start`, `on_panic`, and `on_stop`.
//!
//! The actor trait is designed to support fault tolerance, recovery from panics, and clean termination.
//! Lifecycle hooks allow customization of actor behavior when starting, encountering errors, or shutting down.
//!
//! This module contains the primary `Actor` trait, which all actors must implement,
//! as well as types for managing message queues (mailboxes) and actor references ([`ActorRef`]).
//!
//! # Features
//! - **Asynchronous Message Handling**: Each actor processes messages asynchronously within its own task.
//! - **Lifecycle Hooks**: Customizable hooks ([`on_start`], [`on_stop`], [`on_panic`]) for managing the actor's lifecycle.
//! - **Backpressure**: Mailboxes can be bounded or unbounded, controlling the flow of messages.
//! - **Supervision**: Actors can be linked, enabling robust supervision and error recovery systems.
//!
//! This module allows building resilient, fault-tolerant, distributed systems with flexible control over the actor lifecycle.
//!
//! [`on_start`]: Actor::on_start
//! [`on_stop`]: Actor::on_stop
//! [`on_panic`]: Actor::on_panic

mod actor_ref;
mod id;
mod kind;
mod maybe_distributed;
mod spawn;

use std::{any, ops::ControlFlow};

use futures::Future;

use crate::{
    error::{ActorStopReason, PanicError},
    mailbox::{self, MailboxReceiver, MailboxSender, Signal},
    message::BoxMessage,
    reply::{BoxReplySender, ReplyError},
};

pub use actor_ref::*;
pub use id::*;
pub use spawn::*;

const DEFAULT_MAILBOX_CAPACITY: usize = 64;

/// Core behavior of an actor, including its lifecycle events and how it processes messages.
///
/// Every actor must implement this trait, which provides hooks
/// for the actor's initialization ([`on_start`]), handling errors ([`on_panic`]), and cleanup ([`on_stop`]).
///
/// The actor runs within its own task and processes messages asynchronously from a mailbox.
/// Each actor can be linked to others, allowing for robust supervision and failure recovery mechanisms.
///
/// Methods in this trait that return [`Self::Error`] will cause the actor to stop with the reason
/// [`ActorStopReason::Panicked`] if an error occurs. This enables graceful handling of actor panics
/// or errors.
///
/// # Example with Derive
///
/// ```
/// use kameo::Actor;
///
/// #[derive(Actor)]
/// struct MyActor;
/// ```
///
/// # Example Override Behaviour
///
/// ```
/// use kameo::actor::{Actor, ActorRef, WeakActorRef};
/// use kameo::error::{ActorStopReason, Infallible};
///
/// struct MyActor;
///
/// impl Actor for MyActor {
///     type Args = Self;
///     type Error = Infallible;
///
///     async fn on_start(
///         state: Self::Args,
///         actor_ref: ActorRef<Self>
///     ) -> Result<Self, Self::Error> {
///         println!("actor started");
///         Ok(state)
///     }
///
///     async fn on_stop(
///         &mut self,
///         actor_ref: WeakActorRef<Self>,
///         reason: ActorStopReason,
///     ) -> Result<(), Self::Error> {
///         println!("actor stopped");
///         Ok(())
///     }
/// }
/// ```
///
/// # Lifecycle Hooks
/// - [`on_start`]: Called when the actor starts. This is where initialization happens.
/// - [`on_panic`]: Called when the actor encounters a panic or an error while processing a "tell" message.
/// - [`on_stop`]: Called before the actor is stopped. This allows for cleanup tasks.
/// - [`on_link_died`]: Hook that is invoked when a linked actor dies.
///
/// # Mailboxes
/// Actors use a mailbox to queue incoming messages. You can choose between:
/// - **Bounded Mailbox**: Limits the number of messages that can be queued, providing backpressure.
/// - **Unbounded Mailbox**: Allows an infinite number of messages, but can lead to high memory usage.
///
/// Mailboxes enable efficient asynchronous message passing with support for both backpressure and
/// unbounded queueing depending on system requirements.
///
/// [`on_start`]: Actor::on_start
/// [`on_panic`]: Actor::on_panic
/// [`on_stop`]: Actor::on_stop
/// [`on_link_died`]: Actor::on_link_died
pub trait Actor: Sized + Send + 'static {
    /// Arguments to initialize the actor.
    ///
    /// Its common for `Args = Self`, allowing the actors state to be passed directly,
    /// however for more complex use cases, the args can be any other type.
    type Args: Send;

    /// Actor error type.
    ///
    /// This error is used as the error returned by lifecycle hooks in this actor.
    type Error: ReplyError;

    /// The name of the actor, which can be useful for logging or debugging.
    ///
    /// # Default Implementation
    /// By default, this returns the type name of the actor.
    #[inline]
    fn name() -> &'static str {
        any::type_name::<Self>()
    }

    /// Called when the actor starts, before it processes any messages.
    ///
    /// Messages sent internally by the actor during `on_start` are prioritized and processed
    /// before any externally sent messages, even if external messages are received first.
    ///
    /// This ensures that the actor can properly initialize before handling external messages.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use kameo::actor::{Actor, ActorRef};
    /// # use kameo::error::Infallible;
    /// #
    /// struct MyActor;
    ///
    /// impl Actor for MyActor {
    ///     type Args = Self;
    ///     type Error = Infallible;
    ///
    ///     async fn on_start(
    ///         state: Self::Args,
    ///         _actor_ref: ActorRef<Self>
    ///     ) -> Result<Self, Self::Error> {
    ///         Ok(state)
    ///     }
    /// }
    /// ```
    #[allow(unused_variables)]
    fn on_start(
        args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Called when the actor receives a message to be processed.
    ///
    /// By default, this method handles the incoming message immediately using the
    /// actor's standard message handling logic.
    ///
    /// Advanced use cases can override this method to customize how messages are processed,
    /// such as buffering messages for later processing or implementing custom scheduling.
    ///
    /// # Parameters
    /// - `msg`: The incoming message, wrapped in a `BoxMessage<Self>`.
    /// - `actor_ref`: A reference to the actor itself.
    /// - `tx`: An optional reply sender, used when the message expects a response.
    /// - `stop`: A mutable bool which can be set to true, stopping the actor immediately after processing this message.
    ///
    /// # Returns
    /// A future that resolves to `Result<(), Box<dyn ReplyError>>`. An `Ok(())` indicates successful processing,
    /// while an `Err` indicates an error occurred during message handling.
    ///
    /// # Default Implementation
    /// The default implementation handles the message immediately by calling
    /// `msg.handle_dyn(self, actor_ref, tx).await`.
    ///
    /// # Notes
    /// - Overriding this method allows you to intercept and manipulate messages before they are processed.
    /// - Be cautious when buffering messages, as unbounded buffering can lead to increased memory usage.
    /// - Custom implementations should ensure that messages are eventually handled or appropriately discarded to
    ///   prevent message loss.
    /// - The `tx` (reply sender) is tied to the specific `BoxMessage` it corresponds to,
    ///   and passing an incorrect or mismatched `tx` can lead to a panic.
    /// - The `stop` variable can be set to true in a message handler, by calling `Context::stop`.
    fn on_message(
        &mut self,
        msg: BoxMessage<Self>,
        actor_ref: ActorRef<Self>,
        tx: Option<BoxReplySender>,
        stop: &mut bool,
    ) -> impl Future<Output = Result<(), Box<dyn ReplyError>>> + Send {
        async move { msg.handle_dyn(self, actor_ref, tx, stop).await }
    }

    /// Called when the actor encounters a panic or an error during "tell" message handling.
    ///
    /// This method gives the actor an opportunity to clean up or reset its state and determine
    /// whether it should be stopped or continue processing messages.
    ///
    /// The `PanicError` parameter holds the error that occurred during.
    /// This error can typically be downcasted into one of a few types:
    /// - [`Self::Error`] type, which is the error type when one of the actor's lifecycle functions returns an error.
    /// - An error type returned when handling a message.
    /// - A string type, which can be accessed with `PanicError::with_str`.
    ///   String types are the result of regular panics, typically raised using the [`std::panic!`] macro.
    /// - Any other panic types. Typically uncommon, though possible with [`std::panic::panic_any`].
    ///
    /// # Returns
    /// Whether the actor should stop or continue processing messages.
    #[allow(unused_variables)]
    #[inline]
    fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        async move { Ok(ControlFlow::Break(ActorStopReason::Panicked(err))) }
    }

    /// Called when a linked actor dies.
    ///
    /// By default, the actor will stop if the reason for the linked actor's death is anything other
    /// than `Normal`. You can customize this behavior in the implementation.
    ///
    /// # Returns
    /// Whether the actor should stop or continue processing messages.
    #[allow(unused_variables)]
    #[inline]
    fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorId,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        async move {
            match &reason {
                ActorStopReason::Normal => Ok(ControlFlow::Continue(())),
                ActorStopReason::Killed
                | ActorStopReason::Panicked(_)
                | ActorStopReason::LinkDied { .. } => {
                    Ok(ControlFlow::Break(ActorStopReason::LinkDied {
                        id,
                        reason: Box::new(reason),
                    }))
                }
                #[cfg(feature = "remote")]
                ActorStopReason::PeerDisconnected => {
                    Ok(ControlFlow::Break(ActorStopReason::PeerDisconnected))
                }
            }
        }
    }

    /// Called when a link to an actor is established.
    ///
    /// By default, this hook does nothing and the actor continues processing messages.
    ///
    /// # Returns
    /// Whether the actor should stop or continue processing messages.
    #[allow(unused_variables)]
    #[inline]
    fn on_link_established(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorId,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        async move { Ok(ControlFlow::Continue(())) }
    }

    /// Called before the actor stops.
    ///
    /// This allows the actor to perform any necessary cleanup or release resources before being fully stopped.
    ///
    /// The error returned by this method will be unwraped by kameo, causing a panic in the tokio task or
    /// thread running the actor.
    #[allow(unused_variables)]
    #[inline]
    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Awaits the next signal typically from the mailbox.
    ///
    /// This can be overwritten for more advanced actor behaviour, such as awaiting multiple channels, etc.
    /// The return value is a signal which will be handled by the actor.
    #[allow(unused_variables)]
    #[inline]
    fn next(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> impl Future<Output = Option<Signal<Self>>> + Send {
        mailbox_rx.recv()
    }

    /// Spawns the actor in a Tokio task with a default bounded mailbox.
    ///
    /// This is a convenience method that delegates to [`Spawn::spawn`]. For full documentation
    /// and more spawn options, see the [`Spawn`] trait.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// let actor_ref = <MyActor as Actor>::spawn(MyActor);
    /// # })
    /// ```
    #[inline]
    fn spawn(args: Self::Args) -> ActorRef<Self>
    where
        Self: Spawn,
    {
        <Self as Spawn>::spawn(args)
    }

    /// Spawns the actor with default initialization.
    ///
    /// This is a convenience method that delegates to [`Spawn::spawn_default`].
    #[inline]
    fn spawn_default() -> ActorRef<Self>
    where
        Self: Spawn,
        Self::Args: Default,
    {
        <Self as Spawn>::spawn_default()
    }
}

/// Provides methods for spawning actors with various configurations.
///
/// This trait is automatically implemented for all types that implement [`Actor`], providing
/// convenient methods to spawn actors in different execution contexts and with different
/// mailbox configurations.
///
/// The `Spawn` trait separates actor instantiation from actor behavior, keeping the [`Actor`]
/// trait focused on lifecycle hooks and message handling while providing ergonomic spawn
/// methods through this extension trait.
///
/// # Choosing a Spawn Method
///
/// - **[`spawn`]** or **[`spawn_default`]**: Standard async actor with bounded mailbox (most common)
/// - **[`spawn_with_mailbox`]**: Custom mailbox configuration (unbounded, custom capacity)
/// - **[`spawn_in_thread`]**: Blocking operations requiring dedicated thread
/// - **[`spawn_link`]**: Actor needs supervision link established before spawning
///
/// # Examples
///
/// ## Basic Spawning
///
/// ```
/// use kameo::Actor;
/// use kameo::actor::Spawn;
///
/// #[derive(Actor)]
/// struct Counter {
///     count: i32,
/// }
///
/// # tokio_test::block_on(async {
/// // Spawn with explicit initialization
/// let actor_ref = <Counter as Spawn>::spawn(Counter { count: 0 });
/// # })
/// ```
///
/// ## Default Spawning
///
/// ```
/// use kameo::Actor;
/// use kameo::actor::Spawn;
///
/// #[derive(Actor, Default)]
/// struct Counter {
///     count: i32,
/// }
///
/// # tokio_test::block_on(async {
/// // Spawn with default initialization
/// let actor_ref = <Counter as Spawn>::spawn_default();
/// # })
/// ```
///
/// ## Custom Mailbox
///
/// ```
/// use kameo::Actor;
/// use kameo::actor::Spawn;
/// use kameo::mailbox;
///
/// #[derive(Actor)]
/// struct HighThroughput;
///
/// # tokio_test::block_on(async {
/// // Spawn with unbounded mailbox for high message rates
/// let actor_ref = HighThroughput::spawn_with_mailbox(
///     HighThroughput,
///     mailbox::unbounded()
/// );
/// # })
/// ```
///
/// ## Blocking Operations
///
/// ```no_run
/// use std::fs::File;
/// use kameo::Actor;
/// use kameo::actor::Spawn;
///
/// #[derive(Actor)]
/// struct FileWriter {
///     file: File,
/// }
///
/// // Spawn in dedicated thread for blocking I/O
/// let actor_ref = FileWriter::spawn_in_thread(
///     FileWriter { file: File::create("log.txt").unwrap() }
/// );
/// ```
///
/// ## Supervision
///
/// ```
/// use kameo::Actor;
/// use kameo::actor::Spawn;
///
/// #[derive(Actor)]
/// struct Supervisor;
///
/// #[derive(Actor)]
/// struct Worker;
///
/// # tokio_test::block_on(async {
/// let supervisor = <Supervisor as Spawn>::spawn(Supervisor);
/// // Link worker to supervisor before spawning
/// let worker = <Worker as Spawn>::spawn_link(&supervisor, Worker).await;
/// # })
/// ```
///
/// # Note
///
/// This trait is sealed and cannot be implemented manually. It is automatically available
/// for all [`Actor`] types through a blanket implementation.
///
/// [`spawn`]: Spawn::spawn
/// [`spawn_default`]: Spawn::spawn_default
/// [`spawn_with_mailbox`]: Spawn::spawn_with_mailbox
/// [`spawn_in_thread`]: Spawn::spawn_in_thread
/// [`spawn_link`]: Spawn::spawn_link
pub trait Spawn: Actor + private::Sealed {
    /// Spawns the actor in a Tokio task, running asynchronously with a default bounded mailbox.
    ///
    /// This function spawns the actor in a non-blocking Tokio task, making it suitable for actors that need to
    /// perform asynchronous operations. The actor runs in the background and can be interacted with through
    /// the returned [`ActorRef`].
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom mailbox configuration, use [`Spawn::spawn_with_mailbox`].
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// // Spawns with a default bounded mailbox (capacity 64)
    /// let actor_ref = <MyActor as Spawn>::spawn(MyActor);
    /// # })
    /// ```
    ///
    /// The actor will continue running in the background, and messages can be sent to it via `actor_ref`.
    fn spawn(args: Self::Args) -> ActorRef<Self> {
        Spawn::spawn_with_mailbox(args, mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
    }

    /// Spawns the actor with default initialization in a Tokio task.
    ///
    /// This is a convenience method for actors that implement [`Default`], equivalent to calling
    /// `Self::spawn(Self::default())`. The actor runs asynchronously in a non-blocking Tokio task
    /// and can be interacted with through the returned [`ActorRef`].
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom initialization or mailbox configuration, use [`Spawn::spawn`] or
    /// [`Spawn::spawn_with_mailbox`] instead.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    ///
    /// #[derive(Actor, Default)]
    /// struct MyActor {
    ///     count: i32,
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// // Spawns with default state and bounded mailbox (capacity 64)
    /// let actor_ref = <MyActor as Spawn>::spawn_default();
    /// # })
    /// ```
    ///
    /// # Requirements
    ///
    /// This method requires that `Self::Args` implements [`Default`]. For actors where
    /// `Args = Self`, this means the actor struct itself must implement `Default`.
    fn spawn_default() -> ActorRef<Self>
    where
        Self::Args: Default,
    {
        Spawn::spawn(Self::Args::default())
    }

    /// Spawns the actor in a Tokio task with a specific mailbox configuration.
    ///
    /// This function allows you to explicitly specify a mailbox when spawning an actor.
    /// Use this when you need custom mailbox behavior or capacity.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    /// use kameo::mailbox;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// // Using a bounded mailbox with custom capacity
    /// let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(1000));
    ///
    /// // Using an unbounded mailbox
    /// let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
    /// # })
    /// ```
    fn spawn_with_mailbox(
        args: Self::Args,
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> ActorRef<Self> {
        let prepared_actor = PreparedActor::new((mailbox_tx, mailbox_rx));
        let actor_ref = prepared_actor.actor_ref().clone();
        prepared_actor.spawn(args);
        actor_ref
    }

    /// Spawns and links the actor in a Tokio task with a default bounded mailbox.
    ///
    /// This function is used to ensure an actor is linked with another actor before it's truly spawned,
    /// which avoids possible edge cases where the actor could die before having the chance to be linked.
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom mailbox configuration, use [`Spawn::spawn_link_with_mailbox`].
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    ///
    /// #[derive(Actor)]
    /// struct FooActor;
    ///
    /// #[derive(Actor)]
    /// struct BarActor;
    ///
    /// # tokio_test::block_on(async {
    /// let link_ref = <FooActor as Spawn>::spawn(FooActor);
    /// // Spawns with default bounded mailbox (capacity 64)
    /// let actor_ref = <BarActor as Spawn>::spawn_link(&link_ref, BarActor).await;
    /// # })
    /// ```
    fn spawn_link<L>(
        link_ref: &ActorRef<L>,
        args: Self::Args,
    ) -> impl Future<Output = ActorRef<Self>> + Send
    where
        L: Actor,
    {
        <Self as Spawn>::spawn_link_with_mailbox::<L>(
            link_ref,
            args,
            mailbox::bounded(DEFAULT_MAILBOX_CAPACITY),
        )
    }

    /// Spawns and links the actor in a Tokio task with a specific mailbox configuration.
    ///
    /// This function is used to ensure an actor is linked with another actor before it's truly spawned,
    /// which avoids possible edge cases where the actor could die before having the chance to be linked.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    /// use kameo::mailbox;
    ///
    /// #[derive(Actor)]
    /// struct FooActor;
    ///
    /// #[derive(Actor)]
    /// struct BarActor;
    ///
    /// # tokio_test::block_on(async {
    /// let link_ref = <FooActor as Spawn>::spawn(FooActor);
    /// // Using a custom mailbox
    /// let actor_ref = <BarActor as Spawn>::spawn_link_with_mailbox(&link_ref, BarActor, mailbox::unbounded()).await;
    /// # })
    /// ```
    fn spawn_link_with_mailbox<L>(
        link_ref: &ActorRef<L>,
        args: Self::Args,
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> impl Future<Output = ActorRef<Self>> + Send
    where
        L: Actor,
    {
        async move {
            let prepared_actor = PreparedActor::new((mailbox_tx, mailbox_rx));
            let actor_ref = prepared_actor.actor_ref().clone();
            actor_ref.link(link_ref).await;
            prepared_actor.spawn(args);
            actor_ref
        }
    }

    /// Spawns the actor in its own dedicated thread with a default bounded mailbox.
    ///
    /// This function spawns the actor in a separate thread, making it suitable for actors that perform blocking
    /// operations, such as file I/O or other tasks that cannot be efficiently executed in an asynchronous context.
    /// Despite running in a blocking thread, the actor can still communicate asynchronously with other actors.
    ///
    /// By default, a bounded mailbox with capacity 64 is used to provide backpressure.
    /// For custom mailbox configuration, use [`Spawn::spawn_in_thread_with_mailbox`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::io::{self, Write};
    /// use std::fs::File;
    ///
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    /// use kameo::message::{Context, Message};
    ///
    /// #[derive(Actor)]
    /// struct MyActor {
    ///     file: File,
    /// }
    ///
    /// struct Flush;
    /// impl Message<Flush> for MyActor {
    ///     type Reply = io::Result<()>;
    ///
    ///     async fn handle(&mut self, _: Flush, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
    ///         self.file.flush() // This blocking operation is handled in its own thread
    ///     }
    /// }
    ///
    /// let actor_ref = MyActor::spawn_in_thread(
    ///     MyActor { file: File::create("output.txt").unwrap() }
    /// );
    /// actor_ref.tell(Flush).blocking_send()?;
    /// # Ok::<(), kameo::error::SendError<Flush>>(())
    /// ```
    ///
    /// This function is useful for actors that require or benefit from running blocking operations while still
    /// enabling asynchronous functionality.
    fn spawn_in_thread(args: Self::Args) -> ActorRef<Self> {
        Spawn::spawn_in_thread_with_mailbox(args, mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
    }

    /// Spawns the actor in its own dedicated thread with a specific mailbox configuration.
    ///
    /// This function allows you to explicitly specify a mailbox when spawning an actor in a dedicated thread.
    /// Use this when you need custom mailbox behavior or capacity for actors that perform blocking operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::io::{self, Write};
    /// use std::fs::File;
    ///
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    /// use kameo::mailbox;
    /// use kameo::message::{Context, Message};
    ///
    /// #[derive(Actor)]
    /// struct MyActor {
    ///     file: File,
    /// }
    ///
    /// struct Flush;
    /// impl Message<Flush> for MyActor {
    ///     type Reply = io::Result<()>;
    ///
    ///     async fn handle(&mut self, _: Flush, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
    ///         self.file.flush() // This blocking operation is handled in its own thread
    ///     }
    /// }
    ///
    /// let actor_ref = MyActor::spawn_in_thread_with_mailbox(
    ///     MyActor { file: File::create("output.txt").unwrap() },
    ///     mailbox::bounded(100)
    /// );
    /// actor_ref.tell(Flush).blocking_send()?;
    /// # Ok::<(), kameo::error::SendError<Flush>>(())
    /// ```
    fn spawn_in_thread_with_mailbox(
        args: Self::Args,
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> ActorRef<Self> {
        let prepared_actor = PreparedActor::new((mailbox_tx, mailbox_rx));
        let actor_ref = prepared_actor.actor_ref().clone();
        prepared_actor.spawn_in_thread(args);
        actor_ref
    }

    /// Creates a new prepared actor, allowing access to its [`ActorRef`] before spawning.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    ///
    /// #[derive(Actor)]
    /// struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// let other_actor = <MyActor as Spawn>::spawn(MyActor);
    /// let prepared_actor = MyActor::prepare();
    /// prepared_actor.actor_ref().link(&other_actor).await;
    /// let actor_ref = prepared_actor.spawn(MyActor);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    fn prepare() -> PreparedActor<Self> {
        Spawn::prepare_with_mailbox(mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
    }

    /// Creates a new prepared actor with a specific mailbox configuration, allowing access to its [`ActorRef`] before spawning.
    ///
    /// This function allows you to explicitly specify a mailbox when preparing an actor.
    /// Use this when you need custom mailbox behavior or capacity.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::Actor;
    /// use kameo::actor::Spawn;
    /// use kameo::mailbox;
    ///
    ///  #[derive(Actor)]
    ///  struct MyActor;
    ///
    /// # tokio_test::block_on(async {
    /// let other_actor = <MyActor as Spawn>::spawn(MyActor);
    /// let prepared_actor = MyActor::prepare_with_mailbox(mailbox::unbounded());
    /// prepared_actor.actor_ref().link(&other_actor).await;
    /// let actor_ref = prepared_actor.spawn(MyActor);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    fn prepare_with_mailbox(
        (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>),
    ) -> PreparedActor<Self> {
        PreparedActor::new((mailbox_tx, mailbox_rx))
    }
}

impl<A: Actor> Spawn for A {}

mod private {
    use super::Actor;

    pub trait Sealed {}
    impl<A: Actor> Sealed for A {}
}
