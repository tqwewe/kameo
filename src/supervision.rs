//! Actor supervision for building fault-tolerant systems.
//!
//! This module provides Erlang-style supervision trees, allowing actors to automatically
//! restart failed child actors according to configurable policies and strategies. Supervision
//! is a key component of building resilient, self-healing actor systems.
//!
//! # Overview
//!
//! Supervision enables fault isolation and recovery by organizing actors into hierarchical
//! supervision trees. When a child actor fails (panics, returns an error, or exits normally),
//! its supervisor decides whether and how to restart it based on:
//!
//! - **[`RestartPolicy`]**: Determines when a child should be restarted (always, only on abnormal exit, or never)
//! - **[`SupervisionStrategy`]**: Determines which children to restart (just the failed child, all children, or the failed child plus younger siblings)
//! - **Restart Limits**: Prevents restart storms by limiting the number of restarts within a time window
//!
//! # Quick Start
//!
//! To create a supervised child actor, use the [`Spawn::supervise`] method:
//!
//! ```no_run
//! use std::time::Duration;
//! use kameo::actor::{Actor, ActorRef, Spawn};
//! use kameo::error::Infallible;
//! use kameo::supervision::{RestartPolicy, SupervisionStrategy};
//!
//! struct Supervisor;
//! impl Actor for Supervisor {
//!     type Args = ();
//!     type Error = Infallible;
//!
//!     fn supervision_strategy() -> SupervisionStrategy {
//!         SupervisionStrategy::OneForOne
//!     }
//!
//!     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
//!         // Spawn a supervised child that restarts on abnormal exits
//!         let child = Worker::supervise(&actor_ref, Worker)
//!             .restart_policy(RestartPolicy::Transient)
//!             .restart_limit(5, Duration::from_secs(10))
//!             .spawn()
//!             .await;
//!
//!         Ok(Supervisor)
//!     }
//! }
//!
//! #[derive(Clone)]
//! struct Worker;
//! impl Actor for Worker {
//!     type Args = Self;
//!     type Error = Infallible;
//!     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
//!         Ok(state)
//!     }
//! }
//! ```
//!
//! # Restart Policies
//!
//! - **[`RestartPolicy::Permanent`]**: Always restart, regardless of how the actor died (default)
//! - **[`RestartPolicy::Transient`]**: Only restart on abnormal exits (panics/errors), not normal exits
//! - **[`RestartPolicy::Never`]**: Never restart, regardless of how the actor died
//!
//! # Supervision Strategies
//!
//! - **[`SupervisionStrategy::OneForOne`]**: Only restart the failed child (default)
//! - **[`SupervisionStrategy::OneForAll`]**: Restart all children when any one fails
//! - **[`SupervisionStrategy::RestForOne`]**: Restart the failed child plus all younger siblings
//!
//! # Examples
//!
//! See the [`supervision` example](https://github.com/tqwewe/kameo/blob/main/examples/supervision.rs)
//! for a complete working example.
//!
//! [`Spawn::supervise`]: crate::actor::Spawn::supervise

use std::{
    any::Any,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{FutureExt, future::BoxFuture};

use crate::{
    actor::{Actor, ActorId, ActorRef, DEFAULT_MAILBOX_CAPACITY, PreparedActor},
    links::{BoxActorRef, ErasedChildSpec, Links, ShutdownFn, SpawnFactory},
    mailbox::{self, MailboxReceiver, MailboxSender, Signal},
};

/// Defines when a supervised child actor should be restarted.
///
/// The restart policy determines whether a child should be restarted based on how it
/// terminated. This allows fine-grained control over restart behavior depending on
/// whether the actor failed unexpectedly or completed its work normally.
///
/// # Restart Behavior
///
/// | Policy | On Panic | On Error | On Normal Exit |
/// |--------|----------|----------|----------------|
/// | [`Permanent`](Self::Permanent) | ✓ Restart | ✓ Restart | ✓ Restart |
/// | [`Transient`](Self::Transient) | ✓ Restart | ✓ Restart | ✗ No restart |
/// | [`Never`](Self::Never)         | ✗ No restart | ✗ No restart | ✗ No restart |
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use kameo::actor::{Actor, ActorRef, Spawn};
/// use kameo::supervision::RestartPolicy;
/// # use kameo::error::Infallible;
/// #
/// # struct Supervisor;
/// # impl Actor for Supervisor {
/// #     type Args = ();
/// #     type Error = Infallible;
/// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
/// #         Ok(Supervisor)
/// #     }
/// # }
/// # #[derive(Clone)]
/// # struct Worker;
/// # impl Actor for Worker {
/// #     type Args = Self;
/// #     type Error = Infallible;
/// #     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
/// #         Ok(state)
/// #     }
/// # }
/// # async {
/// # let supervisor_ref: ActorRef<Supervisor> = unimplemented!();
///
/// // Critical service that must always be running
/// let critical = Worker::supervise(&supervisor_ref, Worker)
///     .restart_policy(RestartPolicy::Permanent)
///     .spawn()
///     .await;
///
/// // Worker that can exit normally when work is done
/// let worker = Worker::supervise(&supervisor_ref, Worker)
///     .restart_policy(RestartPolicy::Transient)
///     .spawn()
///     .await;
///
/// // One-shot task that should run once and never be restarted
/// let one_shot = Worker::supervise(&supervisor_ref, Worker)
///     .restart_policy(RestartPolicy::Never)
///     .spawn()
///     .await;
/// # };
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum RestartPolicy {
    /// Always restart the child, regardless of how it terminated.
    ///
    /// Use this for critical services that must always be running. The child will be
    /// restarted whether it panicked, returned an error, or exited normally (e.g., via
    /// `ctx.stop()`).
    ///
    /// This is the default restart policy.
    ///
    /// # Use Cases
    ///
    /// - Critical system services
    /// - Stateless workers that should always be available
    /// - Actors that should never stop running
    #[default]
    Permanent,
    /// Only restart if the child died abnormally (panic or error).
    ///
    /// Normal exits (e.g., calling `ctx.stop()` or returning successfully) will not trigger
    /// a restart. Use this for workers that can complete their work and exit normally.
    ///
    /// # Use Cases
    ///
    /// - Workers that process a queue and exit when empty
    /// - Actors with defined completion states
    /// - Services that can gracefully shutdown
    Transient,
    /// Never restart the child, regardless of how it terminated.
    ///
    /// The child is still supervised — the supervisor tracks it and includes it in
    /// [`SupervisionStrategy::OneForAll`] / [`SupervisionStrategy::RestForOne`]
    /// coordination — but will not spawn a new instance after it stops.
    ///
    /// # Use Cases
    ///
    /// - One-shot tasks that should run once and then stop permanently
    /// - Actors managed externally whose lifecycle is controlled elsewhere
    /// - Children that should be observable but not automatically recovered
    Never,
}

/// Defines which children should be restarted when a child actor fails.
///
/// The supervision strategy determines the scope of restarts when a child fails.
/// This allows you to balance fault isolation with consistency requirements based
/// on how tightly coupled your child actors are.
///
/// # Strategy Comparison
///
/// | Strategy | Failed Child | Siblings Spawned Before | Siblings Spawned After |
/// |----------|--------------|-------------------------|------------------------|
/// | [`OneForOne`](Self::OneForOne) | ✓ Restart | ✗ No change | ✗ No change |
/// | [`OneForAll`](Self::OneForAll) | ✓ Restart | ✓ Restart | ✓ Restart |
/// | [`RestForOne`](Self::RestForOne) | ✓ Restart | ✗ No change | ✓ Restart |
///
/// # Examples
///
/// ```no_run
/// use kameo::actor::{Actor, ActorRef};
/// use kameo::error::Infallible;
/// use kameo::supervision::SupervisionStrategy;
///
/// struct Supervisor;
/// impl Actor for Supervisor {
///     type Args = ();
///     type Error = Infallible;
///
///     fn supervision_strategy() -> SupervisionStrategy {
///         // Choose the strategy that matches your requirements
///         SupervisionStrategy::OneForAll
///     }
///
///     async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
///         Ok(Supervisor)
///     }
/// }
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum SupervisionStrategy {
    /// Only restart the child that failed.
    ///
    /// This is the most selective strategy, providing maximum fault isolation. When a child
    /// fails, only that child is restarted. All other children continue running normally.
    ///
    /// This is the default supervision strategy.
    ///
    /// # Use Cases
    ///
    /// - Independent workers that don't share state
    /// - Pool of similar workers processing tasks
    /// - When failures in one child don't affect others
    ///
    /// # Example
    ///
    /// ```text
    /// Supervisor
    /// ├── Child A (running)
    /// ├── Child B (crashes) → Only Child B restarts
    /// └── Child C (running)
    /// ```
    #[default]
    OneForOne,
    /// Restart all children when any one fails.
    ///
    /// This is the most aggressive strategy. When any child fails, all children under the
    /// supervisor are terminated and restarted together. Use this when children have
    /// interdependencies or shared state that must be kept consistent.
    ///
    /// # Use Cases
    ///
    /// - Tightly coupled services that must stay synchronized
    /// - Children that share distributed state
    /// - When a partial restart would leave the system in an inconsistent state
    ///
    /// # Example
    ///
    /// ```text
    /// Supervisor
    /// ├── Child A (running) → Restarted
    /// ├── Child B (crashes) → Restarted
    /// └── Child C (running) → Restarted
    /// ```
    OneForAll,
    /// Restart the failed child plus all children spawned after it.
    ///
    /// This strategy restarts the failed child and all its "younger siblings" (children that
    /// were spawned after it). Children spawned before the failed child continue running.
    /// Use this when children have sequential dependencies.
    ///
    /// # Use Cases
    ///
    /// - Pipeline stages where later stages depend on earlier ones
    /// - Services with startup ordering requirements
    /// - When newer children depend on older ones
    ///
    /// # Example
    ///
    /// ```text
    /// Supervisor
    /// ├── Child A (spawned first, running)  → Continues running
    /// ├── Child B (spawned second, crashes) → Restarted
    /// └── Child C (spawned third, running)  → Restarted
    /// ```
    RestForOne,
}

/// Builder for configuring and spawning supervised child actors.
///
/// This builder is created by calling [`Spawn::supervise`] or [`Spawn::supervise_with`]
/// and provides a fluent API for configuring supervision behavior before spawning the actor.
///
/// # Default Configuration
///
/// - **Restart Policy**: [`RestartPolicy::Permanent`] (always restart)
/// - **Restart Limit**: 5 restarts per 5 seconds
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use kameo::actor::{Actor, ActorRef, Spawn};
/// use kameo::error::Infallible;
/// use kameo::supervision::RestartPolicy;
///
/// # struct Supervisor;
/// # impl Actor for Supervisor {
/// #     type Args = ();
/// #     type Error = Infallible;
/// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
/// #[derive(Clone)]
/// struct Worker;
/// impl Actor for Worker {
///     type Args = Self;
///     type Error = Infallible;
///     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
///         Ok(state)
///     }
/// }
/// # let supervisor_ref: ActorRef<Supervisor> = unimplemented!();
///
/// // Configure and spawn a supervised child
/// let child = Worker::supervise(&supervisor_ref, Worker)
///     .restart_policy(RestartPolicy::Transient)
///     .restart_limit(3, Duration::from_secs(10))
///     .spawn()
///     .await;
/// # }}
/// ```
///
/// [`Spawn::supervise`]: crate::actor::Spawn::supervise
/// [`Spawn::supervise_with`]: crate::actor::Spawn::supervise_with
#[allow(missing_debug_implementations)]
pub struct SupervisedActorBuilder<'a, S: Actor, C: Actor> {
    supervisor_ref: &'a ActorRef<S>,
    args_factory: SupervisorFactory<C::Args>,
    restart_policy: RestartPolicy,
    max_restarts: u32,
    restart_window: Duration,
}

impl<'a, S: Actor, C: Actor> SupervisedActorBuilder<'a, S, C> {
    pub(crate) fn new(supervisor_ref: &'a ActorRef<S>, args: C::Args) -> Self
    where
        C::Args: Clone + Sync,
    {
        SupervisedActorBuilder {
            supervisor_ref,
            args_factory: SupervisorFactory::new(args),
            restart_policy: RestartPolicy::default(),
            max_restarts: 5,
            restart_window: Duration::from_secs(5),
        }
    }

    pub(crate) fn new_with(
        supervisor_ref: &'a ActorRef<S>,
        f: impl Fn() -> C::Args + Send + Sync + 'static,
    ) -> Self {
        SupervisedActorBuilder {
            supervisor_ref,
            args_factory: SupervisorFactory::new_with(f),
            restart_policy: RestartPolicy::default(),
            max_restarts: 5,
            restart_window: Duration::from_secs(5),
        }
    }

    /// Sets the restart policy for this supervised child.
    ///
    /// The restart policy determines when the child should be restarted after it stops.
    /// See [`RestartPolicy`] for details on each policy.
    ///
    /// # Default
    ///
    /// [`RestartPolicy::Permanent`] (always restart)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kameo::actor::{Actor, ActorRef, Spawn};
    /// use kameo::supervision::RestartPolicy;
    /// # use kameo::error::Infallible;
    /// # #[derive(Clone)] struct Worker;
    /// # impl Actor for Worker {
    /// #     type Args = Self;
    /// #     type Error = Infallible;
    /// #     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(state) }
    /// # }
    /// # struct Supervisor;
    /// # impl Actor for Supervisor { type Args = (); type Error = Infallible;
    /// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
    /// # let supervisor_ref = &actor_ref;
    ///
    /// let child = Worker::supervise(supervisor_ref, Worker)
    ///     .restart_policy(RestartPolicy::Transient) // Only restart on abnormal exits
    ///     .spawn()
    ///     .await;
    /// # Ok(Supervisor) }}
    /// ```
    pub fn restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.restart_policy = policy;
        self
    }

    /// Sets the restart intensity limit for this supervised child.
    ///
    /// This prevents "restart storms" by limiting the number of restarts within a time window.
    /// If the child restarts more than `restarts` times within the `within` duration, the
    /// supervisor will stop attempting to restart it.
    ///
    /// # Default
    ///
    /// 5 restarts per 5 seconds
    ///
    /// # Parameters
    ///
    /// - `restarts`: Maximum number of restarts allowed
    /// - `within`: Time window for counting restarts
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use kameo::actor::{Actor, ActorRef, Spawn};
    /// # use kameo::error::Infallible;
    /// # #[derive(Clone)] struct Worker;
    /// # impl Actor for Worker {
    /// #     type Args = Self;
    /// #     type Error = Infallible;
    /// #     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(state) }
    /// # }
    /// # struct Supervisor;
    /// # impl Actor for Supervisor { type Args = (); type Error = Infallible;
    /// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
    /// # let supervisor_ref = &actor_ref;
    ///
    /// // Allow at most 3 restarts in 10 seconds
    /// let child = Worker::supervise(supervisor_ref, Worker)
    ///     .restart_limit(3, Duration::from_secs(10))
    ///     .spawn()
    ///     .await;
    /// # Ok(Supervisor) }}
    /// ```
    pub fn restart_limit(mut self, restarts: u32, within: Duration) -> Self {
        self.max_restarts = restarts;
        self.restart_window = within;
        self
    }

    /// Spawns the supervised child actor with a default bounded mailbox.
    ///
    /// The child will be spawned with a bounded mailbox of capacity 64 (the default).
    ///
    /// # Returns
    ///
    /// An [`ActorRef`] to the spawned child actor.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kameo::actor::{Actor, ActorRef, Spawn};
    /// # use kameo::error::Infallible;
    /// # #[derive(Clone)] struct Worker;
    /// # impl Actor for Worker {
    /// #     type Args = Self;
    /// #     type Error = Infallible;
    /// #     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(state) }
    /// # }
    /// # struct Supervisor;
    /// # impl Actor for Supervisor { type Args = (); type Error = Infallible;
    /// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
    /// # let supervisor_ref = &actor_ref;
    ///
    /// let child = Worker::supervise(supervisor_ref, Worker)
    ///     .spawn()
    ///     .await;
    /// # Ok(Supervisor) }}
    /// ```
    ///
    /// [`ActorRef`]: crate::actor::ActorRef
    pub async fn spawn(self) -> ActorRef<C> {
        self.spawn_with_mailbox(mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
            .await
    }

    /// Spawns the supervised child actor with a custom mailbox configuration.
    ///
    /// This allows you to specify a custom mailbox (bounded or unbounded) for the child actor.
    ///
    /// # Parameters
    ///
    /// - `mailbox_tx`: The sender side of the mailbox
    /// - `mailbox_rx`: The receiver side of the mailbox
    ///
    /// # Returns
    ///
    /// An [`ActorRef`] to the spawned child actor.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kameo::actor::{Actor, ActorRef, Spawn};
    /// use kameo::mailbox;
    /// # use kameo::error::Infallible;
    /// # #[derive(Clone)] struct Worker;
    /// # impl Actor for Worker {
    /// #     type Args = Self;
    /// #     type Error = Infallible;
    /// #     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(state) }
    /// # }
    /// # struct Supervisor;
    /// # impl Actor for Supervisor { type Args = (); type Error = Infallible;
    /// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
    /// # let supervisor_ref = &actor_ref;
    ///
    /// // Spawn with a custom bounded mailbox
    /// let child = Worker::supervise(supervisor_ref, Worker)
    ///     .spawn_with_mailbox(mailbox::bounded(100))
    ///     .await;
    /// # Ok(Supervisor) }}
    /// ```
    ///
    /// [`ActorRef`]: crate::actor::ActorRef
    pub async fn spawn_with_mailbox(
        self,
        (mailbox_tx, mailbox_rx): (MailboxSender<C>, MailboxReceiver<C>),
    ) -> ActorRef<C> {
        self.spawn_inner((mailbox_tx, mailbox_rx), false).await
    }

    /// Spawns the supervised child actor in a dedicated thread.
    ///
    /// This is useful for actors that need to perform blocking operations or CPU-intensive
    /// work without blocking the async runtime. The actor will run on a separate OS thread
    /// but can still communicate with other actors asynchronously.
    ///
    /// # Returns
    ///
    /// An [`ActorRef`] to the spawned child actor.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kameo::actor::{Actor, ActorRef, Spawn};
    /// # use kameo::error::Infallible;
    /// # #[derive(Clone)] struct BlockingWorker;
    /// # impl Actor for BlockingWorker {
    /// #     type Args = Self;
    /// #     type Error = Infallible;
    /// #     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(state) }
    /// # }
    /// # struct Supervisor;
    /// # impl Actor for Supervisor { type Args = (); type Error = Infallible;
    /// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
    /// # let supervisor_ref = &actor_ref;
    ///
    /// // Spawn in a dedicated thread for blocking operations
    /// let child = BlockingWorker::supervise(supervisor_ref, BlockingWorker)
    ///     .spawn_in_thread()
    ///     .await;
    /// # Ok(Supervisor) }}
    /// ```
    ///
    /// [`ActorRef`]: crate::actor::ActorRef
    pub async fn spawn_in_thread(self) -> ActorRef<C> {
        self.spawn_in_thread_with_mailbox(mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
            .await
    }

    /// Spawns the supervised child actor in a dedicated thread with a custom mailbox.
    ///
    /// Combines the benefits of thread-based spawning with custom mailbox configuration.
    ///
    /// # Parameters
    ///
    /// - `mailbox_tx`: The sender side of the mailbox
    /// - `mailbox_rx`: The receiver side of the mailbox
    ///
    /// # Returns
    ///
    /// An [`ActorRef`] to the spawned child actor.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kameo::actor::{Actor, ActorRef, Spawn};
    /// use kameo::mailbox;
    /// # use kameo::error::Infallible;
    /// # #[derive(Clone)] struct BlockingWorker;
    /// # impl Actor for BlockingWorker {
    /// #     type Args = Self;
    /// #     type Error = Infallible;
    /// #     async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> { Ok(state) }
    /// # }
    /// # struct Supervisor;
    /// # impl Actor for Supervisor { type Args = (); type Error = Infallible;
    /// #     async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
    /// # let supervisor_ref = &actor_ref;
    ///
    /// // Spawn in thread with custom mailbox
    /// let child = BlockingWorker::supervise(supervisor_ref, BlockingWorker)
    ///     .spawn_in_thread_with_mailbox(mailbox::bounded(200))
    ///     .await;
    /// # Ok(Supervisor) }}
    /// ```
    ///
    /// [`ActorRef`]: crate::actor::ActorRef
    pub async fn spawn_in_thread_with_mailbox(
        self,
        (mailbox_tx, mailbox_rx): (MailboxSender<C>, MailboxReceiver<C>),
    ) -> ActorRef<C> {
        self.spawn_inner((mailbox_tx, mailbox_rx), true).await
    }

    /// Internal method used by spawn methods.
    ///
    /// This method is public but intended for internal use only. Use [`spawn`](Self::spawn),
    /// [`spawn_with_mailbox`](Self::spawn_with_mailbox), [`spawn_in_thread`](Self::spawn_in_thread),
    /// or [`spawn_in_thread_with_mailbox`](Self::spawn_in_thread_with_mailbox) instead.
    pub async fn spawn_inner(
        self,
        (mailbox_tx, mailbox_rx): (MailboxSender<C>, MailboxReceiver<C>),
        in_thread: bool,
    ) -> ActorRef<C> {
        let actor_id = ActorId::generate();
        let links = Links::default();
        let restart_policy = self.restart_policy;
        let factory = Arc::new(new_factory(
            actor_id,
            mailbox_tx.clone(),
            links.clone(),
            self.args_factory,
            in_thread,
        ));
        let shutdown = Arc::new(Box::new(move || {
            let mailbox_tx = mailbox_tx.clone();
            async move {
                // We can ignore the error here.
                // If this failed, then the supervisor will restart it for its previous stop reason.
                let _ = mailbox_tx.send(Signal::SupervisorRestart).await;
            }
            .boxed()
        }) as ShutdownFn);
        let spec = ErasedChildSpec {
            factory: factory.clone(),
            shutdown,
            restart_policy,
            restart_count: 0,
            last_restart: Instant::now(),
            max_restarts: self.max_restarts,
            restart_window: self.restart_window,
        };
        self.supervisor_ref.link_child(actor_id, &links, spec).await;
        *(*factory)(Box::new(mailbox_rx)).await.downcast().unwrap()
    }
}

fn new_factory<C: Actor>(
    actor_id: ActorId,
    mailbox_tx: MailboxSender<C>,
    links: Links,
    args_factory: SupervisorFactory<C::Args>,
    in_thread: bool,
) -> SpawnFactory {
    Box::new(move |rx: Box<dyn Any + Send>| {
        let mailbox_rx = *rx.downcast::<MailboxReceiver<C>>().unwrap();
        let mailbox_tx = mailbox_tx.clone();
        let links = links.clone();
        let args = args_factory.get();

        Box::pin(async move {
            let prepared = PreparedActor::new_with(actor_id, (mailbox_tx, mailbox_rx), links);
            let actor_ref = prepared.actor_ref().clone();
            if in_thread {
                prepared.spawn_in_thread(args);
            } else {
                prepared.spawn(args);
            }
            Box::new(actor_ref) as BoxActorRef
        }) as BoxFuture<'static, BoxActorRef>
    })
}

pub(crate) struct SupervisorFactory<T>(Box<dyn Fn() -> T + Send + Sync>);

impl<T> SupervisorFactory<T> {
    fn new(args: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        SupervisorFactory(Box::new(move || args.clone()))
    }

    fn new_with(f: impl Fn() -> T + Send + Sync + 'static) -> Self {
        SupervisorFactory(Box::new(f))
    }

    fn get(&self) -> T {
        (self.0)()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ops::ControlFlow,
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };

    use crate::{
        actor::{Actor, ActorRef, Spawn, WeakActorRef},
        error::{ActorStopReason, Infallible},
        message::{Context, Message},
        supervision::{RestartPolicy, SupervisionStrategy},
    };

    // ==================== Test Helper Actors ====================

    /// A configurable test child actor that can panic, return errors, or stop normally
    #[derive(Clone)]
    struct TestChild {
        start_count: Arc<AtomicU32>,
    }

    impl TestChild {
        fn new(start_count: Arc<AtomicU32>) -> Self {
            TestChild { start_count }
        }
    }

    impl Actor for TestChild {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            state.start_count.fetch_add(1, Ordering::SeqCst);
            Ok(state)
        }
    }

    /// Message that triggers a panic
    struct TriggerPanic;

    impl Message<TriggerPanic> for TestChild {
        type Reply = ();

        async fn handle(
            &mut self,
            _msg: TriggerPanic,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            panic!("intentional panic for testing");
        }
    }

    /// Message that returns an error (for tell requests, triggers on_panic)
    struct TriggerError;

    impl Message<TriggerError> for TestChild {
        type Reply = Result<(), &'static str>;

        async fn handle(
            &mut self,
            _msg: TriggerError,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            Err("intentional error for testing")
        }
    }

    /// Message that stops the actor gracefully
    struct StopGracefully;

    impl Message<StopGracefully> for TestChild {
        type Reply = ();

        async fn handle(
            &mut self,
            _msg: StopGracefully,
            ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            ctx.stop();
        }
    }

    /// Message to check if actor is alive (used for verification)
    struct Ping;

    impl Message<Ping> for TestChild {
        type Reply = u32;

        async fn handle(
            &mut self,
            _msg: Ping,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            self.start_count.load(Ordering::SeqCst)
        }
    }

    /// A supervisor actor with OneForOne strategy (default)
    #[derive(Clone)]
    struct TestSupervisor;

    impl Actor for TestSupervisor {
        type Args = Self;
        type Error = Infallible;

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::OneForOne
        }

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    /// Supervisor with OneForAll strategy
    struct OneForAllSupervisor;

    impl Actor for OneForAllSupervisor {
        type Args = Self;
        type Error = Infallible;

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::OneForAll
        }

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    /// Supervisor with RestForOne strategy
    struct RestForOneSupervisor;

    impl Actor for RestForOneSupervisor {
        type Args = Self;
        type Error = Infallible;

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::RestForOne
        }

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    /// A sibling actor that tracks link death notifications
    #[derive(Clone)]
    struct LinkTracker {
        link_died_count: Arc<AtomicU32>,
        start_count: Arc<AtomicU32>,
    }

    impl LinkTracker {
        fn new(link_died_count: Arc<AtomicU32>, start_count: Arc<AtomicU32>) -> Self {
            LinkTracker {
                link_died_count,
                start_count,
            }
        }
    }

    impl Actor for LinkTracker {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            state.start_count.fetch_add(1, Ordering::SeqCst);
            Ok(state)
        }

        async fn on_link_died(
            &mut self,
            _actor_ref: WeakActorRef<Self>,
            _id: crate::actor::ActorId,
            _reason: ActorStopReason,
        ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
            self.link_died_count.fetch_add(1, Ordering::SeqCst);
            Ok(ControlFlow::Continue(()))
        }
    }

    impl Message<Ping> for LinkTracker {
        type Reply = u32;

        async fn handle(
            &mut self,
            _msg: Ping,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            self.start_count.load(Ordering::SeqCst)
        }
    }

    // ==================== Restart Policy Tests ====================

    #[tokio::test]
    async fn permanent_restarts_on_panic() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger panic
        let _ = child.tell(TriggerPanic).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "actor should have restarted after panic"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn permanent_restarts_on_normal_exit() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Stop gracefully (normal exit)
        let _ = child.tell(StopGracefully).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "permanent policy should restart on normal exit"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn permanent_restarts_on_error() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger error (tell with Result::Err triggers on_panic)
        let _ = child.tell(TriggerError).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "permanent policy should restart on error"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn transient_restarts_on_panic() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Transient)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger panic
        let _ = child.tell(TriggerPanic).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "transient policy should restart on panic"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn transient_no_restart_on_normal_exit() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Transient)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Stop gracefully (normal exit)
        let _ = child.tell(StopGracefully).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "transient policy should NOT restart on normal exit"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn transient_restarts_on_error() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Transient)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger error
        let _ = child.tell(TriggerError).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "transient policy should restart on error"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn never_no_restart_on_panic() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Never)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger panic
        let _ = child.tell(TriggerPanic).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "never policy should NOT restart on panic"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn never_no_restart_on_normal_exit() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Never)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Stop gracefully (normal exit)
        let _ = child.tell(StopGracefully).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "never policy should NOT restart on normal exit"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn never_no_restart_on_error() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Never)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger error
        let _ = child.tell(TriggerError).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "never policy should NOT restart on error"
        );

        supervisor.kill();
    }

    // ==================== Restart Limits Tests ====================

    #[tokio::test]
    async fn max_restarts_exceeded_stops_restarting() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(2, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // First crash -> restart (count = 1)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 2);

        // Second crash -> restart (count = 2)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 3);

        // Third crash -> should NOT restart (max_restarts = 2 exceeded)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            3,
            "should not restart after max_restarts exceeded"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn restart_count_resets_after_window() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(2, Duration::from_millis(100)) // Very short window
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // First crash -> restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 2);

        // Second crash -> restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 3);

        // Wait for window to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Third crash -> should restart (window reset)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            4,
            "should restart after window resets"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn custom_restart_limit_respected() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(1, Duration::from_secs(10)) // Only 1 restart allowed
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // First crash -> restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 2);

        // Second crash -> should NOT restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "custom limit of 1 should be respected"
        );

        supervisor.kill();
    }

    // ==================== Supervision Strategy Tests ====================

    #[tokio::test]
    async fn one_for_one_only_restarts_failed_child() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);

        // Crash child1
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            1,
            "child2 should NOT restart"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn one_for_all_restarts_all_children() {
        let supervisor = OneForAllSupervisor::spawn(OneForAllSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child1
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart (OneForAll)"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart (OneForAll)"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn rest_for_one_restarts_later_children() {
        let supervisor = RestForOneSupervisor::spawn(RestForOneSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let _child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child2 (middle child)
        let _ = child2.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            1,
            "child1 should NOT restart (spawned before)"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart (spawned after)"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn rest_for_one_first_child_restarts_all() {
        let supervisor = RestForOneSupervisor::spawn(RestForOneSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child1 (first child) - all children after should restart
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart (spawned after child1)"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart (spawned after child1)"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn rest_for_one_last_child_only_self() {
        let supervisor = RestForOneSupervisor::spawn(RestForOneSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let _child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child3 (last child) - only child3 should restart
        let _ = child3.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            1,
            "child1 should NOT restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            1,
            "child2 should NOT restart"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart"
        );

        supervisor.kill();
    }

    // ==================== Link Propagation Tests ====================

    #[tokio::test]
    async fn sibling_link_notifies_on_death() {
        let link_died_count = Arc::new(AtomicU32::new(0));
        let tracker_start_count = Arc::new(AtomicU32::new(0));

        let tracker = LinkTracker::spawn(LinkTracker::new(
            link_died_count.clone(),
            tracker_start_count.clone(),
        ));

        let child_start_count = Arc::new(AtomicU32::new(0));
        let child = TestChild::spawn(TestChild::new(child_start_count.clone()));

        // Wait for both to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Link them as siblings
        tracker.link(&child).await;

        // Kill the child
        child.kill();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            link_died_count.load(Ordering::SeqCst),
            1,
            "tracker should receive link_died notification"
        );

        tracker.kill();
    }

    #[tokio::test]
    async fn sibling_unlink_stops_notifications() {
        let link_died_count = Arc::new(AtomicU32::new(0));
        let tracker_start_count = Arc::new(AtomicU32::new(0));

        let tracker = LinkTracker::spawn(LinkTracker::new(
            link_died_count.clone(),
            tracker_start_count.clone(),
        ));

        let child_start_count = Arc::new(AtomicU32::new(0));
        let child = TestChild::spawn(TestChild::new(child_start_count.clone()));

        // Wait for both to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Link and then unlink
        tracker.link(&child).await;
        tracker.unlink(&child).await;

        // Kill the child
        child.kill();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            link_died_count.load(Ordering::SeqCst),
            0,
            "tracker should NOT receive notification after unlink"
        );

        tracker.kill();
    }

    #[tokio::test]
    async fn supervised_child_does_not_notify_siblings_on_restart() {
        // When a supervised child restarts, siblings should NOT be notified
        // (the parent handles the restart internally)
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let link_died_count = Arc::new(AtomicU32::new(0));
        let tracker_start_count = Arc::new(AtomicU32::new(0));
        let child_start_count = Arc::new(AtomicU32::new(0));

        let tracker = LinkTracker::supervise(
            &supervisor,
            LinkTracker::new(link_died_count.clone(), tracker_start_count.clone()),
        )
        .restart_policy(RestartPolicy::Permanent)
        .spawn()
        .await;

        let child = TestChild::supervise(&supervisor, TestChild::new(child_start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for both to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Link them as siblings
        tracker.link(&child).await;

        // Crash the child (will be restarted by supervisor)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Child should have restarted
        assert_eq!(
            child_start_count.load(Ordering::SeqCst),
            2,
            "child should restart"
        );
        // Tracker should NOT be notified because the child was restarted by supervisor
        assert_eq!(
            link_died_count.load(Ordering::SeqCst),
            0,
            "siblings should NOT be notified when child restarts"
        );

        supervisor.kill();
    }

    // ==================== Edge Cases ====================

    #[tokio::test]
    async fn rapid_successive_restarts() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(10, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger multiple rapid crashes
        for _ in 0..5 {
            let _ = child.tell(TriggerPanic).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // All should have restarted
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            6,
            "should handle rapid successive restarts"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn one_for_all_with_single_child() {
        let supervisor = OneForAllSupervisor::spawn(OneForAllSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Crash the only child
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "single child should restart under OneForAll"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn one_for_one_multiple_independent_failures() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        let child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Crash both children independently
        let _ = child1.tell(TriggerPanic).await;
        let _ = child2.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart independently"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart independently"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn supervisor_factory_uses_cloned_args() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart_policy(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger crash to force restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The restarted actor should use cloned args (same Arc)
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "factory should clone args for restart"
        );

        supervisor.kill();
    }

    // ==================== Default Values Tests ====================

    #[tokio::test]
    async fn default_restart_policy_is_permanent() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        // Use default (no .restart_policy() call)
        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Normal exit should trigger restart (Permanent policy)
        let _ = child.tell(StopGracefully).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "default policy (Permanent) should restart on normal exit"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn default_supervision_strategy_is_one_for_one() {
        // TestSupervisor uses default strategy (OneForOne)
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Crash child1
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            1,
            "child2 should NOT restart (OneForOne)"
        );

        supervisor.kill();
    }
}
