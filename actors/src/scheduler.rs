//! Sends messages to actors at scheduled timeouts/intervals.
//!
//! Its common for actors to need to run code at some interval or after a delay. The [`Scheduler`] actor handles this
//! by spawning background tasks for sending messages to actors as needed. It may be common to have a single
//! `Scheduler` actor in a program which handles all time related messaging.
//!
//! # Example
//!
//! ```
//! use std::time::Duration;
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use std::sync::Arc;
//!
//! use kameo::prelude::*;
//! use kameo_actors::scheduler::{Scheduler, SetInterval};
//!
//! #[derive(Actor)]
//! struct Counter(Arc<AtomicU64>);
//!
//! #[derive(Clone)]
//! struct Inc;
//!
//! impl Message<Inc> for Counter {
//!     type Reply = ();
//!
//!     async fn handle(&mut self, _msg: Inc, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
//!         self.0.fetch_add(1, Ordering::Relaxed);
//!     }
//! }
//!
//! # tokio_test::block_on(async {
//! // Spawn a scheduler actor
//! let scheduler_ref = Scheduler::spawn(Scheduler::new());
//!
//! // Spawn a counter actor
//! let counter = Arc::new(AtomicU64::new(0));
//! let counter_ref = Counter::spawn(Counter(counter.clone()));
//!
//! // Increment counter every 100ms
//! let interval = SetInterval::new(counter_ref.downgrade(), Duration::from_millis(100), Inc);
//! scheduler_ref.tell(interval).await?;
//!
//! tokio::time::sleep(Duration::from_millis(500)).await;
//!
//! // Count should have been incremented 5 times
//! assert_eq!(counter.load(Ordering::Relaxed), 5);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::time::Duration;

use kameo::{error::Infallible, mailbox::Signal, prelude::*};
use tokio::{
    task::{AbortHandle, JoinSet},
    time::{Instant, Interval, MissedTickBehavior},
};

/// An actor which handles scheduled messages.
///
/// See [module level docs](crate::scheduler) for more info.
#[derive(Default)]
pub struct Scheduler {
    tasks: JoinSet<()>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            tasks: JoinSet::new(),
        }
    }
}

impl Actor for Scheduler {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn next(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Option<Signal<Self>> {
        loop {
            if self.tasks.is_empty() {
                return mailbox_rx.recv().await;
            } else {
                tokio::select! {
                    signal = mailbox_rx.recv() => {
                        return signal;
                    }
                    _ = self.tasks.join_next() => {}
                }
            }
        }
    }
}

/// Sends a message to an actor after a given duration.
pub struct SetTimeout<A: Actor, M> {
    actor_ref: WeakActorRef<A>,
    deadline: Instant,
    msg: M,
}

impl<A: Actor, M> SetTimeout<A, M> {
    /// Creates a new `SetTimeout` message.
    ///
    /// The message timeout will be from when this `new` method was called, and not when the `Scheduler` actor receives
    /// it.
    pub fn new(actor_ref: WeakActorRef<A>, duration: Duration, msg: M) -> Self {
        SetTimeout {
            actor_ref,
            deadline: Instant::now() + duration,
            msg,
        }
    }
}

impl<A, M> Message<SetTimeout<A, M>> for Scheduler
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    type Reply = AbortHandle;

    async fn handle(
        &mut self,
        msg: SetTimeout<A, M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.tasks.spawn(async move {
            tokio::time::sleep_until(msg.deadline).await;
            if let Some(target) = msg.actor_ref.upgrade() {
                _ = target.tell(msg.msg).await;
            }
        })
    }
}

/// Sends a message to an actor at a fixed interval.
pub struct SetInterval<A: Actor, T> {
    actor_ref: WeakActorRef<A>,
    interval: Interval,
    msg: T,
}

impl<A: Actor, T> SetInterval<A, T> {
    /// Creates a new `SetInterval` message.
    ///
    /// The message interval will be from when this `new` method was called, and not when the `Scheduler` actor
    /// receives it.
    pub fn new(actor_ref: WeakActorRef<A>, period: Duration, msg: T) -> Self {
        SetInterval {
            actor_ref,
            interval: tokio::time::interval(period),
            msg,
        }
    }

    /// Adds a delay before starting the interval.
    pub fn start_delay(mut self, duration: Duration) -> Self {
        self.interval = tokio::time::interval_at(Instant::now() + duration, self.interval.period());
        self
    }

    /// Sets the [`MissedTickBehavior`] strategy that should be used.
    pub fn set_missed_tick_behaviour(mut self, behaviour: MissedTickBehavior) -> Self {
        self.interval.set_missed_tick_behavior(behaviour);
        self
    }
}

impl<A, T> Message<SetInterval<A, T>> for Scheduler
where
    A: Actor + Message<T>,
    T: Clone + Send + 'static,
{
    type Reply = AbortHandle;

    async fn handle(
        &mut self,
        mut msg: SetInterval<A, T>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.tasks.spawn(async move {
            loop {
                msg.interval.tick().await;

                let Some(target) = msg.actor_ref.upgrade() else {
                    return;
                };

                if let Err(SendError::ActorNotRunning(_) | SendError::ActorStopped) =
                    target.tell(msg.msg.clone()).await
                {
                    return;
                }
            }
        })
    }
}
