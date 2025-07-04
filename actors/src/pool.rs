//! Provides a pool of actors for task distribution and load balancing.
//!
//! The `pool` module offers the ability to manage a group of actors that work together to process tasks.
//! It enables the creation of an `ActorPool`, which distributes incoming messages to a fixed set of worker actors
//! in a least-connections fashion. This ensures that messages are always sent to the worker with the least amount of
//! work queued.
//!
//! `ActorPool` must be spawned as an actor, and tasks can be sent to it using the `WorkerMsg` message
//! for individual workers or the `BroadcastMsg` to send a message to all workers in the pool.
//!
//! # Features
//! - **Load Balancing**: Messages are distributed among a fixed set of actors in a least-connections manner.
//! - **Resilience**: Workers that stop or fail are automatically replaced to ensure continued operation.
//! - **Flexible Actor Management**: The pool can manage any type of actor that implements the [Actor] trait,
//!   allowing it to be used for various tasks.
//!
//! # Example
//!
//! ```
//! use kameo::Actor;
//! use kameo_actors::pool::{ActorPool, Broadcast, Dispatch};
//! # use kameo::message::{Context, Message};
//!
//! #[derive(Actor)]
//! struct MyWorker;
//! #
//! # impl Message<&'static str> for MyWorker {
//! #     type Reply = ();
//! #     async fn handle(&mut self, msg: &'static str, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply { }
//! # }
//!
//! # tokio_test::block_on(async {
//! // Spawn the actor pool with 4 workers
//! let pool_actor = ActorPool::spawn(ActorPool::new(4, || MyWorker::spawn(MyWorker)));
//!
//! // Send tasks to the pool
//! pool_actor.tell(Dispatch("Hello worker!")).await?;
//! pool_actor.tell(Broadcast("Hello all workers!")).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::{
    fmt,
    ops::ControlFlow,
    sync::{Arc, Weak},
};

use futures::{
    Future, FutureExt, TryFutureExt,
    future::{BoxFuture, join_all},
};
use kameo::{error::Infallible, prelude::*};

enum Factory<A: Actor> {
    Sync(Box<dyn FnMut() -> ActorRef<A> + Send + Sync + 'static>),
    Async(Box<dyn FnMut() -> BoxFuture<'static, ActorRef<A>> + Send + Sync + 'static>),
}

/// A pool of actor workers designed to distribute tasks among a fixed set of actors.
///
/// The `ActorPool` manages a set of worker actors and implements load balancing
/// by distributing incoming messages to these workers in a least-connections fashion. It ensures
/// that workloads are evenly spread across the available workers to maintain optimal performance
/// and resource utilization. Additionally, it handles the dynamic replacement of workers
/// that stop due to errors or other reasons, maintaining the pool's resilience and reliability.
///
/// The pool can be used either as a standalone object or spawned as an actor. When spawned, tasks can be
/// sent using the `WorkerMsg` and `BroadcastMsg` messages for individual or broadcast communication with workers.
pub struct ActorPool<A: Actor> {
    workers: Vec<(ActorRef<Worker<A>>, Arc<()>)>,
    size: usize,
    factory: Factory<A>,
}

impl<A> ActorPool<A>
where
    A: Actor,
{
    /// Creates a new `ActorPool` with the specified size and a factory function for creating workers.
    ///
    /// The `size` parameter determines the fixed number of workers in the pool. The `factory`
    /// function is used to instantiate new worker actors when the pool is initialized or when
    /// replacing a stopped worker. Each worker is an [`ActorRef<A>`], where `A` implements the [Actor] trait.
    ///
    /// # Arguments
    ///
    /// * `size` - The number of workers to maintain in the pool.
    /// * `factory` - A function that produces new instances of `ActorRef<A>` when called.
    ///
    /// # Panics
    ///
    /// This method panics if `size` is set to 0, as an actor pool cannot function without workers.
    pub fn new(
        size: usize,
        mut factory: impl FnMut() -> ActorRef<A> + Send + Sync + 'static,
    ) -> Self
    where
        A: Actor,
    {
        assert_ne!(size, 0);

        let workers = (0..size)
            .map(|_| {
                let worker = Worker {
                    actor_ref: factory(),
                };
                let mailbox = match worker.actor_ref.mailbox_sender() {
                    MailboxSender::Bounded(tx) => mailbox::bounded(tx.capacity()),
                    MailboxSender::Unbounded(_) => mailbox::unbounded(),
                };
                (Worker::spawn_with_mailbox(worker, mailbox), Arc::new(()))
            })
            .collect();

        ActorPool {
            workers,
            size,
            factory: Factory::Sync(Box::new(factory)),
        }
    }

    /// Creates a new `ActorPool` with the specified size and an async factory function for creating workers.
    ///
    /// This is the same as [ActorPool::new], but allows the factory function to be async.
    pub async fn new_async<F, Fu>(size: usize, mut factory: F) -> Self
    where
        A: Actor,
        F: FnMut() -> Fu + Clone + Send + Sync + 'static,
        Fu: Future<Output = ActorRef<A>> + Send,
    {
        assert_ne!(size, 0);

        let workers = join_all((0..size).map(|_| {
            FutureExt::map(factory(), |actor_ref| {
                let worker = Worker { actor_ref };
                let mailbox = match worker.actor_ref.mailbox_sender() {
                    MailboxSender::Bounded(tx) => mailbox::bounded(tx.capacity()),
                    MailboxSender::Unbounded(_) => mailbox::unbounded(),
                };
                (Worker::spawn_with_mailbox(worker, mailbox), Arc::new(()))
            })
        }))
        .await;

        ActorPool {
            workers,
            size,
            factory: Factory::Async(Box::new(move || {
                let mut factory = factory.clone();
                Box::pin(async move { factory().await })
            })),
        }
    }

    fn next_worker(&self) -> (&ActorRef<Worker<A>>, Weak<()>) {
        self.workers
            .iter()
            .min_by_key(|(_, load)| Arc::weak_count(load))
            .map(|(actor_ref, counter)| (actor_ref, Arc::downgrade(counter)))
            .expect("ActorPool should have at least one worker")
    }
}

impl<A> Actor for ActorPool<A>
where
    A: Actor,
{
    type Args = Self;
    type Error = Infallible;

    fn name() -> &'static str {
        "ActorPool"
    }

    async fn on_start(state: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        for (worker, _) in &state.workers {
            worker.link(&actor_ref).await;
        }

        Ok(state)
    }

    async fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorId,
        _reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        let Some(actor_ref) = actor_ref.upgrade() else {
            return Ok(ControlFlow::Continue(()));
        };
        let Some((i, _)) = self
            .workers
            .iter()
            .enumerate()
            .find(|(_, (worker, _))| worker.id() == id)
        else {
            return Ok(ControlFlow::Continue(()));
        };

        self.workers[i] = match &mut self.factory {
            Factory::Sync(f) => {
                let worker = Worker { actor_ref: f() };
                let mailbox = match worker.actor_ref.mailbox_sender() {
                    MailboxSender::Bounded(tx) => mailbox::bounded(tx.capacity()),
                    MailboxSender::Unbounded(_) => mailbox::unbounded(),
                };
                (Worker::spawn_with_mailbox(worker, mailbox), Arc::new(()))
            }
            Factory::Async(f) => {
                let worker = Worker {
                    actor_ref: f().await,
                };
                let mailbox = match worker.actor_ref.mailbox_sender() {
                    MailboxSender::Bounded(tx) => mailbox::bounded(tx.capacity()),
                    MailboxSender::Unbounded(_) => mailbox::unbounded(),
                };
                (Worker::spawn_with_mailbox(worker, mailbox), Arc::new(()))
            }
        };
        self.workers[i].0.link(&actor_ref).await;

        Ok(ControlFlow::Continue(()))
    }
}

struct Worker<A: Actor> {
    actor_ref: ActorRef<A>,
}

impl<A: Actor> Actor for Worker<A> {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A wrapper type which helps keep track of the load for each worker.
#[doc(hidden)]
#[allow(missing_debug_implementations)]
struct WorkerMsgWrapper<M> {
    msg: M,
    counter: Weak<()>,
}

impl<A, M> Message<WorkerMsgWrapper<M>> for Worker<A>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    type Reply = Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>;

    async fn handle(
        &mut self,
        WorkerMsgWrapper {
            msg,
            counter: _counter,
        }: WorkerMsgWrapper<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.actor_ref.ask(msg).await
    }
}

/// A reply from a worker message.
#[allow(missing_debug_implementations)]
pub enum WorkerReply<A, M>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// The message was forwarded to a worker.
    Forwarded,
    /// The message failed to be sent to a worker.
    Err(SendError<M, <A::Reply as Reply>::Error>),
}

impl<A, M> Reply for WorkerReply<A, M>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    type Ok = <A::Reply as Reply>::Ok;
    type Error = <A::Reply as Reply>::Error;
    type Value = Result<Self::Ok, SendError<M, Self::Error>>;

    fn to_result(self) -> Result<<A::Reply as Reply>::Ok, <A::Reply as Reply>::Error> {
        unimplemented!("a WorkerReply cannot be converted to a result and is only a marker type")
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        match self {
            WorkerReply::Forwarded => None,
            WorkerReply::Err(err) => Some(Box::new(err)),
        }
    }

    fn into_value(self) -> Self::Value {
        unimplemented!("a WorkerReply cannot be converted to a value and is only a marker type")
    }
}

/// A message to be handled by a worker in an actor pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Dispatch<M>(pub M);

impl<A, M, R> Message<Dispatch<M>> for ActorPool<A>
where
    A: Actor + Message<M, Reply = R>,
    M: Send + 'static,
    R: Reply,
{
    type Reply = WorkerReply<A, M>;

    async fn handle(
        &mut self,
        Dispatch(mut msg): Dispatch<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (_, mut reply_sender) = ctx.reply_sender();
        for _ in 0..self.workers.len() {
            let (worker, counter) = self.next_worker();
            match reply_sender {
                Some(tx) => {
                    if let Err(err) = worker
                        .ask(WorkerMsgWrapper { msg, counter })
                        .forward(tx)
                        .await
                    {
                        match err {
                            SendError::ActorNotRunning((m, tx)) => {
                                msg = m.msg;
                                reply_sender = Some(tx);
                            }
                            _ => unreachable!(
                                "message was forwarded, so the only error should be if the actor is not running"
                            ),
                        }
                        continue;
                    }

                    return WorkerReply::Forwarded;
                }
                None => {
                    if let Err(err) = worker.tell(WorkerMsgWrapper { msg, counter }).send().await {
                        match err {
                            SendError::ActorNotRunning(m) => {
                                msg = m.msg;
                                reply_sender = None;
                            }
                            _ => unreachable!(
                                "message was sent with `tell`, so the only error should be if the actor is not running"
                            ),
                        }
                        continue;
                    }

                    return WorkerReply::Forwarded;
                }
            }
        }

        WorkerReply::Err(SendError::ActorNotRunning(msg))
    }
}

/// A message broadcasted to all workers in an actor pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Broadcast<M>(pub M);

impl<A, M> Message<Broadcast<M>> for ActorPool<A>
where
    A: Actor + Message<M>,
    M: Clone + Send + 'static,
{
    type Reply = Vec<Result<(), SendError<M, <A::Reply as Reply>::Error>>>;

    async fn handle(
        &mut self,
        Broadcast(msg): Broadcast<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        join_all(
            self.workers
                .iter()
                .zip(
                    std::iter::repeat_n(msg, self.workers.len()), // Avoids unnecessary clone of msg on last iteration
                )
                .map(|((worker, counter), msg)| {
                    worker
                        .tell(WorkerMsgWrapper {
                            msg,
                            counter: Arc::downgrade(counter),
                        })
                        .send()
                        .map_err(|err| {
                            err.map_msg(|msg| msg.msg)
                                .map_err(|_| {
                                    panic!(
                                        "reset err infallible called on a `SendError::HandlerError`"
                                    )
                                })
                                .flatten()
                        })
                }),
        )
        .await
    }
}

impl<A: Actor> fmt::Debug for ActorPool<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorPool")
            .field("workers", &self.workers)
            .field("size", &self.size)
            .finish()
    }
}
