//! Provides a pool of actors for task distribution and load balancing.
//!
//! The `pool` module offers the ability to manage a group of actors that work together to process tasks.
//! It enables the creation of an `ActorPool`, which distributes incoming messages to a fixed set of worker actors
//! in a round-robin fashion. This ensures that tasks are evenly distributed across workers, improving
//! resource utilization and overall performance.
//!
//! `ActorPool` must be spawned as an actor, and tasks can be sent to it using the `WorkerMsg` message
//! for individual workers or the `BroadcastMsg` to send a message to all workers in the pool.
//!
//! # Features
//! - **Load Balancing**: Messages are distributed among a fixed set of actors in a round-robin manner.
//! - **Resilience**: Workers that stop or fail are automatically replaced to ensure continued operation.
//! - **Flexible Actor Management**: The pool can manage any type of actor that implements the [Actor] trait,
//!   allowing it to be used for various tasks.
//!
//! # Example
//!
//! ```
//! use kameo::Actor;
//! use kameo::actor::pool::{ActorPool, WorkerMsg, BroadcastMsg};
//! # use kameo::message::{Context, Message};
//! use kameo::request::MessageSend;
//!
//! #[derive(Actor)]
//! struct MyWorker;
//! #
//! # impl Message<&'static str> for MyWorker {
//! #     type Reply = ();
//! #     async fn handle(&mut self, msg: &'static str, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply { }
//! # }
//!
//! # tokio_test::block_on(async {
//! // Spawn the actor pool with 4 workers
//! let pool_actor = kameo::spawn(ActorPool::new(4, || kameo::spawn(MyWorker)));
//!
//! // Send tasks to the pool
//! pool_actor.tell(WorkerMsg("Hello worker!")).send().await?;
//! pool_actor.tell(BroadcastMsg("Hello all workers!")).send().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::fmt;

use futures::{
    future::{join_all, BoxFuture},
    Future,
};
use itertools::repeat_n;

use crate::{
    actor::{Actor, ActorRef},
    error::{ActorStopReason, BoxError, SendError},
    mailbox::bounded::BoundedMailbox,
    message::{BoxDebug, Context, Message},
    reply::Reply,
    request::{
        AskRequest, ForwardMessageSend, LocalAskRequest, LocalTellRequest, MessageSend,
        TellRequest, WithoutRequestTimeout,
    },
};

use super::{ActorID, WeakActorRef};

enum Factory<A: Actor> {
    Sync(Box<dyn FnMut() -> ActorRef<A> + Send + Sync + 'static>),
    Async(Box<dyn FnMut() -> BoxFuture<'static, ActorRef<A>> + Send + Sync + 'static>),
}

/// A pool of actor workers designed to distribute tasks among a fixed set of actors.
///
/// The `ActorPool` manages a set of worker actors and implements load balancing
/// by distributing incoming messages to these workers in a round-robin fashion. It ensures
/// that workloads are evenly spread across the available workers to maintain optimal performance
/// and resource utilization. Additionally, it handles the dynamic replacement of workers
/// that stop due to errors or other reasons, maintaining the pool's resilience and reliability.
///
/// The pool can be used either as a standalone object or spawned as an actor. When spawned, tasks can be
/// sent using the `WorkerMsg` and `BroadcastMsg` messages for individual or broadcast communication with workers.
pub struct ActorPool<A: Actor> {
    workers: Vec<ActorRef<A>>,
    size: usize,
    next_idx: usize,
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

        let workers = (0..size).map(|_| factory()).collect();

        ActorPool {
            workers,
            size,
            next_idx: 0,
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

        let workers = join_all((0..size).map(|_| factory())).await;

        ActorPool {
            workers,
            size,
            next_idx: 0,
            factory: Factory::Async(Box::new(move || {
                let mut factory = factory.clone();
                Box::pin(async move { factory().await })
            })),
        }
    }

    /// Gets the [ActorRef] for the next worker in the pool.
    #[inline]
    pub fn get_worker(&self) -> ActorRef<A> {
        self.workers[self.next_idx].clone()
    }

    #[inline]
    fn next_worker(&mut self) -> (usize, &ActorRef<A>) {
        let idx = self.next_idx;
        let worker = &self.workers[idx];
        self.next_idx = (idx + 1) % self.workers.len();
        (idx, worker)
    }
}

impl<A> Actor for ActorPool<A>
where
    A: Actor,
{
    type Mailbox = BoundedMailbox<Self>;

    fn name() -> &'static str {
        "ActorPool"
    }

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        for worker in &self.workers {
            worker.link(&actor_ref).await;
        }

        Ok(())
    }

    async fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorID,
        _reason: ActorStopReason,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        let Some(actor_ref) = actor_ref.upgrade() else {
            return Ok(None);
        };
        let Some((i, _)) = self
            .workers
            .iter()
            .enumerate()
            .find(|(_, worker)| worker.id() == id)
        else {
            return Ok(None);
        };

        self.workers[i] = match &mut self.factory {
            Factory::Sync(f) => f(),
            Factory::Async(f) => f().await,
        };
        self.workers[i].link(&actor_ref).await;

        Ok(None)
    }
}

/// A reply from a worker message.
#[allow(missing_debug_implementations)]
pub enum WorkerReply<A, M>
where
    A: Actor + Message<M>,
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
    <A::Reply as Reply>::Error: fmt::Debug,
{
    type Ok = <A::Reply as Reply>::Ok;
    type Error = <A::Reply as Reply>::Error;
    type Value = <A::Reply as Reply>::Value;

    fn to_result(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("a WorkerReply cannot be converted to a result and is only a marker type")
    }

    fn into_boxed_err(self) -> Option<BoxDebug> {
        match self {
            WorkerReply::Forwarded => None,
            WorkerReply::Err(err) => Some(Box::new(err)),
        }
    }

    fn into_value(self) -> Self::Value {
        unimplemented!("a WorkerReply cannot be converted to a value and is only a marker type")
    }
}

/// A message sent to a worker in an actor pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerMsg<M>(pub M);

impl<A, M, Mb, R> Message<WorkerMsg<M>> for ActorPool<A>
where
    A: Actor<Mailbox = Mb> + Message<M, Reply = R>,
    M: Send + 'static,
    Mb: Send + 'static,
    R: Reply,
    <A::Reply as Reply>::Error: fmt::Debug,
    for<'a> AskRequest<LocalAskRequest<'a, A, Mb>, Mb, M, WithoutRequestTimeout, WithoutRequestTimeout>:
        ForwardMessageSend<A::Reply, M>,
    for<'a> TellRequest<LocalTellRequest<'a, A, Mb>, Mb, M, WithoutRequestTimeout>:
        MessageSend<Ok = (), Error = SendError<M, <A::Reply as Reply>::Error>>,
{
    type Reply = WorkerReply<A, M>;

    async fn handle(
        &mut self,
        WorkerMsg(mut msg): WorkerMsg<M>,
        mut ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let (_, mut reply_sender) = ctx.reply_sender();
        for _ in 0..self.workers.len() {
            let worker = self.next_worker().1.clone();
            match reply_sender {
                Some(tx) => {
                    if let Err(err) = worker.ask(msg).forward(tx).await {
                        match err {
                            SendError::ActorNotRunning((m, tx)) => {
                                msg = m;
                                reply_sender = Some(tx);
                            },
                            _ => unreachable!("message was forwarded, so the only error should be if the actor is not running")
                        }
                        continue;
                    }

                    return WorkerReply::Forwarded;
                }
                None => {
                    if let Err(err) = worker.tell(msg).send().await {
                        match err {
                            SendError::ActorNotRunning(m) => {
                                msg = m;
                                reply_sender = None;
                            },
                            _ => unreachable!("message was sent with `tell`, so the only error should be if the actor is not running")
                        }
                        continue;
                    }

                    return WorkerReply::Forwarded;
                }
            }
        }

        return WorkerReply::Err(SendError::ActorNotRunning(msg));
    }
}

/// A message broadcasted to all workers in an actor pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BroadcastMsg<M>(pub M);

impl<A, M> Message<BroadcastMsg<M>> for ActorPool<A>
where
    A: Actor + Message<M>,
    M: Clone + Send + 'static,
    <A::Reply as Reply>::Error: fmt::Debug,
    for<'a> TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, WithoutRequestTimeout>:
        MessageSend<Ok = (), Error = SendError<M, <A::Reply as Reply>::Error>>,
{
    type Reply = Vec<Result<(), SendError<M, <A::Reply as Reply>::Error>>>;

    async fn handle(
        &mut self,
        BroadcastMsg(msg): BroadcastMsg<M>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        join_all(
            self.workers
                .iter()
                .zip(repeat_n(msg, self.workers.len())) // Avoids unnecessary clone of msg on last iteration
                .map(|(worker, msg)| worker.tell(msg).send()),
        )
        .await
    }
}

impl<A: Actor> fmt::Debug for ActorPool<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorPool")
            .field("workers", &self.workers)
            .field("size", &self.size)
            .field("next_idx", &self.next_idx)
            .finish()
    }
}
