use std::fmt;

use futures::future::join_all;
use tracing::warn;

use crate::{
    actor::{Actor, ActorRef},
    error::SendError,
    message::Message,
    reply::Reply,
};

/// A pool of actor workers designed to distribute tasks among a fixed set of actors.
///
/// The `ActorPool` manages a set of worker actors and implements load balancing
/// by distributing incoming messages to these workers in a round-robin fashion. It ensures
/// that workloads are evenly spread across the available workers to maintain optimal performance
/// and resource utilization. Additionally, it handles the dynamic replacement of workers
/// that stop due to errors or other reasons, maintaining the pool's resilience and reliability.
///
/// The pool is generic over the type of actors it contains, allowing it to manage any actor
/// that implements the `Actor` trait. This design provides flexibility in using the pool
/// with different types of actors for various tasks.
///
/// `ActorPool` can handle any message that the worker actors can handle.
///
/// # Examples
///
/// ```no_run
/// use kameo::{ActorPool, Actor, ActorRef};
///
/// #[derive(Actor)]
/// struct MyActor;
///
/// struct MyMessage;
/// impl Message<MyMessage> for MyActor {
///     // ...
/// }
///
/// // Create a pool with 5 workers.
/// let pool = ActorPool::new(5, || {
///     kameo::spawn(MyActor)
/// });
///
/// pool.send(MyMessage).await?;
/// ```
pub struct ActorPool<A> {
    workers: Vec<ActorRef<A>>,
    size: usize,
    next_idx: usize,
    factory: Box<dyn FnMut() -> ActorRef<A> + Send + Sync>,
}

impl<A> ActorPool<A> {
    /// Creates a new `ActorPool` with the specified size and a factory function for creating workers.
    ///
    /// The `size` parameter determines the fixed number of workers in the pool. The `factory`
    /// function is used to instantiate new worker actors when the pool is initialized or when
    /// replacing a stopped worker. Each worker is an `ActorRef<A>`, where `A` implements the `Actor` trait.
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
        A: Actor + Send + Sync + 'static,
    {
        assert_ne!(size, 0);

        let workers = (0..size).map(|_| factory()).collect();

        ActorPool {
            workers,
            size,
            next_idx: 0,
            factory: Box::new(factory),
        }
    }

    /// Sends a message to a worker in the pool, waiting for a reply.
    pub async fn send<M>(
        &mut self,
        mut msg: M,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>
    where
        A: Actor + Message<M>,
        M: Send + 'static,
    {
        for _ in 0..self.workers.len() * 2 {
            let (idx, worker) = self.next_worker();
            match worker.send(msg).await {
                Err(SendError::ActorNotRunning(v)) => {
                    msg = v;
                    warn!(id = %worker.id(), name = %A::name(), "restarting worker");
                    self.workers[idx] = (self.factory)();
                }
                res => return res,
            }
        }

        Err(SendError::ActorNotRunning(msg))
    }

    /// Sends a message asyncronously to a worker in the pool.
    pub fn send_async<M>(&mut self, mut msg: M) -> Result<(), SendError<M>>
    where
        A: Actor + Message<M>,
        M: Send + 'static,
    {
        for _ in 0..self.workers.len() * 2 {
            let (idx, worker) = self.next_worker();
            match worker.send_async(msg) {
                Err(SendError::ActorNotRunning(v)) => {
                    msg = v;
                    warn!(id = %worker.id(), name = %A::name(), "restarting worker");
                    self.workers[idx] = (self.factory)();
                }
                res => return res,
            }
        }

        Err(SendError::ActorNotRunning(msg))
    }

    /// Broadcasts a message to all workers.
    pub async fn broadcast<M>(
        &mut self,
        msg: M,
    ) -> Vec<Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>>
    where
        A: Actor + Message<M>,
        M: Clone + Send + 'static,
    {
        let results = join_all(self.workers.iter().map(|worker| worker.send(msg.clone()))).await;
        for (i, res) in results.iter().enumerate() {
            if let Err(SendError::ActorNotRunning(_)) = res {
                warn!(id = %self.workers[i].id(), name = %A::name(), "restarting worker");
                self.workers[i] = (self.factory)();
            }
        }

        results
    }

    /// Broadcasts a message to all workers.
    pub fn broadcast_async<M>(&mut self, msg: M) -> Vec<Result<(), SendError<M>>>
    where
        A: Actor + Message<M>,
        M: Clone + Send + 'static,
    {
        let results: Vec<_> = self
            .workers
            .iter()
            .map(|worker| worker.send_async(msg.clone()))
            .collect();
        for (i, res) in results.iter().enumerate() {
            if let Err(SendError::ActorNotRunning(_)) = res {
                warn!(id = %self.workers[i].id(), name = %A::name(), "restarting worker");
                self.workers[i] = (self.factory)();
            }
        }

        results
    }

    fn next_worker(&mut self) -> (usize, &ActorRef<A>) {
        let idx = self.next_idx;
        let worker = &self.workers[self.next_idx];
        self.next_idx = (self.next_idx + 1) % self.workers.len();
        (idx, worker)
    }
}

impl<A> fmt::Debug for ActorPool<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorPool")
            .field("workers", &self.workers)
            .field("size", &self.size)
            .field("next_idx", &self.next_idx)
            .finish()
    }
}
