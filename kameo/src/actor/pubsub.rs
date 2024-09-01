use std::collections::HashMap;

use futures::future::{join_all, BoxFuture};

use crate::{
    error::SendError,
    message::{Context, Message},
    request::Request,
    Actor,
};

use super::{ActorID, ActorRef, BoundedMailbox};

/// A mpsc-like pubsub actor.
#[allow(missing_debug_implementations)]
pub struct PubSub<M> {
    subscribers: HashMap<ActorID, Box<dyn MessageSubscriber<M> + Send + Sync>>,
}

impl<M> PubSub<M> {
    /// Creates a new pubsub instance.
    pub fn new() -> Self {
        PubSub {
            subscribers: HashMap::new(),
        }
    }

    /// Publishes a message to all subscribers.
    pub async fn publish(&mut self, msg: M)
    where
        M: Clone + Send + 'static,
    {
        let results = join_all(self.subscribers.iter().map(|(id, subscriber)| {
            let msg = msg.clone();
            async move { (*id, subscriber.tell(msg).await) }
        }))
        .await;
        for (id, result) in results.into_iter() {
            match result {
                Ok(_) => {}
                Err(SendError::ActorNotRunning(_)) | Err(SendError::ActorStopped) => {
                    self.subscribers.remove(&id);
                }
                Err(SendError::MailboxFull(_))
                | Err(SendError::HandlerError(_))
                | Err(SendError::Timeout(_))
                | Err(SendError::QueriesNotSupported) => {}
            }
        }
    }

    /// Subscribes an actor receive all messages published.
    #[inline]
    pub fn subscribe<A>(&mut self, actor_ref: ActorRef<A>)
    where
        A: Actor + Message<M>,
        M: Send + 'static,
        ActorRef<A>: Request<A, M, A::Mailbox>,
    {
        self.subscribers.insert(actor_ref.id(), Box::new(actor_ref));
    }
}

impl<M> Actor for PubSub<M> {
    type Mailbox = BoundedMailbox<Self>;
}

impl<M> Default for PubSub<M> {
    fn default() -> Self {
        PubSub::new()
    }
}

/// Publishes a message to a pubsub actor.
#[derive(Clone, Debug)]
pub struct Publish<M>(pub M);

impl<M> Message<Publish<M>> for PubSub<M>
where
    M: Clone + Send + 'static,
{
    type Reply = ();

    async fn handle(
        &mut self,
        Publish(msg): Publish<M>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.publish(msg).await
    }
}

/// Subscribes an actor to a pubsub actor.
#[derive(Clone, Debug)]
pub struct Subscribe<A: Actor>(pub ActorRef<A>);

impl<A, M> Message<Subscribe<A>> for PubSub<M>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    ActorRef<A>: Request<A, M, A::Mailbox>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        Subscribe(actor_ref): Subscribe<A>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscribe(actor_ref)
    }
}

trait MessageSubscriber<M> {
    fn tell(&self, msg: M) -> BoxFuture<'_, Result<(), SendError<M, ()>>>;
}

impl<A, M, Mb> MessageSubscriber<M> for ActorRef<A>
where
    A: Actor<Mailbox = Mb> + Message<M>,
    M: Send + 'static,
    Mb: Sync,
    ActorRef<A>: Request<A, M, Mb>,
{
    fn tell(&self, msg: M) -> BoxFuture<'_, Result<(), SendError<M, ()>>> {
        Box::pin(async move {
            Request::tell(self, msg, None, false)
                .await
                .map_err(|err| err.map_err(|_| ()))
        })
    }
}
