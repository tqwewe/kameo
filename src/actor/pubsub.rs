//! Provides a publish-subscribe (pubsub) mechanism for actors.
//!
//! The `pubsub` module allows actors to broadcast messages to multiple subscribers. It offers
//! a lightweight pubsub actor that can manage multiple subscriptions and publish messages to
//! all subscribed actors simultaneously. This is useful in scenarios where multiple actors need
//! to react to the same event or data.
//!
//! `PubSub` can be used either as a standalone object or as a spawned actor. When spawned as an actor,
//! the `Publish(msg)` and `Subscribe(actor_ref)` messages are used to interact with it.
//!
//! # Features
//! - **Publish-Subscribe Pattern**: Actors can subscribe to the `PubSub` actor to receive broadcast messages.
//! - **Message Broadcasting**: Messages published to the `PubSub` actor are sent to all subscribed actors.
//! - **Subscriber Management**: Actors can subscribe and unsubscribe dynamically, allowing flexible message routing.
//!
//! # Example
//!
//! ```
//! use kameo::Actor;
//! use kameo::actor::pubsub::{PubSub, Publish, Subscribe};
//! # use kameo::message::{Context, Message};
//! use kameo::request::MessageSend;
//!
//! #[derive(Actor)]
//! struct MyActor;
//! #
//! # impl Message<&'static str> for MyActor {
//! #     type Reply = ();
//! #     async fn handle(&mut self, msg: &'static str, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply { }
//! # }
//!
//! # tokio_test::block_on(async {
//! let mut pubsub = PubSub::new();
//! let actor_ref = kameo::spawn(MyActor);
//!
//! // Use PubSub as a standalone object
//! pubsub.subscribe(actor_ref.clone());
//! pubsub.publish("Hello, World!").await;
//!
//! // Or spawn PubSub as an actor and use messages
//! let pubsub_actor_ref = kameo::spawn(PubSub::new());
//! pubsub_actor_ref.tell(Subscribe(actor_ref)).send().await?;
//! pubsub_actor_ref.tell(Publish("Hello, spawned world!")).send().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::collections::HashMap;

use futures::future::{join_all, BoxFuture};

use crate::{
    error::SendError,
    mailbox::bounded::BoundedMailbox,
    message::{Context, Message},
    request::{LocalTellRequest, MessageSend, TellRequest, WithoutRequestTimeout},
    Actor, Reply,
};

use super::{ActorID, ActorRef};

/// A publish-subscribe (pubsub) actor that allows message broadcasting to multiple subscribers.
///
/// `PubSub` can be used as a standalone object or spawned as an actor. When spawned, messages can
/// be sent using the `Publish(msg)` and `Subscribe(actor_ref)` messages to publish data and manage subscribers.
/// This provides flexibility in how you interact with the pubsub system, depending on whether you want
/// to manage it directly or interact with it via messages.
#[allow(missing_debug_implementations)]
pub struct PubSub<M> {
    subscribers: HashMap<ActorID, Box<dyn MessageSubscriber<M> + Send + Sync>>,
}

impl<M> PubSub<M> {
    /// Creates a new pubsub instance.
    ///
    /// This initializes the pubsub actor with an empty list of subscribers.
    pub fn new() -> Self {
        PubSub {
            subscribers: HashMap::new(),
        }
    }

    /// Publishes a message to all subscribed actors.
    ///
    /// The message is cloned and sent to each subscriber. Any actor subscribed to the `PubSub` actor
    /// will receive a copy of the message.
    ///
    /// # Example
    ///
    /// ```
    /// use kameo::actor::pubsub::PubSub;
    ///
    /// #[derive(Clone)]
    /// struct Msg(String);
    ///
    /// # tokio_test::block_on(async {
    /// let mut pubsub = PubSub::new();
    /// pubsub.publish(Msg("Hello!".to_string())).await;
    /// # })
    /// ```
    ///
    /// # Requirements
    /// The message type `M` must implement `Clone` and `Send`, since it needs to be duplicated for each subscriber.
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
                | Err(SendError::Timeout(_)) => {}
            }
        }
    }

    /// Subscribes an actor to receive all messages published by the pubsub actor.
    ///
    /// Once subscribed, the actor will receive all messages sent to the pubsub actor via the `publish` method.
    /// The actor reference is stored in the list of subscribers, and messages are sent to the actor asynchronously.
    ///
    /// # Example
    ///
    /// ```
    /// # use kameo::Actor;
    /// use kameo::actor::pubsub::PubSub;
    /// # use kameo::message::{Context, Message};
    ///
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # impl Message<Msg> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: Msg, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// #[derive(Clone)]
    /// struct Msg(String);
    ///
    /// # tokio_test::block_on(async {
    /// let mut pubsub = PubSub::new();
    ///
    /// let actor_ref = kameo::spawn(MyActor);
    /// pubsub.subscribe(actor_ref);
    /// # })
    /// ```
    #[inline]
    pub fn subscribe<A>(&mut self, actor_ref: ActorRef<A>)
    where
        A: Actor + Message<M>,
        M: Send + 'static,
        for<'a> TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, WithoutRequestTimeout>:
            MessageSend<Ok = (), Error = SendError<M, <A::Reply as Reply>::Error>>,
    {
        self.subscribers.insert(actor_ref.id(), Box::new(actor_ref));
    }
}

impl<M: 'static> Actor for PubSub<M> {
    type Mailbox = BoundedMailbox<Self>;
}

impl<M> Default for PubSub<M> {
    fn default() -> Self {
        PubSub::new()
    }
}

/// A message used to publish data to a `PubSub` actor.
///
/// This struct wraps a message of type `M` and is used when sending a message to a pubsub actor.
/// When this message is received, it is broadcast to all subscribers.
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

/// A message used to subscribe an actor to a `PubSub` actor.
///
/// This struct wraps an `ActorRef` and is used to subscribe an actor to a pubsub actor. Once subscribed,
/// the actor will receive all published messages from the pubsub actor.
#[derive(Clone, Debug)]
pub struct Subscribe<A: Actor>(pub ActorRef<A>);

impl<A, M> Message<Subscribe<A>> for PubSub<M>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    for<'a> TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, WithoutRequestTimeout>:
        MessageSend<Ok = (), Error = SendError<M, <A::Reply as Reply>::Error>>,
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
    for<'a> TellRequest<LocalTellRequest<'a, A, Mb>, Mb, M, WithoutRequestTimeout>:
        MessageSend<Ok = (), Error = SendError<M, <A::Reply as Reply>::Error>>,
{
    fn tell(&self, msg: M) -> BoxFuture<'_, Result<(), SendError<M, ()>>> {
        Box::pin(async move {
            self.tell(msg)
                .send()
                .await
                .map_err(|err| err.map_err(|_| ()))
        })
    }
}
