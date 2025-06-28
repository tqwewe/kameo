//! Provides a type-based message bus for the actor system.
//!
//! The `message_bus` module implements a type-based publish/subscribe mechanism that allows
//! actors to communicate based on message types rather than direct references or topics.
//! Messages are automatically routed to actors that have registered to receive that specific type.
//!
//! # Features
//!
//! - **Type-Based Routing**: Messages are automatically routed based on their type.
//! - **Automatic Type Inference**: No need to specify message types explicitly when publishing.
//! - **Multiple Delivery Strategies**: Configure how messages are delivered to handle different reliability needs.
//! - **Automatic Cleanup**: Dead actor references are automatically removed from subscription lists.
//!
//! # Example
//!
//! ```
//! use kameo::Actor;
//! use kameo_actors::message_bus::{MessageBus, Register, Publish};
//! use kameo_actors::DeliveryStrategy;
//! # use kameo::message::{Context, Message};
//!
//! #[derive(Clone)]
//! struct TemperatureUpdate(f32);
//!
//! #[derive(Actor)]
//! struct TemperatureSensor;
//!
//! #[derive(Actor)]
//! struct DisplayActor;
//!
//! # impl Message<TemperatureUpdate> for DisplayActor {
//! #     type Reply = ();
//! #     async fn handle(&mut self, msg: TemperatureUpdate, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply { }
//! # }
//!
//! # tokio_test::block_on(async {
//! // Create a message bus with best effort delivery
//! let message_bus = MessageBus::new(DeliveryStrategy::BestEffort);
//! let message_bus_ref = MessageBus::spawn(message_bus);
//!
//! // Create a display actor and register it for temperature updates
//! let display = DisplayActor::spawn(DisplayActor);
//! message_bus_ref.tell(Register(display.recipient::<TemperatureUpdate>())).await?;
//!
//! // Publish a temperature update - automatically routes to all registered handlers
//! message_bus_ref.tell(Publish(TemperatureUpdate(22.5))).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
};

use kameo::prelude::*;

use crate::DeliveryStrategy;

/// A type-based message bus for broadcasting messages to registered actors.
///
/// The `MessageBus` routes messages to actors based on the message type. Actors register
/// to receive specific message types, and the bus automatically delivers messages to all
/// registered recipients when a message of that type is published.
///
/// Messages are delivered according to the configured delivery strategy, allowing
/// for different reliability and performance trade-offs.
#[derive(Actor, Debug, Default)]
pub struct MessageBus {
    subscriptions: HashMap<TypeId, Vec<Registration>>,
    delivery_strategy: DeliveryStrategy,
}

impl MessageBus {
    /// Creates a new message bus with the specified delivery strategy.
    ///
    /// # Arguments
    ///
    /// * `delivery_strategy` - Determines how messages are delivered to subscribers
    ///
    /// # Returns
    ///
    /// A new `MessageBus` instance with the specified delivery strategy
    pub fn new(delivery_strategy: DeliveryStrategy) -> Self {
        MessageBus {
            subscriptions: HashMap::new(),
            delivery_strategy,
        }
    }

    fn unsubscribe<M: 'static>(&mut self, actor_id: &ActorID) {
        let type_id = TypeId::of::<M>();
        if let Some(recipients) = self.subscriptions.get_mut(&type_id) {
            recipients.retain(|reg| &reg.actor_id != actor_id);
            if recipients.is_empty() {
                self.subscriptions.remove(&type_id);
            }
        }
    }
}

/// Message for registering an actor to receive messages of a specific type.
///
/// When an actor is registered with the message bus using this message, it will
/// receive all future messages of the specified type that are published to the bus.
#[derive(Clone, Debug)]
pub struct Register<M: Send + 'static>(pub Recipient<M>);

impl<M: Send + 'static> Message<Register<M>> for MessageBus {
    type Reply = ();

    async fn handle(
        &mut self,
        Register(recipient): Register<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscriptions
            .entry(TypeId::of::<M>())
            .or_default()
            .push(Registration::new(recipient));
    }
}

/// Message for unregistering an actor from receiving messages of a specific type.
///
/// When an actor is unregistered, it will no longer receive messages of the
/// specified type from the message bus.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unregister<M> {
    actor_id: ActorID,
    phantom: PhantomData<M>,
}

impl<M> Unregister<M> {
    /// Creates a new `Unregister` message for the specified actor.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The ID of the actor to unregister
    ///
    /// # Returns
    ///
    /// A new `Unregister` message that can be sent to the message bus
    pub fn new(actor_id: ActorID) -> Self {
        Unregister {
            actor_id,
            phantom: PhantomData,
        }
    }
}

impl<M: Send + 'static> Message<Unregister<M>> for MessageBus {
    type Reply = ();

    async fn handle(
        &mut self,
        Unregister { actor_id, .. }: Unregister<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.unsubscribe::<M>(&actor_id);
    }
}

/// Message for publishing a value to all registered actors.
///
/// When a message is published using this wrapper, it will be delivered to all
/// actors that have registered to receive messages of this type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publish<M>(pub M);

impl<M: Clone + Send + 'static> Message<Publish<M>> for MessageBus {
    type Reply = ();

    async fn handle(
        &mut self,
        Publish(message): Publish<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut to_remove = Vec::new();

        if let Some(registrations) = self.subscriptions.get(&TypeId::of::<M>()) {
            for Registration {
                actor_id,
                recipient,
            } in registrations
            {
                let recipient: &Recipient<M> = recipient.downcast_ref().unwrap();
                match self.delivery_strategy {
                    DeliveryStrategy::Guaranteed => {
                        let res = recipient.tell(message.clone()).await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_remove.push(*actor_id);
                        }
                    }
                    DeliveryStrategy::BestEffort => {
                        let res = recipient.tell(message.clone()).try_send();
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_remove.push(*actor_id);
                        }
                    }
                    DeliveryStrategy::TimedDelivery(duration) => {
                        let res = recipient
                            .tell(message.clone())
                            .mailbox_timeout(duration)
                            .await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_remove.push(*actor_id);
                        }
                    }
                    DeliveryStrategy::Spawned => {
                        let actor_id = *actor_id;
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let message_bus_ref = ctx.actor_ref();
                        tokio::spawn(async move {
                            let res = recipient.tell(message).send().await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = message_bus_ref.tell(Unregister::<M>::new(actor_id)).await;
                            }
                        });
                    }
                    DeliveryStrategy::SpawnedWithTimeout(duration) => {
                        let actor_id = *actor_id;
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let message_bus_ref = ctx.actor_ref();
                        tokio::spawn(async move {
                            let res = recipient
                                .tell(message)
                                .mailbox_timeout(duration)
                                .send()
                                .await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = message_bus_ref.tell(Unregister::<M>::new(actor_id)).await;
                            }
                        });
                    }
                }
            }
        }

        for actor_id in to_remove {
            self.unsubscribe::<M>(&actor_id);
        }
    }
}

#[derive(Debug)]
struct Registration {
    actor_id: ActorID,
    recipient: Box<dyn Any + Send + Sync>,
}

impl Registration {
    fn new<M: Send + 'static>(recipient: Recipient<M>) -> Self {
        Registration {
            actor_id: recipient.id(),
            recipient: Box::new(recipient),
        }
    }
}
