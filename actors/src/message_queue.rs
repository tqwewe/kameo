//! Provides an AMQP-style message queue system for the actor system.
//!
//! The `message_queue` module implements a flexible message queue system inspired by AMQP, with
//! support for different exchange types (direct, topic, fanout, headers) and queue semantics.
//! It allows actors to communicate through named queues and exchanges with various routing rules.
//!
//! # Features
//!
//! - **Multiple Exchange Types**: Supports direct, topic, fanout, and header-based routing.
//! - **Flexible Routing**: Messages can be routed based on routing keys or header matching.
//! - **Queue Management**: Supports queue declaration, binding, and automatic cleanup.
//! - **Multiple Delivery Strategies**: Configure how messages are delivered to handle different reliability needs.
//! - **Automatic Cleanup**: Dead actor references are automatically removed from consumer lists.
//!
//! # Example
//!
//! ```
//! use kameo::Actor;
//! use kameo_actors::message_queue::{MessageQueue, ExchangeDeclare, QueueDeclare, QueueBind, BasicPublish, BasicConsume, ExchangeType};
//! use kameo_actors::{DeliveryStrategy};
//! use std::collections::HashMap;
//! # use kameo::message::{Context, Message};
//!
//! #[derive(Clone)]
//! struct OrderEvent {
//!     product_id: String,
//!     quantity: u32,
//! }
//!
//! #[derive(Actor)]
//! struct OrderProcessor;
//!
//! # impl Message<OrderEvent> for OrderProcessor {
//! #     type Reply = ();
//! #     async fn handle(&mut self, msg: OrderEvent, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply { }
//! # }
//!
//! # tokio_test::block_on(async {
//! // Create a message queue with best effort delivery
//! let mq = MessageQueue::new(DeliveryStrategy::BestEffort);
//! let mq_ref = MessageQueue::spawn(mq);
//!
//! // Set up exchange and queue
//! mq_ref.tell(ExchangeDeclare {
//!     exchange: "orders".to_string(),
//!     kind: ExchangeType::Topic,
//!     auto_delete: false,
//! }).await?;
//! mq_ref.tell(QueueDeclare {
//!     queue: "order_processing".to_string(),
//!     auto_delete: false,
//! }).await?;
//! mq_ref.tell(QueueBind {
//!     queue: "order_processing".to_string(),
//!     exchange: "orders".to_string(),
//!     routing_key: "order.*".to_string(),
//!     arguments: Default::default(),
//! }).await?;
//!
//! // Register a consumer
//! let processor = OrderProcessor::spawn(OrderProcessor);
//! mq_ref.tell(BasicConsume {
//!     queue: "order_processing".to_string(),
//!     recipient: processor.recipient::<OrderEvent>(),
//!     tags: Default::default(),
//! }).await?;
//!
//! // Publish an order event
//! mq_ref.tell(BasicPublish {
//!     exchange: "orders".to_string(),
//!     routing_key: "order.created".to_string(),
//!     message: OrderEvent {
//!         product_id: "123".to_string(),
//!         quantity: 2,
//!     },
//!     properties: Default::default(),
//! }).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet, hash_map::Entry},
};

use crate::DeliveryStrategy;
use glob::{MatchOptions, Pattern};
use kameo::prelude::*;

pub type FilterFn = fn(&HashMap<String, String>) -> bool;

/// The main message queue actor that manages exchanges, queues and message routing.
///
/// This actor implements AMQP-style messaging semantics with support for:
/// - Multiple exchange types (direct, topic, fanout, headers)
/// - Queue declarations and bindings
/// - Message publishing and consumption
/// - Automatic cleanup of unused resources
///
/// Messages are delivered according to the configured delivery strategy, allowing
/// for different reliability and performance trade-offs.
#[derive(Actor, Debug)]
pub struct MessageQueue {
    /// Registered exchanges by name
    exchanges: HashMap<String, Exchange>,
    /// Registered queues by name
    queues: HashMap<String, Queue>,
    /// The default direct exchange
    default_exchange: Exchange,
    /// Delivery strategy for messages
    delivery_strategy: DeliveryStrategy,
}

/// Properties associated with a published message
#[derive(Debug, Clone, Default)]
pub struct MessageProperties {
    /// Optional headers for header-based routing
    pub headers: Option<HashMap<String, String>>,
    /// Optional filter function for message routing based on consumer tags
    /// The function takes a reference to the consumer's tags and returns true if the message should be delivered
    pub filter: Option<FilterFn>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExchangeType {
    /// Direct exchange - routes based on exact routing key match
    #[default]
    Direct,
    /// Topic exchange - routes based on pattern matching
    Topic,
    /// Fanout exchange - routes to all bound queues
    Fanout,
    /// Headers exchange - routes based on header matching
    Headers,
}

/// Internal enum for header matching rules
#[derive(Debug, Clone)]
enum HeaderMatch {
    /// All specified headers must match
    All(HashMap<String, String>),
    /// Any specified header must match
    Any(HashMap<String, String>),
}

impl HeaderMatch {
    /// Checks if the given headers match the rules
    pub fn matches(&self, headers: &HashMap<String, String>) -> bool {
        match self {
            HeaderMatch::All(rules) => rules
                .iter()
                .all(|(k, v)| headers.get(k).is_some_and(|val| val == v)),
            HeaderMatch::Any(rules) => rules
                .iter()
                .any(|(k, v)| headers.get(k).is_some_and(|val| val == v)),
        }
    }
}

/// Represents an exchange in the message queue
#[derive(Debug)]
struct Exchange {
    /// Exchange name
    name: String,
    /// Exchange type
    kind: ExchangeType,
    /// Whether to auto-delete when unused
    auto_delete: bool,
    /// List of queue bindings
    bindings: Vec<Binding>,
}

/// Represents a binding between an exchange and queue
#[derive(Debug)]
struct Binding {
    /// Name of bound queue
    queue_name: String,
    /// Routing key or pattern
    routing_key: String,
    /// Optional header matching rules
    header_match: Option<HeaderMatch>,
}

/// Represents a message queue
#[derive(Debug)]
struct Queue {
    /// Whether to auto-delete when unused
    auto_delete: bool,
    /// Registered consumers by message type
    recipients: HashMap<TypeId, Vec<Registration>>,
}

/// Represents a registered message consumer
#[derive(Debug)]
struct Registration {
    /// ID of consuming actor
    actor_id: ActorID,
    /// Typed recipient for messages
    recipient: Box<dyn Any + Send + Sync>,
    /// Key-value tags associated with this consumer that can be used for filtered message delivery
    /// These tags are checked against the message's filter function (if provided) to determine
    /// whether this consumer should receive the message
    tags: HashMap<String, String>,
}

/// Error types for message queue operations
#[derive(Debug, thiserror::Error)]
pub enum AmqpError {
    #[error("Exchange already exists")]
    ExchangeAlreadyExists,
    #[error("Queue already exists")]
    QueueAlreadyExists,
    #[error("Exchange not found")]
    ExchangeNotFound,
    #[error("Queue not found")]
    QueueNotFound,
    #[error("Binding already exists")]
    BindingAlreadyExists,
    #[error("Headers required")]
    HeadersRequired,
    #[error("Invalid header match")]
    InvalidHeaderMatch,
    #[error("Exchange in use")]
    ExchangeInUse,
    #[error("Queue in use")]
    QueueInUse,
}

/// Message for declaring a new exchange
#[derive(Debug, Default)]
pub struct ExchangeDeclare {
    /// Exchange name
    pub exchange: String,
    /// Exchange type
    pub kind: ExchangeType,
    /// Whether to auto-delete when unused
    pub auto_delete: bool,
}

/// Message for deleting an exchange
#[derive(Debug, Default)]
pub struct ExchangeDelete {
    /// Exchange name
    pub exchange: String,
    /// Only delete if unused
    pub if_unused: bool,
}

/// Message for declaring a new queue
#[derive(Debug, Default)]
pub struct QueueDeclare {
    /// Queue name
    pub queue: String,
    /// Whether to auto-delete when unused
    pub auto_delete: bool,
}

/// Message for deleting a queue
#[derive(Debug, Default)]
pub struct QueueDelete {
    /// Queue name
    pub queue: String,
    /// Only delete if unused
    pub if_unused: bool,
}

/// Message for binding a queue to an exchange
#[derive(Debug, Default)]
pub struct QueueBind {
    /// Queue name
    pub queue: String,
    /// Exchange name
    pub exchange: String,
    /// Routing key/pattern
    pub routing_key: String,
    /// Additional binding arguments
    pub arguments: HashMap<String, String>,
}

/// Message for unbinding a queue from an exchange
#[derive(Debug, Default)]
pub struct QueueUnbind {
    /// Queue name
    pub queue: String,
    /// Exchange name
    pub exchange: String,
    /// Routing key/pattern
    pub routing_key: String,
}

/// Message for publishing a message to an exchange
#[derive(Debug)]
pub struct BasicPublish<M>
where
    M: Clone + Send + 'static,
{
    /// Exchange name (empty for default)
    pub exchange: String,
    /// Routing key
    pub routing_key: String,
    /// Message payload
    pub message: M,
    /// Message properties
    pub properties: MessageProperties,
}

/// Message for consuming messages from a queue
#[derive(Debug)]
pub struct BasicConsume<M: Send + 'static> {
    /// Queue name
    pub queue: String,
    /// Recipient for messages
    pub recipient: Recipient<M>,
    /// Tags associated with this consumer that can be used for filtered message delivery
    /// These tags will be passed to the message's filter function (if provided) when determining
    /// whether this consumer should receive published messages
    pub tags: HashMap<String, String>,
}

/// Message for canceling a consumer
#[derive(Debug)]
pub struct BasicCancel<M: Send + 'static> {
    /// Queue name
    pub queue: String,
    /// Recipient to cancel
    pub recipient: Recipient<M>,
}

impl MessageQueue {
    /// Creates a new message queue with the specified delivery strategy.
    ///
    /// # Arguments
    ///
    /// * `delivery_strategy` - Determines how messages are delivered to consumers
    ///
    /// # Returns
    ///
    /// A new `MessageQueue` instance with the specified delivery strategy
    pub fn new(delivery_strategy: DeliveryStrategy) -> Self {
        Self {
            exchanges: HashMap::new(),
            queues: HashMap::new(),
            default_exchange: Exchange {
                name: "".to_string(),
                kind: ExchangeType::Direct,
                auto_delete: false,
                bindings: Vec::new(),
            },
            delivery_strategy,
        }
    }

    fn queue_delete(&mut self, queue_name: String, if_unused: bool) -> Result<(), AmqpError> {
        match self.queues.get(&queue_name) {
            Some(queue) => {
                if if_unused && !queue.recipients.is_empty() {
                    return Err(AmqpError::QueueInUse);
                }
                self.queues.remove(&queue_name);
            }
            None => {
                return Err(AmqpError::QueueNotFound);
            }
        }

        self.default_exchange
            .bindings
            .retain(|b| b.queue_name != queue_name);

        let mut exchanges_to_delete = Vec::new();
        for exchange in self.exchanges.values_mut() {
            exchange.bindings.retain(|b| b.queue_name != queue_name);
            if exchange.bindings.is_empty() && exchange.auto_delete {
                exchanges_to_delete.push(exchange.name.clone());
            }
        }
        for exchange_name in exchanges_to_delete {
            self.exchanges.remove(&exchange_name);
        }
        Ok(())
    }

    fn basic_cancel<M: Send + 'static>(
        &mut self,
        queue_name: String,
        recipient: Recipient<M>,
    ) -> Result<(), AmqpError> {
        let queue_delete = {
            let queue = self
                .queues
                .get_mut(&queue_name)
                .ok_or(AmqpError::QueueNotFound)?;
            let type_id = TypeId::of::<M>();
            if let Some(recipients) = queue.recipients.get_mut(&type_id) {
                recipients.retain(|registration| registration.actor_id != recipient.id());
            }
            queue.recipients.retain(|_, v| !v.is_empty());

            queue.auto_delete
        };

        if queue_delete {
            self.queue_delete(queue_name, true)?;
        }

        Ok(())
    }

    async fn delivery_message<M>(
        &mut self,
        queue_name: String,
        message: &M,
        self_ref: ActorRef<Self>,
        filter: impl Fn(&HashMap<String, String>) -> bool,
    ) where
        M: Clone + Send + 'static,
    {
        let mut to_cancel = Vec::new();
        if let Some(recipients) = self
            .queues
            .get(&queue_name)
            .and_then(|queue| queue.recipients.get(&TypeId::of::<M>()))
        {
            for regis in recipients.iter().filter(|regis| filter(&regis.tags)) {
                let queue_name = queue_name.clone();
                let recipient: &Recipient<M> = regis.recipient.downcast_ref().unwrap();
                match self.delivery_strategy {
                    DeliveryStrategy::Guaranteed => {
                        let res = recipient.tell(message.clone()).await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_cancel.push(recipient.clone());
                        }
                    }
                    DeliveryStrategy::BestEffort => {
                        let res = recipient.tell(message.clone()).try_send();
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_cancel.push(recipient.clone());
                        }
                    }
                    DeliveryStrategy::TimedDelivery(duration) => {
                        let res = recipient
                            .tell(message.clone())
                            .mailbox_timeout(duration)
                            .await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_cancel.push(recipient.clone());
                        }
                    }
                    DeliveryStrategy::Spawned => {
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let self_ref = self_ref.clone();
                        tokio::spawn(async move {
                            let res = recipient.tell(message).send().await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = self_ref
                                    .tell(BasicCancel::<M> {
                                        queue: queue_name.clone(),
                                        recipient: recipient.clone(),
                                    })
                                    .await;
                            }
                        });
                    }
                    DeliveryStrategy::SpawnedWithTimeout(duration) => {
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let self_ref = self_ref.clone();
                        tokio::spawn(async move {
                            let res = recipient
                                .tell(message)
                                .mailbox_timeout(duration)
                                .send()
                                .await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = self_ref
                                    .tell(BasicCancel::<M> {
                                        queue: queue_name.clone(),
                                        recipient: recipient.clone(),
                                    })
                                    .await;
                            }
                        });
                    }
                }
            }
        }

        for recipient in to_cancel {
            let _ = self.basic_cancel::<M>(queue_name.clone(), recipient);
        }
    }
}

impl Message<ExchangeDeclare> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: ExchangeDeclare,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if msg.exchange.is_empty() || self.exchanges.contains_key(&msg.exchange) {
            return Err(AmqpError::ExchangeAlreadyExists);
        }

        self.exchanges.insert(
            msg.exchange.clone(),
            Exchange {
                name: msg.exchange,
                kind: msg.kind,
                auto_delete: msg.auto_delete,
                bindings: Vec::new(),
            },
        );
        Ok(())
    }
}

impl Message<ExchangeDelete> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: ExchangeDelete,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self.exchanges.get(&msg.exchange) {
            Some(exchange) => {
                if msg.if_unused && !exchange.bindings.is_empty() {
                    return Err(AmqpError::ExchangeInUse);
                } else {
                    self.exchanges.remove(&msg.exchange);
                }
            }
            None => {
                return Err(AmqpError::ExchangeNotFound);
            }
        }

        Ok(())
    }
}

impl Message<QueueDeclare> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueDeclare,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.queues.contains_key(&msg.queue) {
            return Err(AmqpError::QueueAlreadyExists);
        }

        self.queues.insert(
            msg.queue.clone(),
            Queue {
                auto_delete: msg.auto_delete,
                recipients: HashMap::new(),
            },
        );

        self.default_exchange.bindings.push(Binding {
            queue_name: msg.queue.clone(),
            routing_key: msg.queue.clone(),
            header_match: None,
        });
        Ok(())
    }
}

impl Message<QueueDelete> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueDelete,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.queue_delete(msg.queue.clone(), msg.if_unused)?;
        Ok(())
    }
}

impl Message<QueueBind> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueBind,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if !self.queues.contains_key(&msg.queue) {
            return Err(AmqpError::QueueNotFound);
        }

        let exchange = self
            .exchanges
            .get_mut(&msg.exchange)
            .ok_or(AmqpError::ExchangeNotFound)?;

        if exchange
            .bindings
            .iter()
            .any(|b| b.queue_name == msg.queue && b.routing_key == msg.routing_key)
        {
            return Err(AmqpError::BindingAlreadyExists);
        }

        let header_match = if exchange.kind == ExchangeType::Headers {
            let x_match = msg
                .arguments
                .get("x-match")
                .map(|s| s.as_str())
                .unwrap_or("all");

            let mut match_args = msg.arguments.clone();
            match_args.retain(|key, _| !key.starts_with("x-"));

            match x_match {
                "all" => Some(HeaderMatch::All(match_args)),
                "any" => Some(HeaderMatch::Any(match_args)),
                _ => return Err(AmqpError::InvalidHeaderMatch),
            }
        } else {
            None
        };

        exchange.bindings.push(Binding {
            queue_name: msg.queue,
            routing_key: msg.routing_key,
            header_match,
        });
        Ok(())
    }
}

impl Message<QueueUnbind> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: QueueUnbind,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut entry = match self.exchanges.entry(msg.exchange.clone()) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => return Err(AmqpError::ExchangeNotFound),
        };

        let exchange = entry.get_mut();
        exchange
            .bindings
            .retain(|b| !(b.queue_name == msg.queue && b.routing_key == msg.routing_key));

        if exchange.bindings.is_empty() && exchange.auto_delete {
            entry.remove();
        }
        Ok(())
    }
}

impl<M> Message<BasicPublish<M>> for MessageQueue
where
    M: Clone + Send + Sync + 'static,
{
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: BasicPublish<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let exchange = if msg.exchange.is_empty() {
            &self.default_exchange
        } else if let Some(exchange) = self.exchanges.get(&msg.exchange) {
            exchange
        } else {
            return Err(AmqpError::ExchangeNotFound);
        };

        let filter = msg.properties.filter.unwrap_or(|_| true);
        let mut target_queues = HashSet::new();

        match exchange.kind {
            ExchangeType::Direct => {
                for binding in &exchange.bindings {
                    if binding.routing_key == msg.routing_key {
                        target_queues.insert(binding.queue_name.clone());
                    }
                }
            }
            ExchangeType::Topic => {
                let options = MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                };

                for binding in &exchange.bindings {
                    if Pattern::new(&binding.routing_key)
                        .unwrap()
                        .matches_with(&msg.routing_key, options)
                    {
                        target_queues.insert(binding.queue_name.clone());
                    }
                }
            }
            ExchangeType::Fanout => {
                for binding in &exchange.bindings {
                    target_queues.insert(binding.queue_name.clone());
                }
            }
            ExchangeType::Headers => {
                let message_headers = msg
                    .properties
                    .headers
                    .as_ref()
                    .ok_or(AmqpError::HeadersRequired)?;

                for binding in &exchange.bindings {
                    if let Some(header_match) = &binding.header_match {
                        if header_match.matches(message_headers) {
                            target_queues.insert(binding.queue_name.clone());
                        }
                    }
                }
            }
        }

        for queue_name in target_queues {
            self.delivery_message(queue_name, &msg.message, ctx.actor_ref(), filter)
                .await
        }

        Ok(())
    }
}

impl<M: Send + 'static> Message<BasicConsume<M>> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: BasicConsume<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let queue = self
            .queues
            .get_mut(&msg.queue)
            .ok_or(AmqpError::QueueNotFound)?;
        let actor_id = msg.recipient.id();
        let recipients = queue.recipients.entry(TypeId::of::<M>()).or_default();

        if !recipients.iter().any(|reg| reg.actor_id == actor_id) {
            recipients.push(Registration {
                actor_id,
                recipient: Box::new(msg.recipient),
                tags: msg.tags,
            });
        }
        Ok(())
    }
}

impl<M: Send + 'static> Message<BasicCancel<M>> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: BasicCancel<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.basic_cancel(msg.queue, msg.recipient)
    }
}
