use crate::DeliveryStrategy;
use glob::{MatchOptions, Pattern};
use kameo::prelude::*;
use std::collections::hash_map::Entry;
use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
};
use tokio::sync::RwLock;

#[derive(Actor, Debug)]
pub struct MessageQueue {
    exchanges: RwLock<HashMap<String, Exchange>>,
    queues: RwLock<HashMap<String, Queue>>,
    default_exchange: Exchange,
    delivery_strategy: DeliveryStrategy,
}

#[derive(Debug, Clone, Default)]
pub struct MessageProperties {
    pub headers: Option<HashMap<String, String>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExchangeType {
    #[default]
    Direct,
    Topic,
    Fanout,
    Headers,
}
#[derive(Debug, Clone)]
pub enum HeaderMatch {
    All(HashMap<String, String>),
    Any(HashMap<String, String>),
}

impl HeaderMatch {
    pub fn matches(&self, headers: &HashMap<String, String>) -> bool {
        match self {
            HeaderMatch::All(rules) => rules
                .iter()
                .all(|(k, v)| headers.get(k).map_or(false, |val| val == v)),
            HeaderMatch::Any(rules) => rules
                .iter()
                .any(|(k, v)| headers.get(k).map_or(false, |val| val == v)),
        }
    }
}

#[derive(Debug)]
struct Exchange {
    name: String,
    kind: ExchangeType,
    auto_delete: bool,
    bindings: Vec<Binding>,
}
#[derive(Debug)]
struct Binding {
    queue_name: String,
    routing_key: String,
    header_match: Option<HeaderMatch>,
}

#[derive(Debug)]
struct Queue {
    auto_delete: bool,
    recipients: HashMap<TypeId, Vec<Registration>>,
}

#[derive(Debug)]
struct Registration {
    actor_id: ActorID,
    recipient: Box<dyn Any + Send + Sync>,
}

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

#[derive(Debug, Default)]
pub struct ExchangeDeclare {
    pub exchange: String,
    pub kind: ExchangeType,
    pub auto_delete: bool,
}

#[derive(Debug, Default)]
pub struct ExchangeDelete {
    pub exchange: String,
    pub if_unused: bool,
}

#[derive(Debug, Default)]
pub struct QueueDeclare {
    pub queue: String,
    pub auto_delete: bool,
}

#[derive(Debug, Default)]
pub struct QueueDelete {
    pub queue: String,
    pub if_unused: bool,
}

#[derive(Debug, Default)]
pub struct QueueBind {
    pub queue: String,
    pub exchange: String,
    pub routing_key: String,
    pub arguments: HashMap<String, String>,
}

#[derive(Debug, Default)]
pub struct QueueUnbind {
    pub queue: String,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug)]
pub struct BasicPublish<M: Clone + Send + 'static> {
    pub exchange: String,
    pub routing_key: String,
    pub message: M,
    pub properties: MessageProperties,
}

pub struct BasicConsume<M: Send + 'static> {
    pub queue: String,
    pub recipient: Recipient<M>,
}

#[derive(Debug)]
pub struct BasicCancel<M: Send + 'static> {
    pub queue: String,
    pub recipient: Recipient<M>,
}

impl MessageQueue {
    pub fn new(delivery_strategy: DeliveryStrategy) -> Self {
        Self {
            exchanges: RwLock::new(HashMap::new()),
            queues: RwLock::new(HashMap::new()),
            default_exchange: Exchange {
                name: "".to_string(),
                kind: ExchangeType::Direct,
                auto_delete: false,
                bindings: Vec::new(),
            },
            delivery_strategy,
        }
    }

    async fn queue_delete(&self, queue_name: String, if_unused: bool) -> Result<(), AmqpError> {
        let mut queues = self.queues.write().await;

        match queues.get(&queue_name) {
            Some(queue) => {
                if if_unused && !queue.recipients.is_empty() {
                    return Err(AmqpError::QueueInUse);
                }
                queues.remove(&queue_name);
            }
            None => {
                return Err(AmqpError::QueueNotFound);
            }
        }

        let mut exchanges = self.exchanges.write().await;
        let mut to_delete = Vec::new();
        for exchange in exchanges.values_mut() {
            exchange.bindings.retain(|b| b.queue_name != queue_name);
            if exchange.bindings.is_empty() && exchange.auto_delete {
                to_delete.push(exchange.name.clone());
            }
        }
        for exchange_name in to_delete {
            exchanges.remove(&exchange_name);
        }
        Ok(())
    }

    async fn basic_cancel<M: Send + 'static>(
        &self,
        queue_name: String,
        recipient: Recipient<M>,
    ) -> Result<(), AmqpError> {
        let queue_delete = {
            let mut queues = self.queues.write().await;
            let queue = queues
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
            self.queue_delete(queue_name, true).await?;
        }

        Ok(())
    }

    async fn delivery_message<M: Clone + Send + 'static>(
        &self,
        queue_name: String,
        recipients: &Vec<Registration>,
        message: M,
        self_ref: ActorRef<Self>,
    ) {
        let mut to_cancel = Vec::new();
        for regis in recipients {
            let queue_name = queue_name.clone();
            let recipient: &Recipient<M> = regis.recipient.downcast_ref().unwrap();
            match self.delivery_strategy {
                DeliveryStrategy::Guaranteed => {
                    let res = recipient.tell(message.clone()).await;
                    if let Err(SendError::ActorNotRunning(_)) = res {
                        to_cancel.push(recipient);
                    }
                }
                DeliveryStrategy::BestEffort => {
                    let res = recipient.tell(message.clone()).try_send();
                    if let Err(SendError::ActorNotRunning(_)) = res {
                        to_cancel.push(recipient);
                    }
                }
                DeliveryStrategy::TimedDelivery(duration) => {
                    let res = recipient
                        .tell(message.clone())
                        .mailbox_timeout(duration)
                        .await;
                    if let Err(SendError::ActorNotRunning(_)) = res {
                        to_cancel.push(recipient);
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
        for recipient in to_cancel {
            let _ = self
                .basic_cancel::<M>(queue_name.clone(), recipient.clone())
                .await;
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
        let mut exchanges = self.exchanges.write().await;
        if exchanges.contains_key(&msg.exchange) {
            return Err(AmqpError::ExchangeAlreadyExists);
        }

        exchanges.insert(
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
        let mut exchanges = self.exchanges.write().await;
        match exchanges.get(&msg.exchange) {
            Some(exchange) => {
                if msg.if_unused && !exchange.bindings.is_empty() {
                    return Err(AmqpError::ExchangeInUse);
                } else {
                    exchanges.remove(&msg.exchange);
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
        let mut queues = self.queues.write().await;
        if queues.contains_key(&msg.queue) {
            return Err(AmqpError::QueueAlreadyExists);
        }

        queues.insert(
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
        self.queue_delete(msg.queue.clone(), msg.if_unused).await?;
        self.default_exchange
            .bindings
            .retain(|b| b.queue_name != msg.queue);
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
        let mut exchanges = self.exchanges.write().await;
        let queues = self.queues.read().await;

        if !queues.contains_key(&msg.queue) {
            return Err(AmqpError::QueueNotFound);
        }

        let exchange = exchanges
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
            match_args.remove("x-match");

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
        let mut exchanges = self.exchanges.write().await;
        let mut entry = match exchanges.entry(msg.exchange.clone()) {
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

impl<M: Clone + Send + 'static> Message<BasicPublish<M>> for MessageQueue {
    type Reply = Result<(), AmqpError>;

    async fn handle(
        &mut self,
        msg: BasicPublish<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let exchanges = self.exchanges.read().await;
        let exchange = if msg.exchange.is_empty() {
            &self.default_exchange
        } else if let Some(exchange) = exchanges.get(&msg.exchange) {
            exchange
        } else {
            return Err(AmqpError::ExchangeNotFound);
        };

        let queues = self.queues.read().await;
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
            if let Some(queue) = queues.get(&queue_name) {
                if let Some(recipients) = queue.recipients.get(&TypeId::of::<M>()) {
                    self.delivery_message(
                        queue_name.clone(),
                        recipients,
                        msg.message.clone(),
                        ctx.actor_ref().clone(),
                    )
                    .await
                }
            }
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
        let mut queues = self.queues.write().await;
        let queue = queues.get_mut(&msg.queue).ok_or(AmqpError::QueueNotFound)?;
        let artor_id = msg.recipient.id();
        queue
            .recipients
            .entry(TypeId::of::<M>())
            .or_default()
            .push(Registration {
                actor_id: artor_id,
                recipient: Box::new(msg.recipient),
            });
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
        self.basic_cancel(msg.queue, msg.recipient).await
    }
}
