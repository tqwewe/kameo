//! General-purpose actors for concurrent programming
//!
//! This crate provides reusable actor components:
//!
//! - `broker`: Topic-based message broker
//! - `message_bus`: Type-based message bus
//! - `pool`: Actor pool for managing concurrent task execution
//! - `pubsub`: Publish-subscribe pattern implementation for actor communication
//!
//! # When to use MessageBus vs Broker vs PubSub
//!
//! - Use **MessageBus** when you want to route messages based on their type without explicit topics.
//! - Use **Broker** when you need hierarchical topics, pattern-based subscriptions, or explicit routing.
//! - Use **PubSub** when you need simple broadcast to all listeners with optional predicate-based filtering.

use std::time::Duration;

pub mod broker;
pub mod message_bus;
pub mod message_queue;
pub mod pool;
pub mod pubsub;

/// Strategies for delivering messages to subscribers.
///
/// Different strategies provide different trade-offs between reliability,
/// performance, and resource usage.
#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq)]
pub enum DeliveryStrategy {
    /// Block until all messages are delivered.
    ///
    /// This strategy ensures reliable delivery but may cause the broker
    /// to block if any recipient's mailbox is full.
    Guaranteed,

    /// Skip actors with full mailboxes.
    ///
    /// This strategy attempts to deliver messages immediately without blocking,
    /// but will skip recipients whose mailboxes are full.
    #[default]
    BestEffort,

    /// Try to deliver with timeout (blocks the publisher).
    ///
    /// This strategy waits for each recipient to accept the message, but only
    /// up to the specified timeout duration. The broker will block during delivery.
    TimedDelivery(Duration),

    /// Spawn a task for each delivery (non-blocking).
    ///
    /// This strategy spawns a separate task for each message delivery,
    /// allowing the broker to continue processing other messages immediately.
    /// Tasks will retry indefinitely if mailboxes are full.
    Spawned,

    /// Spawn a task with timeout for each delivery.
    ///
    /// This strategy combines the benefits of spawned delivery with a timeout,
    /// ensuring that delivery attempts don't consume resources indefinitely.
    SpawnedWithTimeout(Duration),
}
