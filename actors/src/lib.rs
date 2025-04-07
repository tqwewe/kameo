//! General-purpose actors for concurrent programming
//!
//! This crate provides reusable actor components:
//!
//! - `broker`: Topic-based message broker
//! - `pool`: Actor pool for managing concurrent task execution
//! - `pubsub`: Publish-subscribe pattern implementation for actor communication

pub mod broker;
pub mod pool;
pub mod pubsub;
