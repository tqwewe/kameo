//! DEPRECATED: Use the unified distributed_actor! macro in distributed_actor.rs instead
//!
//! This module provides internal types still used by the distributed_actor macro.

use bytes::Bytes;

/// Internal message type for distributed messages
#[derive(Debug, Clone)]
pub struct DistributedMessage {
    /// Type hash identifying the message type
    pub type_hash: u32,
    /// Serialized message payload
    pub payload: Bytes,
}
