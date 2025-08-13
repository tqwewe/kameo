//! DEPRECATED: Use the unified distributed_actor! macro in distributed_actor.rs instead
//! 
//! This module provides internal types still used by the distributed_actor macro.

/// Internal message type for distributed messages
#[derive(Debug, Clone)]
pub struct DistributedMessage {
    pub type_hash: u32,
    pub payload: Vec<u8>,
}