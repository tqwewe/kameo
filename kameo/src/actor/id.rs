use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::{fmt, sync::atomic::AtomicU64};

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

static NODE_ID: OnceCell<u16> = OnceCell::new();
static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Represents an `ActorID` which is a wrapper around a `u64`.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorID(u64);

impl ActorID {
    /// Creates a new `ActorID` from a `u64`.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Generates a new `ActorID`.
    ///
    /// If no `NODE_ID` is specified, it is defaulted to `0`.
    pub fn generate() -> Self {
        ActorID::from_node_and_instance(
            NODE_ID.get().copied().unwrap_or(0),
            ACTOR_COUNTER.fetch_add(1, Ordering::Relaxed),
        )
    }

    /// Combines a node ID and an actor instance ID into a single `u64`.
    pub fn from_node_and_instance(node_id: u16, instance_id: u64) -> Self {
        // Ensure that only the lower 48 bits of instance_id are used
        let instance_id = instance_id & 0x0000_FFFF_FFFF_FFFF;
        let combined_id = ((node_id as u64) << 48) | instance_id;
        Self(combined_id)
    }

    /// Extracts the node ID portion from the `ActorID`.
    pub fn node_id(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    /// Extracts the actor instance ID portion from the `ActorID`.
    pub fn instance_id(&self) -> u64 {
        self.0 & 0x0000_FFFF_FFFF_FFFF
    }

    /// Returns the raw `u64` value of the `ActorID`.
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl Deref for ActorID {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorID({}, {})", self.node_id(), self.instance_id())
    }
}

impl fmt::Debug for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorID({:#018x})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_id_creation() {
        let id = ActorID::new(123456789);
        assert_eq!(*id, 123456789);
        assert_eq!(id.raw(), 123456789);
    }

    #[test]
    fn test_node_and_instance_extraction() {
        let node_id = 42;
        let instance_id = 987654321;
        let id = ActorID::from_node_and_instance(node_id, instance_id);

        assert_eq!(id.node_id(), node_id);
        assert_eq!(id.instance_id(), instance_id);
    }

    #[test]
    fn test_display() {
        let id = ActorID::from_node_and_instance(42, 987654321);
        assert_eq!(id.to_string(), "ActorID(42, 987654321)");
    }

    #[test]
    fn test_debug() {
        let id = ActorID::from_node_and_instance(42, 987654321);
        assert_eq!(format!("{:?}", id), "ActorID(0x002a00003ade68b1)");
    }
}
