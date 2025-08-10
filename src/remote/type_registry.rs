//! Type registry for zero-cost message dispatch
//! 
//! This module provides a compile-time registry that maps type hashes
//! to handler functions without dynamic dispatch.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, LazyLock};

use crate::actor::ActorId;

/// Entry in the type registry
#[derive(Clone)]
pub struct TypeRegistryEntry {
    /// The actor ID that handles this message type
    pub actor_id: ActorId,
    /// The handler type name (for debugging)
    pub handler_name: &'static str,
}

/// Global registry mapping type hashes to actor IDs
static TYPE_REGISTRY: LazyLock<Arc<RwLock<HashMap<u32, TypeRegistryEntry>>>> = 
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Register a type hash to actor mapping
pub fn register_type(type_hash: u32, actor_id: ActorId, handler_name: &'static str) {
    TYPE_REGISTRY.write().unwrap().insert(type_hash, TypeRegistryEntry {
        actor_id,
        handler_name,
    });
}

/// Look up which actor handles a given type hash
pub fn lookup_handler(type_hash: u32) -> Option<TypeRegistryEntry> {
    TYPE_REGISTRY.read().unwrap().get(&type_hash).cloned()
}

/// Clear all registrations for an actor
pub fn unregister_actor(actor_id: ActorId) {
    let mut registry = TYPE_REGISTRY.write().unwrap();
    registry.retain(|_, entry| entry.actor_id != actor_id);
}