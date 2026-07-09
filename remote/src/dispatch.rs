//! Per-node dispatch tables routing inbound requests to typed handlers.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use futures::future::BoxFuture;

use crate::messaging::protocol::{RequestFrame, WireError};

/// How an inbound request expects to be handled.
pub(crate) enum InboundKind {
    Ask { reply_timeout: Option<Duration> },
    Tell,
}

/// A type-erased handler for one `(actor, message)` pair.
///
/// Returns `Ok(Some(reply_bytes))` for asks, `Ok(None)` for tells.
pub(crate) type DynHandler = Arc<
    dyn Fn(Vec<u8>, InboundKind) -> BoxFuture<'static, Result<Option<Vec<u8>>, WireError>>
        + Send
        + Sync,
>;

/// Registered actors on this node, keyed by their local sequence id.
pub(crate) struct DispatchTable {
    /// This node incarnation's generation; requests targeting another generation are
    /// stale references from before a restart and are rejected.
    generation_id: u64,
    // Sync lock; the guard is never held across an await.
    actors: RwLock<HashMap<u64, ActorEntry>>,
}

struct ActorEntry {
    actor_remote_id: &'static str,
    handlers: Arc<HashMap<&'static str, DynHandler>>,
    /// Names this actor is registered under; the entry is dropped with its last name.
    names: HashSet<String>,
}

impl DispatchTable {
    pub(crate) fn new(generation_id: u64) -> Self {
        DispatchTable {
            generation_id,
            actors: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn insert(
        &self,
        sequence_id: u64,
        actor_remote_id: &'static str,
        name: String,
        handlers: HashMap<&'static str, DynHandler>,
    ) {
        let mut actors = self.actors.write().unwrap();
        actors
            .entry(sequence_id)
            .or_insert_with(|| ActorEntry {
                actor_remote_id,
                handlers: Arc::new(handlers),
                names: HashSet::new(),
            })
            .names
            .insert(name);
    }

    pub(crate) fn remove_name(&self, sequence_id: u64, name: &str) {
        let mut actors = self.actors.write().unwrap();
        if let Some(entry) = actors.get_mut(&sequence_id) {
            entry.names.remove(name);
            if entry.names.is_empty() {
                actors.remove(&sequence_id);
            }
        }
    }

    /// Resolves a request to its handler. The handler is cloned out so the lock is
    /// released before it is invoked.
    pub(crate) fn resolve(&self, req: &RequestFrame) -> Result<DynHandler, WireError> {
        if req.target_generation_id != self.generation_id {
            // A stale reference into a previous incarnation of this node.
            return Err(WireError::ActorNotRunning);
        }
        let actors = self.actors.read().unwrap();
        let Some(entry) = actors.get(&req.target_sequence_id) else {
            // The actor was deregistered, stopped, or never existed on this node.
            return Err(WireError::ActorNotRunning);
        };
        if entry.actor_remote_id != req.actor_remote_id {
            return Err(WireError::BadActorType);
        }
        match entry.handlers.get(req.message_remote_id.as_str()) {
            Some(handler) => Ok(Arc::clone(handler)),
            None => Err(WireError::UnknownMessage {
                actor_remote_id: req.actor_remote_id.clone(),
                message_remote_id: req.message_remote_id.clone(),
            }),
        }
    }
}
