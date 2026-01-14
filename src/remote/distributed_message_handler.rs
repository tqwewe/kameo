//! Legacy message handler for backward compatibility
//!
//! This module provides basic message routing functionality.
//! Most distributed actor communication now uses DistributedActorRef directly.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use super::message_handler::RemoteMessageHandler;
use super::transport::BoxError;
use super::transport::MessageHandler;
use crate::actor::ActorId;

/// Legacy message handler for backward compatibility only
#[derive(Debug)]
pub struct DistributedMessageHandler {
    legacy_handler: RemoteMessageHandler,
}

impl Default for DistributedMessageHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedMessageHandler {
    /// Creates a new distributed message handler for legacy message routing
    pub fn new() -> Self {
        Self {
            legacy_handler: RemoteMessageHandler::new(),
        }
    }

    /// Register a type hash handler
    pub fn register_type_handler(&self, type_hash: u32, fns: super::_internal::RemoteMessageFns) {
        self.legacy_handler
            .register_type_hash(super::type_hash::TypeHash::from_u32(type_hash), fns);
    }
}

impl MessageHandler for DistributedMessageHandler {
    fn handle_tell(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        // Use legacy handler for backward compatibility
        self.legacy_handler
            .handle_tell(actor_id, message_type, payload)
    }

    fn handle_ask(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, BoxError>> + Send + '_>> {
        // Use legacy handler for backward compatibility
        self.legacy_handler
            .handle_ask(actor_id, message_type, payload)
    }

    fn handle_tell_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        Box::pin(async move {
            // The handler functions are registered in TYPE_HASH_REGISTRY
            // Just forward to the legacy handler which uses that registry
            self.legacy_handler
                .handle_tell_typed(actor_id, type_hash, payload)
                .await
        })
    }

    fn handle_ask_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, BoxError>> + Send + '_>> {
        let legacy_handler = self.legacy_handler.clone();

        Box::pin(async move {
            // Add debug logging to track ask flow
            tracing::debug!(
                "🔍 ASK DEBUG: Processing ask for actor_id={}, type_hash={:08x}, about to call legacy handler",
                actor_id, type_hash
            );

            // Use legacy handler - most traffic now goes through DistributedActorRef directly
            let message_type = format!("hash:{:08x}", type_hash);
            let result = legacy_handler
                .handle_ask(actor_id, &message_type, &payload)
                .await
                .map(|vec| {
                    tracing::debug!(
                        "✅ ASK DEBUG: Got reply from legacy handler, {} bytes",
                        vec.len()
                    );
                    Bytes::from(vec)
                });

            if let Err(ref e) = result {
                tracing::warn!("❌ ASK DEBUG: Legacy handler returned error: {:?}", e);
            }

            result
        })
    }
}
