//! Legacy message handler for backward compatibility
//! 
//! This module provides basic message routing functionality.
//! Most distributed actor communication now uses DistributedActorRef directly.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::actor::ActorId;
use super::transport::BoxError;
use super::message_handler::RemoteMessageHandler;
use super::transport::MessageHandler;


/// Legacy message handler for backward compatibility only
pub struct DistributedMessageHandler {
    legacy_handler: RemoteMessageHandler,
}

impl DistributedMessageHandler {
    pub fn new() -> Self {
        Self {
            legacy_handler: RemoteMessageHandler::new(),
        }
    }
    
    /// Register a type hash handler
    pub fn register_type_handler(&self, type_hash: u32, fns: super::_internal::RemoteMessageFns) {
        self.legacy_handler.register_type_hash(
            super::type_hash::TypeHash::from_u32(type_hash),
            fns
        );
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
        self.legacy_handler.handle_tell(actor_id, message_type, payload)
    }
    
    fn handle_ask(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, BoxError>> + Send + '_>> {
        // Use legacy handler for backward compatibility
        self.legacy_handler.handle_ask(actor_id, message_type, payload)
    }
    
    fn handle_tell_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!(
                "📨 RECV: Received tell message for actor {} with type_hash {:08x}, payload size: {} bytes",
                actor_id, type_hash, payload.len()
            );
            
            // The handler functions are registered in TYPE_HASH_REGISTRY
            // Just forward to the legacy handler which uses that registry
            self.legacy_handler.handle_tell_typed(actor_id, type_hash, payload).await
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
            tracing::info!(
                "📨 RECV: Received ask message for actor {} with type_hash {:08x}, payload size: {} bytes",
                actor_id, type_hash, payload.len()
            );
            
            // Use legacy handler - most traffic now goes through DistributedActorRef directly
            let message_type = format!("hash:{:08x}", type_hash);
            legacy_handler.handle_ask(actor_id, &message_type, &payload).await
                .map(|vec| Bytes::from(vec))
        })
    }
}