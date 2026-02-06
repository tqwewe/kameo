//! Distributed message handler for kameo_remote actor routing.
//!
//! This module provides typed message routing for distributed actors.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use kameo_remote::AlignedBytes;

use super::message_handler::RemoteMessageHandler;
use super::transport::BoxError;
use super::transport::MessageHandler;
use crate::actor::ActorId;

/// Distributed message handler for remote actor routing
#[derive(Debug)]
pub struct DistributedMessageHandler {
    handler: RemoteMessageHandler,
}

impl DistributedMessageHandler {
    /// Creates a new distributed message handler for typed message routing
    pub fn new() -> Self {
        Self {
            handler: RemoteMessageHandler::new(),
        }
    }

    /// Register a type hash handler
    pub fn register_type_handler(&self, type_hash: u32, fns: super::_internal::RemoteMessageFns) {
        self.handler
            .register_type_hash(super::type_hash::TypeHash::from_u32(type_hash), fns);
    }
}

impl Default for DistributedMessageHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageHandler for DistributedMessageHandler {
    fn handle_tell(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        let msg_type = message_type.to_string();
        let _ = (actor_id, payload);
        Box::pin(async move {
            Err(format!("String message types are not supported (got: {})", msg_type).into())
        })
    }

    fn handle_ask(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, BoxError>> + Send + '_>> {
        let msg_type = message_type.to_string();
        let _ = (actor_id, payload);
        Box::pin(async move {
            Err(format!("String message types are not supported (got: {})", msg_type).into())
        })
    }

    fn handle_tell_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: AlignedBytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        self.handler.handle_tell_typed(actor_id, type_hash, payload)
    }

    fn handle_ask_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: AlignedBytes,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, BoxError>> + Send + '_>> {
        let handler = self.handler.clone();

        Box::pin(async move { handler.handle_ask_typed(actor_id, type_hash, payload).await })
    }
}
