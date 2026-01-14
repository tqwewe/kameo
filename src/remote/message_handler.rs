//! Message handler implementation for the transport abstraction

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use bytes::Bytes;

use super::transport::BoxError;
use crate::actor::ActorId;
use crate::error::RemoteSendError;

use super::_internal::RemoteMessageFns;
use super::transport::MessageHandler;
use super::type_hash::TypeHash;

/// Registry for type hash to message function mappings
static TYPE_HASH_REGISTRY: LazyLock<Arc<RwLock<HashMap<u32, RemoteMessageFns>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Implementation of MessageHandler that routes to the existing remote message infrastructure
#[derive(Debug, Clone)]
pub struct RemoteMessageHandler {}

impl RemoteMessageHandler {
    pub fn new() -> Self {
        Self {}
    }

    /// Register a type hash mapping for generic actors
    pub fn register_type_hash(&self, type_hash: TypeHash, message_fns: RemoteMessageFns) {
        TYPE_HASH_REGISTRY
            .write()
            .unwrap()
            .insert(type_hash.as_u32(), message_fns);
    }
}

impl MessageHandler for RemoteMessageHandler {
    fn handle_tell(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        // Legacy string-based routing - try to parse as "hash:XXXXX" format
        if let Some(hash_str) = message_type.strip_prefix("hash:") {
            if let Ok(type_hash) = u32::from_str_radix(hash_str, 16) {
                return self.handle_tell_typed(
                    actor_id,
                    type_hash,
                    Bytes::copy_from_slice(payload),
                );
            }
        }

        let msg_type = message_type.to_string();
        Box::pin(
            async move { Err(format!("Unsupported message type format: {}", msg_type).into()) },
        )
    }

    fn handle_ask(
        &self,
        actor_id: ActorId,
        message_type: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, BoxError>> + Send + '_>> {
        // Legacy string-based routing - try to parse as "hash:XXXXX" format
        if let Some(hash_str) = message_type.strip_prefix("hash:") {
            if let Ok(type_hash) = u32::from_str_radix(hash_str, 16) {
                let payload_bytes = Bytes::copy_from_slice(payload);
                let handler = self.clone();
                return Box::pin(async move {
                    handler
                        .handle_ask_typed(actor_id, type_hash, payload_bytes)
                        .await
                        .map(|bytes| bytes.to_vec())
                });
            }
        }

        let msg_type = message_type.to_string();
        Box::pin(
            async move { Err(format!("Unsupported message type format: {}", msg_type).into()) },
        )
    }

    fn handle_tell_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + '_>> {
        let payload = payload.to_vec();

        Box::pin(async move {
            // First try the type hash registry
            // Extract the function pointer to avoid holding the lock across await
            let tell_fn = {
                let registry = TYPE_HASH_REGISTRY.read().unwrap();
                registry.get(&type_hash).map(|fns| fns.tell.clone())
            };

            if let Some(tell_fn) = tell_fn {
                let mailbox_timeout = Some(Duration::from_secs(30));
                return (tell_fn)(actor_id, payload, mailbox_timeout)
                    .await
                    .map_err(|e| match e {
                        RemoteSendError::UnknownMessage {
                            actor_remote_id,
                            message_remote_id,
                        } => format!("Unknown message: {}:{}", actor_remote_id, message_remote_id)
                            .into(),
                        _ => format!("Remote send error: {:?}", e).into(),
                    });
            }

            // No fallback - type hash not found
            Err(format!("Unknown type hash: {:08x}", type_hash).into())
        })
    }

    fn handle_ask_typed(
        &self,
        actor_id: ActorId,
        type_hash: u32,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, BoxError>> + Send + '_>> {
        let payload = payload.to_vec();

        Box::pin(async move {
            // First try the type hash registry
            // Extract the function pointer to avoid holding the lock across await
            let ask_fn = {
                let registry = TYPE_HASH_REGISTRY.read().unwrap();
                registry.get(&type_hash).map(|fns| fns.ask.clone())
            };

            if let Some(ask_fn) = ask_fn {
                let mailbox_timeout = Some(Duration::from_secs(30));
                let reply_timeout = Some(Duration::from_secs(30));
                return (ask_fn)(actor_id, payload, mailbox_timeout, reply_timeout)
                    .await
                    .map(Bytes::from)
                    .map_err(|e| match e {
                        RemoteSendError::UnknownMessage {
                            actor_remote_id,
                            message_remote_id,
                        } => format!("Unknown message: {}:{}", actor_remote_id, message_remote_id)
                            .into(),
                        _ => format!("Remote send error: {:?}", e).into(),
                    });
            }

            // No fallback - type hash not found
            Err(format!("Unknown type hash: {:08x}", type_hash).into())
        })
    }
}
