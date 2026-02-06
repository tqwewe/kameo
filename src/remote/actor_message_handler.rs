//! Actor message handler registration system
//!
//! This module provides the infrastructure to register message handlers for distributed actors.
//! It bridges the gap between incoming messages with type hashes and actor handler methods.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use kameo_remote::AlignedBytes;

use crate::actor::ActorId;
use crate::error::RemoteSendError;
use super::type_hash::TypeHash;
use super::_internal::RemoteMessageFns;

/// Type alias for a message handler function
pub type MessageHandlerFn = fn(
    actor_id: ActorId,
    payload: AlignedBytes,
) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, RemoteSendError<(), ()>>> + Send>>;

/// Global registry for actor message handlers
static ACTOR_MESSAGE_HANDLERS: std::sync::LazyLock<Arc<RwLock<HashMap<(ActorId, TypeHash), MessageHandlerFn>>>> = 
    std::sync::LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Register a message handler for a specific actor and message type
pub fn register_message_handler(
    actor_id: ActorId,
    type_hash: TypeHash,
    handler: MessageHandlerFn,
) {
    ACTOR_MESSAGE_HANDLERS
        .write()
        .unwrap()
        .insert((actor_id, type_hash), handler);
}

/// Get a message handler for a specific actor and message type
pub fn get_message_handler(
    actor_id: ActorId,
    type_hash: TypeHash,
) -> Option<MessageHandlerFn> {
    ACTOR_MESSAGE_HANDLERS
        .read()
        .unwrap()
        .get(&(actor_id, type_hash))
        .cloned()
}

/// Create RemoteMessageFns for a tell-only message handler
pub fn create_tell_fns(
    actor_id: ActorId,
    type_hash: TypeHash,
) -> RemoteMessageFns {
    RemoteMessageFns {
        tell: Box::new(move |aid, payload, _timeout| {
            Box::pin(async move {
                if let Some(handler) = get_message_handler(aid, type_hash) {
                    match handler(aid, payload).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(RemoteSendError::UnknownMessage {
                        actor_remote_id: format!("actor_{:08x}", aid.into_u64()),
                        message_remote_id: format!("msg_{:08x}", type_hash.as_u32()),
                    })
                }
            })
        }),
        try_tell: Box::new(move |aid, payload, timeout| {
            Box::pin(async move {
                if let Some(handler) = get_message_handler(aid, type_hash) {
                    match handler(aid, payload).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(RemoteSendError::UnknownMessage {
                        actor_remote_id: format!("actor_{:08x}", aid.into_u64()),
                        message_remote_id: format!("msg_{:08x}", type_hash.as_u32()),
                    })
                }
            })
        }),
        ask: Box::new(move |_, _, _, _| {
            Box::pin(async move {
                Err(RemoteSendError::ActorStopped)
            })
        }),
        try_ask: Box::new(move |_, _, _, _| {
            Box::pin(async move {
                Err(RemoteSendError::ActorStopped)
            })
        }),
    }
}
