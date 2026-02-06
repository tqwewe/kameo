//! Message handler implementation for the transport abstraction

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use bytes::Bytes;
use kameo_remote::AlignedBytes;

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
        payload: AlignedBytes,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, BoxError>> + Send + '_>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::_internal::{
        RemoteAskFn, RemoteMessageFns, RemoteTellFn, RemoteTryAskFn, RemoteTryTellFn,
    };
    use crate::remote::type_hash::TypeHash;
    use futures::FutureExt;
    use rkyv::util::AlignedVec;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn make_aligned_payload(data: &[u8]) -> AlignedBytes {
        let mut buf = AlignedVec::<16>::new();
        buf.extend_from_slice(data);
        kameo_remote::AlignedBytes::from_aligned_vec(buf)
    }

    fn noop_tell() -> Arc<RemoteTellFn> {
        Arc::new(|_, _, _| async { Err(RemoteSendError::ActorStopped) }.boxed())
    }

    fn noop_try_tell() -> Arc<RemoteTryTellFn> {
        Arc::new(|_, _| async { Err(RemoteSendError::ActorStopped) }.boxed())
    }

    fn noop_ask() -> Arc<RemoteAskFn> {
        Arc::new(|_, _, _, _| async { Err(RemoteSendError::ActorStopped) }.boxed())
    }

    fn noop_try_ask() -> Arc<RemoteTryAskFn> {
        Arc::new(|_, _, _| async { Err(RemoteSendError::ActorStopped) }.boxed())
    }

    #[tokio::test]
    async fn handle_tell_typed_receives_aligned_payloads() {
        let handler = RemoteMessageHandler::new();
        let actor_id = ActorId::from_u64(7);
        let type_hash = TypeHash::from_u32(0xA57A_A5C0);
        let seen_aligned = Arc::new(AtomicBool::new(false));
        let aligned_tracker = seen_aligned.clone();

        let tell = Arc::new(move |_aid, payload: AlignedBytes, _timeout| {
            let aligned_tracker = aligned_tracker.clone();
            async move {
                let ptr = payload.as_ref().as_ptr() as usize;
                aligned_tracker.store(ptr % 16 == 0, Ordering::SeqCst);
                Ok(())
            }
            .boxed()
        });

        let fns = RemoteMessageFns {
            ask: noop_ask(),
            try_ask: noop_try_ask(),
            tell,
            try_tell: noop_try_tell(),
        };

        handler.register_type_hash(type_hash, fns);

        let payload = make_aligned_payload(b"hello world");
        handler
            .handle_tell_typed(actor_id, type_hash.as_u32(), payload)
            .await
            .expect("tell handler should succeed");

        assert!(
            seen_aligned.load(Ordering::SeqCst),
            "payload should arrive 16-byte aligned"
        );
    }

    #[tokio::test]
    async fn handle_ask_typed_returns_zero_copy_bytes() {
        let handler = RemoteMessageHandler::new();
        let actor_id = ActorId::from_u64(9);
        let type_hash = TypeHash::from_u32(0x9629_FA17);

        let response = Bytes::from_static(b"answer");
        let response_ptr = response.as_ptr();

        let ask = Arc::new(move |_aid, payload: AlignedBytes, _mailbox, _reply| {
            assert_eq!(payload.as_ref(), b"question");
            let response = response.clone();
            async move { Ok(response) }.boxed()
        });

        let fns = RemoteMessageFns {
            ask: ask.clone(),
            try_ask: Arc::new(move |aid, payload, timeout| {
                (ask.clone())(aid, payload, timeout, None)
            }),
            tell: noop_tell(),
            try_tell: noop_try_tell(),
        };

        handler.register_type_hash(type_hash, fns);

        let payload = make_aligned_payload(b"question");
        let bytes = handler
            .handle_ask_typed(actor_id, type_hash.as_u32(), payload)
            .await
            .expect("ask handler should succeed");

        assert_eq!(
            bytes.as_ptr(),
            response_ptr,
            "handler should return Bytes without extra copy"
        );
    }
}
