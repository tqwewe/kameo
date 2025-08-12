//! Distributed actor macro v2 with full ask/reply support
//! 
//! This version properly handles both tell and ask messages by leveraging
//! the Message trait implementations directly.

/// Distributed actor macro with ask/reply support
/// 
/// This macro generates handlers that properly support both tell and ask operations.
/// Messages that implement the Message trait will automatically support ask/reply.
/// 
/// # Example
/// ```ignore
/// distributed_actor_v2! {
///     CalculatorActor {
///         // These messages have Message trait implementations
///         Add,
///         Multiply,
///         Increment,
///     }
/// }
/// ```
#[macro_export]
macro_rules! distributed_actor_v2 {
    (
        $actor:ty {
            $($msg_ty:ty),* $(,)?
        }
    ) => {
        // Generate HasTypeHash for actor type
        $crate::impl_type_hash!($actor);
        
        impl $actor {
            /// Register message handlers for this actor
            pub fn __register_message_types_v2(actor_ref: $crate::actor::ActorRef<Self>) {
                use $crate::remote::type_hash::HasTypeHash;
                use $crate::remote::_internal::RemoteMessageFns;
                use $crate::message::Message;
                use $crate::request::{AskRequest, TellRequest};
                
                let actor_id = actor_ref.id();
                
                // Register handlers for each message type
                $(
                    {
                        let type_hash = <$msg_ty as HasTypeHash>::TYPE_HASH;
                        let actor_ref_for_tell = actor_ref.clone();
                        let actor_ref_for_ask = actor_ref.clone();
                        
                        let fns = RemoteMessageFns {
                            // Tell handler - deserialize and send directly
                            tell: std::sync::Arc::new(move |aid, payload, timeout| {
                                let actor_ref = actor_ref_for_tell.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }
                                    
                                    // Deserialize the message
                                    let msg: $msg_ty = ::rkyv::from_bytes::<$msg_ty, rkyv::rancor::Error>(&payload)
                                        .map_err(|_| $crate::error::RemoteSendError::ActorStopped)?;
                                    
                                    // Send as tell through the actor's mailbox
                                    if let Some(timeout) = timeout {
                                        match actor_ref.tell(msg).mailbox_timeout(timeout).send().await {
                                            Ok(()) => Ok(()),
                                            Err(_) => Err($crate::error::RemoteSendError::ActorStopped),
                                        }
                                    } else {
                                        match actor_ref.tell(msg).send().await {
                                            Ok(()) => Ok(()),
                                            Err(_) => Err($crate::error::RemoteSendError::ActorStopped),
                                        }
                                    }
                                })
                            }),
                            
                            // try_tell handler
                            try_tell: std::sync::Arc::new({
                                let actor_ref = actor_ref.clone();
                                move |aid, payload| {
                                    let actor_ref = actor_ref.clone();
                                    Box::pin(async move {
                                        if aid != actor_id {
                                            return Err($crate::error::RemoteSendError::ActorNotRunning);
                                        }
                                        
                                        // Deserialize the message
                                        let msg: $msg_ty = ::rkyv::from_bytes::<$msg_ty, rkyv::rancor::Error>(&payload)
                                            .map_err(|_| $crate::error::RemoteSendError::ActorStopped)?;
                                        
                                        // Try to send without blocking
                                        match actor_ref.tell(msg).try_send() {
                                            Ok(()) => Ok(()),
                                            Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                        }
                                    })
                                }
                            }),
                            
                            // Ask handler - uses the Message trait directly
                            ask: std::sync::Arc::new(move |aid, payload, mailbox_timeout, reply_timeout| {
                                let actor_ref = actor_ref_for_ask.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }
                                    
                                    // Deserialize the message
                                    let msg: $msg_ty = ::rkyv::from_bytes::<$msg_ty, rkyv::rancor::Error>(&payload)
                                        .map_err(|_| $crate::error::RemoteSendError::ActorStopped)?;
                                    
                                    // Send ask request through the actor's mailbox
                                    // The Message trait implementation provides the Reply type
                                    let result: Result<<$actor as Message<$msg_ty>>::Reply, _> = if let Some(m_timeout) = mailbox_timeout {
                                        if let Some(r_timeout) = reply_timeout {
                                            actor_ref.ask(msg)
                                                .mailbox_timeout(m_timeout)
                                                .reply_timeout(r_timeout)
                                                .send()
                                                .await
                                        } else {
                                            actor_ref.ask(msg)
                                                .mailbox_timeout(m_timeout)
                                                .send()
                                                .await
                                        }
                                    } else if let Some(r_timeout) = reply_timeout {
                                        actor_ref.ask(msg)
                                            .reply_timeout(r_timeout)
                                            .send()
                                            .await
                                    } else {
                                        actor_ref.ask(msg).send().await
                                    };
                                    
                                    match result {
                                        Ok(reply) => {
                                            // Serialize the reply
                                            let reply_bytes = ::rkyv::to_bytes::<rkyv::rancor::Error>(&reply)
                                                .map_err(|_| $crate::error::RemoteSendError::ActorStopped)?;
                                            Ok(reply_bytes.to_vec())
                                        },
                                        Err(_) => Err($crate::error::RemoteSendError::ActorStopped),
                                    }
                                })
                            }),
                            
                            // try_ask handler
                            try_ask: std::sync::Arc::new({
                                let actor_ref = actor_ref.clone();
                                move |aid, payload, reply_timeout| {
                                    let actor_ref = actor_ref.clone();
                                    Box::pin(async move {
                                        if aid != actor_id {
                                            return Err($crate::error::RemoteSendError::ActorNotRunning);
                                        }
                                        
                                        // Deserialize the message
                                        let msg: $msg_ty = ::rkyv::from_bytes::<$msg_ty, rkyv::rancor::Error>(&payload)
                                            .map_err(|_| $crate::error::RemoteSendError::ActorStopped)?;
                                        
                                        // Try ask without blocking on mailbox
                                        // try_send returns Result<Reply::Ok, SendError<M, Reply::Error>>
                                        // For simple replies where Reply::Ok = Reply, this is Result<Reply, SendError<M, Reply::Error>>
                                        let result = if let Some(r_timeout) = reply_timeout {
                                            actor_ref.ask(msg)
                                                .reply_timeout(r_timeout)
                                                .try_send()
                                                .await
                                        } else {
                                            actor_ref.ask(msg).try_send().await
                                        };
                                        
                                        match result {
                                            Ok(reply) => {
                                                // Serialize the reply (reply is Reply::Ok type)
                                                let reply_bytes = ::rkyv::to_bytes::<rkyv::rancor::Error>(&reply)
                                                    .map_err(|_| $crate::error::RemoteSendError::ActorStopped)?;
                                                Ok(reply_bytes.to_vec())
                                            },
                                            Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                        }
                                    })
                                }
                            }),
                        };
                        
                        // Register with the distributed handler
                        $crate::remote::v2_bootstrap::get_distributed_handler()
                            .register_type_handler(type_hash.as_u32(), fns);
                        
                        // Also register in type registry for lookups
                        $crate::remote::type_registry::register_type(
                            type_hash.as_u32(),
                            actor_id,
                            concat!(stringify!($actor), "::", stringify!($msg_ty))
                        );
                    }
                )*
            }
            
            /// Spawn the actor and auto-register message types
            pub fn spawn_v2(args: <Self as $crate::actor::Actor>::Args) -> $crate::actor::ActorRef<Self> {
                let actor_ref = <Self as $crate::actor::Actor>::spawn(args);
                Self::__register_message_types_v2(actor_ref.clone());
                actor_ref
            }
        }
    };
}