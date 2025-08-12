//! Simplified distributed actor registration
//!
//! This module provides a simple macro to register actors for distributed messaging
//! without requiring trait implementations. Actors just need handlers for their message types.

/// Internal message type used for routing distributed messages through the actor's mailbox
#[doc(hidden)]
#[derive(Debug)]
pub struct DistributedMessage {
    pub type_hash: u32,
    pub payload: Vec<u8>,
}

/// Simple macro to register an actor as distributed
/// 
/// This macro generates methods for handling distributed messages with zero-cost dispatch.
/// 
/// # Example
/// 
/// ```ignore
/// simple_distributed_actor! {
///     LoggerActor {
///         LogMessage => handle_log,
///         TellConcrete => handle_tell_concrete,
///     }
/// }
/// ```
#[macro_export]
macro_rules! simple_distributed_actor {
    (
        $actor:ty {
            $($msg_ty:ty => $handler:ident),* $(,)?
        }
    ) => {
        // Generate HasTypeHash for actor type only - messages should use #[derive(RemoteMessage)]
        $crate::impl_type_hash!($actor);
        
        // Implement Message handler for the internal DistributedMessage type
        impl $crate::message::Message<$crate::remote::simple_distributed_actor::DistributedMessage> for $actor {
            type Reply = Result<(), Box<dyn std::error::Error + Send + Sync>>;
            
            async fn handle(
                &mut self,
                msg: $crate::remote::simple_distributed_actor::DistributedMessage,
                _ctx: &mut $crate::message::Context<Self, Self::Reply>,
            ) -> Self::Reply {
                self.__handle_distributed_message(msg.type_hash, &msg.payload).await
            }
        }
        
        impl $actor {
            /// Handle a distributed message by type hash (zero-cost dispatch)
            pub async fn __handle_distributed_message(
                &mut self,
                type_hash: u32,
                payload: &[u8],
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                use $crate::remote::type_hash::HasTypeHash;
                
                // Compile-time match - zero overhead!
                match type_hash {
                    $(
                        hash if hash == <$msg_ty as HasTypeHash>::TYPE_HASH.as_u32() => {
                            // Direct zero-copy access to archived data - no deserialization!
                            // SAFETY: We trust the data coming from our transport layer
                            let archived = unsafe { ::rkyv::access_unchecked::<::rkyv::Archived<$msg_ty>>(payload) };
                            // Direct method call - no dynamic dispatch!
                            self.$handler(archived).await;
                            Ok(())
                        }
                    )*
                    _ => Err(format!("Unknown message type hash: {:08x}", type_hash).into()),
                }
            }
            
            /// Register this actor's message types - AUTOMATIC via spawn
            pub fn __register_message_types(actor_ref: $crate::actor::ActorRef<Self>) {
                use $crate::remote::type_hash::HasTypeHash;
                use $crate::remote::_internal::RemoteMessageFns;
                
                let actor_id = actor_ref.id();
                
                $(
                    // Register type hash → actor_id mapping
                    let type_hash = <$msg_ty as HasTypeHash>::TYPE_HASH;
                    $crate::remote::type_registry::register_type(
                        type_hash.as_u32(),
                        actor_id,
                        concat!(stringify!($actor), "::", stringify!($handler))
                    );
                    
                    // Create handlers that actually send messages to the actor
                    let actor_ref_for_tell = actor_ref.clone();
                    let fns = RemoteMessageFns {
                        tell: std::sync::Arc::new(move |aid, payload, timeout| {
                            let actor_ref = actor_ref_for_tell.clone();
                            Box::pin(async move {
                                if aid != actor_id {
                                    return Err($crate::error::RemoteSendError::ActorNotRunning);
                                }
                                
                                // Create and send the distributed message through the actor's mailbox
                                let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                    type_hash: type_hash.as_u32(),
                                    payload,
                                };
                                
                                use $crate::request::TellRequest;
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
                        try_tell: std::sync::Arc::new({
                            let actor_ref_for_try_tell = actor_ref.clone();
                            move |aid, payload| {
                                let actor_ref = actor_ref_for_try_tell.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }
                                    
                                    let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                        type_hash: type_hash.as_u32(),
                                        payload,
                                    };
                                    
                                    // For try_tell, we use tell without timeout
                                    use $crate::request::TellRequest;
                                    match actor_ref.tell(msg).try_send() {
                                        Ok(()) => Ok(()),
                                        Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                    }
                                })
                            }
                        }),
                        ask: std::sync::Arc::new(|_, _, _, _| {
                            Box::pin(async move {
                                // The distributed_actor macro is for tell messages
                                // Ask messages should use the Message trait implementation
                                Err($crate::error::RemoteSendError::ActorStopped)
                            })
                        }),
                        try_ask: std::sync::Arc::new(|_, _, _| {
                            Box::pin(async move {
                                Err($crate::error::RemoteSendError::ActorStopped)
                            })
                        }),
                    };
                    
                    // Register the handler in TYPE_HASH_REGISTRY
                    $crate::remote::v2_bootstrap::get_distributed_handler()
                        .register_type_handler(type_hash.as_u32(), fns);
                )*
            }
        }
        
        // Override Actor::spawn to auto-register on spawn
        impl $actor {
            pub fn spawn(args: <Self as $crate::actor::Actor>::Args) -> $crate::actor::ActorRef<Self> {
                let actor_ref = <Self as $crate::actor::Actor>::spawn(args);
                // Auto-register message types on spawn
                Self::__register_message_types(actor_ref.clone());
                actor_ref
            }
        }
    };
}

/// Re-export the old macro name for backward compatibility
/// This is now mostly a no-op - actors are registered directly with transport
#[macro_export]
macro_rules! distributed_actor {
    (
        $actor:ty {
            $($msg_ty:ty => $handler:ident),* $(,)?
        }
    ) => {
        $crate::simple_distributed_actor! {
            $actor {
                $($msg_ty => $handler),*
            }
        }
    };
}