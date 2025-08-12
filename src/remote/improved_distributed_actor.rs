//! Improved distributed actor macro that supports ask/reply and delegated replies
//! 
//! This macro generates proper handlers for both tell and ask messages, 
//! detecting when a message type has a Reply and using the Message trait directly.

/// Improved distributed actor macro with full ask/reply support
/// 
/// This macro supports both tell-only messages and ask/reply messages:
/// - Messages with `=> handler` use the handler method (tell-only)
/// - Messages with Message trait implementation support ask/reply
/// 
/// # Example
/// ```ignore
/// distributed_actor! {
///     CalculatorActor {
///         Add => handle_add,        // Can be tell or ask if Message trait exists
///         Multiply => handle_multiply,
///         Increment => handle_increment, // Tell-only
///     }
/// }
/// ```
#[macro_export]
macro_rules! improved_distributed_actor {
    (
        $actor:ty {
            $($msg_ty:ty => $handler:ident),* $(,)?
        }
    ) => {
        // Generate HasTypeHash for actor type
        $crate::impl_type_hash!($actor);
        
        // Implement Message handler for DistributedMessage (for tell-only messages)
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
            /// Handle a distributed message by type hash
            pub async fn __handle_distributed_message(
                &mut self,
                type_hash: u32,
                payload: &[u8],
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                use $crate::remote::type_hash::HasTypeHash;
                
                match type_hash {
                    $(
                        hash if hash == <$msg_ty as HasTypeHash>::TYPE_HASH.as_u32() => {
                            let archived = unsafe { ::rkyv::access_unchecked::<::rkyv::Archived<$msg_ty>>(payload) };
                            self.$handler(archived).await;
                            Ok(())
                        }
                    )*
                    _ => Err(format!("Unknown message type hash: {:08x}", type_hash).into()),
                }
            }
            
            /// Register message handlers for this actor
            pub fn __register_message_types(actor_ref: $crate::actor::ActorRef<Self>) {
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
                            // Tell handler - send through DistributedMessage
                            tell: std::sync::Arc::new(move |aid, payload, timeout| {
                                let actor_ref = actor_ref_for_tell.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }
                                    
                                    // Create and send the distributed message
                                    let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                        type_hash: type_hash.as_u32(),
                                        payload,
                                    };
                                    
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
                                        
                                        let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                            type_hash: type_hash.as_u32(),
                                            payload,
                                        };
                                        
                                        match actor_ref.tell(msg).try_send() {
                                            Ok(()) => Ok(()),
                                            Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                        }
                                    })
                                }
                            }),
                            
                            // Ask handler - check if Message trait is implemented
                            ask: $crate::remote::improved_distributed_actor::create_ask_handler::<$actor, $msg_ty>(actor_ref_for_ask.clone(), actor_id),
                            
                            // try_ask handler
                            try_ask: $crate::remote::improved_distributed_actor::create_try_ask_handler::<$actor, $msg_ty>(actor_ref.clone(), actor_id),
                        };
                        
                        // Register with the distributed handler
                        $crate::remote::v2_bootstrap::get_distributed_handler()
                            .register_type_handler(type_hash.as_u32(), fns);
                        
                        // Also register in type registry for lookups
                        $crate::remote::type_registry::register_type(
                            type_hash.as_u32(),
                            actor_id,
                            concat!(stringify!($actor), "::", stringify!($handler))
                        );
                    }
                )*
            }
            
            // Override spawn to auto-register message types
            pub fn spawn(args: <Self as $crate::actor::Actor>::Args) -> $crate::actor::ActorRef<Self> {
                let actor_ref = <Self as $crate::actor::Actor>::spawn(args);
                Self::__register_message_types(actor_ref.clone());
                actor_ref
            }
        }
    };
}

// Helper functions to create ask handlers based on whether Message trait is implemented
pub fn create_ask_handler<A, M>(
    actor_ref: kameo::actor::ActorRef<A>,
    actor_id: kameo::actor::ActorId,
) -> std::sync::Arc<
    dyn Fn(
            kameo::actor::ActorId,
            Vec<u8>,
            Option<std::time::Duration>,
            Option<std::time::Duration>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Vec<u8>, kameo::error::RemoteSendError>>
                    + Send,
            >,
        > + Send
        + Sync,
>
where
    A: kameo::actor::Actor + kameo::message::Message<M> + 'static,
    M: rkyv::Archive + for<'a> rkyv::Deserialize<M, rkyv::de::Strategy<'a, rkyv::de::Pool>>,
    <A as kameo::message::Message<M>>::Reply: rkyv::Serialize<rkyv::ser::Strategy<rkyv::ser::Composite<rkyv::ser::Seal, rkyv::ser::Allocate>, rkyv::AllocScratch>>,
{
    use kameo::request::AskRequest;
    
    std::sync::Arc::new(move |aid, payload, mailbox_timeout, reply_timeout| {
        let actor_ref = actor_ref.clone();
        Box::pin(async move {
            if aid != actor_id {
                return Err(kameo::error::RemoteSendError::ActorNotRunning);
            }
            
            // Deserialize the message
            let msg: M = rkyv::from_bytes::<M, rkyv::rancor::Error>(&payload)
                .map_err(|_| kameo::error::RemoteSendError::ActorStopped)?;
            
            // Send ask request
            let result = if let Some(m_timeout) = mailbox_timeout {
                if let Some(r_timeout) = reply_timeout {
                    actor_ref.ask(msg).mailbox_timeout(m_timeout).reply_timeout(r_timeout).send().await
                } else {
                    actor_ref.ask(msg).mailbox_timeout(m_timeout).send().await
                }
            } else if let Some(r_timeout) = reply_timeout {
                actor_ref.ask(msg).reply_timeout(r_timeout).send().await
            } else {
                actor_ref.ask(msg).send().await
            };
            
            match result {
                Ok(reply) => {
                    // Serialize the reply
                    let reply_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&reply)
                        .map_err(|_| kameo::error::RemoteSendError::ActorStopped)?;
                    Ok(reply_bytes.to_vec())
                },
                Err(_) => Err(kameo::error::RemoteSendError::ActorStopped),
            }
        })
    })
}

pub fn create_try_ask_handler<A, M>(
    actor_ref: kameo::actor::ActorRef<A>,
    actor_id: kameo::actor::ActorId,
) -> std::sync::Arc<
    dyn Fn(
            kameo::actor::ActorId,
            Vec<u8>,
            Option<std::time::Duration>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Vec<u8>, kameo::error::RemoteSendError>>
                    + Send,
            >,
        > + Send
        + Sync,
>
where
    A: kameo::actor::Actor + kameo::message::Message<M> + 'static,
    M: rkyv::Archive + for<'a> rkyv::Deserialize<M, rkyv::de::Strategy<'a, rkyv::de::Pool>>,
    <A as kameo::message::Message<M>>::Reply: rkyv::Serialize<rkyv::ser::Strategy<rkyv::ser::Composite<rkyv::ser::Seal, rkyv::ser::Allocate>, rkyv::AllocScratch>>,
{
    use kameo::request::AskRequest;
    
    std::sync::Arc::new(move |aid, payload, reply_timeout| {
        let actor_ref = actor_ref.clone();
        Box::pin(async move {
            if aid != actor_id {
                return Err(kameo::error::RemoteSendError::ActorNotRunning);
            }
            
            // Deserialize the message
            let msg: M = rkyv::from_bytes::<M, rkyv::rancor::Error>(&payload)
                .map_err(|_| kameo::error::RemoteSendError::ActorStopped)?;
            
            // Try ask without blocking on mailbox
            let result = if let Some(r_timeout) = reply_timeout {
                actor_ref.ask(msg).reply_timeout(r_timeout).try_send().await
            } else {
                actor_ref.ask(msg).try_send().await
            };
            
            match result {
                Ok(Ok(reply)) => {
                    // Serialize the reply
                    let reply_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&reply)
                        .map_err(|_| kameo::error::RemoteSendError::ActorStopped)?;
                    Ok(reply_bytes.to_vec())
                },
                Ok(Err(_)) => Err(kameo::error::RemoteSendError::ActorStopped),
                Err(_) => Err(kameo::error::RemoteSendError::ActorNotRunning),
            }
        })
    })
}