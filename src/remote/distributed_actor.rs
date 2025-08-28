//! Unified distributed actor macro with full support for both tell and ask/reply
//!
//! This macro supports both zero-copy archived handlers and Message trait implementations,
//! allowing for maximum flexibility and performance.

/// Unified distributed actor macro with ask/reply and zero-copy support
///
/// This macro supports two syntaxes:
/// 1. List syntax: Uses Message trait if available for ask/reply, falls back to convention-based handlers
/// 2. Mapping syntax: Maps messages to specific archived handler methods for zero-copy
///
/// # Usage
///
/// ## List Syntax (uses Message trait)
/// ```ignore
/// distributed_actor! {
///     MyActor {
///         MessageA,
///         MessageB,
///     }
/// }
/// ```
///
/// ## Mapping Syntax (uses archived handlers for zero-copy)
/// ```ignore
/// distributed_actor! {
///     MyActor {
///         MessageA => handle_message_a,  // handler takes &rkyv::Archived<MessageA>
///         MessageB => handle_message_b,
///     }
/// }
/// ```
///
/// ## Mixed Syntax
/// ```ignore
/// distributed_actor! {
///     MyActor {
///         MessageA => handle_a,  // Uses archived handler
///         MessageB,              // Uses Message trait
///     }
/// }
/// ```
#[macro_export]
macro_rules! distributed_actor {
    // Mapping syntax: message => handler
    (
        $actor:ty {
            $($msg_ty:ty => $handler:ident),* $(,)?
        }
    ) => {
        // Generate HasTypeHash for actor type
        $crate::impl_type_hash!($actor);

        // Implement Message handler for DistributedMessage (internal routing)
        impl $crate::message::Message<$crate::remote::simple_distributed_actor::DistributedMessage> for $actor {
            type Reply = Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;

            async fn handle(
                &mut self,
                msg: $crate::remote::simple_distributed_actor::DistributedMessage,
                _ctx: &mut $crate::message::Context<Self, Self::Reply>,
            ) -> Self::Reply {
                self.__handle_distributed_message(msg.type_hash, &msg.payload).await
            }
        }

        // Implement the DistributedActor trait
        impl $crate::remote::DistributedActor for $actor {
            #[doc(hidden)]
            fn __register_distributed_handlers(actor_ref: &$crate::actor::ActorRef<Self>) {
                Self::__internal_register_handlers(actor_ref);
            }
        }

        // Implement ActorRegistration for distributed actors
        impl $crate::actor::ActorRegistration for $actor {
            fn register_actor(actor_ref: &$crate::actor::ActorRef<Self>, name: &str) -> Result<(), $crate::error::RegistryError> {
                // First do local registration
                let was_inserted = $crate::registry::ACTOR_REGISTRY
                    .lock()
                    .unwrap()
                    .insert(name.to_string(), actor_ref.clone());
                if !was_inserted {
                    return Err($crate::error::RegistryError::NameAlreadyRegistered);
                }

                // Then do distributed registration
                #[cfg(feature = "remote")]
                {
                    // tracing::info!("🔄 [DISTRIBUTED] Starting distributed registration for actor '{}' of type '{}'", name, stringify!($actor));

                    // Get the global transport and attempt distributed registration
                    if let Some(transport) = {
                        $crate::remote::distributed_actor_ref::GLOBAL_TRANSPORT.lock().unwrap().clone()
                    } {
                        tracing::info!("🌐 [DISTRIBUTED] Global transport available, spawning registration task for '{}'", name);
                        let actor_ref = actor_ref.clone();
                        let name_owned = name.to_string();
                        tokio::spawn(async move {
                            // tracing::info!("🚀 [DISTRIBUTED] Starting async registration for '{}'", name_owned);
                            match transport.register_distributed_actor(name_owned.clone(), &actor_ref).await {
                                Ok(_) => {
                                    // tracing::info!("✅ [DISTRIBUTED] Successfully registered distributed actor '{}'", name_owned);
                                }
                                Err(e) => {
                                    tracing::warn!("⚠️ [DISTRIBUTED] Failed to register distributed actor '{}': {:?}", name_owned, e);
                                }
                            }
                        });
                    } else {
                        tracing::warn!("❌ [DISTRIBUTED] No global transport available for actor '{}'", name);
                    }
                }

                Ok(())
            }
        }

        impl $actor {
            /// Internal handler registration - not for public use
            #[doc(hidden)]
            fn __internal_register_handlers(actor_ref: &$crate::actor::ActorRef<Self>) {
                use $crate::remote::type_hash::HasTypeHash;
                use $crate::remote::_internal::RemoteMessageFns;

                // Validate that transport is configured before registering handlers
                {
                    let global = $crate::remote::distributed_actor_ref::GLOBAL_TRANSPORT.lock().unwrap();
                    if global.is_none() {
                        panic!("No transport configured - call bootstrap_on() or bootstrap_with_config() before registering distributed actors");
                    }
                }

                let actor_id = actor_ref.id();
                let actor_ref_clone = actor_ref.clone();

                // Register handlers for each message type
                $(
                    {
                        let type_hash = <$msg_ty as HasTypeHash>::TYPE_HASH;
                        let actor_ref_for_tell = actor_ref_clone.clone();
                        let actor_ref_for_try_tell = actor_ref_clone.clone();
                        let actor_ref_for_ask = actor_ref_clone.clone();
                        let actor_ref_for_try_ask = actor_ref_clone.clone();

                        let fns = RemoteMessageFns {
                            // Tell handler - sends DistributedMessage to actor
                            tell: std::sync::Arc::new(move |aid, payload, _timeout| {
                                let actor_ref = actor_ref_for_tell.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }

                                    // Create DistributedMessage and send to actor
                                    let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                        type_hash: type_hash.as_u32(),
                                        payload: payload.to_vec(),
                                    };

                                    match actor_ref.tell(msg).send().await {
                                        Ok(()) => Ok(()),
                                        Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                    }
                                })
                            }),

                            // Try tell handler - simplified version without priority
                            try_tell: std::sync::Arc::new(move |aid, payload| {
                                let actor_ref = actor_ref_for_try_tell.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }

                                    // Create DistributedMessage and send to actor with priority
                                    let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                        type_hash: type_hash.as_u32(),
                                        payload: payload.to_vec(),
                                    };

                                    // Note: Priority is handled by Kameo's request system
                                    match actor_ref.tell(msg).send().await {
                                        Ok(()) => Ok(()),
                                        Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                    }
                                })
                            }),

                            // Ask handler - sends DistributedMessage and waits for reply
                            ask: std::sync::Arc::new(move |aid, payload, mailbox_timeout, reply_timeout| {
                                let actor_ref = actor_ref_for_ask.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }

                                    // Create DistributedMessage and send as ask
                                    let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                        type_hash: type_hash.as_u32(),
                                        payload: payload.to_vec(),
                                    };

                                    // Note: Timeout would need to be implemented via reply_timeout/mailbox_timeout
                                    // For now, use default timeout
                                    let result = actor_ref.ask(msg).send().await;

                                    match result {
                                        Ok(Ok(reply)) => Ok(reply),
                                        Ok(Err(_e)) => Err($crate::error::RemoteSendError::ActorStopped),
                                        Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                    }
                                })
                            }),

                            // Try ask handler - simplified version without priority
                            try_ask: std::sync::Arc::new(move |aid, payload, timeout| {
                                let actor_ref = actor_ref_for_try_ask.clone();
                                Box::pin(async move {
                                    if aid != actor_id {
                                        return Err($crate::error::RemoteSendError::ActorNotRunning);
                                    }

                                    // Create DistributedMessage and send as ask with priority
                                    let msg = $crate::remote::simple_distributed_actor::DistributedMessage {
                                        type_hash: type_hash.as_u32(),
                                        payload: payload.to_vec(),
                                    };

                                    // Note: We don't apply timeout here, would need reply_timeout/mailbox_timeout
                                    let result = actor_ref.ask(msg).send().await;

                                    match result {
                                        Ok(Ok(reply)) => Ok(reply),
                                        Ok(Err(_e)) => Err($crate::error::RemoteSendError::ActorStopped),
                                        Err(_) => Err($crate::error::RemoteSendError::ActorNotRunning),
                                    }
                                })
                            }),
                        };

                        // Register with global distributed handler
                        $crate::remote::v2_bootstrap::get_distributed_handler()
                            .register_type_handler(type_hash.as_u32(), fns);
                    }
                )*
            }

            /// Handle a distributed message by type hash (for archived handlers)
            pub async fn __handle_distributed_message(
                &mut self,
                type_hash: u32,
                payload: &[u8],
            ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
                use $crate::remote::type_hash::HasTypeHash;

                $(
                    if type_hash == <$msg_ty as HasTypeHash>::TYPE_HASH.as_u32() {
                        // Access the message in archived form (zero-copy)
                        let archived = unsafe {
                            ::rkyv::access_unchecked::<::rkyv::Archived<$msg_ty>>(payload)
                        };

                        // Call the handler with archived message
                        let result = self.$handler(archived).await;

                        // Check if the handler returns a value (for ask/reply support)
                        // If it returns (), it's a tell-only message
                        #[allow(unused_assignments)]
                        let mut has_reply = false;

                        // Use a trick to check if result is () or something else
                        if std::mem::size_of_val(&result) > 0 {
                            has_reply = true;
                            // Serialize the reply
                            let reply_bytes = ::rkyv::to_bytes::<rkyv::rancor::Error>(&result)
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                            return Ok(reply_bytes.to_vec());
                        }

                        return Ok(Vec::new());
                    }
                )*

                Err(format!("Unknown message type hash: {:08x}", type_hash).into())
            }
        }

    };

    // List syntax: just message types (uses Message trait or convention)
    (
        $actor:ty {
            $($msg_ty:ty),* $(,)?
        }
    ) => {
        // Generate HasTypeHash for actor type
        $crate::impl_type_hash!($actor);

        // Implement the DistributedActor trait
        impl $crate::remote::DistributedActor for $actor {
            #[doc(hidden)]
            fn __register_distributed_handlers(actor_ref: &$crate::actor::ActorRef<Self>) {
                Self::__internal_register_handlers(actor_ref);
            }
        }

        // Implement ActorRegistration for distributed actors
        impl $crate::actor::ActorRegistration for $actor {
            fn register_actor(actor_ref: &$crate::actor::ActorRef<Self>, name: &str) -> Result<(), $crate::error::RegistryError> {
                // First do local registration
                let was_inserted = $crate::registry::ACTOR_REGISTRY
                    .lock()
                    .unwrap()
                    .insert(name.to_string(), actor_ref.clone());
                if !was_inserted {
                    return Err($crate::error::RegistryError::NameAlreadyRegistered);
                }

                // Then do distributed registration
                #[cfg(feature = "remote")]
                {
                    // tracing::info!("🔄 [DISTRIBUTED] Starting distributed registration for actor '{}' of type '{}'", name, stringify!($actor));

                    // Get the global transport and attempt distributed registration
                    if let Some(transport) = {
                        $crate::remote::distributed_actor_ref::GLOBAL_TRANSPORT.lock().unwrap().clone()
                    } {
                        // tracing::info!("🌐 [DISTRIBUTED] Global transport available, spawning registration task for '{}'", name);
                        let actor_ref = actor_ref.clone();
                        let name_owned = name.to_string();
                        tokio::spawn(async move {
                            tracing::info!("🚀 [DISTRIBUTED] Starting async registration for '{}'", name_owned);
                            match transport.register_distributed_actor(name_owned.clone(), &actor_ref).await {
                                Ok(_) => {
                                    // tracing::info!("✅ [DISTRIBUTED] Successfully registered distributed actor '{}'", name_owned);
                                }
                                Err(e) => {
                                    tracing::warn!("⚠️ [DISTRIBUTED] Failed to register distributed actor '{}': {:?}", name_owned, e);
                                }
                            }
                        });
                    } else {
                        tracing::warn!("❌ [DISTRIBUTED] No global transport available for actor '{}'", name);
                    }
                }

                Ok(())
            }
        }

        impl $actor {
            /// Internal handler registration - not for public use
            #[doc(hidden)]
            fn __internal_register_handlers(actor_ref: &$crate::actor::ActorRef<Self>) {
                use $crate::remote::type_hash::HasTypeHash;
                use $crate::remote::_internal::RemoteMessageFns;
                use $crate::message::Message;
                use $crate::request::{AskRequest, TellRequest};

                // Validate that transport is configured before registering handlers
                {
                    let global = $crate::remote::distributed_actor_ref::GLOBAL_TRANSPORT.lock().unwrap();
                    if global.is_none() {
                        panic!("No transport configured - call bootstrap_on() or bootstrap_with_config() before registering distributed actors");
                    }
                }

                let actor_id = actor_ref.id();
                let actor_ref = actor_ref.clone();

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
        }

    };
}
