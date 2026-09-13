//! Traits declaring which actors and messages are available remotely.

use std::collections::HashMap;

use futures::FutureExt;
use kameo::{Actor, Reply, actor::ActorRef, error::SendError, message::Message};
use serde::{Serialize, de::DeserializeOwned};
use serde_bytes::ByteBuf;
use std::sync::Arc;

use crate::{
    dispatch::{DynHandler, InboundKind},
    messaging::protocol::WireError,
};

/// A message which can be sent to actors on other nodes.
///
/// The `REMOTE_ID` is the message's wire identifier and must be identical across all
/// binaries in the cluster. Prefer a namespaced string such as `"my_crate::Inc"`.
pub trait RemoteMessage: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Stable wire identifier for this message type.
    const REMOTE_ID: &'static str;
}

/// An actor which can be registered and messaged remotely.
///
/// Implementations declare the actor's wire identifier and the set of messages it
/// accepts remotely:
///
/// ```
/// # use kameo::prelude::*;
/// # use kameo_remote::{RemoteActor, RemoteMessage, RemoteMessages};
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Actor)]
/// # struct MyActor { count: i64 }
/// # #[derive(Serialize, Deserialize)]
/// # struct Inc { amount: u32 }
/// # impl Message<Inc> for MyActor {
/// #     type Reply = i64;
/// #     async fn handle(&mut self, msg: Inc, _: &mut Context<Self, Self::Reply>) -> i64 {
/// #         self.count += msg.amount as i64;
/// #         self.count
/// #     }
/// # }
/// # impl RemoteMessage for Inc {
/// #     const REMOTE_ID: &'static str = "example::Inc";
/// # }
/// impl RemoteActor for MyActor {
///     const REMOTE_ID: &'static str = "example::MyActor";
///
///     fn remote_messages(handlers: &mut RemoteMessages<Self>) {
///         handlers.add::<Inc>();
///     }
/// }
/// ```
pub trait RemoteActor: Actor + Sized {
    /// Stable wire identifier for this actor type.
    const REMOTE_ID: &'static str;

    /// Declares which messages this actor accepts remotely.
    fn remote_messages(handlers: &mut RemoteMessages<Self>);
}

/// Collects the remote message handlers for an actor being registered.
pub struct RemoteMessages<A: Actor> {
    actor_ref: ActorRef<A>,
    handlers: HashMap<&'static str, DynHandler>,
}

impl<A: Actor> RemoteMessages<A> {
    pub(crate) fn new(actor_ref: ActorRef<A>) -> Self {
        RemoteMessages {
            actor_ref,
            handlers: HashMap::new(),
        }
    }

    pub(crate) fn into_handlers(self) -> HashMap<&'static str, DynHandler> {
        self.handlers
    }

    /// Makes the message type `M` remotely callable on this actor.
    pub fn add<M>(&mut self)
    where
        A: Message<M>,
        M: RemoteMessage,
        <A::Reply as Reply>::Ok: Serialize,
        <A::Reply as Reply>::Error: Serialize,
    {
        let actor_ref = self.actor_ref.clone();
        let handler: DynHandler = Arc::new(move |payload: Vec<u8>, kind: InboundKind| {
            let actor_ref = actor_ref.clone();
            async move {
                let msg: M = rmp_serde::from_slice(&payload)
                    .map_err(|err| WireError::DeserializeMessage(err.to_string()))?;
                match kind {
                    InboundKind::Tell => match actor_ref.tell(msg).send().await {
                        Ok(()) => Ok(None),
                        Err(err) => Err(tell_error_to_wire(err)),
                    },
                    InboundKind::Ask { reply_timeout } => {
                        // reply_timeout_opt is private in kameo, so branch on the typestate builder.
                        let result = match reply_timeout {
                            Some(timeout) => actor_ref.ask(msg).reply_timeout(timeout).send().await,
                            None => actor_ref.ask(msg).send().await,
                        };
                        match result {
                            Ok(ok) => {
                                Ok(Some(rmp_serde::to_vec_named(&ok).map_err(|err| {
                                    WireError::SerializeReply(err.to_string())
                                })?))
                            }
                            Err(SendError::HandlerError(err)) => {
                                match rmp_serde::to_vec_named(&err) {
                                    Ok(bytes) => Err(WireError::HandlerError(ByteBuf::from(bytes))),
                                    Err(err) => {
                                        Err(WireError::SerializeHandlerError(err.to_string()))
                                    }
                                }
                            }
                            Err(SendError::ActorNotRunning(_) | SendError::ActorRestarting(_)) => {
                                Err(WireError::ActorNotRunning)
                            }
                            Err(SendError::ActorStopped) => Err(WireError::ActorStopped),
                            Err(SendError::MailboxFull(_)) => Err(WireError::MailboxFull),
                            Err(SendError::Timeout(_)) => Err(WireError::ReplyTimeout),
                        }
                    }
                }
            }
            .boxed()
        });
        self.handlers.insert(M::REMOTE_ID, handler);
    }
}

fn tell_error_to_wire<M>(err: SendError<M>) -> WireError {
    match err {
        SendError::ActorNotRunning(_) | SendError::ActorRestarting(_) => WireError::ActorNotRunning,
        SendError::ActorStopped => WireError::ActorStopped,
        SendError::MailboxFull(_) => WireError::MailboxFull,
        SendError::Timeout(_) => WireError::ReplyTimeout,
        SendError::HandlerError(infallible) => match infallible {},
    }
}
