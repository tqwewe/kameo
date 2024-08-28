//! Remote actor functionality based on grpc.

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    time::Duration,
};

use _internal::{
    AskRemoteMessageFn, RemoteMessageRegistrationID, RemoteSpawnFn, TellRemoteMessageFn,
    REMOTE_ACTORS, REMOTE_MESSAGES,
};
use futures::future::BoxFuture;
use once_cell::sync::Lazy;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::{
    actor::ActorRef,
    error::{RemoteSendError, RemoteSpawnError},
    message::Message,
    Actor,
};

use super::ActorID;

pub(crate) static REMOTE_REGISTRY: Lazy<Mutex<HashMap<ActorID, Box<dyn Any + Send + Sync>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static REMOTE_ACTORS_MAP: Lazy<HashMap<&'static str, RemoteSpawnFn>> = Lazy::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_ACTORS {
        if !existing_ids.insert(id) {
            panic!("duplicate remote actor detected for actor '{id}'",);
        }
    }
    REMOTE_ACTORS.iter().copied().collect()
});

static REMOTE_MESSAGES_MAP: Lazy<
    HashMap<RemoteMessageRegistrationID<'static>, (AskRemoteMessageFn, TellRemoteMessageFn)>,
> = Lazy::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_MESSAGES {
        if !existing_ids.insert(id) {
            panic!(
                "duplicate remote message detected for actor '{}' and message '{}'",
                id.actor_name, id.message_name
            );
        }
    }
    REMOTE_MESSAGES.iter().copied().collect()
});

#[doc(hidden)]
#[macro_export]
macro_rules! register_actor {
    ($actor_ty:ty) => {
        impl $crate::actor::remote::RemoteActor for $actor_ty {
            const REMOTE_ID: &'static str =
                ::std::concat!(::std::module_path!(), "::", ::std::stringify!($actor_ty));
        }

        const _: () = {
            #[$crate::actor::remote::_internal::distributed_slice(
                $crate::actor::remote::_internal::REMOTE_ACTORS
            )]
            static REG: (
                &'static str,
                $crate::actor::remote::_internal::RemoteSpawnFn,
            ) = (
                <$actor_ty as $crate::actor::remote::RemoteActor>::REMOTE_ID,
                (|actor: ::std::vec::Vec<u8>| {
                    ::std::boxed::Box::pin($crate::actor::remote::_internal::spawn_remote::<
                        $actor_ty,
                    >(actor))
                }) as $crate::actor::remote::_internal::RemoteSpawnFn,
            );
        };
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! register_message {
    ($actor_ty:ty, $message_ty:ty) => {
        impl $crate::actor::remote::RemoteMessage for $message_ty {
            const REMOTE_ID: &'static str =
                ::std::concat!(::std::module_path!(), "::", ::std::stringify!($message_ty));
        }

        const _: () = {
            #[$crate::actor::remote::_internal::distributed_slice(
                $crate::actor::remote::_internal::REMOTE_MESSAGES
            )]
            static REG: (
                $crate::actor::remote::_internal::RemoteMessageRegistrationID<'static>,
                (
                    $crate::actor::remote::_internal::AskRemoteMessageFn,
                    $crate::actor::remote::_internal::TellRemoteMessageFn,
                ),
            ) = (
                $crate::actor::remote::_internal::RemoteMessageRegistrationID {
                    actor_name: <$actor_ty as $crate::actor::remote::RemoteActor>::REMOTE_ID,
                    message_name: <$message_ty as $crate::actor::remote::RemoteMessage>::REMOTE_ID,
                },
                (
                    (|actor_id: $crate::actor::ActorID,
                      msg: ::std::vec::Vec<u8>,
                      mailbox_timeout: Option<Duration>,
                      reply_timeout: Option<Duration>,
                      immediate: bool| {
                        ::std::boxed::Box::pin(
                            $crate::actor::remote::_internal::ask_remote_message::<
                                $actor_ty,
                                $message_ty,
                            >(
                                actor_id, msg, mailbox_timeout, reply_timeout, immediate
                            ),
                        )
                    }) as $crate::actor::remote::_internal::AskRemoteMessageFn,
                    (|actor_id: $crate::actor::ActorID,
                      msg: ::std::vec::Vec<u8>,
                      mailbox_timeout: Option<Duration>,
                      immediate: bool| {
                        ::std::boxed::Box::pin(
                            $crate::actor::remote::_internal::tell_remote_message::<
                                $actor_ty,
                                $message_ty,
                            >(actor_id, msg, mailbox_timeout, immediate),
                        )
                    }) as $crate::actor::remote::_internal::TellRemoteMessageFn,
                ),
            );
        };
    };
}

/// A trait for identifying actors remotely.
///
/// Every implementation of this must specify a unique string for the `REMOTE_ID`,
/// otherwise remote messaging would fail for ids which conflict.
pub trait RemoteActor {
    /// The remote identifier string.
    const REMOTE_ID: &'static str;
}

/// A trait for identifying messages remotely.
///
/// Every implementation of this must specify a unique string for the `REMOTE_ID`,
/// otherwise remote messaging would fail for ids which conflict.
pub trait RemoteMessage {
    /// The remote identifier string.
    const REMOTE_ID: &'static str;
}

pub(crate) async fn spawn(
    actor_name: String,
    payload: Vec<u8>,
) -> Result<ActorID, RemoteSpawnError> {
    let Some(spawn) = REMOTE_ACTORS_MAP.get(&actor_name.as_str()) else {
        return Err(RemoteSpawnError::UnknownActor(actor_name));
    };
    spawn(payload).await
}

pub(crate) async fn ask(
    actor_id: ActorID,
    actor_name: String,
    message_name: String,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>> {
    let Some((handler, _)) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_name: &actor_name,
        message_name: &message_name,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_name,
            message_name,
        });
    };
    handler(actor_id, payload, mailbox_timeout, reply_timeout, immediate).await
}

pub(crate) async fn tell(
    actor_id: ActorID,
    actor_name: String,
    message_name: String,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError<Vec<u8>>> {
    let Some((_, handler)) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_name: &actor_name,
        message_name: &message_name,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_name,
            message_name,
        });
    };
    handler(actor_id, payload, mailbox_timeout, immediate).await
}

#[doc(hidden)]
pub mod _internal {
    pub use linkme::distributed_slice;
    use serde::Serialize;

    use crate::error::{RemoteSendError, RemoteSpawnError};
    use crate::Reply;

    use super::*;

    #[distributed_slice]
    pub static REMOTE_ACTORS: [(&'static str, RemoteSpawnFn)];

    #[distributed_slice]
    pub static REMOTE_MESSAGES: [(
        RemoteMessageRegistrationID<'static>,
        (AskRemoteMessageFn, TellRemoteMessageFn),
    )];

    pub type RemoteSpawnFn =
        fn(actor: Vec<u8>) -> BoxFuture<'static, Result<ActorID, RemoteSpawnError>>;

    pub type AskRemoteMessageFn =
        fn(
            actor_id: ActorID,
            msg: Vec<u8>,
            mailbox_timeout: Option<Duration>,
            reply_timeout: Option<Duration>,
            immediate: bool,
        ) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>>;

    pub type TellRemoteMessageFn = fn(
        actor_id: ActorID,
        msg: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    )
        -> BoxFuture<'static, Result<(), RemoteSendError<Vec<u8>>>>;

    #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
    pub struct RemoteMessageRegistrationID<'a> {
        pub actor_name: &'a str,
        pub message_name: &'a str,
    }

    pub async fn spawn_remote<A>(actor: Vec<u8>) -> Result<ActorID, RemoteSpawnError>
    where
        A: Actor + DeserializeOwned + Send + Sync + 'static,
    {
        let actor: A = rmp_serde::decode::from_slice(&actor)
            .map_err(|err| RemoteSpawnError::DeserializeActor(err.to_string()))?;
        let actor_ref = crate::spawn(actor);
        let actor_id = actor_ref.id();
        REMOTE_REGISTRY
            .lock()
            .await
            .insert(actor_id, Box::new(actor_ref));

        Ok(actor_id)
    }

    pub async fn ask_remote_message<A, M>(
        actor_id: ActorID,
        msg: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>>
    where
        A: Actor + Message<M>,
        M: DeserializeOwned,
        ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
        <A::Reply as Reply>::Ok: Serialize,
        <A::Reply as Reply>::Error: Serialize,
    {
        let res = ask_remote_message_inner::<A, M>(
            actor_id,
            msg,
            mailbox_timeout,
            reply_timeout,
            immediate,
        )
        .await;
        match res {
            Ok(reply) => Ok(rmp_serde::to_vec_named(&reply)
                .map_err(|err| RemoteSendError::SerializeReply(err.to_string()))?),
            Err(err) => Err(err
                .map_err(|err| match rmp_serde::to_vec_named(&err) {
                    Ok(payload) => RemoteSendError::HandlerError(payload),
                    Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
                })
                .flatten()),
        }
    }

    async fn ask_remote_message_inner<A, M>(
        actor_id: ActorID,
        msg: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>>
    where
        A: Actor + Message<M>,
        M: DeserializeOwned,
        ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
    {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        let actor: &ActorRef<A> = remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast_ref()
            .ok_or(RemoteSendError::BadActorType)?;
        let msg: M = rmp_serde::decode::from_slice(&msg)
            .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

        let reply =
            crate::request::Request::ask(actor, msg, mailbox_timeout, reply_timeout, immediate)
                .await?;

        Ok(reply)
    }

    pub async fn tell_remote_message<A, M>(
        actor_id: ActorID,
        msg: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<(), RemoteSendError<Vec<u8>>>
    where
        A: Actor + Message<M>,
        M: DeserializeOwned,
        ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
        <A::Reply as Reply>::Error: Serialize,
    {
        let res =
            tell_remote_message_inner::<A, M>(actor_id, msg, mailbox_timeout, immediate).await;
        match res {
            Ok(()) => Ok(()),
            Err(err) => Err(err
                .map_err(|err| match rmp_serde::to_vec_named(&err) {
                    Ok(payload) => RemoteSendError::HandlerError(payload),
                    Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
                })
                .flatten()),
        }
    }

    async fn tell_remote_message_inner<A, M>(
        actor_id: ActorID,
        msg: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<(), RemoteSendError<<A::Reply as Reply>::Error>>
    where
        A: Actor + Message<M>,
        M: DeserializeOwned,
        ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
    {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        let actor: &ActorRef<A> = remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast_ref()
            .ok_or(RemoteSendError::BadActorType)?;
        let msg: M = rmp_serde::decode::from_slice(&msg)
            .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

        crate::request::Request::tell(actor, msg, mailbox_timeout, immediate).await?;

        Ok(())
    }
}
