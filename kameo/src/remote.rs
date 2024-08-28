//! Remote actor functionality.

use std::{
    any,
    collections::{HashMap, HashSet},
    time::Duration,
};

use _internal::{
    AskRemoteMessageFn, RemoteMessageRegistrationID, RemoteSpawnFn, TellRemoteMessageFn,
    REMOTE_ACTORS, REMOTE_MESSAGES,
};
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::{
    actor::ActorID,
    error::{RemoteSendError, RemoteSpawnError},
};

#[doc(hidden)]
pub mod _internal;
mod swarm;

pub use swarm::*;

pub(crate) static REMOTE_REGISTRY: Lazy<Mutex<HashMap<ActorID, Box<dyn any::Any + Send + Sync>>>> =
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
#[macro_export]
macro_rules! register_actor {
    ($actor_ty:ty) => {
        impl $crate::remote::RemoteActor for $actor_ty {
            const REMOTE_ID: &'static str =
                ::std::concat!(::std::module_path!(), "::", ::std::stringify!($actor_ty));
        }

        const _: () = {
            #[$crate::remote::_internal::distributed_slice(
                $crate::remote::_internal::REMOTE_ACTORS
            )]
            static REG: (&'static str, $crate::remote::_internal::RemoteSpawnFn) = (
                <$actor_ty as $crate::remote::RemoteActor>::REMOTE_ID,
                (|actor: ::std::vec::Vec<u8>| {
                    ::std::boxed::Box::pin($crate::remote::_internal::spawn_remote::<$actor_ty>(
                        actor,
                    ))
                }) as $crate::remote::_internal::RemoteSpawnFn,
            );
        };
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! register_message {
    ($actor_ty:ty, $message_ty:ty) => {
        impl $crate::remote::RemoteMessage for $message_ty {
            const REMOTE_ID: &'static str =
                ::std::concat!(::std::module_path!(), "::", ::std::stringify!($message_ty));
        }

        const _: () = {
            #[$crate::remote::_internal::distributed_slice(
                $crate::remote::_internal::REMOTE_MESSAGES
            )]
            static REG: (
                $crate::remote::_internal::RemoteMessageRegistrationID<'static>,
                (
                    $crate::remote::_internal::AskRemoteMessageFn,
                    $crate::remote::_internal::TellRemoteMessageFn,
                ),
            ) = (
                $crate::remote::_internal::RemoteMessageRegistrationID {
                    actor_name: <$actor_ty as $crate::remote::RemoteActor>::REMOTE_ID,
                    message_name: <$message_ty as $crate::remote::RemoteMessage>::REMOTE_ID,
                },
                (
                    (|actor_id: $crate::actor::ActorID,
                      msg: ::std::vec::Vec<u8>,
                      mailbox_timeout: Option<Duration>,
                      reply_timeout: Option<Duration>,
                      immediate: bool| {
                        ::std::boxed::Box::pin($crate::remote::_internal::ask_remote_message::<
                            $actor_ty,
                            $message_ty,
                        >(
                            actor_id,
                            msg,
                            mailbox_timeout,
                            reply_timeout,
                            immediate,
                        ))
                    }) as $crate::remote::_internal::AskRemoteMessageFn,
                    (|actor_id: $crate::actor::ActorID,
                      msg: ::std::vec::Vec<u8>,
                      mailbox_timeout: Option<Duration>,
                      immediate: bool| {
                        ::std::boxed::Box::pin($crate::remote::_internal::tell_remote_message::<
                            $actor_ty,
                            $message_ty,
                        >(
                            actor_id, msg, mailbox_timeout, immediate
                        ))
                    }) as $crate::remote::_internal::TellRemoteMessageFn,
                ),
            );
        };
    };
}
