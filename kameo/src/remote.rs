//! Remote actor functionality.

use std::{
    any,
    borrow::Cow,
    collections::{HashMap, HashSet},
    time::Duration,
};

use _internal::{
    AskRemoteMessageFn, RemoteMessageRegistrationID, TellRemoteMessageFn, REMOTE_MESSAGES,
};
pub use libp2p::PeerId;
pub use libp2p_identity::Keypair;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::{actor::ActorID, error::RemoteSendError};

#[doc(hidden)]
pub mod _internal;
mod swarm;

pub use swarm::*;

pub(crate) static REMOTE_REGISTRY: Lazy<Mutex<HashMap<ActorID, Box<dyn any::Any + Send + Sync>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static REMOTE_MESSAGES_MAP: Lazy<
    HashMap<RemoteMessageRegistrationID<'static>, (AskRemoteMessageFn, TellRemoteMessageFn)>,
> = Lazy::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_MESSAGES {
        if !existing_ids.insert(id) {
            panic!(
                "duplicate remote message detected for actor '{}' and message '{}'",
                id.actor_remote_id, id.message_remote_id
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
pub trait RemoteMessage<M> {
    /// The remote identifier string.
    const REMOTE_ID: &'static str;
}

pub(crate) async fn ask(
    actor_id: ActorID,
    actor_remote_id: Cow<'static, str>,
    message_remote_id: Cow<'static, str>,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>> {
    let Some((handler, _)) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    handler(actor_id, payload, mailbox_timeout, reply_timeout, immediate).await
}

pub(crate) async fn tell(
    actor_id: ActorID,
    actor_remote_id: Cow<'static, str>,
    message_remote_id: Cow<'static, str>,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError<Vec<u8>>> {
    let Some((_, handler)) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    handler(actor_id, payload, mailbox_timeout, immediate).await
}
