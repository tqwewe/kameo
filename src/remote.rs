//! # Remote Actors in Kameo
//!
//! The `remote` module in Kameo provides tools for managing distributed actors across nodes,
//! enabling actors to communicate seamlessly in a peer-to-peer (P2P) network. By leveraging
//! the [libp2p](https://libp2p.io) library, Kameo allows you to register actors under unique
//! names and send messages between actors on different nodes as though they were local.
//!
//! ## Key Features
//!
//! - **Swarm Management**: The `ActorSwarm` struct handles a distributed swarm of nodes,
//!   managing peer discovery and communication.
//! - **Actor Registration**: Actors can be registered under a unique name and looked up across
//!   the network using the `RemoteActorRef`.
//! - **Message Routing**: Ensures reliable message delivery between nodes using a combination
//!   of Kademlia DHT and libp2p's networking capabilities.
//!
//! ## Getting Started
//!
//! To use remote actors, you must first initialize an `ActorSwarm`, which will set up the necessary
//! networking components to allow remote actors to communicate across nodes.
//!
//! ```
//! use kameo::remote::ActorSwarm;
//!
//! # tokio_test::block_on(async {
//! // Initialize the actor swarm
//! ActorSwarm::bootstrap()?
//!     .listen_on("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```
//!
//! ## Example Use Case
//!
//! - A distributed chat system where actors represent individual users, and messages are sent between them across multiple nodes.
//!
//! ## Types in the Module
//!
//! - [`ActorSwarm`]: The core struct for managing the distributed swarm of nodes and coordinating actor registration and messaging.
//! - [`SwarmFuture`]: A future that holds the response from the actor swarm.
//! - [`RemoteActor`]: A trait for identifying remote actors via a unique ID.
//! - [`RemoteMessage`]: A trait for identifying remote messages via a unique ID.
//!
//! ### Re-exports
//!
//! - `Keypair`, `PeerId`, `dial_opts`: Re-exported from the libp2p library to assist with handling peer identities and dialing options.

use std::{
    any,
    borrow::Cow,
    collections::{HashMap, HashSet},
    time::Duration,
};

use _internal::{
    RemoteActorFns, RemoteMessageFns, RemoteMessageRegistrationID, REMOTE_ACTORS, REMOTE_MESSAGES,
};
pub use libp2p::swarm::dial_opts;
pub use libp2p::PeerId;
pub use libp2p_identity::Keypair;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::{
    actor::{ActorID, Links},
    error::{ActorStopReason, Infallible, RemoteSendError},
    mailbox::SignalMailbox,
};

#[doc(hidden)]
pub mod _internal;
mod swarm;

pub use swarm::*;

pub(crate) static REMOTE_REGISTRY: Lazy<Mutex<HashMap<ActorID, RemoteRegistryActorRef>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(crate) struct RemoteRegistryActorRef {
    pub(crate) actor_ref: Box<dyn any::Any + Send + Sync>,
    pub(crate) signal_mailbox: Box<dyn SignalMailbox>,
    pub(crate) links: Links,
}

static REMOTE_ACTORS_MAP: Lazy<HashMap<&'static str, RemoteActorFns>> = Lazy::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_ACTORS {
        if !existing_ids.insert(id) {
            panic!("duplicate remote actor detected for actor '{id}'");
        }
    }
    REMOTE_ACTORS.iter().copied().collect()
});

static REMOTE_MESSAGES_MAP: Lazy<HashMap<RemoteMessageRegistrationID<'static>, RemoteMessageFns>> =
    Lazy::new(|| {
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

/// `RemoteActor` is a trait for identifying actors remotely.
///
/// Each remote actor must implement this trait and provide a unique identifier string (`REMOTE_ID`).
/// The identifier is essential to distinguish between different actor types during remote communication.
///
/// ## Example with Derive
///
/// ```
/// use kameo::{Actor, RemoteActor};
///
/// #[derive(Actor, RemoteActor)]
/// pub struct MyActor;
/// ```
///
/// ## Example Manual Implementation
///
/// ```
/// use kameo::remote::RemoteActor;
///
/// pub struct MyActor;
///
/// impl RemoteActor for MyActor {
///     const REMOTE_ID: &'static str = "my_actor_id";
/// }
/// ```
pub trait RemoteActor {
    /// The remote identifier string.
    const REMOTE_ID: &'static str;
}

/// `RemoteMessage` is a trait for identifying messages that are sent between remote actors.
///
/// Each remote message type must implement this trait and provide a unique identifier string (`REMOTE_ID`).
/// The unique ID ensures that each message type is recognized correctly during message passing between nodes.
///
/// This trait is typically implemented automatically with the [`#[remote_message]`](crate::remote_message) macro.
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
    let Some(fns) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    if immediate {
        (fns.try_ask)(actor_id, payload, reply_timeout).await
    } else {
        (fns.ask)(actor_id, payload, mailbox_timeout, reply_timeout).await
    }
}

pub(crate) async fn tell(
    actor_id: ActorID,
    actor_remote_id: Cow<'static, str>,
    message_remote_id: Cow<'static, str>,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError<Vec<u8>>> {
    let Some(fns) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    if immediate {
        (fns.try_tell)(actor_id, payload).await
    } else {
        (fns.tell)(actor_id, payload, mailbox_timeout).await
    }
}

pub(crate) async fn link(
    actor_id: ActorID,
    actor_remote_id: Cow<'static, str>,
    sibbling_id: ActorID,
    sibbling_remote_id: Cow<'static, str>,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor { actor_remote_id });
    };

    (fns.link)(actor_id, sibbling_id, sibbling_remote_id).await
}

pub(crate) async fn unlink(
    actor_id: ActorID,
    actor_remote_id: Cow<'static, str>,
    sibbling_id: ActorID,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor { actor_remote_id });
    };

    (fns.unlink)(actor_id, sibbling_id).await
}

pub(crate) async fn signal_link_died(
    dead_actor_id: ActorID,
    notified_actor_id: ActorID,
    notified_actor_remote_id: Cow<'static, str>,
    stop_reason: ActorStopReason,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*notified_actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor {
            actor_remote_id: notified_actor_remote_id,
        });
    };

    (fns.signal_link_died)(dead_actor_id, notified_actor_id, stop_reason).await
}
