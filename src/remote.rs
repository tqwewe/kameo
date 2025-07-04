//! # Remote Actors in Kameo
//!
//! The `remote` module in Kameo provides tools for managing distributed actors across nodes,
//! enabling actors to communicate seamlessly in a peer-to-peer (P2P) network. By leveraging
//! the [libp2p](https://libp2p.io) library, Kameo allows you to register actors under unique
//! names and send messages between actors on different nodes as though they were local.
//!
//! ## Key Features
//!
//! - **Swarm Management**: The [`ActorSwarm`] struct handles a distributed swarm of nodes,
//!   managing peer discovery and communication.
//! - **Actor Registration**: Actors can be registered under a unique name and looked up across
//!   the network using the [`RemoteActorRef`](crate::actor::RemoteActorRef).
//! - **Message Routing**: Ensures reliable message delivery between nodes using a combination
//!   of Kademlia DHT and libp2p's networking capabilities.

use std::{any, collections::HashMap, str};

use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::{
    actor::{ActorId, Links},
    mailbox::SignalMailbox,
};

#[doc(hidden)]
pub mod _internal;
mod behaviour;
pub mod messaging;
pub mod registry;
mod swarm;

pub use behaviour::*;
pub use swarm::*;

pub(crate) static REMOTE_REGISTRY: Lazy<Mutex<HashMap<ActorId, RemoteRegistryActorRef>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(crate) struct RemoteRegistryActorRef {
    pub(crate) actor_ref: Box<dyn any::Any + Send + Sync>,
    pub(crate) signal_mailbox: Box<dyn SignalMailbox>,
    pub(crate) links: Links,
}

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
