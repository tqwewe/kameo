//! # Remote Actors in Kameo
//!
//! The `remote` module in Kameo provides tools for managing distributed actors across nodes,
//! enabling actors to communicate seamlessly in a peer-to-peer (P2P) network. By leveraging
//! the [libp2p](https://libp2p.io) library, Kameo allows you to register actors under unique
//! names and send messages between actors on different nodes as though they were local.
//!
//! ## Key Features
//!
//! - **Composable Architecture**: The [`Behaviour`] struct implements libp2p's `NetworkBehaviour`,
//!   allowing seamless integration with existing libp2p applications and other protocols.
//! - **Quick Bootstrap**: The [`bootstrap()`] and [`bootstrap_on()`] functions provide one-line
//!   setup for development and simple deployments.
//! - **Custom Transport**: The [`run_swarm()`] function accepts a pre-built swarm with any
//!   transport while handling the event loop for you.
//! - **Actor Registration & Discovery**: Actors can be registered under unique names and looked up
//!   across the network using [`RemoteActorRef`](crate::actor::RemoteActorRef).
//! - **Reliable Messaging**: Ensures reliable message delivery between nodes using a combination
//!   of Kademlia DHT for discovery and request-response protocols for communication.
//! - **Modular Design**: Separate [`messaging`] and [`registry`] modules handle different aspects
//!   of distributed actor communication.
//!
//! ## Getting Started
//!
//! For quick prototyping and development:
//!
//! ```ignore
//! use kameo::remote;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // One line to bootstrap a distributed actor system
//!     let peer_id = remote::bootstrap()?;
//!
//!     // Now use actors normally
//!     // actor_ref.register("my_actor").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! For production deployments with custom configuration:
//!
//! ```ignore
//! use kameo::remote;
//! use libp2p::swarm::NetworkBehaviour;
//!
//! #[derive(NetworkBehaviour)]
//! struct MyBehaviour {
//!     kameo: remote::Behaviour,
//!     // Add other libp2p behaviors as needed
//! }
//!
//! // Create custom libp2p swarm with full control over
//! // transports, discovery, and protocol composition
//! ```

use std::{
    any,
    collections::HashMap,
    str,
    sync::{Arc, LazyLock},
};

#[cfg(feature = "serde-codec")]
use std::error;

use futures::StreamExt;
use libp2p::{PeerId, Swarm, swarm::NetworkBehaviour};
use tokio::sync::Mutex;

use crate::{
    Actor,
    actor::{ActorId, ActorRef, Links, WeakActorRef},
    error::{RegistryError, RemoteSendError},
    mailbox::SignalMailbox,
};

#[cfg(all(feature = "serde-codec", feature = "rkyv-codec"))]
compile_error!("Features `serde-codec` and `rkyv-codec` are mutually exclusive");

#[cfg(not(any(feature = "serde-codec", feature = "rkyv-codec")))]
compile_error!("The `remote` feature requires either `serde-codec` or `rkyv-codec`");

#[doc(hidden)]
pub mod _internal;
mod behaviour;
pub mod codec;
#[allow(missing_docs)] // rkyv::Archive derive generates undocumented archived types
pub mod messaging;
pub mod registry;
mod swarm;
pub mod wire;

pub use behaviour::*;
pub use swarm::*;

pub(crate) static REMOTE_REGISTRY: LazyLock<Mutex<HashMap<ActorId, RemoteRegistryActorRef>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register an actor in the local REMOTE_REGISTRY under a well-known ActorId.
/// This allows `RemoteActorRef::for_peer()` to find the actor without DHT lookup.
pub async fn register_actor_local<A: Actor>(actor_ref: &ActorRef<A>, id: ActorId) {
    let entry = RemoteRegistryActorRef::new(actor_ref.clone(), None);
    REMOTE_REGISTRY.lock().await.insert(id, entry);
}

pub(crate) struct RemoteRegistryActorRef {
    actor_ref: BoxRegisteredActorRef,
    pub(crate) name: Option<Arc<str>>,
    pub(crate) signal_mailbox: Box<dyn SignalMailbox>,
    pub(crate) links: Links,
}

impl RemoteRegistryActorRef {
    pub(crate) fn new<A: Actor>(actor_ref: ActorRef<A>, name: Option<Arc<str>>) -> Self {
        let signal_mailbox = actor_ref.weak_signal_mailbox();
        let links = actor_ref.links.clone();
        Self {
            actor_ref: BoxRegisteredActorRef::Strong(Box::new(actor_ref)),
            name,
            signal_mailbox,
            links,
        }
    }

    pub(crate) fn new_weak<A: Actor>(actor_ref: WeakActorRef<A>, name: Option<Arc<str>>) -> Self {
        let signal_mailbox = actor_ref.weak_signal_mailbox();
        let links = actor_ref.links.clone();
        Self {
            actor_ref: BoxRegisteredActorRef::Weak(Box::new(actor_ref)),
            name,
            signal_mailbox,
            links,
        }
    }

    pub(crate) fn downcast<A: Actor>(
        &self,
    ) -> Result<ActorRef<A>, DowncastRegsiteredActorRefError> {
        match &self.actor_ref {
            BoxRegisteredActorRef::Strong(any) => any
                .downcast_ref::<ActorRef<A>>()
                .ok_or(DowncastRegsiteredActorRefError::BadActorType)
                .cloned(),
            BoxRegisteredActorRef::Weak(any) => any
                .downcast_ref::<WeakActorRef<A>>()
                .ok_or(DowncastRegsiteredActorRefError::BadActorType)?
                .upgrade()
                .ok_or(DowncastRegsiteredActorRefError::ActorNotRunning),
        }
    }
}

pub(crate) enum DowncastRegsiteredActorRefError {
    BadActorType,
    ActorNotRunning,
}

impl<E> From<DowncastRegsiteredActorRefError> for RemoteSendError<E> {
    fn from(err: DowncastRegsiteredActorRefError) -> Self {
        match err {
            DowncastRegsiteredActorRefError::BadActorType => RemoteSendError::BadActorType,
            DowncastRegsiteredActorRefError::ActorNotRunning => RemoteSendError::ActorNotRunning,
        }
    }
}

pub(crate) enum BoxRegisteredActorRef {
    Strong(Box<dyn any::Any + Send + Sync>),
    Weak(Box<dyn any::Any + Send + Sync>),
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

/// Bootstrap a simple actor swarm with mDNS discovery for local development.
///
/// This convenience function creates and runs a libp2p swarm with:
/// - TCP and QUIC transports
/// - mDNS peer discovery (local network only)
/// - Automatic listening on an OS-assigned port
///
/// Requires the `serde-codec` feature (uses CBOR for transport encoding).
///
/// For production use or custom configuration, use `kameo::remote::Behaviour`
/// with your own libp2p swarm setup.
///
/// # Example
/// ```ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // One line to get started!
///     remote::bootstrap()?;
///
///     // Now use remote actors normally
///     let actor_ref = MyActor::spawn_default();
///     actor_ref.register("my_actor").await?;
///     Ok(())
/// }
/// ```
#[cfg(feature = "serde-codec")]
pub fn bootstrap() -> Result<PeerId, Box<dyn error::Error>> {
    bootstrap_on("/ip4/0.0.0.0/tcp/0")
}

/// Bootstrap with a specific listen address.
///
/// Requires the `serde-codec` feature.
#[cfg(feature = "serde-codec")]
pub fn bootstrap_on(addr: &str) -> Result<PeerId, Box<dyn error::Error>> {
    use libp2p::{SwarmBuilder, mdns, noise, swarm::SwarmEvent, tcp, yamux};

    #[derive(NetworkBehaviour)]
    struct BootstrapBehaviour {
        kameo: Behaviour,
        mdns: mdns::tokio::Behaviour,
    }

    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();
            let kameo = Behaviour::new(local_peer_id, messaging::Config::default());
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

            Ok(BootstrapBehaviour { kameo, mdns })
        })?
        .build();

    swarm.behaviour().kameo.try_init_global()?;

    swarm.listen_on(addr.parse()?)?;

    let local_peer_id = *swarm.local_peer_id();

    tokio::spawn(async move {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::Behaviour(BootstrapBehaviourEvent::Mdns(mdns::Event::Discovered(
                    list,
                ))) => {
                    for (peer_id, multiaddr) in list {
                        #[cfg(feature = "tracing")]
                        tracing::info!("mDNS discovered a new peer: {peer_id}");
                        swarm.add_peer_address(peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(BootstrapBehaviourEvent::Mdns(mdns::Event::Expired(
                    list,
                ))) => {
                    for (peer_id, _multiaddr) in list {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("mDNS discover peer has expired: {peer_id}");
                        let _ = swarm.disconnect_peer_id(peer_id);
                    }
                }
                #[cfg(feature = "tracing")]
                SwarmEvent::NewListenAddr { address, .. } => {
                    tracing::info!("ActorSwarm listening on {address}");
                }
                _ => {}
            }
        }
    });

    Ok(local_peer_id)
}

/// Run a pre-built libp2p swarm as the actor swarm event loop.
///
/// This is the most flexible way to use kameo's remote actors with custom
/// transports. You build the `Swarm` yourself (with any transport, encryption,
/// and multiplexing) and include [`Behaviour`] in your composed `NetworkBehaviour`.
///
/// # Prerequisites
///
/// Before calling this function, you must:
/// 1. Build a `Swarm` containing [`Behaviour`] in its `NetworkBehaviour`
/// 2. Call [`Behaviour::try_init_global()`] on the kameo behaviour
/// 3. Call `swarm.listen_on(addr)` if you want the swarm to accept connections
///
/// # Example
///
/// ```no_run
/// use kameo::remote::{self, codec::KameoRkyvCodec};
/// use libp2p::{swarm::NetworkBehaviour, noise, tcp, yamux};
///
/// #[derive(NetworkBehaviour)]
/// struct MyBehaviour {
///     kameo: remote::Behaviour<KameoRkyvCodec>,
/// }
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut swarm = libp2p::SwarmBuilder::with_new_identity()
///     .with_tokio()
///     .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
///     .with_behaviour(|key| {
///         let peer_id = key.public().to_peer_id();
///         let config = remote::messaging::Config::default();
///         let codec = KameoRkyvCodec::new(&config);
///         Ok(MyBehaviour {
///             kameo: remote::Behaviour::with_codec(peer_id, config, codec),
///         })
///     })?
///     .build();
///
/// swarm.behaviour().kameo.try_init_global()?;
/// swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
///
/// let peer_id = remote::run_swarm(swarm);
/// # Ok(())
/// # }
/// ```
pub fn run_swarm<B>(mut swarm: Swarm<B>) -> PeerId
where
    B: NetworkBehaviour + Send + 'static,
    <B as NetworkBehaviour>::ToSwarm: Send,
{
    let local_peer_id = *swarm.local_peer_id();

    tokio::spawn(async move {
        loop {
            let _event = swarm.select_next_some().await;
        }
    });

    local_peer_id
}

/// Unregisters an actor within the swarm.
///
/// This will only unregister an actor previously registered by the current node.
pub async fn unregister(name: impl Into<Arc<str>>) -> Result<(), RegistryError> {
    ActorSwarm::get()
        .ok_or(RegistryError::SwarmNotBootstrapped)?
        .unregister(name.into())
        .await;
    Ok(())
}
