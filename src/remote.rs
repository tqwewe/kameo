//! # Remote Actors in Kameo
//!
//! The `remote` module in Kameo provides tools for managing distributed actors across nodes,
//! enabling actors to communicate seamlessly in a peer-to-peer network using the kameo_remote
//! transport layer with gossip-based discovery and type-erased generic actor support.
//!
//! ## Key Features
//!
//! - **Type-Erased Generic Actors**: Support for generic actors using compile-time type hashing
//! - **Gossip-Based Discovery**: Efficient actor discovery using gossip protocol
//! - **Direct TCP Connections**: Lock-free, high-performance messaging
//! - **Zero Dynamic Dispatch**: Monomorphized message handlers for optimal performance
//!
//! ## Getting Started
//!
//! ```
//! use kameo::remote;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Bootstrap with an explicit keypair (TLS required)
//!     let keypair = kameo::remote::v2_bootstrap::test_keypair(1);
//!     remote::v2_bootstrap::bootstrap_on("127.0.0.1:0".parse()?, keypair).await?;
//!     
//!     // Now use actors normally
//!     // actor_ref.register("my_actor").await?;
//!     
//!     Ok(())
//! }
//! ```

use std::error;

use crate::{actor::ActorRef, error::RegistryError, Actor};

#[doc(hidden)]
pub mod _internal;
pub mod type_registry;

// New transport system modules
pub mod generic_type_hash;
mod kameo_transport;
mod message_handler;
pub mod message_protocol;
pub mod transport;
mod transport_factory;
pub mod type_hash;
// pub mod v2_actor_ref; // Commented out - depends on removed RemoteActor trait
pub mod distributed_actor;
pub mod distributed_actor_messages;
pub mod distributed_actor_ref;
pub mod distributed_message_handler;
pub mod dynamic_distributed_actor_ref;
pub mod remote_message_trait;
pub mod simple_distributed_actor;
pub mod streaming;
pub mod v2_bootstrap;

// Re-export main types
pub use distributed_actor_ref::DistributedActorRef;
pub use dynamic_distributed_actor_ref::DynamicDistributedActorRef;
pub use remote_message_trait::RemoteMessage;
pub use type_hash::{HasTypeHash, TypeHash};

/// Trait for actors that support distributed messaging
/// This is automatically implemented by the `distributed_actor!` macro
pub trait DistributedActor: Actor {
    /// Internal method to register type handlers
    /// This is called automatically by transport.register_actor()
    #[doc(hidden)]
    fn __register_distributed_handlers(actor_ref: &ActorRef<Self>);
}

/// Bootstrap a distributed actor system using kameo_remote with an explicit keypair.
pub async fn bootstrap_with_keypair(
    addr: std::net::SocketAddr,
    keypair: kameo_remote::KeyPair,
) -> Result<(), Box<dyn error::Error>> {
    v2_bootstrap::bootstrap_with_keypair(addr, keypair)
        .await
        .map_err(|e| e as Box<dyn error::Error>)?;
    Ok(())
}

/// Bootstrap with a specific listen address and explicit keypair.
pub async fn bootstrap_on(
    addr: &str,
    keypair: kameo_remote::KeyPair,
) -> Result<(), Box<dyn error::Error>> {
    let addr: std::net::SocketAddr = addr.parse()?;
    bootstrap_with_keypair(addr, keypair).await
}

/// Unregisters an actor within the distributed system.
///
/// This will only unregister an actor previously registered by the current node.
pub async fn unregister(_name: impl Into<String>) -> Result<(), RegistryError> {
    // TODO: Implement unregister in kameo_remote
    Ok(())
}

/// Stream of actor lookups
///
/// This would be returned by RemoteActorRef::lookup_all() but requires
/// access to the transport/registry which should be managed by the application.
use std::marker::PhantomData;

#[derive(Debug)]
pub struct LookupStream {
    _phantom: PhantomData<()>,
}

impl LookupStream {
    /// Create a stream that immediately returns an error
    pub fn new_err() -> Self {
        LookupStream {
            _phantom: PhantomData,
        }
    }
}

impl futures::Stream for LookupStream
{
    type Item = Result<dynamic_distributed_actor_ref::DynamicDistributedActorRef, RegistryError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Always return an error indicating the transport is not available
        std::task::Poll::Ready(Some(Err(RegistryError::SwarmNotBootstrapped)))
    }
}
