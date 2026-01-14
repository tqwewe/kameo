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
//! ```ignore
//! use kameo::remote;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Bootstrap on a specific address
//!     let transport = remote::v2_bootstrap::bootstrap_on("127.0.0.1:9330".parse()?).await?;
//!
//!     // Now use actors normally - register distributed actors with the transport
//!     // transport.register_distributed_actor("my_actor", &actor_ref).await?;
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

/// Bootstrap a distributed actor system using kameo_remote
///
/// This is a convenience function that delegates to v2_bootstrap::bootstrap()
/// for backward compatibility.
///
/// # Example
/// ```ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     remote::bootstrap().await?;
///     
///     // Now use remote actors normally
///     let actor_ref = MyActor::spawn_default();
///     actor_ref.register("my_actor").await?;
///     Ok(())
/// }
/// ```
pub async fn bootstrap() -> Result<(), Box<dyn error::Error>> {
    // Use TLS-enabled bootstrap with generated keypair and default address
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse()?;
    v2_bootstrap::bootstrap_with_keypair(addr, kameo_remote::KeyPair::generate())
        .await
        .map_err(|e| e as Box<dyn error::Error>)?;
    Ok(())
}

/// Bootstrap with a specific listen address
pub async fn bootstrap_on(addr: &str) -> Result<(), Box<dyn error::Error>> {
    let addr: std::net::SocketAddr = addr.parse()?;
    // Use TLS-enabled bootstrap with generated keypair
    v2_bootstrap::bootstrap_with_keypair(addr, kameo_remote::KeyPair::generate())
        .await
        .map_err(|e| e as Box<dyn error::Error>)?;
    Ok(())
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
#[derive(Debug)]
pub struct LookupStream<A> {
    _phantom: std::marker::PhantomData<A>,
}

impl<A> LookupStream<A> {
    /// Create a stream that immediately returns an error
    pub fn new_err() -> Self {
        LookupStream {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<A> futures::Stream for LookupStream<A>
where
    A: crate::Actor,
{
    type Item = Result<distributed_actor_ref::DistributedActorRef, RegistryError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Always return an error indicating the transport is not available
        std::task::Poll::Ready(Some(Err(RegistryError::SwarmNotBootstrapped)))
    }
}
