//! Simplified remote actor system
//! 
//! This module re-exports the remote functionality using the modern
//! kameo_remote transport system.

use crate::error::RegistryError;

// Re-export modules from remote
pub use crate::remote::{
    distributed_message_handler,
    message_protocol,
    transport,
    type_hash,
    distributed_actor_ref,
    dynamic_distributed_actor_ref,
    remote_message_trait,
};

// Re-export the main types
pub use distributed_actor_ref::DistributedActorRef;
pub use dynamic_distributed_actor_ref::DynamicDistributedActorRef;
pub use remote_message_trait::RemoteMessage;
pub use type_hash::{HasTypeHash, TypeHash};

/// Bootstrap a distributed actor system
/// 
/// This creates a kameo_remote based distributed actor system with TLS enabled.
pub async fn bootstrap() -> Result<(), Box<dyn std::error::Error>> {
    // Use TLS-enabled bootstrap with generated keypair and default address
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse()?;
    crate::remote::v2_bootstrap::bootstrap_with_keypair(
        addr,
        kameo_remote::KeyPair::generate(),
    ).await.map_err(|e| e as Box<dyn std::error::Error>)?;
    Ok(())
}

/// Bootstrap with a specific listen address
pub async fn bootstrap_on(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = addr.parse()?;
    // Use TLS-enabled bootstrap with generated keypair
    crate::remote::v2_bootstrap::bootstrap_with_keypair(
        addr,
        kameo_remote::KeyPair::generate(),
    ).await.map_err(|e| e as Box<dyn std::error::Error>)?;
    Ok(())
}