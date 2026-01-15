//! Simplified remote actor system
//!
//! This module re-exports the remote functionality using the modern
//! kameo_remote transport system.

// Re-export modules from remote
pub use crate::remote::{
    distributed_actor_ref, distributed_message_handler, dynamic_distributed_actor_ref,
    message_protocol, remote_message_trait, transport, type_hash,
};

// Re-export the main types
pub use distributed_actor_ref::DistributedActorRef;
pub use dynamic_distributed_actor_ref::DynamicDistributedActorRef;
pub use remote_message_trait::RemoteMessage;
pub use type_hash::{HasTypeHash, TypeHash};

/// Bootstrap a distributed actor system with an explicit keypair.
pub async fn bootstrap_with_keypair(
    addr: std::net::SocketAddr,
    keypair: kameo_remote::KeyPair,
) -> Result<(), Box<dyn std::error::Error>> {
    crate::remote::v2_bootstrap::bootstrap_with_keypair(addr, keypair)
        .await
        .map_err(|e| e as Box<dyn std::error::Error>)?;
    Ok(())
}
