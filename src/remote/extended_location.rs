//! Extended location information that includes ActorId
//! 
//! This module provides an extended version of RemoteActorLocation that includes
//! the ActorId, which is necessary for proper v2 remote actor lookups.

use crate::actor::ActorId;
use serde::{Deserialize, Serialize};

/// Extended remote actor location that includes ActorId
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedRemoteActorLocation {
    /// The socket address of the peer hosting the actor
    pub peer_addr: std::net::SocketAddr,
    /// The unique ID of the actor
    pub actor_id: ActorId,
    /// Additional metadata (e.g., serialized ActorId for gossip)
    pub metadata: Vec<u8>,
}

impl ExtendedRemoteActorLocation {
    /// Create a new extended location
    pub fn new(peer_addr: std::net::SocketAddr, actor_id: ActorId) -> Self {
        // Serialize the ActorId into metadata for gossip propagation
        let metadata = bincode::serialize(&actor_id).unwrap_or_default();
        
        Self {
            peer_addr,
            actor_id,
            metadata,
        }
    }
    
    /// Extract ActorId from metadata
    pub fn extract_actor_id(metadata: &[u8]) -> Option<ActorId> {
        bincode::deserialize(metadata).ok()
    }
}

/// Convert from transport RemoteActorLocation to our extended version
impl From<super::transport::RemoteActorLocation> for ExtendedRemoteActorLocation {
    fn from(loc: super::transport::RemoteActorLocation) -> Self {
        // Try to extract ActorId from metadata, or use the one provided
        let actor_id = if !loc.metadata.is_empty() {
            Self::extract_actor_id(&loc.metadata).unwrap_or(loc.actor_id)
        } else {
            loc.actor_id
        };
        
        Self {
            peer_addr: loc.peer_addr,
            actor_id,
            metadata: loc.metadata,
        }
    }
}

/// Convert to transport RemoteActorLocation
impl From<ExtendedRemoteActorLocation> for super::transport::RemoteActorLocation {
    fn from(loc: ExtendedRemoteActorLocation) -> Self {
        Self {
            peer_addr: loc.peer_addr,
            actor_id: loc.actor_id,
            metadata: loc.metadata,
        }
    }
}