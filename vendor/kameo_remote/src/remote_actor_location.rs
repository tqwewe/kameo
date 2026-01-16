use crate::{current_timestamp, NodeId, RegistrationPriority, VectorClock};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::net::SocketAddr;

/// Location of a remote actor - includes both address and hosting peer
/// For remote actors, we need to know their address and which peer is hosting them
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq)]
pub struct RemoteActorLocation {
    pub address: String,                // Use String for rkyv serialization
    pub peer_id: crate::PeerId,         // Which peer is hosting this actor
    pub node_id: NodeId,                // Node ID for vector clock operations
    pub vector_clock: VectorClock,      // Vector clock for causal ordering
    pub wall_clock_time: u64,           // Still needed for TTL calculations
    pub priority: RegistrationPriority, // Registration priority for propagation
    pub local_registration_time: u128,  // Precise registration time for timing measurements
    pub metadata: Vec<u8>,              // Optional metadata (e.g., serialized ActorId)
}

impl RemoteActorLocation {
    /// Generate a deterministic fallback NodeId from PeerId using SHA-256
    fn generate_fallback_node_id(peer_id: &crate::PeerId) -> NodeId {
        use sha2::{Digest, Sha256};

        // Create a deterministic hash from the peer_id string representation
        let mut hasher = Sha256::new();
        hasher.update(format!("fallback_node_id:{}", peer_id));
        let hash_result = hasher.finalize();

        // Convert the hash to a 32-byte array for NodeId
        let mut node_id_bytes = [0u8; 32];
        node_id_bytes.copy_from_slice(&hash_result[..32]);

        NodeId::from_bytes(&node_id_bytes)
            .expect("SHA-256 output should always be valid NodeId bytes")
    }

    /// Create a new RemoteActorLocation with peer_id
    pub fn new_with_peer(address: SocketAddr, peer_id: crate::PeerId) -> Self {
        // Convert PeerId to NodeId for vector clock operations
        let node_id = peer_id
            .to_verifying_key()
            .ok()
            .and_then(|key| NodeId::from_bytes(key.as_bytes()).ok())
            .unwrap_or_else(|| {
                tracing::warn!(
                    "Failed to convert PeerId to NodeId for {}, using deterministic fallback",
                    peer_id
                );
                Self::generate_fallback_node_id(&peer_id)
            });

        Self {
            address: address.to_string(),
            peer_id,
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: current_timestamp(),
            priority: RegistrationPriority::Normal,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            metadata: Vec::new(),
        }
    }

    /// Create a new RemoteActorLocation with peer_id and metadata
    pub fn new_with_metadata(
        address: SocketAddr,
        peer_id: crate::PeerId,
        metadata: Vec<u8>,
    ) -> Self {
        // Convert PeerId to NodeId for vector clock operations
        let node_id = peer_id
            .to_verifying_key()
            .ok()
            .and_then(|key| NodeId::from_bytes(key.as_bytes()).ok())
            .unwrap_or_else(|| {
                tracing::warn!(
                    "Failed to convert PeerId to NodeId for {}, using deterministic fallback",
                    peer_id
                );
                Self::generate_fallback_node_id(&peer_id)
            });

        Self {
            address: address.to_string(),
            peer_id,
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: current_timestamp(),
            priority: RegistrationPriority::Normal,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            metadata,
        }
    }

    /// Create with specific priority
    pub fn new_with_priority(
        address: SocketAddr,
        peer_id: crate::PeerId,
        priority: RegistrationPriority,
    ) -> Self {
        let node_id = peer_id
            .to_verifying_key()
            .ok()
            .and_then(|key| NodeId::from_bytes(key.as_bytes()).ok())
            .unwrap_or_else(|| {
                tracing::warn!(
                    "Failed to convert PeerId to NodeId for {}, using deterministic fallback",
                    peer_id
                );
                Self::generate_fallback_node_id(&peer_id)
            });

        Self {
            address: address.to_string(),
            peer_id,
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: current_timestamp(),
            priority,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            metadata: Vec::new(),
        }
    }

    /// Create with current wall clock time
    pub fn new_with_wall_time(address: SocketAddr, peer_id: crate::PeerId, wall_time: u64) -> Self {
        let node_id = peer_id
            .to_verifying_key()
            .ok()
            .and_then(|key| NodeId::from_bytes(key.as_bytes()).ok())
            .unwrap_or_else(|| {
                tracing::warn!(
                    "Failed to convert PeerId to NodeId for {}, using deterministic fallback",
                    peer_id
                );
                Self::generate_fallback_node_id(&peer_id)
            });

        Self {
            address: address.to_string(),
            peer_id,
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: wall_time,
            priority: RegistrationPriority::Normal,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            metadata: Vec::new(),
        }
    }

    /// Create with both wall time and priority
    pub fn new_with_wall_time_and_priority(
        address: SocketAddr,
        peer_id: crate::PeerId,
        wall_time: u64,
        priority: RegistrationPriority,
    ) -> Self {
        let node_id = peer_id
            .to_verifying_key()
            .ok()
            .and_then(|key| NodeId::from_bytes(key.as_bytes()).ok())
            .unwrap_or_else(|| NodeId::from_bytes(&[0u8; 32]).unwrap());

        Self {
            address: address.to_string(),
            peer_id,
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: wall_time,
            priority,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            metadata: Vec::new(),
        }
    }

    /// Get the socket address as a SocketAddr
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        self.address.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_location_new() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location = RemoteActorLocation::new_with_peer(addr, peer_id);

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.priority, RegistrationPriority::Normal);
        assert!(location.wall_clock_time > 0);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_new_with_priority() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location =
            RemoteActorLocation::new_with_priority(addr, peer_id, RegistrationPriority::Immediate);

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.priority, RegistrationPriority::Immediate);
        assert!(location.wall_clock_time > 0);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_new_with_wall_time() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let wall_time = 12345678;
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location = RemoteActorLocation::new_with_wall_time(addr, peer_id, wall_time);

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.wall_clock_time, wall_time);
        assert_eq!(location.priority, RegistrationPriority::Normal);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_new_with_wall_time_and_priority() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let wall_time = 12345678;
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location = RemoteActorLocation::new_with_wall_time_and_priority(
            addr,
            peer_id,
            wall_time,
            RegistrationPriority::Immediate,
        );

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.wall_clock_time, wall_time);
        assert_eq!(location.priority, RegistrationPriority::Immediate);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_clone() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location =
            RemoteActorLocation::new_with_priority(addr, peer_id, RegistrationPriority::Immediate);
        let cloned = location.clone();

        assert_eq!(location.address, cloned.address);
        assert_eq!(location.wall_clock_time, cloned.wall_clock_time);
        assert_eq!(location.priority, cloned.priority);
        assert_eq!(
            location.local_registration_time,
            cloned.local_registration_time
        );
    }

    #[test]
    fn test_actor_location_equality() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Create with specific values to test equality
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let node_id = peer_id
            .to_verifying_key()
            .ok()
            .and_then(|key| NodeId::from_bytes(key.as_bytes()).ok())
            .unwrap_or_else(|| NodeId::from_bytes(&[0u8; 32]).unwrap());
        let location1 = RemoteActorLocation {
            address: addr.to_string(),
            peer_id: peer_id.clone(),
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: 1000,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
            metadata: vec![1, 2, 3],
        };
        let location2 = RemoteActorLocation {
            address: addr.to_string(),
            peer_id: peer_id.clone(),
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: 1000,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
            metadata: vec![1, 2, 3],
        };
        assert_eq!(location1, location2);

        // Different timestamps should make them unequal
        let location3 = RemoteActorLocation {
            address: addr.to_string(),
            peer_id: peer_id.clone(),
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: 1001,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
            metadata: vec![1, 2, 3],
        };
        assert_ne!(location1, location3);

        // Different address should make them unequal
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let location4 = RemoteActorLocation {
            address: addr2.to_string(),
            peer_id,
            node_id,
            vector_clock: VectorClock::with_node(node_id),
            wall_clock_time: 1000,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
            metadata: vec![1, 2, 3],
        };
        assert_ne!(location1, location4);
    }

    #[test]
    fn test_actor_location_debug() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location = RemoteActorLocation::new_with_peer(addr, peer_id);
        let debug_str = format!("{:?}", location);

        assert!(debug_str.contains("RemoteActorLocation"));
        assert!(debug_str.contains("127.0.0.1:8080"));
        assert!(debug_str.contains("priority"));
    }

    #[test]
    fn test_actor_location_serialization() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location =
            RemoteActorLocation::new_with_priority(addr, peer_id, RegistrationPriority::Immediate);

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&location).unwrap();
        let deserialized: RemoteActorLocation =
            rkyv::from_bytes::<RemoteActorLocation, rkyv::rancor::Error>(&serialized).unwrap();

        assert_eq!(location.address, deserialized.address);
        assert_eq!(location.wall_clock_time, deserialized.wall_clock_time);
        assert_eq!(location.priority, deserialized.priority);
        assert_eq!(
            location.local_registration_time,
            deserialized.local_registration_time
        );
        assert_eq!(location.metadata, deserialized.metadata);
    }

    #[test]
    fn test_actor_location_with_metadata() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let metadata = vec![0x12, 0x34, 0x56, 0x78];
        let peer_id = crate::KeyPair::new_for_testing("test_peer").peer_id();
        let location = RemoteActorLocation::new_with_metadata(addr, peer_id, metadata.clone());

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.metadata, metadata);
        assert_eq!(location.priority, RegistrationPriority::Normal);
        assert!(location.wall_clock_time > 0);

        // Test serialization with metadata
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&location).unwrap();
        let deserialized: RemoteActorLocation =
            rkyv::from_bytes::<RemoteActorLocation, rkyv::rancor::Error>(&serialized).unwrap();

        assert_eq!(location.metadata, deserialized.metadata);
    }
}
