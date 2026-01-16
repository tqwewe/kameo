pub mod config;
pub mod connection_pool;
pub mod framing;
mod handle;
mod handle_builder;
pub mod handshake;
pub mod peer_discovery;
pub mod priority;
pub mod registry;
pub mod remote_actor_location;
pub mod reply_to;
pub mod stream_writer;
pub mod tls;
pub mod typed;
#[cfg(any(test, feature = "test-helpers", debug_assertions))]
pub mod test_helpers;

use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::error;
use zeroize::{Zeroize, ZeroizeOnDrop};

pub use config::GossipConfig;
pub use connection_pool::{ChannelId, DelegatedReplySender, LockFreeStreamHandle, StreamFrameType};
pub use handle::GossipRegistryHandle;
pub use handle_builder::GossipRegistryBuilder;
pub use priority::{ConsistencyLevel, RegistrationPriority};
pub use remote_actor_location::RemoteActorLocation;
pub use reply_to::{ReplyTo, TimeoutReplyTo};
pub use typed::{decode_typed, encode_typed, WireEncode, WireType};

// =================== New Iroh-style types ===================

/// Public key for node identity - Ed25519 public key
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct PublicKey {
    inner: [u8; 32],
}

impl PublicKey {
    /// Create from raw bytes, validating they form a valid Ed25519 public key
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid public key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);

        // Validate it's a valid Ed25519 public key
        let _ = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))?;

        Ok(Self { inner: key_bytes })
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.inner
    }

    /// Convert to ed25519_dalek::VerifyingKey for crypto operations
    pub fn to_verifying_key(&self) -> Result<VerifyingKey> {
        VerifyingKey::from_bytes(&self.inner)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))
    }

    /// Verify a signature
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        let verifying_key = self.to_verifying_key()?;
        verifying_key.verify(message, signature).map_err(|e| {
            GossipError::InvalidSignature(format!("Signature verification failed: {}", e))
        })
    }

    /// Format first 5 bytes as hex for logging (like Iroh)
    pub fn fmt_short(&self) -> String {
        hex::encode(&self.inner[..5])
    }

    /// Convert to PeerId
    pub fn to_peer_id(&self) -> PeerId {
        PeerId::from_bytes(self.as_bytes()).expect("NodeId should always convert to valid PeerId")
    }
}

impl Hash for PublicKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PublicKey({}…)", self.fmt_short())
    }
}

impl std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fmt_short())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            // Use base32 for human-readable formats
            let encoded = data_encoding::BASE32_NOPAD.encode(&self.inner);
            serializer.serialize_str(&encoded)
        } else {
            // Use raw bytes for binary formats
            serializer.serialize_bytes(&self.inner)
        }
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let bytes = data_encoding::BASE32_NOPAD
                .decode(s.as_bytes())
                .map_err(serde::de::Error::custom)?;
            Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; 32]>::deserialize(deserializer)?;
            Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
        }
    }
}

/// Node identifier - alias for PublicKey (like Iroh)
pub type NodeId = PublicKey;

/// Secret key for node identity - Ed25519 signing key with secure cleanup
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SecretKey {
    #[zeroize(skip)]
    secret: SigningKey,
}

impl SecretKey {
    /// Generate a new random secret key
    pub fn generate() -> Self {
        use rand::Rng;
        let mut rng = rand::rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        let secret = SigningKey::from_bytes(&bytes);
        bytes.zeroize(); // Clear the temporary bytes
        Self { secret }
    }

    /// Create from raw bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid secret key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);
        let secret = SigningKey::from_bytes(&key_bytes);
        key_bytes.zeroize(); // Clear the temporary bytes
        Ok(Self { secret })
    }

    /// Get the corresponding public key
    pub fn public(&self) -> PublicKey {
        let verifying_key = self.secret.verifying_key();
        PublicKey {
            inner: verifying_key.to_bytes(),
        }
    }

    /// Sign a message
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.secret.sign(message)
    }

    /// Get raw bytes (use with caution - these should be zeroized after use)
    pub fn to_bytes(&self) -> [u8; 32] {
        self.secret.to_bytes()
    }

    /// Convert to a KeyPair for existing APIs
    pub fn to_keypair(&self) -> KeyPair {
        let mut key_bytes = self.to_bytes();
        let keypair = KeyPair::from_private_key_bytes(&key_bytes)
            .expect("SecretKey should always convert to valid KeyPair");
        key_bytes.zeroize();
        keypair
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretKey(***)")
    }
}

// =================== End new types ===================

/// Vector clock for tracking causal relationships between events
/// Thread-safe implementation using DashMap internally
pub struct VectorClock {
    // Internal thread-safe storage using DashMap for O(1) operations
    clocks: Arc<DashMap<NodeId, u64>>,
}

impl Clone for VectorClock {
    fn clone(&self) -> Self {
        // Deep clone - create a new DashMap with the same entries
        let new_clocks = Arc::new(DashMap::new());
        for entry in self.clocks.iter() {
            new_clocks.insert(*entry.key(), *entry.value());
        }
        Self { clocks: new_clocks }
    }
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: Arc::new(DashMap::new()),
        }
    }

    pub fn with_node(node_id: NodeId) -> Self {
        let clocks = Arc::new(DashMap::new());
        clocks.insert(node_id, 0);
        Self { clocks }
    }

    /// Increment the clock for a specific node (thread-safe)
    pub fn increment(&self, node_id: NodeId) {
        self.clocks
            .entry(node_id)
            .and_modify(|v| *v = v.saturating_add(1))
            .or_insert(1);
    }

    /// Merge with another vector clock (thread-safe)
    pub fn merge(&self, other: &VectorClock) {
        for entry in other.clocks.iter() {
            let (other_node, other_clock) = entry.pair();
            self.clocks
                .entry(*other_node)
                .and_modify(|v| *v = (*v).max(*other_clock))
                .or_insert(*other_clock);
        }
    }

    /// Compare vector clocks to determine causal relationship
    pub fn compare(&self, other: &VectorClock) -> ClockOrdering {
        let mut self_greater = false;
        let mut other_greater = false;

        // Collect all node IDs from both clocks
        let mut all_nodes = std::collections::HashSet::new();
        for entry in self.clocks.iter() {
            all_nodes.insert(*entry.key());
        }
        for entry in other.clocks.iter() {
            all_nodes.insert(*entry.key());
        }

        // Compare clock values for each node
        for node_id in all_nodes {
            let self_clock = self.clocks.get(&node_id).map(|v| *v).unwrap_or(0);
            let other_clock = other.clocks.get(&node_id).map(|v| *v).unwrap_or(0);

            match self_clock.cmp(&other_clock) {
                std::cmp::Ordering::Greater => self_greater = true,
                std::cmp::Ordering::Less => other_greater = true,
                std::cmp::Ordering::Equal => {}
            }
        }

        match (self_greater, other_greater) {
            (true, false) => ClockOrdering::After,
            (false, true) => ClockOrdering::Before,
            (false, false) => ClockOrdering::Equal,
            (true, true) => ClockOrdering::Concurrent,
        }
    }

    /// Garbage collect entries for nodes not seen recently (thread-safe)
    pub fn gc_old_nodes(&self, active_nodes: &std::collections::HashSet<NodeId>) {
        self.clocks
            .retain(|node_id, _| active_nodes.contains(node_id));
    }

    /// Get the number of entries in the vector clock
    pub fn len(&self) -> usize {
        self.clocks.len()
    }

    /// Check if the vector clock is empty
    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }

    /// Compact the vector clock if it exceeds the maximum size (thread-safe)
    pub fn compact(&self, max_size: usize) {
        if self.clocks.len() <= max_size {
            return;
        }

        // Collect all entries and sort by clock value
        let mut entries: Vec<(NodeId, u64)> = self
            .clocks
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));

        // Keep only the top max_size entries
        entries.truncate(max_size);

        // Clear and rebuild the map
        self.clocks.clear();
        for (node_id, clock) in entries {
            self.clocks.insert(node_id, clock);
        }
    }

    /// Convert to a sorted Vec for serialization
    fn to_vec(&self) -> Vec<(NodeId, u64)> {
        let mut vec: Vec<(NodeId, u64)> = self
            .clocks
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        vec.sort_by_key(|(node_id, _)| *node_id);
        vec
    }

    /// Create from a Vec (used in deserialization)
    fn from_vec(vec: Vec<(NodeId, u64)>) -> Self {
        let clocks = Arc::new(DashMap::new());
        for (node_id, clock) in vec {
            clocks.insert(node_id, clock);
        }
        Self { clocks }
    }

    /// Get the clock value for a specific node
    pub fn get(&self, node_id: &NodeId) -> u64 {
        self.clocks.get(node_id).map(|v| *v).unwrap_or(0)
    }

    /// Check if this vector clock happened before another
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::Before)
    }

    /// Check if this vector clock happened after another
    pub fn happens_after(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::After)
    }

    /// Check if this vector clock is concurrent with another
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::Concurrent)
    }

    /// Get all nodes referenced in this vector clock
    pub fn get_nodes(&self) -> std::collections::HashSet<NodeId> {
        self.clocks.iter().map(|entry| *entry.key()).collect()
    }

    /// Check if this vector clock is "empty" (only has zero entries)
    pub fn is_effectively_empty(&self) -> bool {
        self.clocks.iter().all(|entry| *entry.value() == 0)
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for VectorClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vec = self.to_vec();
        f.debug_struct("VectorClock").field("clocks", &vec).finish()
    }
}

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.compare(other), ClockOrdering::Equal)
    }
}

impl Eq for VectorClock {}

impl std::hash::Hash for VectorClock {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let sorted = self.to_vec();
        for (node_id, clock) in sorted {
            node_id.hash(state);
            clock.hash(state);
        }
    }
}

// For rkyv serialization, we need a simple wrapper type
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone)]
#[rkyv(derive(Debug))]
pub struct VectorClockData {
    pub clocks: Vec<(NodeId, u64)>,
}

impl From<&VectorClock> for VectorClockData {
    fn from(vc: &VectorClock) -> Self {
        VectorClockData {
            clocks: vc.to_vec(),
        }
    }
}

impl From<VectorClockData> for VectorClock {
    fn from(data: VectorClockData) -> Self {
        VectorClock::from_vec(data.clocks)
    }
}

// Custom rkyv implementation that uses VectorClockData
impl rkyv::Archive for VectorClock {
    type Archived = <VectorClockData as rkyv::Archive>::Archived;
    type Resolver = <VectorClockData as rkyv::Archive>::Resolver;

    fn resolve(&self, resolver: Self::Resolver, out: rkyv::Place<Self::Archived>) {
        let data = VectorClockData::from(self);
        data.resolve(resolver, out);
    }
}

impl<S> rkyv::Serialize<S> for VectorClock
where
    S: rkyv::rancor::Fallible + rkyv::ser::Writer + rkyv::ser::Allocator + ?Sized,
    S::Error: rkyv::rancor::Source,
{
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> std::result::Result<Self::Resolver, <S as rkyv::rancor::Fallible>::Error> {
        let data = VectorClockData::from(self);
        data.serialize(serializer)
    }
}

impl<D> rkyv::Deserialize<VectorClock, D> for <VectorClockData as rkyv::Archive>::Archived
where
    D: rkyv::rancor::Fallible + rkyv::de::Pooling + ?Sized,
    D::Error: rkyv::rancor::Source,
{
    fn deserialize(
        &self,
        deserializer: &mut D,
    ) -> std::result::Result<VectorClock, <D as rkyv::rancor::Fallible>::Error> {
        let data: VectorClockData = self.deserialize(deserializer)?;
        Ok(VectorClock::from(data))
    }
}

/// Ordering relationship between vector clocks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockOrdering {
    Before,
    After,
    Equal,
    Concurrent,
}

/// Key pair for node identity using Ed25519 cryptography
#[derive(Clone)]
pub struct KeyPair {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl KeyPair {
    /// Generate a new random keypair
    pub fn generate() -> Self {
        use rand::Rng;
        let mut rng = rand::rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        let signing_key = SigningKey::from_bytes(&bytes);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Create a keypair from private key bytes
    pub fn from_private_key_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid private key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Get the PeerId (public key) for this keypair
    pub fn peer_id(&self) -> PeerId {
        PeerId::from_verifying_key(self.verifying_key)
    }

    /// Get the private key bytes
    pub fn private_key_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    /// Get the public key bytes
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    /// Convert to SecretKey for TLS identity use
    pub fn to_secret_key(&self) -> SecretKey {
        let mut key_bytes = self.private_key_bytes();
        let secret =
            SecretKey::from_bytes(&key_bytes).expect("KeyPair should always convert to SecretKey");
        key_bytes.zeroize();
        secret
    }

    /// Sign a message
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }

    /// For testing - create a deterministic key pair from a seed string
    pub fn new_for_testing(id: impl Into<String>) -> Self {
        let id = id.into();
        let mut seed = [0u8; 32];
        let id_bytes = id.as_bytes();
        let len = id_bytes.len().min(32);
        seed[..len].copy_from_slice(&id_bytes[..len]);

        let signing_key = SigningKey::from_bytes(&seed);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
        }
    }
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair")
            .field("public_key", &hex::encode(self.verifying_key.as_bytes()))
            .finish()
    }
}

/// Peer identifier - contains the Ed25519 public key
#[derive(Clone, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PeerId([u8; 32]);

impl PeerId {
    /// Create a PeerId from a verifying key
    pub fn from_verifying_key(key: VerifyingKey) -> Self {
        Self(key.to_bytes())
    }

    /// Create a PeerId from a PublicKey
    pub fn from_public_key(key: &PublicKey) -> Self {
        Self(*key.as_bytes())
    }

    /// Create a PeerId from public key bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(GossipError::InvalidKeyPair(format!(
                "Invalid public key length: expected 32, got {}",
                bytes.len()
            )));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);
        // Verify it's a valid public key
        let _ = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))?;
        Ok(Self(key_bytes))
    }

    /// Create a PeerId from hex string
    pub fn from_hex(hex: &str) -> Result<Self> {
        let bytes = hex::decode(hex)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid hex: {}", e)))?;
        Self::from_bytes(&bytes)
    }

    /// Get the public key bytes
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Get the verifying key
    pub fn to_verifying_key(&self) -> Result<VerifyingKey> {
        VerifyingKey::from_bytes(&self.0)
            .map_err(|e| GossipError::InvalidKeyPair(format!("Invalid public key: {}", e)))
    }

    /// Get hex representation
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Verify a signature
    pub fn verify_signature(&self, message: &[u8], signature: &Signature) -> Result<()> {
        let verifying_key = self.to_verifying_key()?;
        verifying_key.verify(message, signature).map_err(|e| {
            GossipError::InvalidSignature(format!("Signature verification failed: {}", e))
        })
    }

    /// Convert to NodeId (which is just an alias for PublicKey)
    pub fn to_node_id(&self) -> NodeId {
        NodeId::from_bytes(&self.0).expect("PeerId should always be a valid NodeId")
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PeerId").field(&self.to_hex()).finish()
    }
}

impl From<PublicKey> for PeerId {
    fn from(key: PublicKey) -> Self {
        Self(*key.as_bytes())
    }
}

impl From<&PublicKey> for PeerId {
    fn from(key: &PublicKey) -> Self {
        Self(*key.as_bytes())
    }
}

/// Handle to a configured peer
#[derive(Clone)]
pub struct Peer {
    peer_id: PeerId,
    registry: std::sync::Arc<registry::GossipRegistry>,
}

impl Peer {
    /// Connect to this peer at the specified address
    pub async fn connect(&self, addr: &SocketAddr) -> Result<()> {
        // Validate the address
        if addr.port() == 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid port 0 for peer {}", self.peer_id),
            )));
        }

        // First configure the address for this peer
        {
            let pool = self.registry.connection_pool.lock().await;
            pool.peer_id_to_addr.insert(self.peer_id.clone(), *addr);
            pool.reindex_connection_addr(&self.peer_id, *addr);
        }

        // Add the peer to gossip state so it can be selected for gossip rounds
        {
            tracing::debug!(
                "🎯 Peer::connect - About to add peer {} to gossip state",
                self.peer_id
            );
            let mut gossip_state = self.registry.gossip_state.lock().await;
            let current_time = crate::current_timestamp();
            let peers_before = gossip_state.peers.len();

            // Convert PeerId to NodeId for TLS
            let node_id = Some(self.peer_id.to_node_id());
            if node_id.is_some() {
                tracing::debug!("🔐 Converted PeerId {} to NodeId for TLS", self.peer_id);
            }

            gossip_state.peers.insert(
                *addr,
                crate::registry::PeerInfo {
                    address: *addr,
                    peer_address: None,
                    node_id,                    // Set the NodeId for TLS verification
                    failures: 0,                // Start with 0 failures
                    last_attempt: current_time, // Set last_attempt to now
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                },
            );
            let peers_after = gossip_state.peers.len();
            tracing::debug!(
                peer_id = %self.peer_id,
                addr = %addr,
                peers_before = peers_before,
                peers_after = peers_after,
                "🎯 Added peer to gossip state for gossip rounds"
            );
        }

        // Then attempt to connect with enhanced error context
        match self.registry.connect_to_peer(&self.peer_id).await {
            Ok(()) => {
                tracing::info!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Successfully connected to peer"
                );
                // Trigger an immediate gossip round to sync
                let _ = self.registry.trigger_immediate_gossip().await;
                Ok(())
            }
            Err(GossipError::Network(io_err)) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    error = %io_err,
                    "Network error connecting to peer"
                );

                // Update peer failure state in gossip state
                {
                    let mut gossip_state = self.registry.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(addr) {
                        peer_info.failures = self.registry.config.max_peer_failures;
                        peer_info.last_failure_time = Some(crate::current_timestamp());
                        tracing::debug!(
                            peer_id = %self.peer_id,
                            addr = %addr,
                            failures = peer_info.failures,
                            "Updated peer failure state after connection error"
                        );
                    }
                }

                Err(GossipError::Network(std::io::Error::new(
                    io_err.kind(),
                    format!(
                        "Failed to connect to peer {} at {}: {}",
                        self.peer_id, addr, io_err
                    ),
                )))
            }
            Err(GossipError::Timeout) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Connection timeout when connecting to peer"
                );

                // Update peer failure state in gossip state
                {
                    let mut gossip_state = self.registry.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(addr) {
                        peer_info.failures = self.registry.config.max_peer_failures;
                        peer_info.last_failure_time = Some(crate::current_timestamp());
                    }
                }

                Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Connection timeout to peer {} at {}", self.peer_id, addr),
                )))
            }
            Err(GossipError::ConnectionExists) => {
                tracing::debug!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Connection already exists to peer"
                );
                // This is not really an error - connection already exists
                Ok(())
            }
            Err(GossipError::Shutdown) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Registry is shutting down, cannot connect to peer"
                );
                Err(GossipError::Shutdown)
            }
            Err(other_err) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    error = %other_err,
                    "Unexpected error connecting to peer"
                );
                Err(other_err)
            }
        }
    }

    /// Connect to this peer with retry attempts
    pub async fn connect_with_retry(
        &self,
        addr: &SocketAddr,
        max_retries: u32,
        retry_delay: std::time::Duration,
    ) -> Result<()> {
        let mut last_error = None;

        for attempt in 0..=max_retries {
            match self.connect(addr).await {
                Ok(()) => return Ok(()),
                Err(GossipError::Shutdown) => {
                    // Don't retry if registry is shutting down
                    return Err(GossipError::Shutdown);
                }
                Err(err) => {
                    last_error = Some(err);
                    if attempt < max_retries {
                        tracing::warn!(
                            peer_id = %self.peer_id,
                            addr = %addr,
                            attempt = attempt + 1,
                            max_retries = max_retries,
                            "Connection attempt failed, retrying in {:?}",
                            retry_delay
                        );
                        tokio::time::sleep(retry_delay).await;
                    }
                }
            }
        }

        // All retries failed
        let final_error = last_error.unwrap_or_else(|| {
            GossipError::Network(std::io::Error::other(
                "Unknown error during connection attempts",
            ))
        });

        tracing::error!(
            peer_id = %self.peer_id,
            addr = %addr,
            max_retries = max_retries,
            "All connection attempts failed"
        );

        Err(final_error)
    }

    /// Check if this peer is currently connected
    pub async fn is_connected(&self) -> bool {
        let pool = self.registry.connection_pool.lock().await;

        // Check if we have a connection by peer ID
        if let Some(conn) = pool.get_connection_by_peer_id(&self.peer_id) {
            conn.is_connected()
        } else {
            false
        }
    }

    /// Disconnect from this peer
    pub async fn disconnect(&self) -> Result<()> {
        let mut pool = self.registry.connection_pool.lock().await;

        if let Some(conn) = pool.get_connection_by_peer_id(&self.peer_id) {
            // Mark connection as disconnected
            conn.set_state(crate::connection_pool::ConnectionState::Disconnected);

            // Get the peer address for mark_disconnected
            let peer_addr = pool
                .peer_id_to_addr
                .get(&self.peer_id)
                .map(|addr| *addr.value());
            if let Some(addr) = peer_addr {
                pool.mark_disconnected(addr);
            }

            tracing::info!(
                peer_id = %self.peer_id,
                "Disconnected from peer"
            );
            Ok(())
        } else {
            tracing::debug!(
                peer_id = %self.peer_id,
                "No connection found to disconnect"
            );
            Ok(()) // Not an error if no connection exists
        }
    }

    /// Get the peer ID
    pub fn id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Wait for the initial sync with this peer to complete
    ///
    /// This waits for:
    /// 1. The connection to be established
    /// 2. The initial FullSync to be exchanged
    /// 3. The actor registry to be updated
    pub async fn wait_for_sync(&self, timeout: Duration) -> Result<()> {
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;

        // Wait for the connection to be established
        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(GossipError::Timeout);
            }

            // Check if we have a connection to this peer
            {
                let pool = self.registry.connection_pool.lock().await;
                if pool.get_connection_by_peer_id(&self.peer_id).is_some() {
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait for gossip sync to complete by checking if we've received actors
        let mut last_actor_count = 0;
        let mut stable_iterations = 0;

        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(GossipError::Timeout);
            }

            // Get current actor count
            let current_count = self.registry.get_actor_count().await;

            // If the count is stable for 3 iterations (30ms), we're synced
            if current_count == last_actor_count && current_count > 0 {
                stable_iterations += 1;
                if stable_iterations >= 3 {
                    tracing::info!(
                        peer_id = %self.peer_id,
                        actor_count = current_count,
                        elapsed = ?start.elapsed(),
                        "Initial sync completed"
                    );
                    return Ok(());
                }
            } else {
                stable_iterations = 0;
                last_actor_count = current_count;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Message types for the request-response protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Gossip protocol message (registry sync, actor registrations, etc.)
    Gossip = 0,
    /// Request expecting a response (ask)
    Ask = 1,
    /// Response to an ask request
    Response = 2,
    /// Direct actor tell message (no wrapping)
    ActorTell = 3,
    /// Direct actor ask message (no wrapping)
    ActorAsk = 4,
    /// Start of a streaming transfer
    StreamStart = 0x10,
    /// Streaming data chunk
    StreamData = 0x11,
    /// End of streaming transfer
    StreamEnd = 0x12,
}

impl MessageType {
    /// Parse message type from byte
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(MessageType::Gossip),
            1 => Some(MessageType::Ask),
            2 => Some(MessageType::Response),
            3 => Some(MessageType::ActorTell),
            4 => Some(MessageType::ActorAsk),
            0x10 => Some(MessageType::StreamStart),
            0x11 => Some(MessageType::StreamData),
            0x12 => Some(MessageType::StreamEnd),
            _ => None,
        }
    }
}

/// Header for streaming protocol messages
#[derive(Debug, Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StreamHeader {
    /// Unique stream identifier
    pub stream_id: u64,
    /// Total size of the complete message
    pub total_size: u64,
    /// Size of this chunk (0 for start/end markers)
    pub chunk_size: u32,
    /// Chunk sequence number
    pub chunk_index: u32,
    /// Message type hash
    pub type_hash: u32,
    /// Target actor ID
    pub actor_id: u64,
}

impl StreamHeader {
    /// Size of the serialized header
    pub const SERIALIZED_SIZE: usize = 8 + 8 + 4 + 4 + 4 + 8; // 36 bytes

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_SIZE);
        bytes.extend_from_slice(&self.stream_id.to_be_bytes());
        bytes.extend_from_slice(&self.total_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_index.to_be_bytes());
        bytes.extend_from_slice(&self.type_hash.to_be_bytes());
        bytes.extend_from_slice(&self.actor_id.to_be_bytes());
        bytes
    }

    /// Parse header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::SERIALIZED_SIZE {
            return None;
        }

        Some(Self {
            stream_id: u64::from_be_bytes(bytes[0..8].try_into().ok()?),
            total_size: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            chunk_size: u32::from_be_bytes(bytes[16..20].try_into().ok()?),
            chunk_index: u32::from_be_bytes(bytes[20..24].try_into().ok()?),
            type_hash: u32::from_be_bytes(bytes[24..28].try_into().ok()?),
            actor_id: u64::from_be_bytes(bytes[28..36].try_into().ok()?),
        })
    }
}

/// Errors that can occur in the gossip registry
#[derive(Error, Debug)]
pub enum GossipError {
    #[error("network error: {0}")]
    Network(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] rkyv::rancor::Error),

    #[error("message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("connection timeout")]
    Timeout,

    #[error("peer not found: {0}")]
    PeerNotFound(SocketAddr),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("TLS configuration error: {0}")]
    TlsConfigError(String),

    #[error("actor not found: {0}")]
    ActorNotFound(String),

    #[error("registry shutdown")]
    Shutdown,

    #[error("invalid keypair: {0}")]
    InvalidKeyPair(String),

    #[error("invalid signature: {0}")]
    InvalidSignature(String),

    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("TLS handshake failed: {0}")]
    TlsHandshakeFailed(String),

    #[error("delta too old: requested {requested}, oldest available {oldest}")]
    DeltaTooOld { requested: u64, oldest: u64 },

    #[error("full sync required")]
    FullSyncRequired,

    #[error("connection already exists")]
    ConnectionExists,

    #[error("actor '{0}' already exists")]
    ActorAlreadyExists(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, GossipError>;

/// Get current timestamp in seconds (still used for TTL)
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

/// Get current timestamp in nanoseconds for high precision timing
pub fn current_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

/// Get high resolution instant for precise timing measurements
pub fn current_instant() -> std::time::Instant {
    std::time::Instant::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_timestamp() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let timestamp = current_timestamp();

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        assert!(timestamp >= before);
        assert!(timestamp <= after);
    }

    #[test]
    fn test_gossip_error_display() {
        let err = GossipError::Network(io::Error::other("test error"));
        assert_eq!(err.to_string(), "network error: test error");

        let err = GossipError::MessageTooLarge {
            size: 1000,
            max: 500,
        };
        assert_eq!(err.to_string(), "message too large: 1000 bytes (max: 500)");

        let err = GossipError::Timeout;
        assert_eq!(err.to_string(), "connection timeout");

        let err = GossipError::PeerNotFound("127.0.0.1:8080".parse().unwrap());
        assert_eq!(err.to_string(), "peer not found: 127.0.0.1:8080");

        let err = GossipError::ActorNotFound("test_actor".to_string());
        assert_eq!(err.to_string(), "actor not found: test_actor");

        let err = GossipError::Shutdown;
        assert_eq!(err.to_string(), "registry shutdown");

        let err = GossipError::DeltaTooOld {
            requested: 10,
            oldest: 20,
        };
        assert_eq!(
            err.to_string(),
            "delta too old: requested 10, oldest available 20"
        );

        let err = GossipError::FullSyncRequired;
        assert_eq!(err.to_string(), "full sync required");

        let err = GossipError::ConnectionExists;
        assert_eq!(err.to_string(), "connection already exists");

        let err = GossipError::ActorAlreadyExists("test_actor".to_string());
        assert_eq!(err.to_string(), "actor 'test_actor' already exists");
    }

    #[test]
    fn test_error_conversions() {
        // Test From<io::Error>
        let io_err = io::Error::other("io error");
        let gossip_err: GossipError = io_err.into();
        match gossip_err {
            GossipError::Network(_) => (),
            _ => panic!("Expected Network error"),
        }

        // Test that error variants work correctly - using a different approach
        let timeout_err = GossipError::Timeout;
        match timeout_err {
            GossipError::Timeout => (),
            _ => panic!("Expected Timeout error"),
        }
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        match ok_result {
            Ok(value) => assert_eq!(value, 42),
            Err(_) => panic!("Expected Ok result"),
        }

        let err_result: Result<i32> = Err(GossipError::Timeout);
        assert!(err_result.is_err());
    }
}
