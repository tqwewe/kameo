use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::{Mutex, RwLock};

use rand::seq::SliceRandom;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tracing::{debug, error, info, warn};

use crate::{
    connection_pool::ConnectionPool,
    current_timestamp,
    handshake::PeerCapabilities,
    peer_discovery::{PeerDiscovery, PeerDiscoveryConfig},
    GossipConfig, GossipError, NodeId, RegistrationPriority, RemoteActorLocation, Result,
};

/// Future type for actor message handling responses
pub type ActorMessageFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Vec<u8>>>> + Send + 'a>>;

/// Callback trait for handling incoming actor messages
pub trait ActorMessageHandler: Send + Sync {
    /// Handle an incoming actor message
    fn handle_actor_message(
        &self,
        actor_id: &str,
        type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_>;
}

/// Registry change types for delta tracking with vector clocks
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub enum RegistryChange {
    /// Actor was added or updated
    ActorAdded {
        name: String,
        location: RemoteActorLocation,
        priority: RegistrationPriority,
    },
    /// Actor was removed
    ActorRemoved {
        name: String,
        vector_clock: crate::VectorClock,
        removing_node_id: crate::NodeId, // Node that performed the removal
        priority: RegistrationPriority,
    },
}

/// Delta representing changes since a specific sequence number
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct RegistryDelta {
    pub since_sequence: u64,
    pub current_sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub sender_peer_id: crate::PeerId, // Peer's unique identifier
    pub wall_clock_time: u64,          // For debugging/monitoring only
    pub precise_timing_nanos: u64,     // High precision timing for latency measurements
}

/// Peer health status from a reporter's perspective
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct PeerHealthStatus {
    /// Is the peer reachable from this reporter
    pub is_alive: bool,
    /// Last successful contact timestamp
    pub last_contact: u64,
    /// Number of failed connection attempts
    pub failure_count: u32,
}

/// Pending peer failure awaiting consensus
#[derive(Debug, Clone)]
pub struct PendingFailure {
    /// When we first detected the failure
    pub first_detected: u64,
    /// Timeout for collecting consensus
    pub consensus_deadline: u64,
    /// Have we queried other peers yet
    pub query_sent: bool,
}

/// Message types for the gossip protocol
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub enum RegistryMessage {
    /// Delta gossip message containing only changes
    DeltaGossip { delta: RegistryDelta },
    /// Response to delta gossip with our own delta
    DeltaGossipResponse { delta: RegistryDelta },
    /// Request for full sync (fallback when deltas are unavailable)
    FullSyncRequest {
        sender_peer_id: crate::PeerId, // Peer's unique identifier
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Full synchronization message
    FullSync {
        local_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        known_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        sender_peer_id: crate::PeerId,                    // Peer's unique identifier
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Response to full sync
    FullSyncResponse {
        local_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        known_actors: Vec<(String, RemoteActorLocation)>, // Use Vec for rkyv serialization
        sender_peer_id: crate::PeerId,                    // Peer's unique identifier
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Peer health status report
    PeerHealthReport {
        reporter: crate::PeerId,
        peer_statuses: Vec<(String, PeerHealthStatus)>, // Use Vec for rkyv serialization
        timestamp: u64,
    },
    /// Lightweight ACK for immediate registrations
    ImmediateAck { actor_name: String, success: bool },
    /// Query for peer health consensus
    PeerHealthQuery {
        sender: crate::PeerId,
        target_peer: String,
        timestamp: u64,
    },
    /// Direct actor message (tell or ask)
    ActorMessage {
        actor_id: String,
        type_hash: u32,
        payload: Vec<u8>,
        correlation_id: Option<u16>,
    },
    /// Peer list gossip for automatic peer discovery
    /// Contains list of known peers with their connection info
    PeerListGossip {
        /// List of known peers (address as string for rkyv, peer info)
        peers: Vec<PeerInfoGossip>,
        /// Timestamp when this gossip was generated
        timestamp: u64,
        /// Sender's advertised address (so receiver can add us to their peer list)
        sender_addr: String,
    },
}

/// Statistics about the gossip registry
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct RegistryStats {
    pub local_actors: usize,
    pub known_actors: usize,
    pub active_peers: usize,
    pub failed_peers: usize,
    pub total_gossip_rounds: u64,
    pub current_sequence: u64,
    pub uptime_seconds: u64,
    pub last_gossip_timestamp: u64,
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub delta_history_size: usize,
    pub avg_delta_size: f64,
    // Peer discovery metrics (Phase 5)
    /// Number of peers discovered via gossip (in known_peers cache)
    pub discovered_peers: usize,
    /// Number of failed peer discovery attempts (connection failures to discovered peers)
    pub failed_discovery_attempts: u64,
    /// Average mesh connectivity (connected_peers / known_peers ratio)
    pub avg_mesh_connectivity: f64,
    /// Time taken to form initial mesh (first N peers connected), in milliseconds
    pub mesh_formation_time_ms: Option<u64>,
}

/// Peer information with failure tracking and delta state
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub address: SocketAddr,              // Listening address
    pub peer_address: Option<SocketAddr>, // Actual connection address (may be NATed)
    pub node_id: Option<crate::NodeId>,   // NodeId for TLS verification (may be learned on connect)
    pub failures: usize,
    pub last_attempt: u64,
    pub last_success: u64,
    pub last_sequence: u64,
    /// Last sequence we successfully sent to this peer
    pub last_sent_sequence: u64,
    /// Number of consecutive delta exchanges with this peer
    pub consecutive_deltas: u64,
    /// When this peer last failed (for tracking permanent failures)
    pub last_failure_time: Option<u64>,
}

impl PeerInfo {
    /// Create a PeerInfo for the local node (self) for gossip inclusion
    /// Used when including ourselves in peer list gossip
    pub fn local(advertise_addr: SocketAddr) -> Self {
        let now = crate::current_timestamp();
        Self {
            address: advertise_addr,
            peer_address: None,
            node_id: None,
            failures: 0,
            last_attempt: now,
            last_success: now, // Important: prevents pruning
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
        }
    }

    /// Convert to gossip-serializable format
    pub fn to_gossip(&self) -> PeerInfoGossip {
        PeerInfoGossip {
            address: self.address.to_string(),
            peer_address: self.peer_address.map(|a| a.to_string()),
            node_id: self.node_id,
            failures: self.failures,
            last_attempt: self.last_attempt,
            last_success: self.last_success,
        }
    }

    /// Create from gossip-serializable format
    pub fn from_gossip(gossip: &PeerInfoGossip) -> Option<Self> {
        let address: SocketAddr = gossip.address.parse().ok()?;
        let peer_address = gossip.peer_address.as_ref().and_then(|a| a.parse().ok());

        Some(Self {
            address,
            peer_address,
            node_id: gossip.node_id,
            failures: gossip.failures,
            last_attempt: gossip.last_attempt,
            last_success: gossip.last_success,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
        })
    }
}

/// Peer information for gossip (rkyv-serializable version)
/// Uses String instead of SocketAddr for rkyv serialization
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct PeerInfoGossip {
    /// Listening address (as string for rkyv serialization)
    pub address: String,
    /// Actual connection address if different (NAT)
    pub peer_address: Option<String>,
    /// NodeId for TLS verification
    pub node_id: Option<crate::NodeId>,
    /// Number of consecutive failures
    pub failures: usize,
    /// Last connection attempt timestamp
    pub last_attempt: u64,
    /// Last successful connection timestamp
    pub last_success: u64,
}

/// Historical delta for efficient incremental updates
#[derive(Debug, Clone)]
pub struct HistoricalDelta {
    pub sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub wall_clock_time: u64,
}

/// Data needed to perform gossip with a single peer
#[derive(Debug)]
pub struct GossipTask {
    pub peer_addr: SocketAddr,
    pub message: RegistryMessage,
    pub current_sequence: u64,
}

/// Result of a gossip operation
#[derive(Debug)]
pub struct GossipResult {
    pub peer_addr: SocketAddr,
    pub sent_sequence: u64,
    pub outcome: Result<Option<RegistryMessage>>,
}

/// Separated actor state for read-heavy operations (now with vector clocks)
#[derive(Debug)]
pub struct ActorState {
    pub local_actors: HashMap<String, RemoteActorLocation>,
    pub known_actors: HashMap<String, RemoteActorLocation>,
}

/// Gossip coordination state for write-heavy operations
#[derive(Debug)]
pub struct GossipState {
    pub gossip_sequence: u64,
    pub pending_changes: Vec<RegistryChange>,
    pub urgent_changes: Vec<RegistryChange>, // High/Critical priority changes
    pub delta_history: Vec<HistoricalDelta>,
    pub peers: HashMap<SocketAddr, PeerInfo>,
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub shutdown: bool,
    /// Track which actors are connected from which peer address
    pub peer_to_actors: HashMap<SocketAddr, HashSet<String>>,
    /// Track peer health reports from different observers
    pub peer_health_reports: HashMap<SocketAddr, HashMap<SocketAddr, PeerHealthStatus>>,
    /// Pending peer failures that need consensus
    pub pending_peer_failures: HashMap<SocketAddr, PendingFailure>,

    // =================== Peer Discovery State ===================
    /// Last time we sent peer list gossip (for rate limiting)
    pub last_peer_gossip_time: u64,
    /// Peer discovery manager (None if peer discovery is disabled)
    pub peer_discovery: Option<PeerDiscovery>,
    /// LRU cache of known peers discovered via gossip
    pub known_peers: LruCache<SocketAddr, PeerInfo>,
    /// Timestamp (in millis since start_time) when mesh formation completed
    pub mesh_formation_time_ms: Option<u64>,
}

/// Core gossip registry implementation with separated locks
#[derive(Clone)]
pub struct GossipRegistry {
    // Immutable config
    pub bind_addr: SocketAddr,
    pub peer_id: crate::PeerId, // Unique peer identifier (public key)
    pub config: GossipConfig,
    pub start_time: u64,
    pub start_instant: Instant,

    // Optional TLS configuration for encrypted connections
    pub tls_config: Option<Arc<crate::tls::TlsConfig>>,

    // Separated lockable state
    pub actor_state: Arc<RwLock<ActorState>>,
    pub gossip_state: Arc<Mutex<GossipState>>,
    pub connection_pool: Arc<Mutex<ConnectionPool>>,
    pub peer_capabilities: Arc<DashMap<SocketAddr, crate::handshake::PeerCapabilities>>,
    pub peer_capabilities_by_node: Arc<DashMap<crate::NodeId, crate::handshake::PeerCapabilities>>,
    pub peer_capability_addr_to_node: Arc<DashMap<SocketAddr, crate::NodeId>>,

    // Actor message handler callback
    pub actor_message_handler: Arc<Mutex<Option<Arc<dyn ActorMessageHandler>>>>,

    // Stream assembly state
    pub stream_assemblies: Arc<Mutex<HashMap<u64, StreamAssembly>>>,

    // Pending ACKs for synchronous registrations
    pub pending_acks: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<bool>>>>,
}

unsafe impl Send for GossipRegistry {}
unsafe impl Sync for GossipRegistry {}

/// State for assembling streamed messages
#[derive(Debug)]
pub struct StreamAssembly {
    pub header: crate::StreamHeader,
    pub chunks: std::collections::BTreeMap<u32, Vec<u8>>,
    pub received_bytes: usize,
}

impl StreamAssembly {
    /// Check if the stream assembly is complete (all chunks received with no gaps)
    pub fn is_complete(&self) -> bool {
        let expected_chunks = self
            .header
            .total_size
            .div_ceil(crate::connection_pool::STREAM_CHUNK_SIZE as u64);

        if self.chunks.len() as u64 != expected_chunks {
            return false;
        }

        // Verify all indices 0..N-1 are present (no gaps)
        for i in 0..expected_chunks as u32 {
            if !self.chunks.contains_key(&i) {
                return false;
            }
        }

        true
    }
}

impl GossipRegistry {
    /// Create a new gossip registry
    pub fn new(bind_addr: SocketAddr, config: GossipConfig) -> Self {
        // Use public key from config (required for TLS identity)
        let peer_id = config
            .key_pair
            .as_ref()
            .expect("GossipConfig.key_pair is required for TLS-only mode")
            .peer_id();

        info!(
            bind_addr = %bind_addr,
            peer_id = %peer_id,
            "creating new gossip registry"
        );

        let connection_pool =
            ConnectionPool::new(config.max_pooled_connections, config.connection_timeout);
        let peer_capabilities = Arc::new(DashMap::new());

        Self {
            bind_addr,
            peer_id,
            config: config.clone(),
            start_time: current_timestamp(),
            start_instant: crate::current_instant(),
            tls_config: None,
            actor_state: Arc::new(RwLock::new(ActorState {
                local_actors: HashMap::new(),
                known_actors: HashMap::new(),
            })),
            gossip_state: Arc::new(Mutex::new(GossipState {
                gossip_sequence: 0,
                pending_changes: Vec::new(),
                urgent_changes: Vec::new(),
                delta_history: Vec::new(),
                peers: HashMap::new(),
                delta_exchanges: 0,
                full_sync_exchanges: 0,
                shutdown: false,
                peer_to_actors: HashMap::new(),
                peer_health_reports: HashMap::new(),
                pending_peer_failures: HashMap::new(),
                // Peer discovery state
                last_peer_gossip_time: 0,
                peer_discovery: if config.enable_peer_discovery {
                    Some(PeerDiscovery::new(
                        bind_addr,
                        PeerDiscoveryConfig {
                            max_peers: config.max_peers,
                            allow_private_discovery: config.allow_private_discovery,
                            allow_loopback_discovery: config.allow_loopback_discovery,
                            allow_link_local_discovery: config.allow_link_local_discovery,
                            fail_ttl: config.fail_ttl,
                            pending_ttl: config.pending_ttl,
                        },
                    ))
                } else {
                    None
                },
                known_peers: LruCache::new(
                    NonZeroUsize::new(config.known_peers_capacity)
                        .unwrap_or(NonZeroUsize::new(10_000).unwrap()),
                ),
                mesh_formation_time_ms: None,
            })),
            connection_pool: Arc::new(Mutex::new(connection_pool)),
            peer_capabilities: peer_capabilities.clone(),
            peer_capabilities_by_node: Arc::new(DashMap::new()),
            peer_capability_addr_to_node: Arc::new(DashMap::new()),
            actor_message_handler: Arc::new(Mutex::new(None)),
            stream_assemblies: Arc::new(Mutex::new(HashMap::new())),
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Enable TLS for secure connections
    /// This must be called before starting the registry to enable TLS
    pub fn enable_tls(&mut self, secret_key: crate::SecretKey) -> Result<()> {
        let tls_config = crate::tls::TlsConfig::with_peer_discovery(
            secret_key,
            self.config.enable_peer_discovery,
        )
        .map_err(|e| GossipError::TlsConfigError(format!("Failed to create TLS config: {}", e)))?;
        self.tls_config = Some(Arc::new(tls_config));
        info!("TLS enabled for gossip registry");
        Ok(())
    }

    /// Track negotiated peer capabilities for a peer connection
    pub fn set_peer_capabilities(&self, addr: SocketAddr, caps: PeerCapabilities) {
        eprintln!("set_peer_capabilities addr {}", addr);
        self.peer_capabilities.insert(addr, caps);
    }

    /// Attach capabilities recorded for an address to a specific NodeId (once known)
    pub async fn associate_peer_capabilities_with_node(&self, addr: SocketAddr, node_id: NodeId) {
        if let Some(entry) = self.peer_capabilities.get(&addr) {
            let caps = entry.value().clone();
            self.peer_capabilities_by_node.insert(node_id, caps);
        }
        self.peer_capability_addr_to_node.insert(addr, node_id);
        self.propagate_node_id_to_known_addresses(addr, node_id)
            .await;
    }

    /// Remove stored capabilities for a peer (e.g., when connection closes)
    pub fn clear_peer_capabilities(&self, addr: &SocketAddr) {
        eprintln!("clear_peer_capabilities addr {}", addr);
        self.peer_capabilities.remove(addr);
        if let Some((_, node_id)) = self.peer_capability_addr_to_node.remove(addr) {
            let still_has_addr = self
                .peer_capability_addr_to_node
                .iter()
                .any(|entry| *entry.value() == node_id);
            if !still_has_addr {
                self.peer_capabilities_by_node.remove(&node_id);
            }
        }
    }

    /// Determine whether a peer supports receiving PeerListGossip
    pub async fn peer_supports_peer_list(&self, addr: &SocketAddr) -> bool {
        eprintln!(
            "peer_supports called for {} entries {}",
            addr,
            self.peer_capabilities.len()
        );
        for entry in self.peer_capabilities.iter() {
            eprintln!(
                " capability entry addr {} can {}",
                entry.key(),
                entry.value().can_send_peer_list()
            );
        }

        if let Some(entry) = self.peer_capabilities.get(addr) {
            return entry.value().can_send_peer_list();
        }

        if let Some(entry) = self.peer_capability_addr_to_node.get(addr) {
            if let Some(cap_entry) = self.peer_capabilities_by_node.get(entry.value()) {
                return cap_entry.value().can_send_peer_list();
            }
        }

        if let Some(node_id) = self.lookup_node_id(addr).await {
            self.peer_capability_addr_to_node.insert(*addr, node_id);
            if let Some(entry) = self.peer_capabilities_by_node.get(&node_id) {
                return entry.value().can_send_peer_list();
            }
        }

        for entry in self.peer_capabilities.iter() {
            if entry.key().ip() == addr.ip() && entry.value().can_send_peer_list() {
                return true;
            }
        }

        false
    }

    async fn propagate_node_id_to_known_addresses(&self, addr: SocketAddr, node_id: NodeId) {
        let mut gossip_state = self.gossip_state.lock().await;
        if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
            peer_info.node_id = Some(node_id);
        } else {
            gossip_state.known_peers.put(
                addr,
                PeerInfo {
                    address: addr,
                    peer_address: None,
                    node_id: Some(node_id),
                    failures: 0,
                    last_attempt: current_timestamp(),
                    last_success: current_timestamp(),
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                },
            );
        }
    }

    /// Register an actor message handler callback
    pub async fn set_actor_message_handler(&self, handler: Arc<dyn ActorMessageHandler>) {
        let mut callback_guard = self.actor_message_handler.lock().await;
        *callback_guard = Some(handler);
        info!("actor message handler registered");
    }

    /// Remove the actor message handler callback
    pub async fn clear_actor_message_handler(&self) {
        let mut callback_guard = self.actor_message_handler.lock().await;
        *callback_guard = None;
        info!("actor message handler cleared");
    }

    /// Handle an incoming actor message by forwarding to the registered callback
    pub async fn handle_actor_message(
        &self,
        actor_id: &str,
        type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> Result<Option<Vec<u8>>> {
        let handler_guard = self.actor_message_handler.lock().await;
        if let Some(ref handler) = *handler_guard {
            debug!(
                actor_id = %actor_id,
                type_hash = %format!("{:08x}", type_hash),
                payload_len = payload.len(),
                "forwarding actor message to handler"
            );
            handler
                .handle_actor_message(actor_id, type_hash, payload, correlation_id)
                .await
        } else {
            warn!(
                actor_id = %actor_id,
                type_hash = %format!("{:08x}", type_hash),
                "no actor message handler registered - message dropped"
            );
            Ok(None)
        }
    }

    // Add bootstrap peers for initial connection (DEPRECATED)
    // pub async fn add_bootstrap_peers(&self, bootstrap_peers: Vec<SocketAddr>) {
    //     let mut gossip_state = self.gossip_state.lock().await;
    //     let current_time = current_timestamp();

    //     for peer in bootstrap_peers {
    //         if peer != self.bind_addr {
    //             gossip_state.peers.insert(
    //                 peer,
    //                 PeerInfo {
    //                     address: peer,
    //                     peer_address: None,
    //                     // Start with max failures - peers are offline until proven otherwise
    //                     failures: self.config.max_peer_failures,
    //                     last_attempt: 0,
    //                     last_success: 0,
    //                     last_sequence: 0,
    //                     last_sent_sequence: 0,
    //                     consecutive_deltas: 0,
    //                     // Mark the failure time so retry logic works
    //                     last_failure_time: Some(current_time),
    //                 },
    //             );
    //             info!(peer = %peer, "added bootstrap peer as initially offline");
    //         }
    //     }
    //     info!(
    //         peer_count = gossip_state.peers.len(),
    //         "added bootstrap peers (all initially offline)"
    //     );
    // }

    // /// Add bootstrap peers with their expected node names
    // pub async fn add_bootstrap_peers_with_names(&self, bootstrap_peers: Vec<crate::PeerConfig>) {
    //     let mut gossip_state = self.gossip_state.lock().await;
    //     let pool = self.connection_pool.lock().await;
    //     let current_time = current_timestamp();

    //     for peer_config in bootstrap_peers {
    //         if peer_config.addr != self.bind_addr {
    //             // Store the peer with its address
    //             gossip_state.peers.insert(
    //                 peer_config.addr,
    //                 PeerInfo {
    //                     address: peer_config.addr,
    //                     peer_address: None,
    //                     // Start with max failures - peers are offline until proven otherwise
    //                     failures: self.config.max_peer_failures,
    //                     last_attempt: 0,
    //                     last_success: 0,
    //                     last_sequence: 0,
    //                     last_sent_sequence: 0,
    //                     consecutive_deltas: 0,
    //                     // Mark the failure time so retry logic works
    //                     last_failure_time: Some(current_time),
    //                 },
    //             );

    //             // Map the expected node name to this address
    //             pool.update_node_address(&peer_config.node_name, peer_config.addr);
    //             info!("Bootstrap peer: {} -> {}", peer_config.node_name, peer_config.addr);
    //         }
    //     }
    //     info!(
    //         peer_count = gossip_state.peers.len(),
    //         "added bootstrap peers with names (all initially offline)"
    //     );
    // }

    /// Add a new peer (called when receiving connections)
    pub async fn add_peer(&self, peer_addr: SocketAddr) {
        self.add_peer_with_node_id(peer_addr, None).await;
    }

    /// Add a new peer with NodeId for TLS verification
    pub async fn add_peer_with_node_id(
        &self,
        peer_addr: SocketAddr,
        node_id: Option<crate::NodeId>,
    ) {
        debug!(peer = %peer_addr, self_addr = %self.bind_addr, has_node_id = node_id.is_some(), "add_peer_with_node_id called");
        if peer_addr != self.bind_addr {
            let mut gossip_state = self.gossip_state.lock().await;

            // Check if we already have this peer
            if let Some(existing_peer) = gossip_state.peers.get_mut(&peer_addr) {
                // Update NodeId if provided and not already set
                if node_id.is_some() && existing_peer.node_id.is_none() {
                    existing_peer.node_id = node_id;
                    debug!(peer = %peer_addr, "updated existing peer with NodeId");
                } else {
                    debug!(peer = %peer_addr, "peer already tracked");
                }
                return;
            }

            if let Entry::Vacant(e) = gossip_state.peers.entry(peer_addr) {
                let current_time = current_timestamp();
                e.insert(PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    node_id,
                    failures: 0,
                    last_attempt: current_time,
                    last_success: current_time,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                });
                if let Some(node_id) = node_id {
                    self.peer_capability_addr_to_node.insert(peer_addr, node_id);
                    if let Some(entry) = self.peer_capabilities.get(&peer_addr) {
                        self.peer_capabilities_by_node
                            .insert(node_id, entry.value().clone());
                    }
                }
                debug!(
                    peer = %peer_addr,
                    peers_count = gossip_state.peers.len(),
                    has_node_id = node_id.is_some(),
                    "📌 Added new peer (listening address)"
                );

                // Pre-warm connection for immediate gossip (non-blocking)
                // if self.config.immediate_propagation_enabled {
                //     // Note: We'll create the connection lazily when first needed
                //     debug!(peer = %peer_addr, "will create persistent connection on first use");
                // }
            }
        } else {
            info!(peer = %peer_addr, "not adding peer - same as self");
        }
    }

    /// Configure a peer by peer ID and its expected connection address
    pub async fn configure_peer(&self, peer_id: crate::PeerId, connect_addr: SocketAddr) {
        let pool = self.connection_pool.lock().await;
        info!(peer_id = %peer_id, addr = %connect_addr, "Configured peer");
        pool.peer_id_to_addr.insert(peer_id.clone(), connect_addr);
        pool.reindex_connection_addr(&peer_id, connect_addr);
    }

    /// Connect to a configured peer by peer ID
    pub async fn connect_to_peer(&self, peer_id: &crate::PeerId) -> Result<()> {
        let mut pool = self.connection_pool.lock().await;
        pool.get_connection_to_peer(peer_id).await?;
        info!(peer_id = %peer_id, "Connected to peer");
        Ok(())
    }

    /// Register a local actor (fast path - minimal locking) with vector clock increment
    pub async fn register_actor(&self, name: String, location: RemoteActorLocation) -> Result<()> {
        self.register_actor_with_priority(name, location, RegistrationPriority::Normal)
            .await
    }

    /// Register actor with confirmation from at least one peer
    /// Returns when first peer ACKs or timeout
    pub async fn register_actor_sync(
        &self,
        name: String,
        location: RemoteActorLocation,
        timeout: Duration,
    ) -> Result<()> {
        // Step 1: Check if we have any healthy peers
        let peer_count = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(_, info)| info.failures < self.config.max_peer_failures)
                .count()
        };

        if peer_count == 0 {
            // No peers - just do local registration and return
            self.register_actor_with_priority(name, location, RegistrationPriority::Immediate)
                .await?;

            info!("Sync registration completed immediately (no peers to confirm)");
            return Ok(());
        }

        // Have peers - wait for ACK from at least one
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

        // Store pending ACK handler
        {
            let mut pending = self.pending_acks.lock().await;
            pending.insert(name.clone(), ack_tx);
        }

        // Register with immediate priority (triggers instant gossip to peers)
        self.register_actor_with_priority(name.clone(), location, RegistrationPriority::Immediate)
            .await?;

        // Wait for first ACK or timeout
        match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(success)) if success => {
                info!("Sync registration confirmed by peer for actor '{}'", name);
                Ok(())
            }
            Ok(Ok(_)) => {
                warn!(
                    "Sync registration failed according to peer for actor '{}'",
                    name
                );
                Err(GossipError::Network(io::Error::other(
                    "Peer rejected registration",
                )))
            }
            Ok(Err(_)) => {
                // Channel closed unexpectedly
                warn!("ACK channel closed for actor '{}'", name);
                self.pending_acks.lock().await.remove(&name);
                Ok(())
            }
            Err(_) => {
                // Timeout - maybe peer is slow or disconnected
                warn!("Sync registration timed out waiting for peer ACK for actor '{}', continuing anyway", name);
                self.pending_acks.lock().await.remove(&name);
                Ok(()) // Still return Ok - gossip will eventually propagate
            }
        }
    }

    /// Get the current number of actors in the registry (both local and known)
    pub async fn get_actor_count(&self) -> usize {
        let actor_state = self.actor_state.read().await;
        actor_state.local_actors.len() + actor_state.known_actors.len()
    }

    /// Register a local actor with specific priority
    pub async fn register_actor_with_priority(
        &self,
        name: String,
        mut location: RemoteActorLocation,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let register_start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        // Update the location with current wall time and priority
        location.wall_clock_time = current_timestamp();
        location.priority = priority;

        // Single write lock acquisition for actor state - atomic operation
        {
            let mut actor_state = self.actor_state.write().await;
            if actor_state.local_actors.contains_key(&name)
                || actor_state.known_actors.contains_key(&name)
            {
                return Err(GossipError::ActorAlreadyExists(name));
            }

            // Increment vector clock inside the lock for atomicity
            location.vector_clock.increment(location.node_id);

            actor_state
                .local_actors
                .insert(name.clone(), location.clone());
        }

        // Update gossip state with pending change - choose queue based on priority
        let should_trigger_immediate = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Check shutdown
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }

            let change = RegistryChange::ActorAdded {
                name: name.clone(),
                location,
                priority,
            };

            if priority.should_trigger_immediate_gossip() {
                gossip_state.urgent_changes.push(change);
                true
            } else {
                gossip_state.pending_changes.push(change);
                false
            }
        };

        if priority.should_trigger_immediate_gossip() {
            let gossip_trigger_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let registration_duration_ms =
                (gossip_trigger_time - register_start_time) as f64 / 1_000_000.0;

            info!(
                actor_name = %name,
                bind_addr = %self.bind_addr,
                priority = ?priority,
                registration_duration_ms = registration_duration_ms,
                "🚀 REGISTERED_ACTOR_IMMEDIATE: Will trigger immediate propagation"
            );
        } else {
            info!(
                actor_name = %name,
                bind_addr = %self.bind_addr,
                priority = ?priority,
                "REGISTERED_ACTOR"
            );
        }

        // Trigger immediate gossip if this was an urgent change
        if should_trigger_immediate {
            if let Err(err) = self.trigger_immediate_gossip().await {
                warn!(error = %err, "failed to trigger immediate gossip");
            }
        }

        Ok(())
    }

    /// Unregister a local actor
    pub async fn unregister_actor(&self, name: &str) -> Result<Option<RemoteActorLocation>> {
        // Remove from actor state
        let removed = {
            let mut actor_state = self.actor_state.write().await;
            actor_state.local_actors.remove(name)
        };

        if let Some(ref location) = removed {
            info!(actor_name = %name, "unregistered local actor");

            // Track this change for delta gossip - use the priority from the removed actor
            let should_trigger_immediate = {
                let mut gossip_state = self.gossip_state.lock().await;

                // Check shutdown
                if gossip_state.shutdown {
                    return Err(GossipError::Shutdown);
                }

                // Create a new vector clock for the removal with proper causality
                let removal_clock = location.vector_clock.clone();
                removal_clock.increment(self.peer_id.to_node_id());

                let change = RegistryChange::ActorRemoved {
                    name: name.to_string(),
                    vector_clock: removal_clock,
                    removing_node_id: self.peer_id.to_node_id(),
                    priority: location.priority,
                };

                if location.priority.should_trigger_immediate_gossip() {
                    gossip_state.urgent_changes.push(change);
                    true
                } else {
                    gossip_state.pending_changes.push(change);
                    false
                }
            };

            // Trigger immediate gossip if this was an urgent change
            if should_trigger_immediate {
                if let Err(err) = self.trigger_immediate_gossip().await {
                    warn!(error = %err, "failed to trigger immediate gossip for actor removal");
                }
            }
        }
        Ok(removed)
    }

    /// Lookup an actor (read-only fast path)
    pub async fn lookup_actor(&self, name: &str) -> Option<RemoteActorLocation> {
        let actor_state = self.actor_state.read().await;

        // Check local actors first
        if let Some(location) = actor_state.local_actors.get(name) {
            debug!(actor_name = %name, location = "local", "actor found");
            return Some(location.clone());
        }

        // Check known remote actors
        if let Some(location) = actor_state.known_actors.get(name) {
            let now = current_timestamp();
            let age_secs = now.saturating_sub(location.wall_clock_time);
            if age_secs < self.config.actor_ttl.as_secs() {
                debug!(
                    actor_name = %name,
                    location = "remote",
                    age_seconds = age_secs,
                    "actor found"
                );
                return Some(location.clone());
            }
        }

        debug!(actor_name = %name, "actor not found");
        None
    }

    /// Get registry statistics
    pub async fn get_stats(&self) -> RegistryStats {
        let (local_actors, known_actors) = {
            let actor_state = self.actor_state.read().await;
            (
                actor_state.local_actors.len(),
                actor_state.known_actors.len(),
            )
        };

        let (
            gossip_sequence,
            active_peers,
            failed_peers,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
            discovered_peers,
            failed_discovery_attempts,
            avg_mesh_connectivity,
            mesh_formation_time_ms,
        ) = {
            let gossip_state = self.gossip_state.lock().await;
            let active_peers = gossip_state
                .peers
                .values()
                .filter(|p| p.failures < self.config.max_peer_failures)
                .count();
            let failed_peers = gossip_state.peers.len() - active_peers;

            // Peer discovery metrics (Phase 5)
            let discovered_peers = gossip_state.known_peers.len();
            let failed_discovery_attempts = gossip_state
                .peer_discovery
                .as_ref()
                .map(|pd| pd.failed_peer_count() as u64)
                .unwrap_or(0);

            // Calculate mesh connectivity: active connections / discovered peers
            let avg_mesh_connectivity = if discovered_peers > 0 {
                active_peers as f64 / discovered_peers as f64
            } else {
                0.0
            };

            (
                gossip_state.gossip_sequence,
                active_peers,
                failed_peers,
                gossip_state.delta_exchanges,
                gossip_state.full_sync_exchanges,
                gossip_state.delta_history.len(),
                discovered_peers,
                failed_discovery_attempts,
                avg_mesh_connectivity,
                gossip_state.mesh_formation_time_ms,
            )
        };

        let current_time = current_timestamp();
        let avg_delta_size = if delta_exchanges > 0 {
            // This is approximate since we don't hold the lock
            delta_history_size as f64
        } else {
            0.0
        };

        RegistryStats {
            local_actors,
            known_actors,
            active_peers,
            failed_peers,
            total_gossip_rounds: gossip_sequence,
            current_sequence: gossip_sequence,
            uptime_seconds: current_time.saturating_sub(self.start_time),
            last_gossip_timestamp: current_time,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
            avg_delta_size,
            discovered_peers,
            failed_discovery_attempts,
            avg_mesh_connectivity,
            mesh_formation_time_ms,
        }
    }

    /// Apply delta changes from a peer
    pub async fn apply_delta(&self, delta: RegistryDelta) -> Result<()> {
        let total_changes = delta.changes.len();
        let sender_peer_id = delta.sender_peer_id.clone();

        // Pre-compute priority flags to avoid redundant checks
        let has_immediate = delta.changes.iter().any(|change| match change {
            RegistryChange::ActorAdded { priority, .. } => {
                priority.should_trigger_immediate_gossip()
            }
            RegistryChange::ActorRemoved { priority, .. } => {
                priority.should_trigger_immediate_gossip()
            }
        });

        if has_immediate {
            error!(
                "🎯 RECEIVING IMMEDIATE CHANGES: {} total changes from {}",
                total_changes, sender_peer_id
            );
        }

        let mut peer_actors_added = std::collections::HashSet::new();
        let mut peer_actors_removed = std::collections::HashSet::new();

        // Pre-capture timing info outside lock for better performance
        let received_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        eprintln!("🔍 RECEIVED_TIMESTAMP: {}ns", received_timestamp);

        // Apply changes atomically under write lock
        let applied_count = {
            let mut actor_state = self.actor_state.write().await;
            let mut applied = 0;

            for change in delta.changes {
                match &change {
                    RegistryChange::ActorAdded {
                        name,
                        location,
                        priority: _,
                    } => {
                        // Don't override local actors - early exit
                        if actor_state.local_actors.contains_key(name.as_str()) {
                            debug!(
                                actor_name = %name,
                                "skipping remote actor update - actor is local"
                            );
                            continue;
                        }

                        // Check if we already know about this actor
                        let should_apply = match actor_state.known_actors.get(name.as_str()) {
                            Some(existing_location) => {
                                // Use vector clock for causal ordering
                                match location
                                    .vector_clock
                                    .compare(&existing_location.vector_clock)
                                {
                                    crate::ClockOrdering::After => true,
                                    crate::ClockOrdering::Concurrent => {
                                        // For concurrent updates, use deterministic tiebreaker
                                        // Hash of (actor_name + address + wall_clock) ensures consistency
                                        use std::hash::{Hash, Hasher};
                                        let mut hasher1 =
                                            std::collections::hash_map::DefaultHasher::new();
                                        name.hash(&mut hasher1);
                                        location.address.hash(&mut hasher1);
                                        location.wall_clock_time.hash(&mut hasher1);
                                        let hash1 = hasher1.finish();

                                        let mut hasher2 =
                                            std::collections::hash_map::DefaultHasher::new();
                                        name.hash(&mut hasher2);
                                        existing_location.address.hash(&mut hasher2);
                                        existing_location.wall_clock_time.hash(&mut hasher2);
                                        let hash2 = hasher2.finish();

                                        // Use hash comparison for deterministic resolution
                                        // If hashes are equal (extremely rare), prefer newer by wall clock
                                        if hash1 != hash2 {
                                            hash1 > hash2
                                        } else {
                                            location.wall_clock_time
                                                > existing_location.wall_clock_time
                                        }
                                    }
                                    _ => false, // Keep existing for Before or Equal
                                }
                            }
                            None => {
                                debug!(
                                    actor_name = %name,
                                    "applying new actor"
                                );
                                true // New actor
                            }
                        };

                        if should_apply {
                            // Only clone when actually inserting
                            let actor_name = name.clone();
                            actor_state
                                .known_actors
                                .insert(actor_name.clone(), location.clone());
                            applied += 1;

                            // Track this actor as belonging to the sender
                            peer_actors_added.insert(actor_name);

                            // Move timing calculations outside critical section for logging
                            if tracing::enabled!(tracing::Level::INFO) {
                                let propagation_time_nanos =
                                    received_timestamp - location.local_registration_time;
                                let propagation_time_ms =
                                    propagation_time_nanos as f64 / 1_000_000.0;

                                // Calculate time from when delta was sent (network + processing)
                                let network_processing_time_nanos =
                                    received_timestamp - delta.precise_timing_nanos as u128;
                                let network_processing_time_ms =
                                    network_processing_time_nanos as f64 / 1_000_000.0;

                                // Calculate pure serialization + processing time (excluding network)
                                let processing_only_time_ms =
                                    propagation_time_ms - network_processing_time_ms;

                                info!(
                                    actor_name = %name,
                                    priority = ?location.priority,
                                    propagation_time_ms = propagation_time_ms,
                                    network_processing_time_ms = network_processing_time_ms,
                                    processing_only_time_ms = processing_only_time_ms,
                                    "RECEIVED_ACTOR"
                                );
                            }
                        }
                    }
                    RegistryChange::ActorRemoved {
                        name,
                        vector_clock,
                        removing_node_id,
                        priority: _,
                    } => {
                        // Don't remove local actors - early exit
                        if actor_state.local_actors.contains_key(name.as_str()) {
                            debug!(
                                actor_name = %name,
                                "skipping actor removal - actor is local"
                            );
                            continue;
                        }

                        // Check vector clock ordering before applying removal
                        let should_remove = match actor_state.known_actors.get(name.as_str()) {
                            Some(existing_location) => {
                                // Use vector clock for causal ordering
                                match vector_clock.compare(&existing_location.vector_clock) {
                                    crate::ClockOrdering::After => {
                                        // Removal is causally after current state
                                        debug!(
                                            actor_name = %name,
                                            "removal is causally after current state - applying"
                                        );
                                        true
                                    }
                                    crate::ClockOrdering::Concurrent => {
                                        // For concurrent removals, use deterministic tiebreaker
                                        // Removal should win if it's "newer" based on hash
                                        use std::hash::{Hash, Hasher};
                                        let mut hasher1 =
                                            std::collections::hash_map::DefaultHasher::new();
                                        name.hash(&mut hasher1);
                                        removing_node_id.hash(&mut hasher1);
                                        vector_clock.hash(&mut hasher1);
                                        let hash1 = hasher1.finish();

                                        let mut hasher2 =
                                            std::collections::hash_map::DefaultHasher::new();
                                        name.hash(&mut hasher2);
                                        existing_location.node_id.hash(&mut hasher2);
                                        existing_location.vector_clock.hash(&mut hasher2);
                                        let hash2 = hasher2.finish();

                                        // Removal wins if its hash is greater (deterministic across all nodes)
                                        let should_apply = hash1 > hash2;
                                        debug!(
                                            actor_name = %name,
                                            removing_node = %removing_node_id.fmt_short(),
                                            existing_node = %existing_location.node_id.fmt_short(),
                                            should_apply = should_apply,
                                            "removal is concurrent with current state - using node_id tiebreaker"
                                        );
                                        should_apply
                                    }
                                    _ => {
                                        // Removal is outdated (Before or Equal) - ignore it
                                        debug!(
                                            actor_name = %name,
                                            "removal is outdated - ignoring"
                                        );
                                        false
                                    }
                                }
                            }
                            None => {
                                // Actor doesn't exist, nothing to remove
                                debug!(actor_name = %name, "actor not found - ignoring removal");
                                false
                            }
                        };

                        if should_remove && actor_state.known_actors.remove(name.as_str()).is_some()
                        {
                            applied += 1;
                            peer_actors_removed.insert(name.clone());
                            debug!(actor_name = %name, "applied actor removal");
                        }
                    }
                }
            }

            applied
        };

        let peer_actor_changes = peer_actors_added.len() + peer_actors_removed.len();

        if let Some(sender_addr) = {
            let pool = self.connection_pool.lock().await;
            pool.peer_id_to_addr
                .get(&sender_peer_id)
                .map(|entry| *entry)
        } {
            let mut gossip_state = self.gossip_state.lock().await;
            let entry = gossip_state
                .peer_to_actors
                .entry(sender_addr)
                .or_insert_with(std::collections::HashSet::new);

            for name in peer_actors_removed {
                entry.remove(&name);
            }
            for name in peer_actors_added {
                entry.insert(name);
            }
        } else {
            debug!(
                sender = %sender_peer_id,
                "no address mapping for sender; skipping peer_to_actors update"
            );
        }

        debug!(
            sender = %sender_peer_id,
            total_changes,
            applied_changes = applied_count,
            peer_actor_changes = peer_actor_changes,
            "completed delta application with vector clock conflict resolution"
        );

        Ok(())
    }

    /// Determine whether to use delta or full sync for a peer
    fn should_use_delta_state(&self, gossip_state: &GossipState, peer_info: &PeerInfo) -> bool {
        // For small clusters (≤ 5 total nodes), always use full sync to ensure
        // proper transitive propagation in star topologies
        let healthy_peers = gossip_state
            .peers
            .values()
            .filter(|p| p.failures < self.config.max_peer_failures)
            .count();
        let total_healthy_nodes = healthy_peers + 1;
        if total_healthy_nodes <= self.config.small_cluster_threshold {
            debug!(
                "using full sync for small cluster of {} healthy nodes",
                total_healthy_nodes
            );
            return false;
        }

        // Use full sync for new peers or if delta history is insufficient
        if peer_info.last_sequence == 0 {
            return false;
        }

        // Force full sync periodically
        if peer_info.consecutive_deltas >= self.config.full_sync_interval {
            return false;
        }

        // Check if we have the required delta history
        let oldest_available = gossip_state
            .delta_history
            .first()
            .map(|d| d.sequence)
            .unwrap_or(gossip_state.gossip_sequence);

        peer_info.last_sequence >= oldest_available
    }

    /// Create a delta containing changes since the specified sequence
    async fn create_delta_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        since_sequence: u64,
    ) -> Result<RegistryDelta> {
        let estimated_size =
            local_actors.len() + known_actors.len() + gossip_state.pending_changes.len();
        let mut changes = Vec::with_capacity(estimated_size);
        let current_time = current_timestamp();

        // If this is a brand new peer (since_sequence = 0), include all actors we know about
        if since_sequence == 0 {
            // Include all local actors as additions
            for (name, location) in local_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    priority: location.priority,
                });
            }

            // Include all known remote actors as additions
            for (name, location) in known_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    priority: location.priority,
                });
            }
        }

        // Include urgent changes first (they have higher priority)
        changes.extend(gossip_state.urgent_changes.clone());

        // Include pending changes from current round
        changes.extend(gossip_state.pending_changes.clone());

        // Include historical changes since the requested sequence
        for delta in &gossip_state.delta_history {
            if delta.sequence > since_sequence {
                changes.extend(delta.changes.clone());
            }
        }

        // Deduplicate changes to send only the most recent change for each actor
        let deduped_changes = Self::deduplicate_changes(changes);

        Ok(RegistryDelta {
            since_sequence,
            current_sequence: gossip_state.gossip_sequence,
            changes: deduped_changes,
            sender_peer_id: self.peer_id.clone(),
            wall_clock_time: current_time,
            precise_timing_nanos: crate::current_timestamp_nanos(), // Set high precision timing
        })
    }

    /// Create a full sync message from state
    async fn create_full_sync_message_from_state(
        &self,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        sequence: u64,
    ) -> RegistryMessage {
        debug!(
            "Creating full sync message: {} local actors, {} known actors",
            local_actors.len(),
            known_actors.len()
        );
        RegistryMessage::FullSync {
            local_actors: local_actors
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            known_actors: known_actors
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            sender_peer_id: self.peer_id.clone(), // Use peer ID
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a full sync response message from state
    pub async fn create_full_sync_response_from_state(
        &self,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        sequence: u64,
    ) -> RegistryMessage {
        RegistryMessage::FullSyncResponse {
            local_actors: local_actors
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            known_actors: known_actors
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            sender_peer_id: self.peer_id.clone(), // Use peer ID
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a delta response for incoming gossip
    pub async fn create_delta_response_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, RemoteActorLocation>,
        known_actors: &HashMap<String, RemoteActorLocation>,
        since_sequence: u64,
    ) -> Result<RegistryMessage> {
        let delta = self
            .create_delta_from_state(gossip_state, local_actors, known_actors, since_sequence)
            .await?;
        Ok(RegistryMessage::DeltaGossipResponse { delta })
    }

    /// Prepare gossip round with consistent lock ordering to prevent deadlocks
    pub async fn prepare_gossip_round(&self) -> Result<Vec<GossipTask>> {
        debug!("Starting gossip round");

        // Step 1: Check shutdown status first
        {
            let gossip_state = self.gossip_state.lock().await;
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }
        }

        // Step 2: Atomically commit pending changes
        let (current_sequence, has_peers) = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Double-check shutdown after acquiring locks
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }

            // Check if we have changes to commit
            let had_changes = !gossip_state.pending_changes.is_empty();

            // Increment sequence if we have changes
            if had_changes {
                // Increment sequence number
                gossip_state.gossip_sequence += 1;

                // Commit pending changes to history
                let delta = HistoricalDelta {
                    sequence: gossip_state.gossip_sequence,
                    changes: std::mem::take(&mut gossip_state.pending_changes),
                    wall_clock_time: current_timestamp(),
                };

                gossip_state.delta_history.push(delta);

                // Trim history if needed
                if gossip_state.delta_history.len() > self.config.max_delta_history {
                    gossip_state.delta_history.remove(0);
                }
            }

            (gossip_state.gossip_sequence, !gossip_state.peers.is_empty())
            // Lock is automatically released here
        };

        if !has_peers {
            return Ok(Vec::new());
        }

        // Step 2: Get actor state for message creation
        let (local_actors, known_actors) = {
            let actor_state = self.actor_state.read().await;
            (
                actor_state.local_actors.clone(),
                actor_state.known_actors.clone(),
            )
        };

        // Step 3: Select peers and create messages
        let tasks = {
            let gossip_state = self.gossip_state.lock().await;

            // Debug log all peers and their states
            debug!(
                "🔍 Gossip round: examining {} total peers",
                gossip_state.peers.len()
            );
            let current_time = current_timestamp();
            for (addr, peer_info) in &gossip_state.peers {
                let time_since_last_attempt = current_time.saturating_sub(peer_info.last_attempt);
                let retry_eligible = peer_info.failures >= self.config.max_peer_failures
                    && time_since_last_attempt > self.config.peer_retry_interval.as_secs();
                debug!(
                    peer = %addr,
                    failures = peer_info.failures,
                    last_attempt = peer_info.last_attempt,
                    time_since_last_attempt = time_since_last_attempt,
                    retry_interval = self.config.peer_retry_interval.as_secs(),
                    retry_eligible = retry_eligible,
                    max_failures = self.config.max_peer_failures,
                    "📊 Peer state in gossip round"
                );
            }

            let available_peers: Vec<SocketAddr> = gossip_state
                .peers
                .values()
                .filter(|peer| {
                    peer.failures < self.config.max_peer_failures
                        || (current_time - peer.last_attempt)
                            > self.config.peer_retry_interval.as_secs()
                })
                .map(|peer| peer.address)
                .collect();

            if available_peers.is_empty() {
                info!(
                    total_peers = gossip_state.peers.len(),
                    max_failures = self.config.max_peer_failures,
                    "❌ No available peers for gossip round"
                );
                return Ok(Vec::new());
            }

            debug!(
                available_count = available_peers.len(),
                "✅ Found {} available peers for gossip",
                available_peers.len()
            );

            // Select peers using adaptive fanout
            let adaptive_fanout = std::cmp::min(
                std::cmp::max(3, (available_peers.len() as f64).log2().ceil() as usize),
                self.config.max_gossip_peers,
            );

            let selected_peers: Vec<SocketAddr> = {
                let mut rng = rand::rng();
                let mut peers = available_peers;
                peers.shuffle(&mut rng);
                peers.into_iter().take(adaptive_fanout).collect()
            };

            debug!(
                selected_count = selected_peers.len(),
                selected_peers = ?selected_peers,
                "📮 Selected {} peers for gossip",
                selected_peers.len()
            );

            // Log if we're retrying any failed peers
            for peer in &selected_peers {
                if let Some(peer_info) = gossip_state.peers.get(peer) {
                    if peer_info.failures >= self.config.max_peer_failures {
                        let time_since_failure = peer_info
                            .last_failure_time
                            .map(|t| current_time - t)
                            .unwrap_or(0);
                        let time_since_last_attempt = current_time - peer_info.last_attempt;
                        info!(
                            peer = %peer,
                            failures = peer_info.failures,
                            time_since_failure_secs = time_since_failure,
                            time_since_last_attempt_secs = time_since_last_attempt,
                            retry_interval_secs = self.config.peer_retry_interval.as_secs(),
                            "🔄 GOSSIP RETRY: Including previously failed peer in gossip round"
                        );
                    }
                }
            }

            let mut tasks = Vec::new();
            for peer_addr in selected_peers {
                let peer_info = gossip_state
                    .peers
                    .get(&peer_addr)
                    .cloned()
                    .unwrap_or(PeerInfo {
                        address: peer_addr,
                        peer_address: None,
                        node_id: None,
                        failures: 0,
                        last_attempt: 0,
                        last_success: 0,
                        last_sequence: 0,
                        last_sent_sequence: 0,
                        consecutive_deltas: 0,
                        last_failure_time: None,
                    });

                let use_delta = self.should_use_delta_state(&gossip_state, &peer_info);

                let message = if use_delta {
                    match self
                        .create_delta_from_state(
                            &gossip_state,
                            &local_actors,
                            &known_actors,
                            peer_info.last_sequence,
                        )
                        .await
                    {
                        Ok(delta) => RegistryMessage::DeltaGossip { delta },
                        Err(err) => {
                            debug!(
                                peer = %peer_addr,
                                error = %err,
                                "failed to create delta, falling back to full sync"
                            );
                            self.create_full_sync_message_from_state(
                                &local_actors,
                                &known_actors,
                                current_sequence,
                            )
                            .await
                        }
                    }
                } else {
                    self.create_full_sync_message_from_state(
                        &local_actors,
                        &known_actors,
                        current_sequence,
                    )
                    .await
                };

                tasks.push(GossipTask {
                    peer_addr,
                    message,
                    current_sequence,
                });
            }

            tasks
        };

        debug!(
            task_count = tasks.len(),
            current_sequence = current_sequence,
            "prepared gossip round with atomic sequence/vector clock increment"
        );

        Ok(tasks)
    }

    /// Apply results from gossip tasks
    pub async fn apply_gossip_results(&self, results: Vec<GossipResult>) {
        let current_time = current_timestamp();

        for result in results {
            match result.outcome {
                Ok(response_opt) => {
                    // Success case - we successfully sent a message
                    // But with persistent connections, this doesn't mean the peer is alive
                    // We'll only reset failures when we receive messages from the peer
                    {
                        let mut gossip_state = self.gossip_state.lock().await;
                        if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                            // Don't reset failures here - only update attempt time
                            peer_info.last_attempt = current_time;
                            peer_info.last_sent_sequence = result.sent_sequence;

                            // Only update last_success if we're not in a failed state
                            if peer_info.failures < self.config.max_peer_failures {
                                peer_info.last_success = current_time;
                            }
                        }
                    }

                    // Record that this peer is active (we successfully communicated with it)
                    self.record_peer_activity(result.peer_addr).await;

                    // Process response if we got one
                    if let Some(response) = response_opt {
                        if let Err(err) = self
                            .handle_gossip_response(result.peer_addr, response)
                            .await
                        {
                            warn!(peer = %result.peer_addr, error = %err, "failed to handle gossip response");
                        }
                    }
                }
                Err(err) => {
                    // Failure case
                    warn!(peer = %result.peer_addr, error = %err, "failed to gossip to peer");
                    let mut gossip_state = self.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                        // Only increment failures if not already at max
                        // This prevents indefinite failure count growth
                        if peer_info.failures < self.config.max_peer_failures {
                            peer_info.failures += 1;
                            info!(peer = %result.peer_addr,
                                  new_failures = peer_info.failures,
                                  max_failures = self.config.max_peer_failures,
                                  "incremented peer failure count");

                            // Mark failure time if this puts us at max failures
                            if peer_info.failures == self.config.max_peer_failures {
                                peer_info.last_failure_time = Some(current_time);
                                info!(peer = %result.peer_addr, "peer reached max failures");
                            }
                        } else {
                            // Already at max failures, just update attempt time
                            debug!(peer = %result.peer_addr,
                                   failures = peer_info.failures,
                                   "peer already at max failures, not incrementing");
                        }
                        peer_info.last_attempt = current_time;
                    }
                }
            }
        }
    }

    /// Handle gossip response with vector clock updates
    pub async fn handle_gossip_response(
        &self,
        addr: SocketAddr,
        response: RegistryMessage,
    ) -> Result<()> {
        match response {
            RegistryMessage::DeltaGossipResponse { delta } => {
                info!(
                    peer = %addr,
                    sender = %delta.sender_peer_id,
                    changes = delta.changes.len(),
                    "📥 GOSSIP: Received delta gossip response"
                );

                let delta_sequence = delta.current_sequence;

                self.apply_delta(delta).await?;
                // Don't add peer here - peers are managed through handle_connection

                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    peer_info.last_sequence = delta_sequence;
                    peer_info.consecutive_deltas += 1;
                }
                gossip_state.delta_exchanges += 1;
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_peer_id,
                sequence,
                wall_clock_time,
            } => {
                info!(
                    peer = %addr,
                    sender = %sender_peer_id,
                    sequence = sequence,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "📥 GOSSIP: Received full sync response"
                );

                // Use the peer address we're connected to
                self.merge_full_sync(
                    local_actors.into_iter().collect(),
                    known_actors.into_iter().collect(),
                    addr,
                    sequence,
                    wall_clock_time,
                )
                .await;

                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    peer_info.consecutive_deltas = 0;
                    peer_info.last_sequence = sequence;
                }
                gossip_state.full_sync_exchanges += 1;
            }
            _ => {
                warn!(peer = %addr, "received unexpected message type in response");
            }
        }

        Ok(())
    }

    /// Merge incoming full sync data with vector clock-based conflict resolution
    pub async fn merge_full_sync(
        &self,
        remote_local: HashMap<String, RemoteActorLocation>,
        remote_known: HashMap<String, RemoteActorLocation>,
        sender_addr: SocketAddr,
        sequence: u64,
        _wall_clock_time: u64,
    ) {
        // Don't add peer here - peers are managed through handle_connection

        // Record comprehensive node activity

        // Check if we've already processed this or a newer sequence from this peer
        {
            let gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get(&sender_addr) {
                if sequence < peer_info.last_sequence {
                    debug!(
                        last_sequence = peer_info.last_sequence,
                        received_sequence = sequence,
                        "ignoring old gossip message"
                    );
                    return;
                }
            }
        }

        // Update peer sequence and vector clock
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr) {
                peer_info.last_sequence = std::cmp::max(peer_info.last_sequence, sequence);
            }
        }

        let mut new_actors = 0;
        let mut updated_actors = 0;
        let mut peer_actors = std::collections::HashSet::new();

        // Pre-compute all updates outside the lock to minimize lock hold time
        let mut updates_to_apply = Vec::new();

        // STEP 1: Read current state with read lock (fast, non-blocking)
        let (local_actors, known_actors) = {
            let actor_state = self.actor_state.read().await;
            (
                actor_state.local_actors.clone(),
                actor_state.known_actors.clone(),
            )
        };

        // STEP 2: Compute all updates outside any lock (fast, pure computation)
        // Process remote local actors
        for (name, location) in remote_local {
            peer_actors.insert(name.clone());
            if !local_actors.contains_key(&name) {
                match known_actors.get(&name) {
                    Some(existing_location) => {
                        // Use vector clock for causal ordering
                        match location
                            .vector_clock
                            .compare(&existing_location.vector_clock)
                        {
                            crate::ClockOrdering::After => {
                                // New location is causally after existing
                                updates_to_apply.push((name.clone(), location));
                                updated_actors += 1;
                            }
                            crate::ClockOrdering::Concurrent => {
                                // Concurrent updates - use deterministic hash-based tiebreaker
                                use std::hash::{Hash, Hasher};
                                let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
                                name.hash(&mut hasher1);
                                location.address.hash(&mut hasher1);
                                location.wall_clock_time.hash(&mut hasher1);
                                let hash1 = hasher1.finish();

                                let mut hasher2 = std::collections::hash_map::DefaultHasher::new();
                                name.hash(&mut hasher2);
                                existing_location.address.hash(&mut hasher2);
                                existing_location.wall_clock_time.hash(&mut hasher2);
                                let hash2 = hasher2.finish();

                                if hash1 > hash2
                                    || (hash1 == hash2
                                        && location.wall_clock_time
                                            > existing_location.wall_clock_time)
                                {
                                    updates_to_apply.push((name.clone(), location));
                                    updated_actors += 1;
                                }
                            }
                            _ => {} // Keep existing for Before or Equal
                        }
                    }
                    None => {
                        updates_to_apply.push((name.clone(), location));
                        new_actors += 1;
                    }
                }
            }
        }

        // Process remote known actors
        for (name, location) in remote_known {
            if local_actors.contains_key(&name) {
                continue; // Don't override local actors
            }

            match known_actors.get(&name) {
                Some(existing_location) => {
                    // Use vector clock for causal ordering
                    match location
                        .vector_clock
                        .compare(&existing_location.vector_clock)
                    {
                        crate::ClockOrdering::After => {
                            // New location is causally after existing
                            updates_to_apply.push((name, location));
                            updated_actors += 1;
                        }
                        crate::ClockOrdering::Concurrent => {
                            // Concurrent updates - use deterministic hash-based tiebreaker
                            use std::hash::{Hash, Hasher};
                            let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
                            name.hash(&mut hasher1);
                            location.address.hash(&mut hasher1);
                            location.wall_clock_time.hash(&mut hasher1);
                            let hash1 = hasher1.finish();

                            let mut hasher2 = std::collections::hash_map::DefaultHasher::new();
                            name.hash(&mut hasher2);
                            existing_location.address.hash(&mut hasher2);
                            existing_location.wall_clock_time.hash(&mut hasher2);
                            let hash2 = hasher2.finish();

                            if hash1 > hash2
                                || (hash1 == hash2
                                    && location.wall_clock_time > existing_location.wall_clock_time)
                            {
                                updates_to_apply.push((name, location));
                                updated_actors += 1;
                            }
                        }
                        _ => {} // Keep existing for Before or Equal
                    }
                }
                None => {
                    updates_to_apply.push((name, location));
                    new_actors += 1;
                }
            }
        }

        // STEP 3: Apply all updates with write lock (fast, minimal work under lock)
        if !updates_to_apply.is_empty() {
            let mut actor_state = self.actor_state.write().await;
            for (name, location) in &updates_to_apply {
                // Also ensure the peer's NodeId is in the gossip state for TLS
                if let Ok(addr) = location.address.parse::<SocketAddr>() {
                    // Convert PeerId to NodeId for TLS
                    let node_id = Some(location.peer_id.to_node_id());
                    if node_id.is_some() {
                        // This will be used later when we need to connect to this peer for TLS
                        self.add_peer_with_node_id(addr, node_id).await;
                        debug!(actor = %name, peer_addr = %addr, "Added NodeId to gossip state for actor's host");
                    }
                }
                actor_state
                    .known_actors
                    .insert(name.clone(), location.clone());
            }
        }

        // Update peer-to-actors mapping for failure tracking
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peer_to_actors
                .insert(sender_addr, peer_actors.clone());
        }

        debug!(
            new_actors = new_actors,
            updated_actors = updated_actors,
            peer = %sender_addr,
            peer_actor_count = peer_actors.len(),
            "merged gossip data using vector clock conflict resolution"
        );
    }

    /// Clean up stale actor entries (using wall clock for TTL)
    pub async fn cleanup_stale_actors(&self) {
        let now = current_timestamp();
        let ttl_secs = self.config.actor_ttl.as_secs();

        // Clean up stale known actors (using wall clock time for TTL)
        {
            let mut actor_state = self.actor_state.write().await;
            let before_count = actor_state.known_actors.len();
            actor_state
                .known_actors
                .retain(|_, location| now - location.wall_clock_time < ttl_secs);

            let removed = before_count - actor_state.known_actors.len();
            if removed > 0 {
                info!(removed_count = removed, "cleaned up stale actor entries");
            }
        }

        // Clean up old delta history (using wall clock)
        {
            let mut gossip_state = self.gossip_state.lock().await;
            let history_ttl = self.config.actor_ttl.as_secs() * 2;
            gossip_state
                .delta_history
                .retain(|delta| now - delta.wall_clock_time < history_ttl);
        }

        // Enforce bounds on data structures
        self.enforce_bounds().await;

        // Clean up connection pool
        {
            let mut connection_pool = self.connection_pool.lock().await;
            connection_pool.cleanup_stale_connections();
        }
    }

    /// Clean up actors from peers that have been disconnected for longer than dead_peer_timeout
    /// IMPORTANT: We keep the peer itself to allow reconnection, only clean up their actors
    pub async fn cleanup_dead_peers(&self) {
        let current_time = current_timestamp();
        let dead_peer_timeout_secs = self.config.dead_peer_timeout.as_secs();

        let peers_to_cleanup: Vec<SocketAddr> = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(_, info)| {
                    // Check if peer has been disconnected for too long
                    info.failures >= self.config.max_peer_failures
                        && info.last_failure_time.is_some_and(|failure_time| {
                            (current_time - failure_time) > dead_peer_timeout_secs
                        })
                })
                .map(|(addr, _)| *addr)
                .collect()
        };

        if !peers_to_cleanup.is_empty() {
            // IMPORTANT: Always acquire locks in consistent order to prevent deadlocks
            // Order: actor_state before gossip_state
            let mut actor_state = self.actor_state.write().await;
            let mut gossip_state = self.gossip_state.lock().await;

            for peer_addr in &peers_to_cleanup {
                // IMPORTANT: We do NOT remove the peer itself - it stays in the peer list
                // This allows us to reconnect when the peer comes back online

                // Remove peer's actors from known_actors to free memory
                if let Some(actor_names) = gossip_state.peer_to_actors.get(peer_addr).cloned() {
                    for actor_name in &actor_names {
                        actor_state.known_actors.remove(actor_name);
                    }
                    gossip_state.peer_to_actors.remove(peer_addr);

                    info!(
                        peer = %peer_addr,
                        actors_removed = actor_names.len(),
                        timeout_minutes = dead_peer_timeout_secs / 60,
                        "cleaned up actors from long-disconnected peer (peer retained for reconnection)"
                    );
                }

                // Clean up health reports but keep the peer entry
                gossip_state.peer_health_reports.remove(peer_addr);
                gossip_state.pending_peer_failures.remove(peer_addr);
            }
        }
    }

    /// Run vector clock garbage collection to prevent unbounded growth
    pub async fn run_vector_clock_gc(&self) {
        let (active_nodes, dead_nodes_with_timeout) = {
            // Collect all active node IDs and dead nodes with timeout info
            let gossip_state = self.gossip_state.lock().await;
            let mut active = HashSet::new();
            let mut dead = HashMap::new();

            // Add our own node ID - always active
            active.insert(self.peer_id.to_node_id());

            // Add all known peer node IDs based on their status
            let current_time = current_timestamp();
            for peer_info in gossip_state.peers.values() {
                if let Some(node_id) = &peer_info.node_id {
                    if peer_info.failures < self.config.max_peer_failures {
                        // Peer is healthy - keep their entries
                        active.insert(*node_id);
                    } else if let Some(failure_time) = peer_info.last_failure_time {
                        // Peer is failed - check how long it's been dead
                        let time_since_failure = current_time - failure_time;
                        let retention_secs = self.config.vector_clock_retention_period.as_secs();

                        if time_since_failure > retention_secs {
                            // Dead for longer than retention period - can be GC'd
                            dead.insert(*node_id, time_since_failure);
                        } else {
                            // Dead but within retention period - keep their entries
                            active.insert(*node_id);
                        }
                    } else {
                        // Failed but no failure time recorded - keep to be safe
                        active.insert(*node_id);
                    }
                }
            }

            (active, dead)
        };

        // Run GC on all actor vector clocks
        let mut gc_count = 0;
        let mut largest_clock_size = 0;
        {
            let actor_state = self.actor_state.write().await;

            // GC local actors
            for location in actor_state.local_actors.values() {
                let before_size = location.vector_clock.len();
                location.vector_clock.gc_old_nodes(&active_nodes);
                let after_size = location.vector_clock.len();
                if before_size > after_size {
                    gc_count += before_size - after_size;
                }
                largest_clock_size = largest_clock_size.max(after_size);
            }

            // GC known actors
            for location in actor_state.known_actors.values() {
                let before_size = location.vector_clock.len();
                location.vector_clock.gc_old_nodes(&active_nodes);
                let after_size = location.vector_clock.len();
                if before_size > after_size {
                    gc_count += before_size - after_size;
                }
                largest_clock_size = largest_clock_size.max(after_size);
            }
        }

        if gc_count > 0 {
            info!(
                entries_removed = gc_count,
                active_nodes = active_nodes.len(),
                dead_nodes_removed = dead_nodes_with_timeout.len(),
                largest_clock_size = largest_clock_size,
                "vector clock garbage collection completed"
            );

            // Log details about removed nodes
            for (node_id, time_dead) in dead_nodes_with_timeout {
                debug!(
                    node_id = ?node_id,
                    dead_for_secs = time_dead,
                    "removed dead node from vector clocks"
                );
            }
        }

        // Warn if clocks are still large after GC
        if largest_clock_size > 1000 {
            warn!(
                largest_size = largest_clock_size,
                active_nodes = active_nodes.len(),
                "Vector clocks still large after GC. Consider shorter retention period or investigating node churn."
            );
        }
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        debug!("shutting down gossip registry");

        // Set shutdown flag
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.shutdown = true;
        }

        // Close all connections in the pool
        {
            let mut connection_pool = self.connection_pool.lock().await;
            connection_pool.close_all_connections();
        }

        // Clear actor state
        {
            let mut actor_state = self.actor_state.write().await;
            actor_state.local_actors.clear();
            actor_state.known_actors.clear();
        }

        // Clear gossip state
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.pending_changes.clear();
            gossip_state.urgent_changes.clear();
            gossip_state.delta_history.clear();
            gossip_state.peers.clear();
        }

        debug!("gossip registry shutdown complete");
    }

    /// Get a connection handle for direct communication (for performance testing)
    pub async fn get_connection(
        &self,
        addr: SocketAddr,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        self.connection_pool.lock().await.get_connection(addr).await
    }

    /// Get a connection handle directly from the pool without mutex lock
    /// Only works for already established connections
    pub fn get_connection_direct(
        &self,
        addr: SocketAddr,
    ) -> Option<crate::connection_pool::ConnectionHandle> {
        // Best-effort: avoid await by using try_lock; return None if busy or not connected.
        self.connection_pool
            .try_lock()
            .ok()
            .and_then(|mut pool| pool.get_existing_connection(addr))
    }

    pub async fn is_shutdown(&self) -> bool {
        let gossip_state = self.gossip_state.lock().await;
        gossip_state.shutdown
    }

    /// Record that a peer (by address) is active
    pub async fn record_peer_activity(&self, peer_addr: SocketAddr) {
        let gossip_state = self.gossip_state.lock().await;

        // Peer activity is now tracked directly by address
        if let Some(_peer_info) = gossip_state.peers.get(&peer_addr) {
            // Activity is recorded through last_success/last_attempt timestamps
        }
    }

    /// Handle peer connection failure - start consensus process
    /// This is called for socket disconnections (not timeouts)
    pub async fn handle_peer_connection_failure(&self, failed_peer_addr: SocketAddr) -> Result<()> {
        info!(
            failed_peer = %failed_peer_addr,
            "socket disconnection detected, marking connection as failed (actors remain available)"
        );

        let current_time = current_timestamp();

        // IMMEDIATELY mark the connection as failed and remove from pool
        {
            let pool = self.connection_pool.lock().await;
            if pool.has_connection(&failed_peer_addr) {
                pool.remove_connection(failed_peer_addr);
                info!(addr = %failed_peer_addr, "removed disconnected connection from pool");
            }
        }

        // IMMEDIATELY mark peer as failed in our local state
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&failed_peer_addr) {
                peer_info.failures = self.config.max_peer_failures;
                peer_info.last_failure_time = Some(current_time);
                peer_info.last_attempt = current_time; // Update last_attempt so retry happens after interval
                info!(
                    peer = %failed_peer_addr,
                    retry_after_secs = self.config.peer_retry_interval.as_secs(),
                    "marked peer as disconnected in local state, will retry after interval"
                );
            }
        }

        // Now start consensus process for actor invalidation
        {
            let mut gossip_state = self.gossip_state.lock().await;

            // Record our own health report
            let our_report = PeerHealthStatus {
                is_alive: false,
                last_contact: current_time,
                failure_count: 1,
            };

            gossip_state
                .peer_health_reports
                .entry(failed_peer_addr)
                .or_insert_with(HashMap::new)
                .insert(self.bind_addr, our_report);

            // If we don't have a pending failure, create one
            if let std::collections::hash_map::Entry::Vacant(e) =
                gossip_state.pending_peer_failures.entry(failed_peer_addr)
            {
                let pending = PendingFailure {
                    first_detected: current_time,
                    consensus_deadline: current_time + 5, // 5 second timeout
                    query_sent: false,
                };
                e.insert(pending);

                info!(
                    failed_peer = %failed_peer_addr,
                    deadline = current_time + 5,
                    "created pending failure record, waiting for consensus on actor invalidation"
                );
            }
        }

        // Don't query immediately - give other nodes time to detect their own disconnections
        // Schedule the query for 100ms later to avoid race conditions
        let registry = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            info!(
                failed_peer = %failed_peer_addr,
                "delayed query: now checking peer consensus after 100ms"
            );

            // Query other peers for their view of the failed peer
            // This is to determine if we should invalidate actors, not the connection status
            if let Err(e) = registry.query_peer_health_consensus(failed_peer_addr).await {
                warn!(error = %e, "failed to query peer health consensus");
            }
        });

        Ok(())
    }

    /// Handle a peer connection failure by peer ID instead of address
    pub async fn handle_peer_connection_failure_by_peer_id(
        &self,
        failed_peer_id: &crate::PeerId,
    ) -> Result<()> {
        info!(
            failed_peer_id = %failed_peer_id,
            "node disconnection detected by ID, marking connection as failed (actors remain available)"
        );

        // First, find the peer address from the node ID
        let failed_peer_addr = {
            let pool = self.connection_pool.lock().await;

            // Try to find the address from our node ID mapping
            let addr_opt = pool.peer_id_to_addr.get(failed_peer_id).map(|entry| *entry);

            match addr_opt {
                Some(addr) => addr,
                None => {
                    warn!(
                        peer_id = %failed_peer_id,
                        "cannot find address for failed peer ID - may have already been removed"
                    );
                    return Ok(());
                }
            }
        };

        let current_time = current_timestamp();

        // IMMEDIATELY remove the connection from pool
        // remove_connection handles both address and node ID mappings
        {
            let pool = self.connection_pool.lock().await;

            if pool.has_connection(&failed_peer_addr) {
                pool.remove_connection(failed_peer_addr);
                info!(
                    addr = %failed_peer_addr,
                    node_id = %failed_peer_id,
                    "removed disconnected connection from pool (both address and node ID mappings)"
                );
            } else {
                info!(
                    addr = %failed_peer_addr,
                    node_id = %failed_peer_id,
                    "connection already removed from pool"
                );
            }

            info!(
                node_id = %failed_peer_id,
                addr = %failed_peer_addr,
                connections_remaining = pool.connection_count(),
                "connection cleanup complete"
            );
        }

        // IMMEDIATELY mark peer as failed in our local state
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&failed_peer_addr) {
                peer_info.failures = self.config.max_peer_failures;
                peer_info.last_failure_time = Some(current_time);
                peer_info.last_attempt = current_time; // Update last_attempt so retry happens after interval
                info!(
                    peer = %failed_peer_addr,
                    node_id = %failed_peer_id,
                    retry_after_secs = self.config.peer_retry_interval.as_secs(),
                    "marked peer as disconnected in local state, will retry after interval"
                );
            }
        }

        // Now start consensus process for actor invalidation (same as address-based method)
        {
            let mut gossip_state = self.gossip_state.lock().await;

            // Record our own health report
            let our_report = PeerHealthStatus {
                is_alive: false,
                last_contact: current_time,
                failure_count: 1,
            };

            gossip_state
                .peer_health_reports
                .entry(failed_peer_addr)
                .or_insert_with(HashMap::new)
                .insert(self.bind_addr, our_report);

            // If we don't have a pending failure, create one
            if let std::collections::hash_map::Entry::Vacant(e) =
                gossip_state.pending_peer_failures.entry(failed_peer_addr)
            {
                let pending = PendingFailure {
                    first_detected: current_time,
                    consensus_deadline: current_time + 5, // 5 second timeout
                    query_sent: false,
                };
                e.insert(pending);

                info!(
                    failed_peer = %failed_peer_addr,
                    node_id = %failed_peer_id,
                    deadline = current_time + 5,
                    "created pending failure record, waiting for consensus on actor invalidation"
                );
            }
        }

        // Don't query immediately - give other nodes time to detect their own disconnections
        // Schedule the query for 100ms later to avoid race conditions
        let registry = self.clone();
        let failed_addr_for_spawn = failed_peer_addr;
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            info!(
                failed_peer = %failed_addr_for_spawn,
                "delayed query: now checking peer consensus after 100ms"
            );

            // Query other peers for their view of the failed peer
            // This is to determine if we should invalidate actors, not the connection status
            if let Err(e) = registry
                .query_peer_health_consensus(failed_addr_for_spawn)
                .await
            {
                warn!(error = %e, "failed to query peer health consensus");
            }
        });

        Ok(())
    }

    /// Query other peers for their view of a potentially failed peer
    async fn query_peer_health_consensus(&self, target_peer: SocketAddr) -> Result<()> {
        // First check if we still have an active connection
        let has_active_connection = {
            let pool = self.connection_pool.lock().await;
            pool.has_connection(&target_peer)
        };

        if has_active_connection {
            error!(
                target_peer = %target_peer,
                "🚨 CONSENSUS WARNING: Starting consensus query for a peer we still have an ACTIVE SOCKET CONNECTION to! This indicates a logic error."
            );
        }

        let query_msg = RegistryMessage::PeerHealthQuery {
            sender: self.peer_id.clone(),
            target_peer: target_peer.to_string(),
            timestamp: current_timestamp(),
        };

        // Get list of healthy peers to query
        let peers_to_query = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(addr, info)| {
                    **addr != target_peer && // Don't query the target
                    info.failures < self.config.max_peer_failures // Only query healthy peers
                })
                .map(|(addr, _)| *addr)
                .collect::<Vec<_>>()
        };

        info!(
            target_peer = %target_peer,
            querying_peers = peers_to_query.len(),
            "querying peers for health consensus"
        );

        // Send queries to all healthy peers
        for peer in peers_to_query {
            if let Ok(data) = rkyv::to_bytes::<rkyv::rancor::Error>(&query_msg) {
                // Try to send through existing connection
                let mut pool = self.connection_pool.lock().await;
                if let Ok(conn) = pool.get_connection(peer).await {
                    // Create message buffer with length header using buffer pool
                    let buffer = pool.create_message_buffer(&data);
                    let _ = conn.send_data(buffer).await;
                }
            }
        }

        // Mark query as sent
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(pending) = gossip_state.pending_peer_failures.get_mut(&target_peer) {
                pending.query_sent = true;
            }
        }

        Ok(())
    }

    /// Check if we have consensus about any pending peer failures
    pub async fn check_peer_consensus(&self) {
        let current_time = current_timestamp();

        {
            let mut gossip_state = self.gossip_state.lock().await;
            let mut completed_failures = Vec::new();

            for (peer_addr, pending) in &gossip_state.pending_peer_failures {
                // Check if we've reached the deadline or have enough reports
                let reports = gossip_state.peer_health_reports.get(peer_addr);
                let total_peers = gossip_state.peers.len();

                if let Some(reports) = reports {
                    let alive_count = reports.values().filter(|r| r.is_alive).count();
                    let dead_count = reports.values().filter(|r| !r.is_alive).count();
                    let total_reports = reports.len();

                    info!(
                        peer = %peer_addr,
                        alive_votes = alive_count,
                        dead_votes = dead_count,
                        total_reports = total_reports,
                        total_peers = total_peers,
                        "checking peer consensus"
                    );

                    // We NO LONGER remove actors even if consensus says the peer is dead
                    // The actors remain configured and available for when the node reconnects

                    if total_peers <= 1 {
                        // Only us and the failed peer
                        completed_failures.push(*peer_addr);
                        info!(
                            peer = %peer_addr,
                            "2-node cluster: peer is disconnected, keeping actors for potential reconnection"
                        );
                    } else {
                        // Multiple nodes - check consensus
                        let majority = total_peers.div_ceil(2);

                        if dead_count >= majority || current_time >= pending.consensus_deadline {
                            completed_failures.push(*peer_addr);

                            // Check if we have an active connection to this peer
                            let has_active_connection = {
                                let pool = self.connection_pool.lock().await;
                                pool.has_connection(peer_addr)
                            };

                            if dead_count > alive_count {
                                // Majority says dead
                                if has_active_connection {
                                    error!(
                                        peer = %peer_addr,
                                        dead_votes = dead_count,
                                        alive_votes = alive_count,
                                        "🚨 CONSENSUS CONFLICT: Majority says peer is dead but we have an ACTIVE SOCKET CONNECTION! This should not happen!"
                                    );
                                } else {
                                    info!(
                                        peer = %peer_addr,
                                        dead_votes = dead_count,
                                        alive_votes = alive_count,
                                        "consensus: majority says peer is dead, but keeping actors for reconnection"
                                    );
                                }
                            } else if alive_count > dead_count {
                                // Majority says alive
                                info!(
                                    peer = %peer_addr,
                                    alive_votes = alive_count,
                                    dead_votes = dead_count,
                                    "consensus: peer is alive elsewhere, keeping actors"
                                );
                            } else {
                                // Tie or timeout
                                if has_active_connection {
                                    error!(
                                        peer = %peer_addr,
                                        "🚨 CONSENSUS CONFLICT: Consensus timeout/tie but we have an ACTIVE SOCKET CONNECTION!"
                                    );
                                } else {
                                    info!(
                                        peer = %peer_addr,
                                        "consensus timeout or tie, keeping actors"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Remove completed failures
            for peer in completed_failures {
                gossip_state.pending_peer_failures.remove(&peer);
                gossip_state.peer_health_reports.remove(&peer);
            }
        }

        // We no longer invalidate actors based on consensus
        // The connection state is already updated, actors remain available
    }

    /// Deduplicate changes, keeping only the most recent change for each actor
    pub fn deduplicate_changes(changes: Vec<RegistryChange>) -> Vec<RegistryChange> {
        let mut actor_changes: HashMap<String, RegistryChange> = HashMap::new();

        for change in changes {
            let actor_name = Self::get_change_actor_name(&change);
            // Simply keep the last change for each actor
            actor_changes.insert(actor_name, change);
        }

        actor_changes.into_values().collect()
    }

    /// Extract the actor name from a registry change
    fn get_change_actor_name(change: &RegistryChange) -> String {
        match change {
            RegistryChange::ActorAdded { name, .. } | RegistryChange::ActorRemoved { name, .. } => {
                name.clone()
            }
        }
    }

    /// Enforce bounds on gossip state data structures to prevent unbounded growth
    async fn enforce_bounds(&self) {
        let mut gossip_state = self.gossip_state.lock().await;

        // Apply vector clock compaction to all changes before bounds enforcement
        let max_clock_size = self.config.max_vector_clock_size;

        // Compact vector clocks in pending changes
        for change in &gossip_state.pending_changes {
            match change {
                RegistryChange::ActorAdded { location, .. } => {
                    if location.vector_clock.len() > max_clock_size {
                        location.vector_clock.compact(max_clock_size);
                    }
                }
                RegistryChange::ActorRemoved { vector_clock, .. } => {
                    if vector_clock.len() > max_clock_size {
                        vector_clock.compact(max_clock_size);
                    }
                }
            }
        }

        // Compact vector clocks in urgent changes
        for change in &gossip_state.urgent_changes {
            match change {
                RegistryChange::ActorAdded { location, .. } => {
                    if location.vector_clock.len() > max_clock_size {
                        location.vector_clock.compact(max_clock_size);
                    }
                }
                RegistryChange::ActorRemoved { vector_clock, .. } => {
                    if vector_clock.len() > max_clock_size {
                        vector_clock.compact(max_clock_size);
                    }
                }
            }
        }

        // Compact vector clocks in delta history
        for delta in &gossip_state.delta_history {
            for change in &delta.changes {
                match change {
                    RegistryChange::ActorAdded { location, .. } => {
                        if location.vector_clock.len() > max_clock_size {
                            location.vector_clock.compact(max_clock_size);
                        }
                    }
                    RegistryChange::ActorRemoved { vector_clock, .. } => {
                        if vector_clock.len() > max_clock_size {
                            vector_clock.compact(max_clock_size);
                        }
                    }
                }
            }
        }

        // Bound pending changes
        let max_pending = 1000;
        if gossip_state.pending_changes.len() > max_pending {
            debug!(
                "Trimming pending changes from {} to {}",
                gossip_state.pending_changes.len(),
                max_pending
            );
            gossip_state.pending_changes.truncate(max_pending);
        }

        // Bound urgent changes (smaller limit since these are high priority)
        let max_urgent = 100;
        if gossip_state.urgent_changes.len() > max_urgent {
            debug!(
                "Trimming urgent changes from {} to {}",
                gossip_state.urgent_changes.len(),
                max_urgent
            );
            gossip_state.urgent_changes.truncate(max_urgent);
        }

        // Bound delta history
        if gossip_state.delta_history.len() > self.config.max_delta_history {
            let excess = gossip_state.delta_history.len() - self.config.max_delta_history;
            debug!("Trimming delta history by {} entries", excess);
            gossip_state.delta_history.drain(0..excess);
        }

        // Bound peers list
        let max_peers = 1000;
        if gossip_state.peers.len() > max_peers {
            debug!(
                "Trimming peers from {} to {}",
                gossip_state.peers.len(),
                max_peers
            );
            let _current_time = current_timestamp();
            let mut peers_by_age: Vec<_> = gossip_state
                .peers
                .iter()
                .map(|(addr, peer)| (*addr, peer.last_success))
                .collect();
            peers_by_age.sort_by_key(|(_, last_success)| *last_success);

            let to_remove = gossip_state.peers.len() - max_peers;
            let addrs_to_remove: Vec<_> = peers_by_age
                .iter()
                .take(to_remove)
                .map(|(addr, _)| *addr)
                .collect();
            for addr in addrs_to_remove {
                gossip_state.peers.remove(&addr);
            }
        }
    }

    /// Trigger immediate gossip for urgent changes - optimized for speed
    pub async fn trigger_immediate_gossip(&self) -> Result<()> {
        if !self.config.immediate_propagation_enabled {
            return Ok(());
        }

        // Fast path: get urgent changes and peers in one go
        let (urgent_changes, critical_peers) = {
            let mut gossip_state = self.gossip_state.lock().await;

            if gossip_state.urgent_changes.is_empty() {
                return Ok(());
            }

            // Take all urgent changes for immediate propagation (avoid clone)
            let changes = std::mem::take(&mut gossip_state.urgent_changes);

            // Get healthy peers quickly - just take the first few healthy ones
            let peers: Vec<_> = gossip_state
                .peers
                .iter()
                .filter(|(_, peer)| peer.failures < self.config.max_peer_failures)
                .take(self.config.urgent_gossip_fanout)
                .map(|(addr, _)| *addr)
                .collect();

            (changes, peers)
        };

        if urgent_changes.is_empty() {
            return Ok(());
        }

        let urgent_changes_for_retry = urgent_changes.clone();

        if critical_peers.is_empty() {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.pending_changes.extend(urgent_changes);
            return Ok(());
        }

        for change in &urgent_changes {
            match change {
                RegistryChange::ActorAdded {
                    name,
                    location,
                    priority,
                    ..
                } => {
                    error!(
                        "  ➕ IMMEDIATE: Adding actor {} at {} (priority: {:?})",
                        name, location.address, priority
                    );
                }
                RegistryChange::ActorRemoved { name, priority, .. } => {
                    error!(
                        "  ➖ IMMEDIATE: Removing actor {} (priority: {:?})",
                        name, priority
                    );
                }
            }
        }

        // Store count before moving
        let urgent_changes_count = urgent_changes.len();

        let wall_clock_time = current_timestamp(); // For debugging/monitoring only
        let precise_timing_nanos = crate::current_timestamp_nanos(); // High precision timing

        // Serialize message(s), chunking if we exceed max_message_size
        let serialization_start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut serialized_messages: Vec<Arc<rkyv::util::AlignedVec>> = Vec::new();
        let mut current_changes: Vec<RegistryChange> = Vec::new();

        for change in urgent_changes {
            current_changes.push(change);

            let message = RegistryMessage::DeltaGossip {
                delta: RegistryDelta {
                    sender_peer_id: self.peer_id.clone(),
                    since_sequence: 0,   // Not used for immediate gossip
                    current_sequence: 0, // Not used for immediate gossip
                    changes: current_changes.clone(),
                    wall_clock_time,
                    precise_timing_nanos,
                },
            };
            let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&message)?;

            if serialized.len() > self.config.max_message_size && current_changes.len() > 1 {
                let last = current_changes.pop().unwrap();
                let chunk_message = RegistryMessage::DeltaGossip {
                    delta: RegistryDelta {
                        sender_peer_id: self.peer_id.clone(),
                        since_sequence: 0,
                        current_sequence: 0,
                        changes: current_changes.clone(),
                        wall_clock_time,
                        precise_timing_nanos,
                    },
                };
                let chunk_serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk_message)?;
                serialized_messages.push(Arc::new(chunk_serialized));
                current_changes.clear();
                current_changes.push(last);
            } else if serialized.len() > self.config.max_message_size {
                warn!(
                    size = serialized.len(),
                    max = self.config.max_message_size,
                    "Immediate gossip change exceeds max message size; sending as single chunk"
                );
                serialized_messages.push(Arc::new(serialized));
                current_changes.clear();
            }
        }

        if !current_changes.is_empty() {
            let message = RegistryMessage::DeltaGossip {
                delta: RegistryDelta {
                    sender_peer_id: self.peer_id.clone(),
                    since_sequence: 0,
                    current_sequence: 0,
                    changes: current_changes,
                    wall_clock_time,
                    precise_timing_nanos,
                },
            };
            let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&message)?;
            serialized_messages.push(Arc::new(serialized));
        }

        let serialization_end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let serialization_duration_ms =
            (serialization_end - serialization_start) as f64 / 1_000_000.0;

        if serialized_messages.len() > 1 {
            warn!(
                chunks = serialized_messages.len(),
                max = self.config.max_message_size,
                "Immediate gossip split into multiple chunks to honor max message size"
            );
        }

        // Log immediate propagation with timing
        error!(
            "🚀 IMMEDIATE GOSSIP: Broadcasting {} urgent changes to {} peers (serialization: {:.3}ms, chunks: {})",
            urgent_changes_count,
            critical_peers.len(),
            serialization_duration_ms,
            serialized_messages.len()
        );

        // Pre-establish all connections and pre-create buffers (single lock acquisition)
        let peer_connections_buffers: Vec<(
            SocketAddr,
            crate::connection_pool::ConnectionHandle,
            Vec<Vec<u8>>,
        )> = {
            let mut pool_guard = self.connection_pool.lock().await;
            let mut connections_buffers = Vec::new();

            for peer_addr in &critical_peers {
                if let Ok(conn) = pool_guard.get_connection(*peer_addr).await {
                    let mut buffers = Vec::new();
                    for serialized_data in &serialized_messages {
                        // Create message buffer with length header using buffer pool
                        let buffer = pool_guard.create_message_buffer(serialized_data);
                        buffers.push(buffer);
                    }
                    connections_buffers.push((*peer_addr, conn, buffers));
                }
            }

            connections_buffers
        };

        if peer_connections_buffers.is_empty() {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state
                .pending_changes
                .extend(urgent_changes_for_retry);
            return Ok(());
        }

        // Send to all peers concurrently with pre-established connections and buffers
        let mut join_handles = Vec::new();

        for (peer_addr, conn, buffers) in peer_connections_buffers {
            let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
                // Measure pure network send time
                let send_start = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();

                for buffer in buffers {
                    conn.send_data(buffer).await?;
                }

                let send_end = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let send_time_ms = (send_end - send_start) as f64 / 1_000_000.0;

                debug!(peer = %peer_addr, send_time_ms = send_time_ms, "Network send completed");

                Ok(())
            });

            join_handles.push(handle);
        }

        // Wait for all sends to complete
        for handle in join_handles {
            if let Err(e) = handle.await {
                warn!("immediate gossip task failed: {}", e);
            }
        }

        Ok(())
    }

    /// Immediately invalidate all actors from a failed peer
    pub async fn invalidate_peer_actors(&self, failed_peer_addr: SocketAddr) -> Result<()> {
        info!(failed_peer = %failed_peer_addr, "invalidating actors from failed peer");

        // Get the list of actors to invalidate from this peer
        let (actors_to_remove, should_trigger_immediate) = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Get actors belonging to the failed peer
            let actors_to_remove = gossip_state
                .peer_to_actors
                .remove(&failed_peer_addr)
                .unwrap_or_default();

            if actors_to_remove.is_empty() {
                debug!(failed_peer = %failed_peer_addr, "no actors to invalidate");
                return Ok(());
            }

            // Create removal changes for each actor with proper vector clocks
            let mut removal_changes = Vec::new();

            // We need to get the current vector clocks for these actors
            let actor_state = self.actor_state.read().await;
            for actor_name in &actors_to_remove {
                // Get the existing vector clock for this actor if it exists
                let removal_clock = if let Some(location) = actor_state.known_actors.get(actor_name)
                {
                    // Use the actor's current vector clock and increment it
                    let clock = location.vector_clock.clone();
                    clock.increment(self.peer_id.to_node_id());
                    clock
                } else {
                    // If actor not found (shouldn't happen), create a new clock with our increment
                    let clock = crate::VectorClock::new();
                    clock.increment(self.peer_id.to_node_id());
                    clock
                };

                let change = RegistryChange::ActorRemoved {
                    name: actor_name.clone(),
                    vector_clock: removal_clock,
                    removing_node_id: self.peer_id.to_node_id(),
                    priority: RegistrationPriority::Immediate, // Node failures are always immediate
                };
                removal_changes.push(change);
            }
            drop(actor_state); // Release the read lock before acquiring write lock

            // Add all removal changes to urgent queue
            gossip_state.urgent_changes.extend(removal_changes);

            (actors_to_remove, !gossip_state.urgent_changes.is_empty())
        };

        // Remove actors from known_actors (they shouldn't be in local_actors if they're from a failed node)
        let removed_count = {
            let mut actor_state = self.actor_state.write().await;
            let mut removed = 0;

            for actor_name in &actors_to_remove {
                if actor_state.known_actors.remove(actor_name).is_some() {
                    removed += 1;
                }
                // Also remove from local_actors if somehow present (defensive)
                if actor_state.local_actors.remove(actor_name).is_some() {
                    removed += 1;
                }
            }
            removed
        };

        info!(
            failed_peer = %failed_peer_addr,
            actors_removed = removed_count,
            actors_invalidated = ?actors_to_remove,
            "PEER_FAILURE_INVALIDATION"
        );

        // Trigger immediate gossip to propagate the failures
        if should_trigger_immediate {
            if let Err(err) = self.trigger_immediate_gossip().await {
                warn!(error = %err, "failed to trigger immediate gossip for node failure");
            }
        }

        Ok(())
    }

    /// Start assembling a streamed message
    pub async fn start_stream_assembly(&self, header: crate::StreamHeader) {
        let mut assemblies = self.stream_assemblies.lock().await;
        assemblies.insert(
            header.stream_id,
            StreamAssembly {
                header,
                chunks: std::collections::BTreeMap::new(),
                received_bytes: 0,
            },
        );
        debug!(stream_id = header.stream_id, "Started stream assembly");
    }

    /// Add a chunk to stream assembly
    pub async fn add_stream_chunk(&self, header: crate::StreamHeader, chunk_data: Vec<u8>) {
        let mut assemblies = self.stream_assemblies.lock().await;
        if let Some(assembly) = assemblies.get_mut(&header.stream_id) {
            assembly.received_bytes += chunk_data.len();
            assembly.chunks.insert(header.chunk_index, chunk_data);
            // debug!(
            //     stream_id = header.stream_id,
            //     chunk_index = header.chunk_index,
            //     received_bytes = assembly.received_bytes,
            //     "Added chunk to stream assembly"
            // );
        } else {
            warn!(
                stream_id = header.stream_id,
                "Stream assembly not found for chunk"
            );
        }
    }

    /// Complete stream assembly and return the complete message
    pub async fn complete_stream_assembly(&self, stream_id: u64) -> Option<Vec<u8>> {
        let mut assemblies = self.stream_assemblies.lock().await;
        if let Some(assembly) = assemblies.remove(&stream_id) {
            // Verify we have all chunks with proper gap detection
            let expected_chunks = assembly
                .header
                .total_size
                .div_ceil(crate::connection_pool::STREAM_CHUNK_SIZE as u64);

            // ✅ PROPER: Check both count and verify all chunk indices are present
            if assembly.chunks.len() as u64 != expected_chunks {
                warn!(
                    stream_id = stream_id,
                    expected = expected_chunks,
                    received = assembly.chunks.len(),
                    "Incomplete stream assembly - wrong count"
                );
                return None;
            }

            // ✅ PROPER: Verify all indices 0..N-1 are present (no gaps)
            for i in 0..expected_chunks as u32 {
                if !assembly.chunks.contains_key(&i) {
                    warn!(
                        stream_id = stream_id,
                        expected_chunks = expected_chunks,
                        missing_chunk = i,
                        received_chunks = ?assembly.chunks.keys().collect::<Vec<_>>(),
                        "Incomplete stream assembly - missing chunk index"
                    );
                    return None;
                }
            }

            // ✅ PROPER: Reassemble in correct order using chunk indices
            let mut complete = Vec::with_capacity(assembly.header.total_size as usize);
            for i in 0..expected_chunks as u32 {
                if let Some(chunk) = assembly.chunks.get(&i) {
                    complete.extend_from_slice(chunk);
                } else {
                    // This should never happen due to the gap check above
                    error!(
                        stream_id = stream_id,
                        chunk_index = i,
                        "Critical error: chunk missing during assembly after gap check"
                    );
                    return None;
                }
            }

            info!(
                stream_id = stream_id,
                total_size = complete.len(),
                "Completed stream assembly"
            );

            Some(complete)
        } else {
            warn!(
                stream_id = stream_id,
                "Stream assembly not found for completion"
            );
            None
        }
    }

    // =================== Peer Discovery Methods ===================

    /// Maximum size of peer list in gossip messages (resource exhaustion protection)
    pub const MAX_PEER_LIST_SIZE: usize = 1000;

    /// Create a snapshot of current peers for gossip
    /// Includes self (using advertised_address from config)
    pub async fn peers_snapshot(&self) -> Vec<PeerInfoGossip> {
        let gossip_state = self.gossip_state.lock().await;
        let mut peers: Vec<PeerInfoGossip> = Vec::new();

        // Include self (using advertised address or bind address)
        let self_addr = self.config.advertise_address.unwrap_or(self.bind_addr);
        let self_info = PeerInfo::local(self_addr);
        peers.push(self_info.to_gossip());

        // Include active peers
        for (_, peer_info) in gossip_state.peers.iter() {
            // Skip failed peers
            if peer_info.failures >= self.config.max_peer_failures {
                continue;
            }
            peers.push(peer_info.to_gossip());
        }

        // Include known peers from LRU cache (up to limit)
        let remaining = Self::MAX_PEER_LIST_SIZE.saturating_sub(peers.len());
        for (_, peer_info) in gossip_state.known_peers.iter().take(remaining) {
            peers.push(peer_info.to_gossip());
        }

        // Truncate to max size
        peers.truncate(Self::MAX_PEER_LIST_SIZE);

        peers
    }

    /// Periodic gossip of peer list to random subset of peers
    /// Returns gossip tasks for the caller to send.
    pub async fn gossip_peer_list(&self) -> Vec<GossipTask> {
        // Check if peer discovery is enabled
        if !self.config.enable_peer_discovery {
            return Vec::new();
        }

        // Check gossip interval
        let now = current_timestamp();
        let current_sequence = {
            let gossip_state = self.gossip_state.lock().await;
            if let Some(interval) = self.config.peer_gossip_interval {
                let interval_secs = interval.as_secs();
                if now
                    < gossip_state
                        .last_peer_gossip_time
                        .saturating_add(interval_secs)
                {
                    return Vec::new(); // Not time yet
                }
            }
            gossip_state.gossip_sequence
        };

        // Get peer snapshot
        let peers = self.peers_snapshot().await;
        if peers.is_empty() {
            return Vec::new();
        }

        // Get active peers to gossip to
        let targets: Vec<SocketAddr> = {
            let gossip_state = self.gossip_state.lock().await;
            let mut active_peers: Vec<SocketAddr> = gossip_state
                .peers
                .iter()
                .filter(|(_, info)| info.failures < self.config.max_peer_failures)
                .map(|(addr, _)| *addr)
                .collect();

            // Shuffle and take max_peer_gossip_targets
            active_peers.shuffle(&mut rand::rng());
            active_peers.truncate(self.config.max_peer_gossip_targets);
            active_peers
        };

        if targets.is_empty() {
            return Vec::new();
        }

        // Update last gossip time
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.last_peer_gossip_time = now;
        }

        // Create message
        let self_addr = self.config.advertise_address.unwrap_or(self.bind_addr);
        let msg = RegistryMessage::PeerListGossip {
            peers,
            timestamp: now,
            sender_addr: self_addr.to_string(),
        };

        // Prepare tasks for caller to send
        let target_count = targets.len();
        let tasks: Vec<GossipTask> = targets
            .into_iter()
            .map(|peer_addr| GossipTask {
                peer_addr,
                message: msg.clone(),
                current_sequence,
            })
            .collect();

        let peer_count = if let RegistryMessage::PeerListGossip { ref peers, .. } = msg {
            peers.len()
        } else {
            0
        };
        info!(
            targets = target_count,
            peer_count = peer_count,
            "peer list gossip round completed"
        );

        tasks
    }

    /// Handle incoming peer list gossip
    /// Returns candidates to connect to
    ///
    /// IMPORTANT: "Don't penalize the messenger" principle (Phase 4):
    /// - Unreachable peers in the list do NOT cause sender to be penalized
    /// - We only penalize the sender for INVALID data (bogon IPs, malformed data)
    /// - Backoff is applied to TARGET peers only, not the gossip source
    pub async fn on_peer_list_gossip(
        &self,
        peers: Vec<PeerInfoGossip>,
        sender_addr: &str,
        timestamp: u64,
    ) -> Vec<SocketAddr> {
        // Resource exhaustion protection - sender is sending suspicious data
        if peers.len() > Self::MAX_PEER_LIST_SIZE {
            warn!(
                size = peers.len(),
                max = Self::MAX_PEER_LIST_SIZE,
                sender = %sender_addr,
                "peer list too large, rejecting - potential attack"
            );
            // Note: We could penalize sender here, but for now just reject
            return vec![];
        }

        // Check if peer discovery is enabled
        if !self.config.enable_peer_discovery {
            debug!("peer discovery disabled, ignoring peer list gossip");
            return vec![];
        }

        let _now = current_timestamp();

        // DON'T PENALIZE THE MESSENGER:
        // Count bogon IPs to detect if sender is sending suspicious data
        // We only penalize for INVALID data, not for unreachable peers
        let mut bogon_count = 0;
        for peer_gossip in &peers {
            if let Ok(addr) = peer_gossip.address.parse::<SocketAddr>() {
                let ip = addr.ip();
                // Check for bogon IPs that should never be in peer lists
                if ip.is_loopback() && !self.config.allow_loopback_discovery {
                    bogon_count += 1;
                }
                // Check link-local
                if let std::net::IpAddr::V4(v4) = ip {
                    if v4.is_link_local() && !self.config.allow_link_local_discovery {
                        bogon_count += 1;
                    }
                }
            }
        }

        // If more than 50% of peers are bogons, log warning (could penalize sender)
        if !peers.is_empty() && bogon_count * 2 > peers.len() {
            warn!(
                bogon_count = bogon_count,
                total = peers.len(),
                sender = %sender_addr,
                "peer list contains mostly bogon IPs - sender may be malicious"
            );
            // Note: We choose to log but not block - could be misconfiguration
        }

        // Ingest peers into known_peers and get candidates
        let candidates = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Update known_peers LRU cache
            for peer_gossip in &peers {
                if let Some(peer_info) = PeerInfo::from_gossip(peer_gossip) {
                    // Conservative merge: only update if newer
                    if let Some(existing) = gossip_state.known_peers.get_mut(&peer_info.address) {
                        // Only update if the incoming info is newer
                        if peer_gossip.last_success > existing.last_success {
                            existing.last_success = peer_gossip.last_success;
                            existing.last_attempt = peer_gossip.last_attempt;
                            // Don't overwrite local failure count
                        }
                    } else {
                        // New peer, add to cache
                        gossip_state.known_peers.put(peer_info.address, peer_info);
                    }
                }
            }

            // Get candidates from peer discovery manager
            // Note: PeerDiscovery filters out unsafe addresses via is_safe_to_dial()
            // but does NOT penalize the sender - only skips unsafe targets
            if let Some(ref mut discovery) = gossip_state.peer_discovery {
                discovery.on_peer_list_gossip(&peers)
            } else {
                vec![]
            }
        };

        debug!(
            peer_count = peers.len(),
            candidates = candidates.len(),
            sender = %sender_addr,
            timestamp = timestamp,
            "processed peer list gossip"
        );

        candidates
    }

    /// Prune stale peers from known_peers based on TTLs
    pub async fn prune_stale_peers(&self) {
        let now = current_timestamp();

        let mut gossip_state = self.gossip_state.lock().await;

        // Prune from known_peers based on TTLs
        let fail_ttl_secs = self.config.fail_ttl.as_secs();
        let stale_ttl_secs = self.config.stale_ttl.as_secs();

        // Collect keys to remove
        let to_remove: Vec<SocketAddr> = gossip_state
            .known_peers
            .iter()
            .filter(|(_, info)| {
                // Remove if:
                // 1. Failed and exceeded fail_ttl
                if info.failures > 0 {
                    if let Some(failure_time) = info.last_failure_time {
                        if now > failure_time.saturating_add(fail_ttl_secs) {
                            return true;
                        }
                    }
                }
                // 2. Stale (no success for stale_ttl)
                if info.last_success > 0 && now > info.last_success.saturating_add(stale_ttl_secs) {
                    return true;
                }
                false
            })
            .map(|(addr, _)| *addr)
            .collect();

        // Remove stale peers
        for addr in &to_remove {
            gossip_state.known_peers.pop(addr);
        }

        if !to_remove.is_empty() {
            debug!(
                removed = to_remove.len(),
                remaining = gossip_state.known_peers.len(),
                "pruned stale peers from known_peers"
            );
        }

        // Also prune from peer_discovery if enabled
        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            let stats = discovery.cleanup_expired(now);
            if stats.pending_removed > 0 || stats.failed_removed > 0 {
                debug!(
                    pending_removed = stats.pending_removed,
                    failed_removed = stats.failed_removed,
                    "peer discovery cleanup removed expired entries"
                );
            }
        }
    }

    /// Lookup advertised address for a NodeId
    /// First checks active peers, then falls back to known_peers
    pub async fn lookup_advertised_addr(&self, node_id: &crate::NodeId) -> Option<SocketAddr> {
        let gossip_state = self.gossip_state.lock().await;

        // First check active peers
        for (addr, peer_info) in gossip_state.peers.iter() {
            if peer_info.node_id.as_ref() == Some(node_id) {
                return Some(*addr);
            }
        }

        // Fallback to known_peers
        for (addr, peer_info) in gossip_state.known_peers.iter() {
            if peer_info.node_id.as_ref() == Some(node_id) {
                return Some(*addr);
            }
        }

        None
    }

    /// Lookup NodeId for a given address (active peers first, then known_peers)
    pub async fn lookup_node_id(&self, addr: &SocketAddr) -> Option<crate::NodeId> {
        let mut gossip_state = self.gossip_state.lock().await;

        if let Some(peer_info) = gossip_state.peers.get(addr) {
            if let Some(node_id) = peer_info.node_id {
                return Some(node_id);
            }
        }

        if let Some(peer_info) = gossip_state.known_peers.get(addr) {
            if let Some(node_id) = peer_info.node_id {
                return Some(node_id);
            }
        }

        None
    }

    /// Connect-on-demand for actor messaging (Phase 4)
    ///
    /// This method allows connecting to a node for actor messaging even if the
    /// max_peers soft cap has been reached. The soft cap only limits automatic
    /// peer discovery, not direct actor communication.
    ///
    /// First checks active connections, then uses known_peers to look up the address.
    pub async fn ensure_connection_for_actor(
        &self,
        node_id: &crate::NodeId,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        // Check if we have an active connection to the node already
        if let Some(addr) = self.lookup_advertised_addr(node_id).await {
            if self.has_active_connection(&addr).await {
                debug!(node_id = %node_id.fmt_short(), addr = %addr, "using existing connection for actor messaging");
                return self.get_connection(addr).await;
            }

            // Connect-on-demand: This can exceed max_peers soft cap
            // because actor messaging takes priority over peer discovery limits
            debug!(
                node_id = %node_id.fmt_short(),
                addr = %addr,
                "connect-on-demand for actor messaging (may exceed soft cap)"
            );
            return self.get_connection(addr).await;
        }

        Err(GossipError::ActorNotFound(format!(
            "no known address for node {}",
            node_id.fmt_short()
        )))
    }

    /// Check if we have an active connection to a peer
    /// Used for "local connection wins" - we trust our direct connection over gossip reports
    pub async fn has_active_connection(&self, addr: &SocketAddr) -> bool {
        let pool = self.connection_pool.lock().await;
        pool.has_connection(addr)
    }

    /// Mark a peer connection as established (clears failure state)
    pub async fn mark_peer_connected(&self, addr: SocketAddr) {
        let mut gossip_state = self.gossip_state.lock().await;

        // Update known_peers
        if let Some(peer_info) = gossip_state.known_peers.get_mut(&addr) {
            peer_info.failures = 0;
            peer_info.last_failure_time = None;
            peer_info.last_success = current_timestamp();
            if let Some(node_id) = peer_info.node_id {
                self.peer_capability_addr_to_node.insert(addr, node_id);
                if let Some(entry) = self.peer_capabilities_by_node.get(&node_id) {
                    self.peer_capabilities.insert(addr, entry.value().clone());
                }
            }
        }

        // Update peer_discovery
        let should_track_mesh_time =
            self.config.mesh_formation_target > 0 && gossip_state.mesh_formation_time_ms.is_none();

        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            discovery.on_peer_connected(addr);

            if should_track_mesh_time {
                let target = self.config.mesh_formation_target;
                if discovery.connected_peer_count() >= target {
                    let elapsed_ms = self.start_instant.elapsed().as_millis() as u64;
                    gossip_state.mesh_formation_time_ms = Some(elapsed_ms);
                }
            }
        }

        debug!(addr = %addr, "marked peer as connected");
    }

    /// Mark a peer connection as failed (applies backoff)
    /// Implements "local connection wins" - if we have an active connection, we trust it
    /// over gossip reports and skip marking as failed.
    pub async fn mark_peer_failed(&self, addr: SocketAddr) {
        // LOCAL CONNECTION WINS: If we have an active connection to this peer,
        // don't mark it as failed based on gossip reports from other nodes.
        // We trust our direct connection over third-party reports.
        {
            let pool = self.connection_pool.lock().await;
            if pool.has_connection(&addr) {
                debug!(
                    addr = %addr,
                    "local connection wins - skipping failure mark for connected peer"
                );
                return;
            }
        }

        let mut gossip_state = self.gossip_state.lock().await;

        let now = current_timestamp();

        // Update known_peers
        if let Some(peer_info) = gossip_state.known_peers.get_mut(&addr) {
            peer_info.failures = peer_info.failures.saturating_add(1);
            peer_info.last_failure_time = Some(now);
            peer_info.last_attempt = now;
        }

        // Update peer_discovery
        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            discovery.on_peer_failure(addr);
        }

        debug!(addr = %addr, "marked peer as failed");
    }

    /// Mark a peer as disconnected
    pub async fn mark_peer_disconnected(&self, addr: SocketAddr) {
        let mut gossip_state = self.gossip_state.lock().await;

        if let Some(ref mut discovery) = gossip_state.peer_discovery {
            discovery.on_peer_disconnected(addr);
        }

        debug!(addr = %addr, "marked peer as disconnected");
    }

    /// Duplicate connection tie-breaker
    /// When both nodes try to connect simultaneously, use NodeId comparison:
    /// - Lower NodeId keeps outbound connection
    /// - Higher NodeId keeps inbound connection
    ///
    /// Returns true if this connection should be kept.
    pub fn should_keep_connection(
        &self,
        remote_peer_id: &crate::PeerId,
        is_outbound: bool,
    ) -> bool {
        let local_id = self.peer_id.to_node_id();
        let remote_id = remote_peer_id.to_node_id();

        match local_id.as_bytes().cmp(remote_id.as_bytes()) {
            std::cmp::Ordering::Less => is_outbound,
            std::cmp::Ordering::Greater => !is_outbound,
            std::cmp::Ordering::Equal => {
                // Same node ID shouldn't happen in practice
                warn!(local = %local_id, remote = %remote_id, "duplicate connection from same NodeId");
                true
            }
        }
    }

    /// Ensure a peer exists in the active peer map.
    ///
    /// This is primarily used for inbound TLS connections where the remote
    /// node dialed us first. Without inserting an entry into `gossip_state.peers`
    /// we would never target that connection for urgent gossip, which means
    /// actor registrations on the inbound-only node would never propagate.
    pub async fn ensure_peer_entry(
        &self,
        addr: SocketAddr,
        node_id: Option<crate::NodeId>,
    ) {
        let mut gossip_state = self.gossip_state.lock().await;
        let current_time = crate::current_timestamp();

        let entry = gossip_state.peers.entry(addr).or_insert_with(|| PeerInfo {
            address: addr,
            peer_address: None,
            node_id,
            failures: 0,
            last_attempt: current_time,
            last_success: current_time,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
        });

        if node_id.is_some() {
            entry.node_id = node_id;
        }
    }

    /// Check if we already have a connection to a peer by peer ID
    pub async fn has_connection_to_peer(&self, peer_id: &crate::PeerId) -> bool {
        let pool = self.connection_pool.lock().await;
        pool.has_connection_by_peer_id(peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyPair, PeerId};
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn test_peer_id(seed: &str) -> PeerId {
        KeyPair::new_for_testing(seed).peer_id()
    }

    fn test_location(addr: SocketAddr) -> RemoteActorLocation {
        RemoteActorLocation::new_with_peer(addr, test_peer_id("test_peer"))
    }

    fn test_config() -> GossipConfig {
        GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("registry_tests")),
            gossip_interval: Duration::from_millis(100),
            cleanup_interval: Duration::from_millis(200),
            peer_retry_interval: Duration::from_millis(50),
            immediate_propagation_enabled: true, // Enable for testing
            ..Default::default()
        }
    }

    #[test]
    fn test_registry_change_serialization() {
        let location = test_location(test_addr(8080));
        let change = RegistryChange::ActorAdded {
            name: "test".to_string(),
            location,
            priority: RegistrationPriority::Immediate,
        };

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&change).unwrap();
        let deserialized: RegistryChange =
            rkyv::from_bytes::<RegistryChange, rkyv::rancor::Error>(&serialized).unwrap();

        match deserialized {
            RegistryChange::ActorAdded { name, .. } => {
                assert_eq!(name, "test");
            }
            _ => panic!("Wrong change type"),
        }
    }

    #[test]
    fn test_registry_delta_serialization() {
        let delta = RegistryDelta {
            since_sequence: 10,
            current_sequence: 15,
            changes: vec![],
            sender_peer_id: test_peer_id("test_peer"),
            wall_clock_time: 1000,
            precise_timing_nanos: 1_000_000_000_000, // 1000 seconds in nanoseconds
        };

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&delta).unwrap();
        let deserialized: RegistryDelta =
            rkyv::from_bytes::<RegistryDelta, rkyv::rancor::Error>(&serialized).unwrap();

        assert_eq!(deserialized.since_sequence, 10);
        assert_eq!(deserialized.current_sequence, 15);
        assert_eq!(deserialized.sender_peer_id, test_peer_id("test_peer"));
    }

    #[test]
    fn test_peer_health_status() {
        let status = PeerHealthStatus {
            is_alive: true,
            last_contact: 1000,
            failure_count: 2,
        };

        assert!(status.is_alive);
        assert_eq!(status.last_contact, 1000);
        assert_eq!(status.failure_count, 2);
    }

    #[test]
    fn test_registry_message_variants() {
        // Test DeltaGossip
        let delta = RegistryDelta {
            since_sequence: 1,
            current_sequence: 2,
            changes: vec![],
            sender_peer_id: test_peer_id("test_peer"),
            wall_clock_time: 1000,
            precise_timing_nanos: 1_000_000_000_000, // 1000 seconds in nanoseconds
        };
        let msg = RegistryMessage::DeltaGossip { delta };
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();
        let deserialized: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&serialized).unwrap();
        match deserialized {
            RegistryMessage::DeltaGossip { .. } => (),
            _ => panic!("Wrong message type"),
        }

        // Test FullSyncRequest
        let msg = RegistryMessage::FullSyncRequest {
            sender_peer_id: test_peer_id("test_peer"),
            sequence: 10,
            wall_clock_time: 1000,
        };
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();
        let deserialized: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&serialized).unwrap();
        match deserialized {
            RegistryMessage::FullSyncRequest { sequence, .. } => {
                assert_eq!(sequence, 10);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_registry_stats() {
        let stats = RegistryStats {
            local_actors: 5,
            known_actors: 10,
            active_peers: 3,
            failed_peers: 1,
            total_gossip_rounds: 100,
            current_sequence: 100,
            uptime_seconds: 3600,
            last_gossip_timestamp: 1000,
            delta_exchanges: 50,
            full_sync_exchanges: 10,
            delta_history_size: 20,
            avg_delta_size: 5.5,
            // Peer discovery metrics (Phase 5)
            discovered_peers: 15,
            failed_discovery_attempts: 2,
            avg_mesh_connectivity: 0.2,
            mesh_formation_time_ms: Some(500),
        };

        assert_eq!(stats.local_actors, 5);
        assert_eq!(stats.known_actors, 10);
        assert_eq!(stats.active_peers, 3);
        assert_eq!(stats.failed_peers, 1);
        assert_eq!(stats.discovered_peers, 15);
        assert_eq!(stats.failed_discovery_attempts, 2);
    }

    #[test]
    fn test_peer_info() {
        let mut peer = PeerInfo {
            address: test_addr(8080),
            peer_address: Some(test_addr(8081)),
            node_id: None,
            failures: 0,
            last_attempt: 100,
            last_success: 100,
            last_sequence: 5,
            last_sent_sequence: 5,
            consecutive_deltas: 3,
            last_failure_time: None,
        };

        assert_eq!(peer.address, test_addr(8080));
        assert_eq!(peer.failures, 0);

        peer.failures += 1;
        peer.last_failure_time = Some(200);
        assert_eq!(peer.failures, 1);
        assert_eq!(peer.last_failure_time, Some(200));
    }

    #[test]
    fn test_deduplicate_changes() {
        let location1 = test_location(test_addr(8080));
        let location2 = test_location(test_addr(8081));

        let changes = vec![
            RegistryChange::ActorAdded {
                name: "actor1".to_string(),
                location: location1.clone(),
                priority: RegistrationPriority::Normal,
            },
            RegistryChange::ActorAdded {
                name: "actor1".to_string(),
                location: location2,
                priority: RegistrationPriority::Immediate,
            },
            RegistryChange::ActorRemoved {
                name: "actor2".to_string(),
                vector_clock: crate::VectorClock::new(),
                removing_node_id: crate::SecretKey::generate().public(),
                priority: RegistrationPriority::Normal,
            },
        ];

        let deduped = GossipRegistry::deduplicate_changes(changes);
        assert_eq!(deduped.len(), 2); // Only one change per actor

        // Verify we kept the last change for actor1
        let actor1_changes: Vec<_> = deduped
            .iter()
            .filter(|c| match c {
                RegistryChange::ActorAdded { name, .. } => name == "actor1",
                _ => false,
            })
            .collect();
        assert_eq!(actor1_changes.len(), 1);
    }

    #[test]
    fn test_get_change_actor_name() {
        let location = test_location(test_addr(8080));
        let add_change = RegistryChange::ActorAdded {
            name: "test_actor".to_string(),
            location,
            priority: RegistrationPriority::Normal,
        };
        assert_eq!(
            GossipRegistry::get_change_actor_name(&add_change),
            "test_actor"
        );

        let remove_change = RegistryChange::ActorRemoved {
            name: "test_actor".to_string(),
            vector_clock: crate::VectorClock::new(),
            removing_node_id: crate::SecretKey::generate().public(),
            priority: RegistrationPriority::Normal,
        };
        assert_eq!(
            GossipRegistry::get_change_actor_name(&remove_change),
            "test_actor"
        );
    }

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());
        assert_eq!(registry.bind_addr, test_addr(8080));
        assert!(!registry.is_shutdown().await);
    }

    // #[tokio::test]
    // async fn test_add_bootstrap_peers() {
    //     let registry = GossipRegistry::new(test_addr(8080), test_config());
    //     let peers = vec![test_addr(8081), test_addr(8082), test_addr(8080)]; // Including self

    //     registry.add_bootstrap_peers(peers).await;

    //     let gossip_state = registry.gossip_state.lock().await;
    //     assert_eq!(gossip_state.peers.len(), 2); // Should exclude self
    //     assert!(gossip_state.peers.contains_key(&test_addr(8081)));
    //     assert!(gossip_state.peers.contains_key(&test_addr(8082)));
    //     assert!(!gossip_state.peers.contains_key(&test_addr(8080))); // Self excluded
    // }

    #[tokio::test]
    async fn test_add_peer() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        registry.add_peer(test_addr(8081)).await;
        registry.add_peer(test_addr(8080)).await; // Try to add self
        registry.add_peer(test_addr(8081)).await; // Try to add duplicate

        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.peers.len(), 1);
        assert!(gossip_state.peers.contains_key(&test_addr(8081)));
    }

    #[tokio::test]
    async fn test_register_actor() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        let result = registry
            .register_actor("test_actor".to_string(), location)
            .await;
        assert!(result.is_ok());

        // Verify actor is in local_actors
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.local_actors.contains_key("test_actor"));

        // Verify pending change was created
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.pending_changes.len(), 1);
    }

    #[tokio::test]
    async fn test_register_actor_duplicate() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        registry
            .register_actor("test_actor".to_string(), location.clone())
            .await
            .unwrap();

        // Try to register again
        let result = registry
            .register_actor("test_actor".to_string(), location)
            .await;
        assert!(matches!(result, Err(GossipError::ActorAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_register_actor_with_priority() {
        let mut config = test_config();
        config.immediate_propagation_enabled = false; // Disable to test queuing
        let registry = GossipRegistry::new(test_addr(8080), config);

        let location = test_location(test_addr(9001));
        let result = registry
            .register_actor_with_priority(
                "urgent_actor".to_string(),
                location,
                RegistrationPriority::Immediate,
            )
            .await;
        assert!(result.is_ok());

        // Verify urgent change was created (not cleared since immediate propagation is disabled)
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.urgent_changes.len(), 1);
        assert_eq!(gossip_state.pending_changes.len(), 0);
    }

    #[tokio::test]
    async fn test_unregister_actor() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        registry
            .register_actor("test_actor".to_string(), location)
            .await
            .unwrap();

        let removed = registry.unregister_actor("test_actor").await.unwrap();
        assert!(removed.is_some());

        // Verify actor is removed
        let actor_state = registry.actor_state.read().await;
        assert!(!actor_state.local_actors.contains_key("test_actor"));

        // Verify removal change was created
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.pending_changes.len(), 2); // Add + Remove
    }

    #[tokio::test]
    async fn test_lookup_actor() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Test local actor
        let location = test_location(test_addr(9001));
        registry
            .register_actor("local_actor".to_string(), location.clone())
            .await
            .unwrap();

        let found = registry.lookup_actor("local_actor").await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().socket_addr().unwrap(), test_addr(9001));

        // Test non-existent actor
        let not_found = registry.lookup_actor("missing_actor").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_lookup_actor_ttl() {
        let mut config = test_config();
        config.actor_ttl = Duration::from_millis(50); // Very short TTL for testing
        let registry = GossipRegistry::new(test_addr(8080), config);

        // Add a known actor with old timestamp
        let mut location = test_location(test_addr(9001));
        location.wall_clock_time = current_timestamp() - 100; // Old timestamp

        {
            let mut actor_state = registry.actor_state.write().await;
            actor_state
                .known_actors
                .insert("old_actor".to_string(), location);
        }

        // Should not find due to TTL
        let found = registry.lookup_actor("old_actor").await;
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_get_stats() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add some data
        registry
            .register_actor("actor1".to_string(), test_location(test_addr(9001)))
            .await
            .unwrap();
        registry.add_peer(test_addr(8081)).await;

        let stats = registry.get_stats().await;
        assert_eq!(stats.local_actors, 1);
        assert_eq!(stats.active_peers, 1);
        assert_eq!(stats.failed_peers, 0);
        assert_eq!(stats.uptime_seconds, 0); // Just created
    }

    #[tokio::test]
    async fn test_mesh_formation_metric_records_when_threshold_met() {
        let mut config = test_config();
        config.enable_peer_discovery = true;
        config.mesh_formation_target = 1;

        let registry = GossipRegistry::new(test_addr(8080), config);
        registry.mark_peer_connected(test_addr(8082)).await;

        let stats = registry.get_stats().await;
        assert!(
            stats.mesh_formation_time_ms.is_some(),
            "mesh formation metric should be recorded when threshold met"
        );
    }

    #[tokio::test]
    async fn test_apply_delta() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = test_location(test_addr(9001));
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "remote_actor".to_string(),
                location,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: test_peer_id("node_b"),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        registry.apply_delta(delta).await.unwrap();

        // Verify actor was added to known_actors
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.known_actors.contains_key("remote_actor"));
    }

    #[tokio::test]
    async fn test_apply_delta_skip_local() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Register local actor
        let local_location = test_location(test_addr(9001));
        registry
            .register_actor("local_actor".to_string(), local_location)
            .await
            .unwrap();

        // Try to override with remote update
        let remote_location = test_location(test_addr(9002));
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "local_actor".to_string(),
                location: remote_location,
                priority: RegistrationPriority::Normal,
            }],
            sender_peer_id: test_peer_id("node_b"),
            wall_clock_time: current_timestamp(),
            precise_timing_nanos: crate::current_timestamp_nanos(),
        };

        registry.apply_delta(delta).await.unwrap();

        // Verify local actor wasn't overridden
        let actor_state = registry.actor_state.read().await;
        let actor = actor_state.local_actors.get("local_actor").unwrap();
        assert_eq!(actor.socket_addr().unwrap(), test_addr(9001)); // Still local address
    }

    #[tokio::test]
    async fn test_should_use_delta_state() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let gossip_state = registry.gossip_state.lock().await;

        // New peer should use full sync
        let new_peer = PeerInfo {
            address: test_addr(8081),
            peer_address: None,
            node_id: None,
            failures: 0,
            last_attempt: 0,
            last_success: 0,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
        };
        assert!(!registry.should_use_delta_state(&gossip_state, &new_peer));

        // Peer with history should use delta
        let established_peer = PeerInfo {
            address: test_addr(8081),
            peer_address: None,
            node_id: None,
            failures: 0,
            last_attempt: 100,
            last_success: 100,
            last_sequence: 5,
            last_sent_sequence: 5,
            consecutive_deltas: 10,
            last_failure_time: None,
        };
        // Add some peers to make it not a small cluster
        drop(gossip_state);
        for i in 0..10 {
            registry.add_peer(test_addr(8090 + i)).await;
        }
        let gossip_state = registry.gossip_state.lock().await;
        assert!(registry.should_use_delta_state(&gossip_state, &established_peer));
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_no_peers() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_with_peers() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add peers
        registry.add_peer(test_addr(8081)).await;
        registry.add_peer(test_addr(8082)).await;

        // Add some changes
        registry
            .register_actor("actor1".to_string(), test_location(test_addr(9001)))
            .await
            .unwrap();

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert!(!tasks.is_empty());
        assert!(tasks.len() <= 2); // Should gossip to available peers
    }

    #[tokio::test]
    async fn test_shutdown() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add some data
        registry
            .register_actor("actor1".to_string(), test_location(test_addr(9001)))
            .await
            .unwrap();
        registry.add_peer(test_addr(8081)).await;

        assert!(!registry.is_shutdown().await);

        registry.shutdown().await;

        assert!(registry.is_shutdown().await);

        // Verify data was cleared
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.local_actors.is_empty());
        assert!(actor_state.known_actors.is_empty());

        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peers.is_empty());
    }

    #[tokio::test]
    async fn test_handle_peer_connection_failure() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add a peer
        registry.add_peer(test_addr(8081)).await;

        // Simulate failure
        registry
            .handle_peer_connection_failure(test_addr(8081))
            .await
            .unwrap();

        // Check peer is marked as failed
        let gossip_state = registry.gossip_state.lock().await;
        let peer = gossip_state.peers.get(&test_addr(8081)).unwrap();
        assert_eq!(peer.failures, registry.config.max_peer_failures);
        assert!(peer.last_failure_time.is_some());
    }

    #[tokio::test]
    async fn test_cleanup_stale_actors() {
        let mut config = test_config();
        config.actor_ttl = Duration::from_millis(50);
        let registry = GossipRegistry::new(test_addr(8080), config);

        // Add old actor
        let mut old_location = test_location(test_addr(9001));
        old_location.wall_clock_time = current_timestamp() - 100;

        {
            let mut actor_state = registry.actor_state.write().await;
            actor_state
                .known_actors
                .insert("old_actor".to_string(), old_location);
        }

        registry.cleanup_stale_actors().await;

        // Verify old actor was removed
        let actor_state = registry.actor_state.read().await;
        assert!(!actor_state.known_actors.contains_key("old_actor"));
    }

    #[tokio::test]
    async fn test_cleanup_dead_peers() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::new(test_addr(8080), config);

        // Add a failed peer with old failure time
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                test_addr(8081),
                PeerInfo {
                    address: test_addr(8081),
                    peer_address: None,
                    node_id: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp() - 100),
                },
            );
        }

        // Add some actors from the failed peer
        {
            let mut actor_state = registry.actor_state.write().await;
            actor_state
                .known_actors
                .insert("peer_actor".to_string(), test_location(test_addr(9001)));

            let mut gossip_state = registry.gossip_state.lock().await;
            let mut actors = HashSet::new();
            actors.insert("peer_actor".to_string());
            gossip_state.peer_to_actors.insert(test_addr(8081), actors);
        }

        registry.cleanup_dead_peers().await;

        // Verify peer is KEPT but its actors were removed
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peers.contains_key(&test_addr(8081))); // Peer is still there!
        assert!(!gossip_state.peer_to_actors.contains_key(&test_addr(8081))); // But actors mapping is gone

        drop(gossip_state);
        let actor_state = registry.actor_state.read().await;
        assert!(!actor_state.known_actors.contains_key("peer_actor")); // Actors are cleaned up
    }

    #[tokio::test]
    async fn test_merge_full_sync() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let mut remote_local = HashMap::new();
        remote_local.insert("remote_actor1".to_string(), test_location(test_addr(9001)));

        let mut remote_known = HashMap::new();
        remote_known.insert("remote_actor2".to_string(), test_location(test_addr(9002)));

        registry
            .merge_full_sync(
                remote_local,
                remote_known,
                test_addr(8081),
                1,
                current_timestamp(),
            )
            .await;

        // Verify actors were merged
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.known_actors.contains_key("remote_actor1"));
        assert!(actor_state.known_actors.contains_key("remote_actor2"));
    }

    #[tokio::test]
    async fn test_pending_failure() {
        let pending = PendingFailure {
            first_detected: 1000,
            consensus_deadline: 1005,
            query_sent: false,
        };

        assert_eq!(pending.first_detected, 1000);
        assert_eq!(pending.consensus_deadline, 1005);
        assert!(!pending.query_sent);
    }

    #[tokio::test]
    async fn test_historical_delta() {
        let delta = HistoricalDelta {
            sequence: 10,
            changes: vec![],
            wall_clock_time: 1000,
        };

        assert_eq!(delta.sequence, 10);
        assert!(delta.changes.is_empty());
        assert_eq!(delta.wall_clock_time, 1000);
    }

    #[tokio::test]
    async fn test_gossip_task() {
        let task = GossipTask {
            peer_addr: test_addr(8081),
            message: RegistryMessage::FullSyncRequest {
                sender_peer_id: test_peer_id("test_peer"),
                sequence: 10,
                wall_clock_time: 1000,
            },
            current_sequence: 10,
        };

        assert_eq!(task.peer_addr, test_addr(8081));
        assert_eq!(task.current_sequence, 10);
    }

    #[tokio::test]
    async fn test_gossip_result() {
        let result = GossipResult {
            peer_addr: test_addr(8081),
            sent_sequence: 10,
            outcome: Ok(None),
        };

        assert_eq!(result.peer_addr, test_addr(8081));
        assert_eq!(result.sent_sequence, 10);
        assert!(result.outcome.is_ok());
    }

    #[tokio::test]
    async fn test_trigger_immediate_gossip() {
        let mut config = test_config();
        config.immediate_propagation_enabled = true;
        let registry = GossipRegistry::new(test_addr(8080), config);

        // NOTE: Don't add a peer here - tests only the no-peers path.
        // (Adding a peer would require TLS setup which is tested in integration tests)

        // Add urgent change
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state
                .urgent_changes
                .push(RegistryChange::ActorAdded {
                    name: "urgent".to_string(),
                    location: test_location(test_addr(9001)),
                    priority: RegistrationPriority::Immediate,
                });
        }

        // Should return Ok when no peers are available
        let result = registry.trigger_immediate_gossip().await;
        assert!(result.is_ok());

        // Urgent changes are cleared (taken) even when there are no peers
        // (current implementation takes changes before checking for peers)
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.urgent_changes.is_empty());
    }

    #[tokio::test]
    async fn test_enforce_bounds() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add many pending changes
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            for i in 0..2000 {
                gossip_state
                    .pending_changes
                    .push(RegistryChange::ActorAdded {
                        name: format!("actor{}", i),
                        location: test_location(test_addr(9000 + i as u16)),
                        priority: RegistrationPriority::Normal,
                    });
            }
        }

        registry.enforce_bounds().await;

        // Verify bounds were enforced
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.pending_changes.len() <= 1000);
    }

    #[tokio::test]
    async fn test_check_peer_consensus() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add a pending failure
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            let pending = PendingFailure {
                first_detected: current_timestamp() - 10,
                consensus_deadline: current_timestamp() - 5, // Past deadline
                query_sent: true,
            };
            gossip_state
                .pending_peer_failures
                .insert(test_addr(8081), pending);

            // Add some health reports
            let mut reports = HashMap::new();
            reports.insert(
                test_addr(8080),
                PeerHealthStatus {
                    is_alive: false,
                    last_contact: current_timestamp(),
                    failure_count: 1,
                },
            );
            gossip_state
                .peer_health_reports
                .insert(test_addr(8081), reports);
        }

        registry.check_peer_consensus().await;

        // Verify pending failure was processed
        let gossip_state = registry.gossip_state.lock().await;
        assert!(!gossip_state
            .pending_peer_failures
            .contains_key(&test_addr(8081)));
    }

    // =================== Phase 1: PeerListGossip Tests ===================

    #[test]
    fn test_peer_list_gossip_serialization() {
        // Test rkyv round-trip for PeerListGossip message
        let peer1 = PeerInfoGossip {
            address: "127.0.0.1:8080".to_string(),
            peer_address: Some("192.168.1.100:8080".to_string()),
            node_id: None,
            failures: 0,
            last_attempt: 1000,
            last_success: 1000,
        };
        let peer2 = PeerInfoGossip {
            address: "127.0.0.1:8081".to_string(),
            peer_address: None,
            node_id: None,
            failures: 2,
            last_attempt: 2000,
            last_success: 1500,
        };

        let msg = RegistryMessage::PeerListGossip {
            peers: vec![peer1, peer2],
            timestamp: 12345,
            sender_addr: "127.0.0.1:9000".to_string(),
        };

        // Serialize
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();

        // Deserialize
        let deserialized: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&serialized).unwrap();

        // Verify
        match deserialized {
            RegistryMessage::PeerListGossip {
                peers,
                timestamp,
                sender_addr,
            } => {
                assert_eq!(peers.len(), 2);
                assert_eq!(peers[0].address, "127.0.0.1:8080");
                assert_eq!(
                    peers[0].peer_address,
                    Some("192.168.1.100:8080".to_string())
                );
                assert_eq!(peers[0].failures, 0);
                assert_eq!(peers[1].address, "127.0.0.1:8081");
                assert_eq!(peers[1].peer_address, None);
                assert_eq!(peers[1].failures, 2);
                assert_eq!(timestamp, 12345);
                assert_eq!(sender_addr, "127.0.0.1:9000");
            }
            _ => panic!("Expected PeerListGossip message"),
        }
    }

    #[tokio::test]
    async fn test_peer_list_gossip_tasks_created() {
        let config = GossipConfig {
            enable_peer_discovery: true,
            peer_gossip_interval: None,
            max_peer_gossip_targets: 1,
            allow_loopback_discovery: true,
            ..test_config()
        };

        let registry = GossipRegistry::new("127.0.0.1:9000".parse().unwrap(), config);
        registry.add_peer(test_addr(9001)).await;
        registry.add_peer(test_addr(9002)).await;

        let tasks = registry.gossip_peer_list().await;
        assert_eq!(tasks.len(), 1);

        match &tasks[0].message {
            RegistryMessage::PeerListGossip { sender_addr, .. } => {
                assert_eq!(sender_addr, "127.0.0.1:9000");
            }
            _ => panic!("Expected PeerListGossip message"),
        }
    }

    #[tokio::test]
    async fn test_on_peer_list_gossip_ingests_known_peers_and_candidates() {
        let config = GossipConfig {
            enable_peer_discovery: true,
            allow_loopback_discovery: true,
            max_peers: 1,
            ..test_config()
        };

        let registry = GossipRegistry::new("127.0.0.1:9000".parse().unwrap(), config);

        let node_id = crate::SecretKey::generate().public();
        let peers = vec![
            PeerInfoGossip {
                address: "127.0.0.1:9001".to_string(),
                peer_address: None,
                node_id: Some(node_id),
                failures: 0,
                last_attempt: 1,
                last_success: 1,
            },
            PeerInfoGossip {
                address: "127.0.0.1:9002".to_string(),
                peer_address: None,
                node_id: None,
                failures: 0,
                last_attempt: 1,
                last_success: 1,
            },
        ];

        let candidates = registry
            .on_peer_list_gossip(peers, "127.0.0.1:9003", 1)
            .await;

        assert!(candidates.len() <= 1);

        let mut gossip_state = registry.gossip_state.lock().await;
        let addr_9001: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        assert!(gossip_state.known_peers.get(&addr_9001).is_some());
    }

    #[test]
    fn test_peer_info_local_factory() {
        let addr = test_addr(8080);
        let peer_info = PeerInfo::local(addr);

        assert_eq!(peer_info.address, addr);
        assert!(peer_info.peer_address.is_none());
        assert!(peer_info.node_id.is_none());
        assert_eq!(peer_info.failures, 0);
        assert!(peer_info.last_success > 0); // Should have current timestamp
        assert!(peer_info.last_attempt > 0);
        assert_eq!(peer_info.last_sequence, 0);
        assert_eq!(peer_info.last_sent_sequence, 0);
        assert_eq!(peer_info.consecutive_deltas, 0);
        assert!(peer_info.last_failure_time.is_none());
    }

    #[test]
    fn test_peer_info_gossip_conversion() {
        let addr = test_addr(8080);
        let peer_info = PeerInfo {
            address: addr,
            peer_address: Some(test_addr(8081)),
            node_id: None,
            failures: 3,
            last_attempt: 1000,
            last_success: 900,
            last_sequence: 10,
            last_sent_sequence: 8,
            consecutive_deltas: 5,
            last_failure_time: Some(950),
        };

        // Convert to gossip format
        let gossip = peer_info.to_gossip();
        assert_eq!(gossip.address, "127.0.0.1:8080");
        assert_eq!(gossip.peer_address, Some("127.0.0.1:8081".to_string()));
        assert_eq!(gossip.failures, 3);
        assert_eq!(gossip.last_attempt, 1000);
        assert_eq!(gossip.last_success, 900);

        // Convert back from gossip format
        let restored = PeerInfo::from_gossip(&gossip).unwrap();
        assert_eq!(restored.address, addr);
        assert_eq!(restored.peer_address, Some(test_addr(8081)));
        assert_eq!(restored.failures, 3);
        assert_eq!(restored.last_attempt, 1000);
        assert_eq!(restored.last_success, 900);
        // These fields are not transmitted over gossip, so they should be reset
        assert_eq!(restored.last_sequence, 0);
        assert_eq!(restored.last_sent_sequence, 0);
        assert_eq!(restored.consecutive_deltas, 0);
        assert!(restored.last_failure_time.is_none());
    }

    #[test]
    fn test_peer_info_gossip_serialization() {
        let gossip = PeerInfoGossip {
            address: "10.0.0.1:9000".to_string(),
            peer_address: Some("192.168.1.50:9000".to_string()),
            node_id: None,
            failures: 5,
            last_attempt: 5000,
            last_success: 4000,
        };

        // Serialize
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&gossip).unwrap();

        // Deserialize
        let deserialized: PeerInfoGossip =
            rkyv::from_bytes::<PeerInfoGossip, rkyv::rancor::Error>(&serialized).unwrap();

        assert_eq!(deserialized.address, "10.0.0.1:9000");
        assert_eq!(
            deserialized.peer_address,
            Some("192.168.1.50:9000".to_string())
        );
        assert_eq!(deserialized.failures, 5);
        assert_eq!(deserialized.last_attempt, 5000);
        assert_eq!(deserialized.last_success, 4000);
    }
}
