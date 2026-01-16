//! Peer Discovery Manager
//!
//! Handles automatic peer discovery through gossip, including:
//! - Filtering discovered peers (self, already connected, SSRF/bogon)
//! - Exponential backoff for failed connection attempts
//! - Soft cap enforcement for connection limits

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tracing::{debug, warn};

use crate::current_timestamp;
use crate::registry::PeerInfoGossip;

/// Maximum consecutive failures before removing a peer from discovery
pub const MAX_PEER_FAILURES: u8 = 10;

/// Maximum backoff time in seconds (1 hour)
pub const MAX_BACKOFF_SECONDS: u64 = 3600;

/// Failure state for tracking backoff
#[derive(Debug, Clone)]
pub struct FailureState {
    /// Number of consecutive failures
    pub consecutive_failures: u8,
    /// Timestamp of last failure
    pub last_failure: u64,
}

impl FailureState {
    /// Create a new failure state with initial failure
    pub fn new() -> Self {
        Self {
            consecutive_failures: 1,
            last_failure: current_timestamp(),
        }
    }

    /// Calculate backoff time in seconds using exponential backoff
    /// Formula: 2^failures seconds, capped at MAX_BACKOFF_SECONDS
    pub fn backoff_seconds(&self) -> u64 {
        let backoff = 2u64.saturating_pow(self.consecutive_failures as u32);
        backoff.min(MAX_BACKOFF_SECONDS)
    }

    /// Check if we should retry connecting to this peer
    pub fn should_retry(&self, now: u64) -> bool {
        let backoff = self.backoff_seconds();
        now >= self.last_failure.saturating_add(backoff)
    }
}

impl Default for FailureState {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for peer discovery
#[derive(Debug, Clone)]
pub struct PeerDiscoveryConfig {
    /// Maximum number of peers to maintain (soft cap)
    pub max_peers: usize,
    /// Allow discovery of private IP addresses (10.x, 172.16-31.x, 192.168.x)
    pub allow_private_discovery: bool,
    /// Allow discovery of loopback addresses (127.x.x.x)
    pub allow_loopback_discovery: bool,
    /// Allow discovery of link-local addresses (169.254.x.x)
    pub allow_link_local_discovery: bool,
    /// Time-to-live for failed peers before eviction
    pub fail_ttl: Duration,
    /// Time-to-live for pending peers before eviction
    pub pending_ttl: Duration,
}

impl Default for PeerDiscoveryConfig {
    fn default() -> Self {
        Self {
            max_peers: 100,
            allow_private_discovery: true, // Allow private IPs by default
            allow_loopback_discovery: false, // Block loopback by default
            allow_link_local_discovery: false, // Block link-local by default
            fail_ttl: Duration::from_secs(6 * 60 * 60), // 6h matches GossipConfig default
            pending_ttl: Duration::from_secs(60 * 60), // 1h matches GossipConfig default
        }
    }
}

/// Summary of expired entries removed during cleanup
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PeerDiscoveryCleanupStats {
    pub pending_removed: usize,
    pub failed_removed: usize,
}

/// Peer Discovery Manager
///
/// Manages peer discovery through gossip, implementing:
/// - Self and duplicate filtering
/// - SSRF/bogon protection
/// - Exponential backoff for failed peers
/// - Soft cap enforcement
#[derive(Debug)]
pub struct PeerDiscovery {
    /// Configuration
    config: PeerDiscoveryConfig,
    /// Local address (for self-filtering)
    local_addr: SocketAddr,
    /// Currently connected peers
    connected_peers: HashSet<SocketAddr>,
    /// Peers pending connection (addr -> timestamp when added to pending)
    pending_peers: HashMap<SocketAddr, u64>,
    /// Failed peers with backoff state
    failed_peers: HashMap<SocketAddr, FailureState>,
}

impl PeerDiscovery {
    /// Create a new peer discovery manager
    pub fn new(local_addr: SocketAddr, config: PeerDiscoveryConfig) -> Self {
        Self {
            config,
            local_addr,
            connected_peers: HashSet::new(),
            pending_peers: HashMap::new(),
            failed_peers: HashMap::new(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(local_addr: SocketAddr) -> Self {
        Self::new(local_addr, PeerDiscoveryConfig::default())
    }

    /// Process incoming peer list gossip and return candidates to connect to
    ///
    /// Filters out:
    /// - Self
    /// - Already connected peers
    /// - Pending connection peers
    /// - Peers in backoff
    /// - Unsafe addresses (bogon/SSRF)
    ///
    /// Returns candidates limited by soft cap
    pub fn on_peer_list_gossip(&mut self, peers: &[PeerInfoGossip]) -> Vec<SocketAddr> {
        let now = current_timestamp();
        let mut candidates = Vec::new();

        // Calculate how many more peers we can connect to
        let current_count = self.connected_peers.len();
        let remaining_slots = self.config.max_peers.saturating_sub(current_count);

        if remaining_slots == 0 {
            debug!(
                current = current_count,
                max = self.config.max_peers,
                "at soft cap, not accepting new peer candidates"
            );
            return candidates;
        }

        for peer_gossip in peers {
            // Parse the address
            let addr: SocketAddr = match peer_gossip.address.parse() {
                Ok(a) => a,
                Err(e) => {
                    debug!(addr = %peer_gossip.address, error = %e, "failed to parse peer address");
                    continue;
                }
            };

            // Filter self
            if addr == self.local_addr {
                continue;
            }

            // Filter already connected
            if self.connected_peers.contains(&addr) {
                continue;
            }

            // Filter pending
            if self.pending_peers.contains_key(&addr) {
                continue;
            }

            // Filter peers in backoff
            if let Some(failure_state) = self.failed_peers.get(&addr) {
                if !failure_state.should_retry(now) {
                    debug!(
                        addr = %addr,
                        failures = failure_state.consecutive_failures,
                        backoff_secs = failure_state.backoff_seconds(),
                        "peer in backoff, skipping"
                    );
                    continue;
                }
                // Backoff expired, can retry
            }

            // Filter unsafe addresses
            if !self.is_safe_to_dial(&addr) {
                debug!(addr = %addr, "address blocked by security filter");
                continue;
            }

            candidates.push(addr);

            // Stop at soft cap
            if candidates.len() >= remaining_slots {
                break;
            }
        }

        // Mark candidates as pending
        for addr in &candidates {
            self.pending_peers.insert(*addr, now);
        }

        candidates
    }

    /// Check if an address is safe to dial (SSRF/bogon filtering)
    pub fn is_safe_to_dial(&self, addr: &SocketAddr) -> bool {
        match addr.ip() {
            IpAddr::V4(ipv4) => {
                // Check loopback (127.x.x.x)
                if ipv4.is_loopback() && !self.config.allow_loopback_discovery {
                    return false;
                }

                // Check link-local (169.254.x.x)
                if ipv4.is_link_local() && !self.config.allow_link_local_discovery {
                    return false;
                }

                // Check private (10.x, 172.16-31.x, 192.168.x)
                if ipv4.is_private() && !self.config.allow_private_discovery {
                    return false;
                }

                // Check unspecified (0.0.0.0)
                if ipv4.is_unspecified() {
                    return false;
                }

                // Check broadcast (255.255.255.255)
                if ipv4.is_broadcast() {
                    return false;
                }

                // Check documentation ranges (192.0.2.x, 198.51.100.x, 203.0.113.x)
                if ipv4.is_documentation() {
                    return false;
                }

                true
            }
            IpAddr::V6(ipv6) => self.is_safe_ipv6(&ipv6),
        }
    }

    fn is_safe_ipv6(&self, ipv6: &Ipv6Addr) -> bool {
        if ipv6.is_loopback() && !self.config.allow_loopback_discovery {
            return false;
        }

        if ipv6.is_unspecified() {
            return false;
        }

        if ipv6.is_unicast_link_local() && !self.config.allow_link_local_discovery {
            return false;
        }

        if ipv6.is_unique_local() && !self.config.allow_private_discovery {
            return false;
        }

        true
    }

    /// Calculate if we should retry connecting to a peer based on backoff
    pub fn should_retry(&self, addr: &SocketAddr) -> bool {
        match self.failed_peers.get(addr) {
            Some(state) => state.should_retry(current_timestamp()),
            None => true, // No failure record, can try
        }
    }

    /// Record a connection failure for a peer
    ///
    /// Returns true if the peer should be removed (exceeded max failures)
    pub fn on_peer_failure(&mut self, addr: SocketAddr) -> bool {
        // Remove from pending if present
        self.pending_peers.remove(&addr);

        let now = current_timestamp();

        let should_remove = if let Some(state) = self.failed_peers.get_mut(&addr) {
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            state.last_failure = now;

            if state.consecutive_failures >= MAX_PEER_FAILURES {
                warn!(
                    addr = %addr,
                    failures = state.consecutive_failures,
                    "peer exceeded max failures, removing from discovery"
                );
                true
            } else {
                debug!(
                    addr = %addr,
                    failures = state.consecutive_failures,
                    backoff_secs = state.backoff_seconds(),
                    "peer connection failed, applying backoff"
                );
                false
            }
        } else {
            // First failure
            self.failed_peers.insert(addr, FailureState::new());
            debug!(
                addr = %addr,
                failures = 1,
                "first connection failure for peer"
            );
            false
        };

        if should_remove {
            self.failed_peers.remove(&addr);
        }

        should_remove
    }

    /// Record a successful connection to a peer
    pub fn on_peer_connected(&mut self, addr: SocketAddr) {
        // Remove from pending and failed
        self.pending_peers.remove(&addr);
        self.failed_peers.remove(&addr);

        // Add to connected
        self.connected_peers.insert(addr);

        debug!(
            addr = %addr,
            connected_count = self.connected_peers.len(),
            "peer connected"
        );
    }

    /// Record a peer disconnection
    pub fn on_peer_disconnected(&mut self, addr: SocketAddr) {
        self.connected_peers.remove(&addr);

        debug!(
            addr = %addr,
            connected_count = self.connected_peers.len(),
            "peer disconnected"
        );
    }

    /// Get the current number of connected peers
    pub fn peer_count(&self) -> usize {
        self.connected_peers.len()
    }

    /// Check if we're at the soft cap
    pub fn at_soft_cap(&self) -> bool {
        self.connected_peers.len() >= self.config.max_peers
    }

    /// Get remaining slots available for new connections
    pub fn remaining_slots(&self) -> usize {
        self.config
            .max_peers
            .saturating_sub(self.connected_peers.len())
    }

    /// Clear the pending state for an address (e.g., after timeout)
    pub fn clear_pending(&mut self, addr: &SocketAddr) {
        self.pending_peers.remove(addr);
    }

    /// Get configuration
    pub fn config(&self) -> &PeerDiscoveryConfig {
        &self.config
    }

    /// Get the count of failed peers (for monitoring metrics)
    pub fn failed_peer_count(&self) -> usize {
        self.failed_peers.len()
    }

    /// Get the count of connected peers
    pub fn connected_peer_count(&self) -> usize {
        self.connected_peers.len()
    }

    /// Get the count of pending peers
    pub fn pending_peer_count(&self) -> usize {
        self.pending_peers.len()
    }

    /// Remove expired pending/failed peers based on configured TTLs
    pub fn cleanup_expired(&mut self, now: u64) -> PeerDiscoveryCleanupStats {
        let mut stats = PeerDiscoveryCleanupStats::default();

        // Remove pending peers that exceeded pending_ttl
        let pending_ttl = self.config.pending_ttl.as_secs();
        if pending_ttl > 0 {
            self.pending_peers.retain(|_, added_at| {
                if now.saturating_sub(*added_at) > pending_ttl {
                    stats.pending_removed += 1;
                    false
                } else {
                    true
                }
            });
        }

        // Remove failed peers that exceeded fail_ttl
        let fail_ttl = self.config.fail_ttl.as_secs();
        if fail_ttl > 0 {
            self.failed_peers.retain(|_, state| {
                if now.saturating_sub(state.last_failure) > fail_ttl {
                    stats.failed_removed += 1;
                    false
                } else {
                    true
                }
            });
        }

        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port)
    }

    fn loopback_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn link_local_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)), port)
    }

    fn private_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100)), port)
    }

    fn public_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), port)
    }

    fn ipv6_loopback_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)
    }

    fn ipv6_link_local_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)), port)
    }

    fn ipv6_unique_local_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)), port)
    }

    fn create_peer_gossip(addr: &str) -> PeerInfoGossip {
        PeerInfoGossip {
            address: addr.to_string(),
            peer_address: None,
            node_id: None,
            failures: 0,
            last_attempt: 0,
            last_success: 0,
        }
    }

    #[test]
    fn test_filter_self() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peers = vec![
            create_peer_gossip("10.0.0.1:8080"), // self
            create_peer_gossip("10.0.0.2:8080"), // different peer
        ];

        let candidates = discovery.on_peer_list_gossip(&peers);

        // Should only return the different peer, not self
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0],
            test_addr(8080)
                .ip()
                .to_string()
                .replace("10.0.0.1", "10.0.0.2")
                .parse::<SocketAddr>()
                .unwrap_or(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                    8080
                ))
        );
    }

    #[test]
    fn test_filter_connected_peers() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        // Mark peer as connected
        let connected_peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 8081);
        discovery.on_peer_connected(connected_peer);

        let peers = vec![
            create_peer_gossip("10.0.0.2:8081"), // already connected
            create_peer_gossip("10.0.0.3:8082"), // new peer
        ];

        let candidates = discovery.on_peer_list_gossip(&peers);

        // Should only return the new peer
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 8082)
        );
    }

    #[test]
    fn test_exponential_backoff() {
        let mut state = FailureState {
            consecutive_failures: 0,
            last_failure: 0,
        };

        // Test exponential backoff: 2^n
        state.consecutive_failures = 1;
        assert_eq!(state.backoff_seconds(), 2); // 2^1 = 2

        state.consecutive_failures = 2;
        assert_eq!(state.backoff_seconds(), 4); // 2^2 = 4

        state.consecutive_failures = 3;
        assert_eq!(state.backoff_seconds(), 8); // 2^3 = 8

        state.consecutive_failures = 5;
        assert_eq!(state.backoff_seconds(), 32); // 2^5 = 32

        state.consecutive_failures = 10;
        assert_eq!(state.backoff_seconds(), 1024); // 2^10 = 1024

        // Test cap at MAX_BACKOFF_SECONDS (3600)
        state.consecutive_failures = 12;
        assert_eq!(state.backoff_seconds(), 3600); // 2^12 = 4096, capped at 3600

        state.consecutive_failures = 20;
        assert_eq!(state.backoff_seconds(), 3600); // Still capped
    }

    #[test]
    fn test_bogon_filtering_loopback() {
        let local = public_addr(8080);

        // With loopback disabled (default)
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&loopback_addr(8080)));

        // With loopback enabled
        let config = PeerDiscoveryConfig {
            allow_loopback_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&loopback_addr(8080)));
    }

    #[test]
    fn test_bogon_filtering_link_local() {
        let local = public_addr(8080);

        // With link-local disabled (default)
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&link_local_addr(8080)));

        // With link-local enabled
        let config = PeerDiscoveryConfig {
            allow_link_local_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&link_local_addr(8080)));
    }

    #[test]
    fn test_private_allowed_default() {
        let local = public_addr(8080);

        // Private IPs should be allowed by default
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(discovery.is_safe_to_dial(&private_addr(8080)));

        // With private disabled
        let config = PeerDiscoveryConfig {
            allow_private_discovery: false,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(!discovery.is_safe_to_dial(&private_addr(8080)));
    }

    #[test]
    fn test_ipv6_loopback_blocked_by_default() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&ipv6_loopback_addr(8080)));

        let config = PeerDiscoveryConfig {
            allow_loopback_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&ipv6_loopback_addr(8080)));
    }

    #[test]
    fn test_ipv6_link_local_blocked_by_default() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&ipv6_link_local_addr(8080)));

        let config = PeerDiscoveryConfig {
            allow_link_local_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&ipv6_link_local_addr(8080)));
    }

    #[test]
    fn test_ipv6_unique_local_respects_private_flag() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(discovery.is_safe_to_dial(&ipv6_unique_local_addr(8080)));

        let config = PeerDiscoveryConfig {
            allow_private_discovery: false,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(!discovery.is_safe_to_dial(&ipv6_unique_local_addr(8080)));
    }

    #[test]
    fn test_max_failures_removal() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // Apply failures up to MAX_PEER_FAILURES - 1
        for i in 1..MAX_PEER_FAILURES {
            let removed = discovery.on_peer_failure(peer);
            assert!(!removed, "peer should not be removed after {} failures", i);
            assert!(discovery.failed_peers.contains_key(&peer));
        }

        // The MAX_PEER_FAILURES-th failure should remove the peer
        let removed = discovery.on_peer_failure(peer);
        assert!(
            removed,
            "peer should be removed after {} failures",
            MAX_PEER_FAILURES
        );
        assert!(!discovery.failed_peers.contains_key(&peer));
    }

    #[test]
    fn test_soft_cap_limiting() {
        let local = test_addr(8080);
        let config = PeerDiscoveryConfig {
            max_peers: 3,
            ..Default::default()
        };
        let mut discovery = PeerDiscovery::new(local, config);

        // Connect 2 peers (leaving 1 slot)
        discovery.on_peer_connected(test_addr(9001));
        discovery.on_peer_connected(test_addr(9002));

        assert_eq!(discovery.peer_count(), 2);
        assert_eq!(discovery.remaining_slots(), 1);

        // Try to add 5 more peers via gossip
        let peers = vec![
            create_peer_gossip("10.0.0.10:8000"),
            create_peer_gossip("10.0.0.11:8001"),
            create_peer_gossip("10.0.0.12:8002"),
            create_peer_gossip("10.0.0.13:8003"),
            create_peer_gossip("10.0.0.14:8004"),
        ];

        let candidates = discovery.on_peer_list_gossip(&peers);

        // Should only return 1 candidate (remaining slot based on connected peers)
        assert_eq!(candidates.len(), 1);
        // remaining_slots only considers connected peers, not pending
        assert_eq!(discovery.remaining_slots(), 1);
        // The candidate should be in pending
        assert_eq!(discovery.pending_peers.len(), 1);
        assert!(discovery.pending_peers.contains_key(&candidates[0]));
    }

    #[test]
    fn test_peer_connection_lifecycle() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // Initially not connected
        assert_eq!(discovery.peer_count(), 0);
        assert!(!discovery.at_soft_cap());

        // Connect peer
        discovery.on_peer_connected(peer);
        assert_eq!(discovery.peer_count(), 1);
        assert!(discovery.connected_peers.contains(&peer));

        // Disconnect peer
        discovery.on_peer_disconnected(peer);
        assert_eq!(discovery.peer_count(), 0);
        assert!(!discovery.connected_peers.contains(&peer));
    }

    #[test]
    fn test_should_retry_backoff() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // No failure record - should retry
        assert!(discovery.should_retry(&peer));

        // Add failure (backoff = 2 seconds)
        discovery.on_peer_failure(peer);

        // Immediately after failure - should NOT retry
        // (depends on timing, but backoff is at least 2 seconds)
        // We check the state directly
        let state = discovery.failed_peers.get(&peer).unwrap();
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.backoff_seconds(), 2);
    }

    #[test]
    fn test_unspecified_and_broadcast_blocked() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);

        // Unspecified (0.0.0.0) should be blocked
        let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080);
        assert!(!discovery.is_safe_to_dial(&unspecified));

        // Broadcast (255.255.255.255) should be blocked
        let broadcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)), 8080);
        assert!(!discovery.is_safe_to_dial(&broadcast));
    }

    #[test]
    fn test_clear_pending() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peers = vec![create_peer_gossip("10.0.0.10:8000")];
        let candidates = discovery.on_peer_list_gossip(&peers);

        assert_eq!(candidates.len(), 1);
        let peer = candidates[0];

        // Peer should be in pending
        assert!(discovery.pending_peers.contains_key(&peer));

        // Clear pending
        discovery.clear_pending(&peer);
        assert!(!discovery.pending_peers.contains_key(&peer));
    }

    #[test]
    fn test_connection_clears_failure_state() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // Add some failures
        discovery.on_peer_failure(peer);
        discovery.on_peer_failure(peer);
        assert!(discovery.failed_peers.contains_key(&peer));

        // Connect the peer
        discovery.on_peer_connected(peer);

        // Failure state should be cleared
        assert!(!discovery.failed_peers.contains_key(&peer));
        assert!(discovery.connected_peers.contains(&peer));
    }

    #[test]
    fn test_cleanup_expired_entries() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::new(
            local,
            PeerDiscoveryConfig {
                pending_ttl: Duration::from_secs(1),
                fail_ttl: Duration::from_secs(2),
                ..Default::default()
            },
        );

        let pending_peer = test_addr(9000);
        let failed_peer = test_addr(9001);

        discovery.pending_peers.insert(pending_peer, 0);
        discovery.failed_peers.insert(
            failed_peer,
            FailureState {
                consecutive_failures: 1,
                last_failure: 1,
            },
        );

        // Advance time beyond pending TTL but within failed TTL
        let now = 3;

        let stats = discovery.cleanup_expired(now);
        assert_eq!(stats.pending_removed, 1);
        assert_eq!(stats.failed_removed, 0);
        assert!(!discovery.pending_peers.contains_key(&pending_peer));
        assert!(discovery.failed_peers.contains_key(&failed_peer));

        // Advance past fail_ttl as well
        let stats = discovery.cleanup_expired(now + 2);
        assert_eq!(stats.failed_removed, 1);
        assert!(!discovery.failed_peers.contains_key(&failed_peer));
    }
}
