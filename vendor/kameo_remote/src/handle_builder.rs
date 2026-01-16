use crate::{GossipConfig, GossipRegistryHandle, PeerId, Result};
use std::net::SocketAddr;

/// Builder for creating a GossipRegistryHandle with proper peer configuration
pub struct GossipRegistryBuilder {
    bind_addr: SocketAddr,
    peers: Vec<(PeerId, SocketAddr)>,
    config: Option<GossipConfig>,
}

impl GossipRegistryBuilder {
    /// Create a new builder with the bind address
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            peers: Vec::new(),
            config: None,
        }
    }

    /// Set the gossip configuration
    pub fn with_config(mut self, config: GossipConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Add a peer with a specific ID and address
    pub fn add_peer(mut self, peer_id: PeerId, addr: SocketAddr) -> Self {
        self.peers.push((peer_id, addr));
        self
    }

    /// Build and start the gossip registry
    pub async fn build(self) -> Result<GossipRegistryHandle> {
        let config = self.config.unwrap_or_default();
        let keypair = config
            .key_pair
            .as_ref()
            .ok_or_else(|| crate::GossipError::InvalidKeyPair("missing key_pair".to_string()))?;
        let secret_key = keypair.to_secret_key();

        // Create the handle with TLS enabled
        let handle =
            GossipRegistryHandle::new_with_tls(self.bind_addr, secret_key, Some(config)).await?;

        // Add peers with proper IDs
        for (peer_id, addr) in self.peers {
            let peer = handle.add_peer(&peer_id).await;
            // Try to connect but don't fail if the peer isn't up yet
            let _ = peer.connect(&addr).await;
        }

        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    #[ignore = "gossip propagation timing variance in CI"]
    async fn test_builder_pattern() {
        let node1_addr = "127.0.0.1:36001".parse().unwrap();
        let node2_addr = "127.0.0.1:36002".parse().unwrap();

        // Create node1 with builder
        let node1_keypair = crate::KeyPair::new_for_testing("node1");
        let node2_keypair = crate::KeyPair::new_for_testing("node2");
        let node1_id = node1_keypair.peer_id();
        let node2_id = node2_keypair.peer_id();

        let handle1 = GossipRegistryBuilder::new(node1_addr)
            .with_config(GossipConfig {
                key_pair: Some(node1_keypair),
                ..Default::default()
            })
            .add_peer(node2_id, node2_addr)
            .build()
            .await
            .unwrap();

        // Create node2 with builder
        let handle2 = GossipRegistryBuilder::new(node2_addr)
            .with_config(GossipConfig {
                key_pair: Some(node2_keypair),
                ..Default::default()
            })
            .add_peer(node1_id, node1_addr)
            .build()
            .await
            .unwrap();

        // Wait for connection establishment
        sleep(Duration::from_millis(500)).await;

        // Register actors with socket addresses (kameo_remote still uses SocketAddr for actor locations)
        handle1
            .register("actor1".to_string(), "127.0.0.1:47001".parse().unwrap())
            .await
            .unwrap();
        handle2
            .register("actor2".to_string(), "127.0.0.1:47002".parse().unwrap())
            .await
            .unwrap();

        // Wait for gossip to propagate (gossip interval is typically 1s)
        sleep(Duration::from_secs(2)).await;

        // Test discovery
        let actor2_from_1 = handle1.lookup("actor2").await;
        let actor1_from_2 = handle2.lookup("actor1").await;

        assert!(actor2_from_1.is_some());
        assert!(actor1_from_2.is_some());

        // Cleanup
        handle1.shutdown().await;
        handle2.shutdown().await;
    }
}
