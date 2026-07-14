//! Encrypted UDP transport for gossip.
//!
//! Every datagram is sealed with XChaCha20-Poly1305 under a key derived from the
//! cluster key, with the cluster id as associated data. Packets that fail to
//! authenticate are dropped, so nodes without the key can neither read the registry
//! nor inject anything into it.
//!
//! Wire format: `[version: 1 byte][nonce: 24 bytes][ciphertext + tag]`. Random
//! 24-byte nonces stay collision-free without coordination even though every node in
//! the cluster encrypts under the same key.

use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use chacha20poly1305::{
    KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, AeadCore, OsRng, Payload},
};
use chitchat::{
    ChitchatMessage, Deserializable, Serializable,
    transport::{Socket, Transport},
};

use crate::security::GossipKey;

const WIRE_VERSION: u8 = 1;
const NONCE_LEN: usize = 24;
const AAD_PREFIX: &[u8] = b"kameo-remote gossip v1";
/// The largest payload that fits in a single UDP datagram, mirroring chitchat's
/// internal constant.
const MAX_UDP_PAYLOAD: usize = 65_507;
const RECV_WARN_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) struct EncryptedUdpTransport {
    key: GossipKey,
    cluster_id: String,
}

impl EncryptedUdpTransport {
    pub(crate) fn new(key: GossipKey, cluster_id: String) -> Self {
        EncryptedUdpTransport { key, cluster_id }
    }
}

#[async_trait]
impl Transport for EncryptedUdpTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let socket = tokio::net::UdpSocket::bind(listen_addr)
            .await
            .with_context(|| format!("failed to bind to {listen_addr}/UDP for gossip"))?;
        let mut aad = AAD_PREFIX.to_vec();
        aad.extend_from_slice(self.cluster_id.as_bytes());
        Ok(Box::new(EncryptedUdpSocket {
            socket,
            cipher: XChaCha20Poly1305::new((&self.key.0).into()),
            aad,
            buf_send: Vec::with_capacity(MAX_UDP_PAYLOAD),
            buf_recv: Box::new([0u8; MAX_UDP_PAYLOAD]),
            last_recv_warn: None,
        }))
    }
}

struct EncryptedUdpSocket {
    socket: tokio::net::UdpSocket,
    cipher: XChaCha20Poly1305,
    aad: Vec<u8>,
    buf_send: Vec<u8>,
    buf_recv: Box<[u8; MAX_UDP_PAYLOAD]>,
    last_recv_warn: Option<Instant>,
}

#[async_trait]
impl Socket for EncryptedUdpSocket {
    async fn send(&mut self, to_addr: SocketAddr, message: ChitchatMessage) -> anyhow::Result<()> {
        self.buf_send.clear();
        message.serialize(&mut self.buf_send);
        // The 41-byte overhead can push a message chitchat budgeted at exactly the
        // datagram maximum over the limit; real gossip messages are far smaller.
        let packet = seal(&self.cipher, &self.aad, &self.buf_send);
        self.socket
            .send_to(&packet, to_addr)
            .await
            .context("failed to send chitchat message to peer")?;
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        loop {
            if let Some(message) = self.receive_one().await? {
                return Ok(message);
            }
        }
    }
}

impl EncryptedUdpSocket {
    async fn receive_one(&mut self) -> anyhow::Result<Option<(SocketAddr, ChitchatMessage)>> {
        let (len, from_addr) = match self.socket.recv_from(&mut self.buf_recv[..]).await {
            Ok(result) => result,
            Err(err) if is_transient_io_error(&err) => {
                tracing::warn!("transient udp recv error: {err}");
                return Ok(None);
            }
            // A recv error ends the gossip server, matching the plaintext transport;
            // unauthenticated packets below never do.
            Err(err) => return Err(err).context("fatal udp recv error"),
        };
        let Some(plaintext) = open(&self.cipher, &self.aad, &self.buf_recv[..len]) else {
            self.log_dropped(from_addr);
            return Ok(None);
        };
        let mut buf = plaintext.as_slice();
        match ChitchatMessage::deserialize(&mut buf) {
            Ok(message) => Ok(Some((from_addr, message))),
            Err(err) => {
                tracing::warn!(from = %from_addr, "invalid gossip payload: {err}");
                Ok(None)
            }
        }
    }

    /// Rate-limited: a peer with the wrong key retries every gossip round, which
    /// would otherwise flood the log.
    fn log_dropped(&mut self, from_addr: SocketAddr) {
        let now = Instant::now();
        let warn = self
            .last_recv_warn
            .is_none_or(|last| now.duration_since(last) >= RECV_WARN_INTERVAL);
        if warn {
            self.last_recv_warn = Some(now);
            tracing::warn!(from = %from_addr, "dropping unauthenticated gossip packet");
        } else {
            tracing::debug!(from = %from_addr, "dropping unauthenticated gossip packet");
        }
    }
}

fn is_transient_io_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::OutOfMemory
            // On Windows, sending to a closed peer causes recv_from to fail
            // with ConnectionReset (WSAECONNRESET / 10054).
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
    )
}

fn seal(cipher: &XChaCha20Poly1305, aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .expect("xchacha20poly1305 encryption of an in-memory buffer cannot fail");
    let mut packet = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
    packet.push(WIRE_VERSION);
    packet.extend_from_slice(&nonce);
    packet.extend_from_slice(&ciphertext);
    packet
}

fn open(cipher: &XChaCha20Poly1305, aad: &[u8], packet: &[u8]) -> Option<Vec<u8>> {
    let rest = packet.strip_prefix(&[WIRE_VERSION])?;
    if rest.len() < NONCE_LEN {
        return None;
    }
    let (nonce, ciphertext) = rest.split_at(NONCE_LEN);
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::{ClusterKey, derive_keys};

    fn cipher(key_byte: u8) -> XChaCha20Poly1305 {
        let (gossip_key, _) = derive_keys(&ClusterKey::new([key_byte; 32]));
        XChaCha20Poly1305::new((&gossip_key.0).into())
    }

    #[test]
    fn seal_open_round_trip() {
        let cipher = cipher(1);
        let packet = seal(&cipher, b"aad", b"hello gossip");
        assert_eq!(open(&cipher, b"aad", &packet).unwrap(), b"hello gossip");
    }

    #[test]
    fn tampered_packet_rejected() {
        let cipher = cipher(1);
        let mut packet = seal(&cipher, b"aad", b"hello gossip");
        let last = packet.len() - 1;
        packet[last] ^= 1;
        assert!(open(&cipher, b"aad", &packet).is_none());
    }

    #[test]
    fn wrong_key_rejected() {
        let packet = seal(&cipher(1), b"aad", b"hello gossip");
        assert!(open(&cipher(2), b"aad", &packet).is_none());
    }

    #[test]
    fn wrong_cluster_aad_rejected() {
        let cipher = cipher(1);
        let packet = seal(&cipher, b"cluster-a", b"hello gossip");
        assert!(open(&cipher, b"cluster-b", &packet).is_none());
    }

    #[test]
    fn wrong_version_rejected() {
        let cipher = cipher(1);
        let mut packet = seal(&cipher, b"aad", b"hello gossip");
        packet[0] = 2;
        assert!(open(&cipher, b"aad", &packet).is_none());
    }

    #[test]
    fn truncated_packet_rejected() {
        let cipher = cipher(1);
        let packet = seal(&cipher, b"aad", b"hello gossip");
        assert!(open(&cipher, b"aad", &packet[..NONCE_LEN]).is_none());
        assert!(open(&cipher, b"aad", &[]).is_none());
    }

    fn gossip_key(key_byte: u8) -> GossipKey {
        derive_keys(&ClusterKey::new([key_byte; 32])).0
    }

    /// The Socket trait hides the bound address, so reserve an ephemeral port first
    /// (best-effort, matching how bootstrap allocates ephemeral gossip ports).
    fn reserve_udp_port() -> SocketAddr {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    }

    #[tokio::test]
    async fn socket_pair_round_trip() {
        let addr_a = reserve_udp_port();
        let mut socket_a = EncryptedUdpTransport::new(gossip_key(1), "kameo".to_string())
            .open(addr_a)
            .await
            .unwrap();
        let mut socket_b = EncryptedUdpTransport::new(gossip_key(1), "kameo".to_string())
            .open("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        socket_b
            .send(addr_a, ChitchatMessage::BadCluster)
            .await
            .unwrap();
        let (_, message) = tokio::time::timeout(Duration::from_secs(5), socket_a.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(message, ChitchatMessage::BadCluster));
    }

    #[tokio::test]
    async fn recv_skips_garbage_and_wrong_key_packets() {
        let addr_a = reserve_udp_port();
        let mut socket_a = EncryptedUdpTransport::new(gossip_key(1), "kameo".to_string())
            .open(addr_a)
            .await
            .unwrap();
        let mut wrong_key = EncryptedUdpTransport::new(gossip_key(2), "kameo".to_string())
            .open("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let mut socket_b = EncryptedUdpTransport::new(gossip_key(1), "kameo".to_string())
            .open("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let raw = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        raw.send_to(b"garbage", addr_a).await.unwrap();
        wrong_key
            .send(addr_a, ChitchatMessage::BadCluster)
            .await
            .unwrap();
        socket_b
            .send(addr_a, ChitchatMessage::BadCluster)
            .await
            .unwrap();

        // Only the authentic packet comes through; the garbage and wrong-key packets
        // are dropped without ending the socket.
        let (_, message) = tokio::time::timeout(Duration::from_secs(5), socket_a.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(message, ChitchatMessage::BadCluster));
        let second = tokio::time::timeout(Duration::from_millis(500), socket_a.recv()).await;
        assert!(second.is_err(), "dropped packets must not be delivered");
    }
}
