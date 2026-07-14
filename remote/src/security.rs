//! The pre-shared cluster key and the subkeys derived from it.

use std::fmt;

use chacha20poly1305::aead::{OsRng, rand_core::RngCore};
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroize;

/// HKDF salt and MAC label prefix; bump together with the wire protocol version.
const KDF_SALT: &[u8] = b"kameo-remote v1";

/// A 32-byte pre-shared cluster secret.
///
/// Nodes configured with the same key form a closed cluster: gossip datagrams are
/// encrypted and authenticated with a key derived from it, and messaging connections
/// prove possession of it during the handshake without ever sending it over the wire.
///
/// The key must be 32 uniformly random bytes (from [`generate`](ClusterKey::generate)
/// or a secret manager's random generator), not a passphrase: handshake MACs and
/// gossip packets are observable by anyone on the network, so a guessable key can be
/// brute-forced offline. If a passphrase is all you have, run it through a password
/// KDF (argon2, scrypt) first.
///
/// The key is redacted from `Debug` output and zeroed on drop. Distributing it to
/// nodes (environment, secret manager, ...) is up to the application.
#[derive(Clone)]
pub struct ClusterKey([u8; 32]);

impl ClusterKey {
    /// Creates a key from 32 raw bytes.
    pub fn new(key: [u8; 32]) -> Self {
        ClusterKey(key)
    }

    /// Generates a random key, for provisioning new clusters.
    pub fn generate() -> Self {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        ClusterKey(key)
    }
}

impl From<[u8; 32]> for ClusterKey {
    fn from(key: [u8; 32]) -> Self {
        ClusterKey(key)
    }
}

impl fmt::Debug for ClusterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClusterKey(..)")
    }
}

impl Drop for ClusterKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// The subkey encrypting gossip datagrams.
pub(crate) struct GossipKey(pub(crate) [u8; 32]);

/// The subkey authenticating messaging handshakes.
pub(crate) struct HandshakeKey(pub(crate) [u8; 32]);

impl Drop for GossipKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl Drop for HandshakeKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Derives independent subkeys from the cluster key, so gossip ciphertexts and
/// handshake MACs can never be confused for one another.
pub(crate) fn derive_keys(key: &ClusterKey) -> (GossipKey, HandshakeKey) {
    let hkdf = Hkdf::<Sha256>::new(Some(KDF_SALT), &key.0);
    let mut gossip = [0u8; 32];
    hkdf.expand(b"gossip aead", &mut gossip)
        .expect("32 bytes is a valid hkdf-sha256 output length");
    let mut handshake = [0u8; 32];
    hkdf.expand(b"messaging handshake mac", &mut handshake)
        .expect("32 bytes is a valid hkdf-sha256 output length");
    (GossipKey(gossip), HandshakeKey(handshake))
}

/// Everything a messaging connection (either direction) needs to secure itself.
pub(crate) struct ConnSecurity {
    pub(crate) cluster_id: String,
    pub(crate) handshake_key: Option<HandshakeKey>,
    #[cfg(feature = "tls")]
    pub(crate) tls: Option<crate::tls::TlsConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivation_is_deterministic() {
        let key = ClusterKey::new([7u8; 32]);
        let (gossip_a, handshake_a) = derive_keys(&key);
        let (gossip_b, handshake_b) = derive_keys(&key);
        assert_eq!(gossip_a.0, gossip_b.0);
        assert_eq!(handshake_a.0, handshake_b.0);
    }

    #[test]
    fn subkeys_are_independent() {
        let key = ClusterKey::new([7u8; 32]);
        let (gossip, handshake) = derive_keys(&key);
        assert_ne!(gossip.0, handshake.0);
        assert_ne!(gossip.0, [7u8; 32]);
        assert_ne!(handshake.0, [7u8; 32]);
    }

    #[test]
    fn different_keys_derive_different_subkeys() {
        let (gossip_a, _) = derive_keys(&ClusterKey::new([1u8; 32]));
        let (gossip_b, _) = derive_keys(&ClusterKey::new([2u8; 32]));
        assert_ne!(gossip_a.0, gossip_b.0);
    }

    #[test]
    fn debug_redacts_key_bytes() {
        let key = ClusterKey::new([0xAB; 32]);
        let output = format!("{key:?}");
        assert_eq!(output, "ClusterKey(..)");
    }

    #[test]
    fn generate_produces_distinct_keys() {
        let a = ClusterKey::generate();
        let b = ClusterKey::generate();
        assert_ne!(a.0, b.0);
    }
}
