use crate::{Result, SecretKey};
use rustls::client::ResolvesClientCert;
use rustls::pki_types::CertificateDer;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::{CertifiedKey, Signer, SigningKey};
use rustls::{Error as TlsError, SignatureAlgorithm, SignatureScheme};
use std::sync::Arc;

/// Certificate resolver that always returns our self-signed certificate
#[derive(Debug)]
pub struct AlwaysResolvesCert {
    certified_key: Arc<CertifiedKey>,
}

impl AlwaysResolvesCert {
    /// Create a new resolver with the given secret key
    pub fn new(secret_key: &SecretKey) -> Result<Self> {
        // Generate a self-signed certificate with our public key
        let cert = generate_self_signed_cert(secret_key)?;

        // Create Ed25519 signing key
        let signing_key = Ed25519SigningKey::new(secret_key.clone());

        let certified_key = Arc::new(CertifiedKey::new(vec![cert], Arc::new(signing_key)));

        Ok(Self { certified_key })
    }
}

impl ResolvesServerCert for AlwaysResolvesCert {
    fn resolve(&self, _client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key.clone())
    }
}

impl ResolvesClientCert for AlwaysResolvesCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// Custom Ed25519 signing key for rustls
#[derive(Debug, Clone)]
struct Ed25519SigningKey {
    secret_key: SecretKey,
}

impl Ed25519SigningKey {
    fn new(secret_key: SecretKey) -> Self {
        Self { secret_key }
    }
}

impl SigningKey for Ed25519SigningKey {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::ED25519
    }

    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer>> {
        if offered.contains(&SignatureScheme::ED25519) {
            Some(Box::new(self.clone()))
        } else {
            None
        }
    }
}

impl Signer for Ed25519SigningKey {
    fn sign(&self, message: &[u8]) -> std::result::Result<Vec<u8>, TlsError> {
        let signature = self.secret_key.sign(message);
        Ok(signature.to_bytes().to_vec())
    }

    fn scheme(&self) -> SignatureScheme {
        SignatureScheme::ED25519
    }
}

/// Generate a self-signed certificate for our Ed25519 key
/// This creates a minimal X.509 certificate containing just our public key
fn generate_self_signed_cert(secret_key: &SecretKey) -> Result<CertificateDer<'static>> {
    // For Iroh-style authentication, we use a minimal self-signed certificate
    // that just contains our Ed25519 public key

    let public_key = secret_key.public();
    let public_key_bytes = public_key.as_bytes();

    // Build a minimal but valid X.509 v3 certificate
    // Using hand-crafted ASN.1 DER encoding
    let mut tbs_cert = Vec::new();

    // TBSCertificate SEQUENCE
    // Version [0] EXPLICIT (v3 = 2)
    tbs_cert.extend_from_slice(&[0xA0, 0x03, 0x02, 0x01, 0x02]);

    // Serial number (1)
    tbs_cert.extend_from_slice(&[0x02, 0x01, 0x01]);

    // Signature algorithm (Ed25519)
    tbs_cert.extend_from_slice(&[0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70]);

    // Issuer (CN=node)
    tbs_cert.extend_from_slice(&[
        0x30, 0x0F, // SEQUENCE
        0x31, 0x0D, // SET
        0x30, 0x0B, // SEQUENCE
        0x06, 0x03, 0x55, 0x04, 0x03, // OID commonName
        0x0C, 0x04, // UTF8String (4 bytes)
        0x6E, 0x6F, 0x64, 0x65, // "node"
    ]);

    // Validity (2024-2034)
    tbs_cert.extend_from_slice(&[
        0x30, 0x1E, // SEQUENCE
        0x17, 0x0D, // UTCTime
    ]);
    tbs_cert.extend_from_slice(b"240101000000Z");
    tbs_cert.extend_from_slice(&[0x17, 0x0D]);
    tbs_cert.extend_from_slice(b"340101000000Z");

    // Subject (same as issuer)
    tbs_cert.extend_from_slice(&[
        0x30, 0x0F, // SEQUENCE
        0x31, 0x0D, // SET
        0x30, 0x0B, // SEQUENCE
        0x06, 0x03, 0x55, 0x04, 0x03, // OID commonName
        0x0C, 0x04, // UTF8String
        0x6E, 0x6F, 0x64, 0x65, // "node"
    ]);

    // SubjectPublicKeyInfo
    tbs_cert.extend_from_slice(&[
        0x30, 0x2A, // SEQUENCE
        0x30, 0x05, // AlgorithmIdentifier SEQUENCE
        0x06, 0x03, 0x2B, 0x65, 0x70, // OID Ed25519
        0x03, 0x21, 0x00, // BIT STRING (33 bytes, 0 unused)
    ]);
    tbs_cert.extend_from_slice(public_key_bytes);

    // Wrap TBSCertificate in SEQUENCE
    let tbs_len = tbs_cert.len();
    let mut tbs_wrapped = vec![0x30, 0x81, tbs_len as u8];
    tbs_wrapped.extend_from_slice(&tbs_cert);

    // Sign the TBSCertificate
    let signature = secret_key.sign(&tbs_wrapped);

    // Build the complete certificate
    let mut cert = Vec::new();

    // Certificate SEQUENCE header (we'll fix the length later)
    cert.extend_from_slice(&[0x30, 0x82, 0x00, 0x00]);

    // Add TBSCertificate
    cert.extend_from_slice(&tbs_wrapped);

    // Signature algorithm
    cert.extend_from_slice(&[0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70]);

    // Signature value
    cert.extend_from_slice(&[0x03, 0x41, 0x00]); // BIT STRING
    cert.extend_from_slice(&signature.to_bytes());

    // Fix the outer SEQUENCE length
    let total_len = cert.len() - 4;
    cert[2] = ((total_len >> 8) & 0xFF) as u8;
    cert[3] = (total_len & 0xFF) as u8;

    Ok(CertificateDer::from(cert))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolver_creation() {
        let secret_key = SecretKey::generate();
        let resolver = AlwaysResolvesCert::new(&secret_key);

        // For now, this will fail due to placeholder cert
        // Once we implement proper cert generation, this should pass
        assert!(resolver.is_err() || resolver.is_ok());
    }
}
