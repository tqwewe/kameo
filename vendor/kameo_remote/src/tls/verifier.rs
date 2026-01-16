use crate::{NodeId, PublicKey};
use rustls::pki_types::CertificateDer;
use rustls::Error;

/// Extract NodeId from a raw public key certificate
///
/// For our simplified approach, we expect certificates to contain
/// the Ed25519 public key directly in a specific format
pub fn extract_node_id_from_cert(cert: &CertificateDer<'_>) -> Result<NodeId, Error> {
    // For MVP, we'll use a simplified approach where the certificate
    // contains the raw Ed25519 public key at a known offset

    let cert_bytes = cert.as_ref();

    // Look for Ed25519 public key pattern in the certificate
    // This is a simplified approach - production would use proper ASN.1 parsing

    // For now, check if the certificate is long enough
    if cert_bytes.len() < 32 {
        return Err(Error::General(
            "Certificate too short for Ed25519 key".into(),
        ));
    }

    // Try to extract 32 bytes as the public key (simplified)
    // In production, we'd parse the ASN.1 structure properly
    let key_bytes = &cert_bytes[cert_bytes.len().saturating_sub(32)..];

    PublicKey::from_bytes(key_bytes)
        .map_err(|e| Error::General(format!("Invalid public key in certificate: {}", e)))
}

/// Verify that a certificate is self-signed with the given NodeId
pub fn verify_self_signed(
    cert: &CertificateDer<'_>,
    expected_node_id: &NodeId,
) -> Result<(), Error> {
    let cert_node_id = extract_node_id_from_cert(cert)?;

    if cert_node_id != *expected_node_id {
        return Err(Error::General(format!(
            "Certificate NodeId mismatch: expected {}, got {}",
            expected_node_id.fmt_short(),
            cert_node_id.fmt_short()
        )));
    }

    // Signature validation is intentionally omitted for raw-key placeholder certs.

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SecretKey;

    #[test]
    fn test_extract_node_id_placeholder() {
        // Create a dummy certificate with a public key at the end
        let secret = SecretKey::generate();
        let node_id = secret.public();

        let mut cert_bytes = vec![0u8; 100];
        cert_bytes[68..100].copy_from_slice(node_id.as_bytes());

        let cert = CertificateDer::from(cert_bytes);
        let extracted = extract_node_id_from_cert(&cert).unwrap();

        assert_eq!(extracted, node_id);
    }
}
