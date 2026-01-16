use crate::{NodeId, PublicKey};
use data_encoding::BASE32_DNSSEC;

/// Encode a NodeId as a DNS-safe name
/// Format: {base32_encoded_nodeid}.kameo.invalid
pub fn encode(node_id: &NodeId) -> String {
    let encoded = BASE32_DNSSEC.encode(node_id.as_bytes());
    format!("{}.kameo.invalid", encoded.to_lowercase())
}

/// Decode a DNS name back to NodeId
/// Expects format: {base32_encoded_nodeid}.kameo.invalid
pub fn decode(name: &str) -> Option<NodeId> {
    // Remove the .kameo.invalid suffix if present
    let node_part = if let Some(stripped) = name.strip_suffix(".kameo.invalid") {
        stripped
    } else if let Some(first_part) = name.split('.').next() {
        first_part
    } else {
        return None;
    };

    // Decode from base32
    let bytes = BASE32_DNSSEC
        .decode(node_part.to_uppercase().as_bytes())
        .ok()?;

    // Convert to NodeId
    PublicKey::from_bytes(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SecretKey;

    #[test]
    fn test_encode_decode_roundtrip() {
        let secret = SecretKey::generate();
        let node_id = secret.public();

        let encoded = encode(&node_id);
        assert!(encoded.ends_with(".kameo.invalid"));

        let decoded = decode(&encoded).unwrap();
        assert_eq!(node_id, decoded);
    }

    #[test]
    fn test_decode_without_suffix() {
        let secret = SecretKey::generate();
        let node_id = secret.public();

        let full_name = encode(&node_id);
        let base32_part = full_name.strip_suffix(".kameo.invalid").unwrap();

        // Should decode even without the suffix
        let decoded = decode(base32_part).unwrap();
        assert_eq!(node_id, decoded);
    }

    #[test]
    fn test_decode_invalid() {
        assert!(decode("invalid-base32").is_none());
        assert!(decode("").is_none());
        assert!(decode("too-short").is_none());
    }

    #[test]
    fn test_dns_name_valid() {
        let secret = SecretKey::generate();
        let node_id = secret.public();

        let encoded = encode(&node_id);

        // Check it's a valid DNS name (base32 includes digits 2-7)
        assert!(encoded
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-'));

        // Check label length (each part between dots must be < 63 chars)
        for label in encoded.split('.') {
            assert!(label.len() < 63, "DNS label too long: {}", label.len());
        }
    }
}
