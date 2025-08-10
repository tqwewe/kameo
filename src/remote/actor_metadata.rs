//! Typed metadata for remote actors
//! 
//! This module provides a typed structure for metadata that gets propagated
//! through the gossip protocol, ensuring type safety and forward compatibility.

use serde::{Deserialize, Serialize};
use crate::actor::ActorId;

/// Version of the metadata format for forward compatibility
const METADATA_VERSION: u8 = 1;

/// Typed metadata that gets propagated with actor registrations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorMetadata {
    /// Version number for forward compatibility
    pub version: u8,
    
    /// The actor's unique ID
    pub actor_id: ActorId,
    
    /// Optional additional fields for future expansion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<MetadataExtensions>,
}

/// Extension fields for future use
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataExtensions {
    /// Actor capabilities (e.g., supported message types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
    
    /// Custom application-specific data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<serde_json::Value>,
}

impl ActorMetadata {
    /// Create new metadata with just an ActorId
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            version: METADATA_VERSION,
            actor_id,
            extensions: None,
        }
    }
    
    /// Create metadata with extensions
    pub fn with_extensions(actor_id: ActorId, extensions: MetadataExtensions) -> Self {
        Self {
            version: METADATA_VERSION,
            actor_id,
            extensions: Some(extensions),
        }
    }
    
    /// Serialize metadata to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
    
    /// Deserialize metadata from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
    
    /// Check if this metadata version is compatible
    pub fn is_compatible(&self) -> bool {
        // For now, we only support version 1
        // In the future, we might support reading older versions
        self.version == METADATA_VERSION
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metadata_serialization() {
        let actor_id = ActorId::new(12345);
        let metadata = ActorMetadata::new(actor_id);
        
        // Serialize
        let bytes = metadata.to_bytes().expect("Failed to serialize");
        
        // Deserialize
        let deserialized = ActorMetadata::from_bytes(&bytes)
            .expect("Failed to deserialize");
        
        assert_eq!(deserialized.version, METADATA_VERSION);
        assert_eq!(deserialized.actor_id, actor_id);
        assert!(deserialized.extensions.is_none());
    }
    
    #[test]
    fn test_metadata_with_extensions() {
        let actor_id = ActorId::new(12345);
        let extensions = MetadataExtensions {
            capabilities: Some(vec!["remote".to_string(), "persistent".to_string()]),
            custom: None,
        };
        let metadata = ActorMetadata::with_extensions(actor_id, extensions);
        
        // Serialize
        let bytes = metadata.to_bytes().expect("Failed to serialize");
        
        // Deserialize
        let deserialized = ActorMetadata::from_bytes(&bytes)
            .expect("Failed to deserialize");
        
        assert!(deserialized.extensions.is_some());
        let ext = deserialized.extensions.unwrap();
        assert_eq!(ext.capabilities.unwrap().len(), 2);
    }
}