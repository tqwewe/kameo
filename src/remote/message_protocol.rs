//! Extended message protocol with type hash support
//!
//! This module defines the new message format that includes type hashes
//! for generic actor support while maintaining backward compatibility.

use bitflags::bitflags;
use std::io::{Read, Write};

use super::type_hash::TypeHash;

bitflags! {
    /// Message flags for extended protocol features
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MessageFlags: u8 {
        /// Message is for a generic actor (uses type hash routing)
        const GENERIC_ACTOR = 0b00000001;
        /// Payload is compressed
        const COMPRESSED = 0b00000010;
        /// High priority message
        const PRIORITY = 0b00000100;
        /// Extended header format (v2)
        const EXTENDED_HEADER = 0b00001000;
    }
}

/// Message types supported by the protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Tell message (one-way)
    Tell = 0,
    /// Ask message (expects response)
    Ask = 1,
    /// Response to an ask
    Response = 2,
}

impl TryFrom<u8> for MessageType {
    type Error = InvalidMessageType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageType::Tell),
            1 => Ok(MessageType::Ask),
            2 => Ok(MessageType::Response),
            _ => Err(InvalidMessageType(value)),
        }
    }
}

/// Error returned when attempting to convert an invalid u8 to a MessageType
#[derive(Debug, thiserror::Error)]
#[error("Invalid message type: {0}")]
pub struct InvalidMessageType(u8);

/// Extended message header with type hash support
///
/// Layout (12 bytes):
/// - msg_type: 1 byte
/// - correlation_id: 2 bytes
/// - flags: 1 byte
/// - type_hash: 4 bytes (truncated from 64-bit hash)
/// - reserved: 4 bytes (for future use)
#[derive(Debug, Clone)]
pub struct MessageHeaderV2 {
    /// The type of message (tell, ask, or response)
    pub msg_type: MessageType,
    /// Correlation ID for matching requests to responses
    pub correlation_id: u16,
    /// Flags indicating message features (generic actor, compression, etc.)
    pub flags: MessageFlags,
    /// Truncated type hash for actor/message type identification
    pub type_hash: u32,
    /// Reserved bytes for future protocol extensions
    pub reserved: u32,
}

impl MessageHeaderV2 {
    /// Size of the serialized header in bytes
    pub const SIZE: usize = 12;

    /// Create a new message header
    pub fn new(msg_type: MessageType, correlation_id: u16, type_hash: TypeHash) -> Self {
        Self {
            msg_type,
            correlation_id,
            flags: MessageFlags::EXTENDED_HEADER | MessageFlags::GENERIC_ACTOR,
            type_hash: type_hash.as_u32(),
            reserved: 0,
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0] = self.msg_type as u8;
        bytes[1..3].copy_from_slice(&self.correlation_id.to_be_bytes());
        bytes[3] = self.flags.bits();
        bytes[4..8].copy_from_slice(&self.type_hash.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.reserved.to_be_bytes());
        bytes
    }

    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MessageHeaderError> {
        if bytes.len() < Self::SIZE {
            return Err(MessageHeaderError::InvalidSize);
        }

        let msg_type =
            MessageType::try_from(bytes[0]).map_err(|_| MessageHeaderError::InvalidMessageType)?;
        let correlation_id = u16::from_be_bytes([bytes[1], bytes[2]]);
        let flags = MessageFlags::from_bits(bytes[3]).ok_or(MessageHeaderError::InvalidFlags)?;
        let type_hash = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let reserved = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);

        Ok(Self {
            msg_type,
            correlation_id,
            flags,
            type_hash,
            reserved,
        })
    }

    /// Write header to a writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.to_bytes())
    }

    /// Read header from a reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self, MessageHeaderError> {
        let mut bytes = [0u8; Self::SIZE];
        reader
            .read_exact(&mut bytes)
            .map_err(MessageHeaderError::Io)?;
        Self::from_bytes(&bytes)
    }
}

/// Errors that can occur when parsing or handling message headers
#[derive(Debug, thiserror::Error)]
pub enum MessageHeaderError {
    /// The header has an invalid size
    #[error("Invalid header size")]
    InvalidSize,
    /// The message type byte is invalid
    #[error("Invalid message type")]
    InvalidMessageType,
    /// The flags byte contains invalid flags
    #[error("Invalid flags")]
    InvalidFlags,
    /// An I/O error occurred while reading or writing the header
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Compatibility layer for old-style string-based messages
///
/// This header is used when communicating with nodes that don't support
/// type hashes yet.
#[derive(Debug, Clone)]
pub struct MessageHeaderV1 {
    /// The type of message (tell, ask, or response)
    pub msg_type: MessageType,
    /// Correlation ID for matching requests to responses
    pub correlation_id: u16,
    /// String-based actor identifier (legacy)
    pub actor_id: String,
    /// String-based message identifier (legacy)
    pub message_id: String,
}

impl MessageHeaderV1 {
    /// Try to convert to V2 header using a string-to-hash mapping
    pub fn to_v2(&self, mapper: &StringToHashMapper) -> Option<MessageHeaderV2> {
        let type_hash = mapper.get_hash(&self.actor_id, &self.message_id)?;
        Some(MessageHeaderV2 {
            msg_type: self.msg_type,
            correlation_id: self.correlation_id,
            flags: MessageFlags::empty(), // No generic actor flag for legacy
            type_hash: type_hash.as_u32(),
            reserved: 0,
        })
    }
}

/// Maps legacy string IDs to type hashes
#[derive(Debug)]
pub struct StringToHashMapper {
    mappings: std::collections::HashMap<(String, String), TypeHash>,
}

impl Default for StringToHashMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl StringToHashMapper {
    /// Create a new empty string-to-hash mapper
    pub fn new() -> Self {
        Self {
            mappings: std::collections::HashMap::new(),
        }
    }

    /// Register a mapping from string IDs to type hash
    pub fn register(&mut self, actor_id: String, message_id: String, type_hash: TypeHash) {
        self.mappings.insert((actor_id, message_id), type_hash);
    }

    /// Get type hash for string IDs
    pub fn get_hash(&self, actor_id: &str, message_id: &str) -> Option<TypeHash> {
        self.mappings
            .get(&(actor_id.to_string(), message_id.to_string()))
            .copied()
    }

    /// Create a mapper with common built-in mappings
    pub fn with_defaults() -> Self {
        // Add default mappings for backward compatibility
        // These would be the existing RemoteActor implementations
        // Example:
        // mapper.register(
        //     "StringCache".to_string(),
        //     "Get".to_string(),
        //     TypeHash::from_bytes(b"StringCache:Get")
        // );

        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_header_v2_roundtrip() {
        let type_hash = TypeHash::from_bytes(b"TestActor");
        let header = MessageHeaderV2::new(MessageType::Ask, 42, type_hash);

        let bytes = header.to_bytes();
        let header2 = MessageHeaderV2::from_bytes(&bytes).unwrap();

        assert_eq!(header.msg_type, header2.msg_type);
        assert_eq!(header.correlation_id, header2.correlation_id);
        assert_eq!(header.flags, header2.flags);
        assert_eq!(header.type_hash, header2.type_hash);
    }

    #[test]
    fn test_message_flags() {
        let flags = MessageFlags::GENERIC_ACTOR | MessageFlags::PRIORITY;
        assert!(flags.contains(MessageFlags::GENERIC_ACTOR));
        assert!(flags.contains(MessageFlags::PRIORITY));
        assert!(!flags.contains(MessageFlags::COMPRESSED));
    }

    #[test]
    fn test_string_to_hash_mapper() {
        let mut mapper = StringToHashMapper::new();
        let hash = TypeHash::from_bytes(b"TestActor:TestMessage");

        mapper.register("TestActor".to_string(), "TestMessage".to_string(), hash);

        assert_eq!(mapper.get_hash("TestActor", "TestMessage"), Some(hash));
        assert_eq!(mapper.get_hash("OtherActor", "TestMessage"), None);
    }
}
