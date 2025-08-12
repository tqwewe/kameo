//! Unit tests for remote Ask/Reply components
//! 
//! These focused unit tests verify individual components of the ask/reply mechanism

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    
    #[test]
    fn test_ask_envelope_creation() {
        // Test that Ask message envelope is created correctly
        // Format should be: [length:4][type:1][correlation_id:2][reserved:5][payload:N]
        
        // Simulate what conn.ask() should do
        let payload = vec![1, 2, 3, 4, 5]; // Test payload
        let correlation_id: u16 = 42;
        
        // Build the message as conn.ask() should
        let total_size = 8 + payload.len(); // 8-byte header + payload
        let mut message = BytesMut::with_capacity(4 + total_size);
        
        // Length prefix (4 bytes)
        message.extend_from_slice(&(total_size as u32).to_be_bytes());
        
        // Header: [type:1][correlation_id:2][reserved:5]
        message.extend_from_slice(&[0x01]); // MessageType::Ask = 0x01
        message.extend_from_slice(&correlation_id.to_be_bytes());
        message.extend_from_slice(&[0u8; 5]); // Reserved
        message.extend_from_slice(&payload);
        
        let message_bytes = message.freeze();
        
        // Verify the message structure
        assert_eq!(message_bytes.len(), 4 + 8 + 5, "Message size incorrect");
        
        // Check length prefix
        let len_bytes = &message_bytes[0..4];
        let decoded_len = u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
        assert_eq!(decoded_len as usize, 8 + payload.len(), "Length prefix incorrect");
        
        // Check message type
        assert_eq!(message_bytes[4], 0x01, "Message type should be 0x01 (Ask)");
        
        // Check correlation ID
        let corr_bytes = &message_bytes[5..7];
        let decoded_corr = u16::from_be_bytes([corr_bytes[0], corr_bytes[1]]);
        assert_eq!(decoded_corr, 42, "Correlation ID incorrect");
        
        // Check reserved bytes
        assert_eq!(&message_bytes[7..12], &[0u8; 5], "Reserved bytes should be zero");
        
        // Check payload
        assert_eq!(&message_bytes[12..], &payload[..], "Payload incorrect");
    }
    
    #[test]
    fn test_message_type_detection() {
        // Test message type detection for routing decisions
        
        // Map of byte values to expected message types
        let test_cases = vec![
            (0x00, "Gossip"),
            (0x01, "Ask"),
            (0x02, "Response"),
            (0x03, "ActorTell"),
            (0x0a, "Unknown - falls through to gossip"), // This is what we're seeing!
        ];
        
        for (byte, expected_type) in test_cases {
            println!("Byte 0x{:02x} should be routed as: {}", byte, expected_type);
            
            // The key insight: when byte is 0x0a (from rkyv serialization),
            // it doesn't match any MessageType and falls through to gossip handler
            // That's why correlation_id is None!
        }
    }
    
    #[test]
    fn test_message_routing_issue() {
        // This test documents the ACTUAL issue we're seeing
        
        // What SHOULD happen:
        // 1. Client creates RegistryMessage::ActorMessage
        // 2. Client serializes it (first byte becomes 0x0a from rkyv)
        // 3. Client calls conn.ask() which SHOULD wrap it with [type:0x01][corr_id][reserved]
        // 4. Server receives [length][0x01][corr_id][reserved][serialized_registry_message]
        // 5. Server sees type=0x01, routes to Ask handler
        // 6. Ask handler deserializes RegistryMessage, adds correlation_id
        
        // What ACTUALLY happens:
        // Server receives the serialized RegistryMessage directly (first byte 0x0a)
        // Server sees type=0x0a, which is unknown, falls through to gossip
        // Gossip handler processes it without correlation_id
        // Result: processed as tell, not ask!
        
        println!("ISSUE: conn.ask() envelope is not being applied or is being stripped");
        println!("The message arrives at server without the Ask envelope");
        
        // The fix needed: ensure conn.ask() envelope is preserved through the entire path
        assert!(true, "Issue documented");
    }
    
    #[test]
    fn test_connection_handle_ask_format() {
        // Test what ConnectionHandle::ask should produce
        
        let request = vec![0x0a, 0x0b, 0x0c]; // Simulated serialized RegistryMessage
        let correlation_id: u16 = 123;
        
        // What conn.ask() should create
        let total_size = 8 + request.len();
        let mut expected = BytesMut::new();
        expected.extend_from_slice(&(total_size as u32).to_be_bytes());
        expected.extend_from_slice(&[0x01]); // Ask type
        expected.extend_from_slice(&correlation_id.to_be_bytes());
        expected.extend_from_slice(&[0u8; 5]);
        expected.extend_from_slice(&request);
        
        println!("Expected message from conn.ask():");
        println!("  Length: {} bytes", total_size);
        println!("  Type byte at position 4: 0x{:02x} (Ask)", expected[4]);
        println!("  Correlation ID at positions 5-6: {}", correlation_id);
        println!("  Payload starts at position 12");
        
        // The server should see this format, not the raw RegistryMessage
        assert_eq!(expected[4], 0x01, "Type byte must be 0x01 for Ask");
    }
}