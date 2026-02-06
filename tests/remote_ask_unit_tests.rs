#![cfg(feature = "remote")]
//! Unit tests for the zero-copy actor ask/tell framing.

#[cfg(test)]
mod tests {
    #[test]
    fn actor_frame_header_is_16_byte_aligned() {
        let actor_id = 0x0102030405060708u64;
        let type_hash = 0xA5A5_F0F0u32;
        let payload_len = 64;
        let correlation_id = 42;

        let header = kameo_remote::framing::write_actor_frame_header(
            kameo_remote::MessageType::ActorAsk,
            correlation_id,
            actor_id,
            type_hash,
            None,
            payload_len,
        );

        assert_eq!(
            header.len(),
            kameo_remote::framing::ACTOR_FRAME_HEADER_LEN,
            "header must include length prefix + padded actor fields"
        );
        assert_eq!(header[4], kameo_remote::MessageType::ActorAsk as u8);
        assert_eq!(u16::from_be_bytes([header[5], header[6]]), correlation_id);
        assert_eq!(&header[7..16], &[0u8; 9], "reserved padding must be zeroed");
        assert_eq!(
            u64::from_be_bytes(header[16..24].try_into().unwrap()),
            actor_id
        );
        assert_eq!(
            u32::from_be_bytes(header[24..28].try_into().unwrap()),
            type_hash
        );
        assert_eq!(
            u32::from_be_bytes(header[28..32].try_into().unwrap()),
            payload_len as u32
        );

        // Payload offset must be 32 bytes (length prefix + padded header) for 16-byte alignment.
        assert_eq!(
            kameo_remote::framing::LENGTH_PREFIX_LEN + kameo_remote::framing::ACTOR_HEADER_LEN,
            32
        );
    }
}
