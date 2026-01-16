use crate::MessageType;

pub const LENGTH_PREFIX_LEN: usize = 4;
pub const ASK_RESPONSE_HEADER_LEN: usize = 4; // type(1) + correlation_id(2) + pad(1)
pub const GOSSIP_HEADER_LEN: usize = 4; // type(1) + pad(3)
pub const ACTOR_HEADER_LEN: usize = 20; // type(1) + correlation_id(2) + pad(1) + actor_id(8) + type_hash(4) + payload_len(4)
pub const STREAM_HEADER_PREFIX_LEN: usize = 8; // type(1) + correlation_id(2) + reserved(5)

pub const ASK_RESPONSE_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN;
pub const GOSSIP_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN;
pub const ACTOR_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN;

pub fn write_ask_response_header(
    msg_type: MessageType,
    correlation_id: u16,
    payload_len: usize,
) -> [u8; ASK_RESPONSE_FRAME_HEADER_LEN] {
    debug_assert!(matches!(msg_type, MessageType::Ask | MessageType::Response));

    let total_size = ASK_RESPONSE_HEADER_LEN + payload_len;
    let mut header = [0u8; ASK_RESPONSE_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = msg_type as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    header[7] = 0; // pad for 8-byte alignment
    header
}

pub fn write_gossip_frame_prefix(payload_len: usize) -> [u8; GOSSIP_FRAME_HEADER_LEN] {
    let total_size = GOSSIP_HEADER_LEN + payload_len;
    let mut header = [0u8; GOSSIP_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = MessageType::Gossip as u8;
    header[5] = 0;
    header[6] = 0;
    header[7] = 0;
    header
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALIGNMENT: usize = 8;

    fn is_aligned(offset: usize) -> bool {
        offset % ALIGNMENT == 0
    }

    #[test]
    fn ask_response_payload_offset_aligned_with_length_prefix() {
        let offset = LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN;
        assert!(is_aligned(offset));
    }

    #[test]
    fn gossip_payload_offset_aligned_with_length_prefix() {
        let offset = LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN;
        assert!(is_aligned(offset));
    }

    #[test]
    fn actor_payload_offset_aligned_with_length_prefix() {
        let offset = LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN;
        assert!(is_aligned(offset));
    }

    #[test]
    fn codec_trap_length_prefix_stripped_requires_more_padding() {
        let ask_header_without_pad = 1 + 2; // type + correlation_id
        let ask_pad_needed = (ALIGNMENT - (ask_header_without_pad % ALIGNMENT)) % ALIGNMENT;
        assert_eq!(ask_pad_needed, 5);

        let gossip_header_without_pad = 1; // type
        let gossip_pad_needed = (ALIGNMENT - (gossip_header_without_pad % ALIGNMENT)) % ALIGNMENT;
        assert_eq!(gossip_pad_needed, 7);

        let actor_header_without_pad = 1 + 2 + 8 + 4 + 4; // type + correlation_id + actor_id + type_hash + payload_len
        let actor_pad_needed = (ALIGNMENT - (actor_header_without_pad % ALIGNMENT)) % ALIGNMENT;
        assert_eq!(actor_pad_needed, 5);
    }

    #[test]
    fn write_ask_response_header_sets_length_and_pad() {
        let payload_len = 3;
        let header = write_ask_response_header(MessageType::Ask, 0x1234, payload_len);
        let total = (ASK_RESPONSE_HEADER_LEN + payload_len) as u32;
        assert_eq!(u32::from_be_bytes(header[0..4].try_into().unwrap()), total);
        assert_eq!(header[4], MessageType::Ask as u8);
        assert_eq!(u16::from_be_bytes(header[5..7].try_into().unwrap()), 0x1234);
        assert_eq!(header[7], 0);
    }

    #[test]
    fn write_gossip_frame_prefix_sets_padding() {
        let payload_len = 10;
        let header = write_gossip_frame_prefix(payload_len);
        let total = (GOSSIP_HEADER_LEN + payload_len) as u32;
        assert_eq!(u32::from_be_bytes(header[0..4].try_into().unwrap()), total);
        assert_eq!(header[4], MessageType::Gossip as u8);
        assert_eq!(&header[5..8], &[0u8; 3]);
    }

    #[test]
    fn header_lengths_hold_for_varied_payload_sizes() {
        for payload_len in 0..256 {
            let ask_header = write_ask_response_header(MessageType::Ask, 0, payload_len);
            let ask_total = (ASK_RESPONSE_HEADER_LEN + payload_len) as u32;
            assert_eq!(
                u32::from_be_bytes(ask_header[0..4].try_into().unwrap()),
                ask_total
            );

            let gossip_header = write_gossip_frame_prefix(payload_len);
            let gossip_total = (GOSSIP_HEADER_LEN + payload_len) as u32;
            assert_eq!(
                u32::from_be_bytes(gossip_header[0..4].try_into().unwrap()),
                gossip_total
            );

            assert!(is_aligned(LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN));
            assert!(is_aligned(LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN));
            assert!(is_aligned(LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN));
        }
    }
}
