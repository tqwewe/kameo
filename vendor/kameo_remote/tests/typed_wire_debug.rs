#[cfg(debug_assertions)]
mod tests {
    use kameo_remote::{decode_typed, encode_typed, wire_type};

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Ping {
        id: u64,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Pong {
        id: u64,
    }

    wire_type!(Ping, "kameo.remote.Ping");
    wire_type!(Pong, "kameo.remote.Pong");

    #[test]
    fn typed_roundtrip_ok() {
        let msg = Ping { id: 7 };
        let payload = encode_typed(&msg).expect("encode_typed should succeed");
        let decoded: Ping =
            decode_typed(payload.as_ref()).expect("decode_typed should succeed");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn typed_hash_mismatch_errors_in_debug() {
        let msg = Ping { id: 42 };
        let payload = encode_typed(&msg).expect("encode_typed should succeed");
        let err = decode_typed::<Pong>(payload.as_ref()).unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("hash mismatch"),
            "expected hash mismatch error, got: {err_str}"
        );
    }
}
