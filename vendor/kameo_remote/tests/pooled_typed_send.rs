use kameo_remote::connection_pool::BufferConfig;
use kameo_remote::typed::{encode_typed, encode_typed_pooled, typed_payload_parts};
use kameo_remote::{ChannelId, LockFreeStreamHandle};
use tokio::io::AsyncReadExt;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
struct WireMsg {
    value: u64,
}

kameo_remote::wire_type!(WireMsg, "tests::WireMsg");

#[tokio::test]
async fn test_pooled_typed_send_matches_wire_bytes() {
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);
    let handle = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
    );

    let msg = WireMsg { value: 99 };
    let expected = encode_typed(&msg).expect("encode_typed");

    let pooled = encode_typed_pooled(&msg).expect("encode_typed_pooled");
    let (payload, prefix, payload_len) = typed_payload_parts::<WireMsg>(pooled);
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&(payload_len as u32).to_be_bytes());
    let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
    handle
        .write_pooled_control_inline(header, 4, prefix, prefix_len, payload)
        .await
        .unwrap();

    let mut len_buf = [0u8; 4];
    tokio::io::AsyncReadExt::read_exact(&mut reader, &mut len_buf)
        .await
        .unwrap();
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; payload_len];
    tokio::io::AsyncReadExt::read_exact(&mut reader, &mut payload)
        .await
        .unwrap();

    assert_eq!(payload, expected.as_ref());

    handle.shutdown();
}

#[tokio::test]
async fn test_write_ask_progresses_under_load() {
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);
    let handle = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
    );

    let reader_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let send_result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        for _ in 0..2000 {
            let header = kameo_remote::framing::write_ask_response_header(
                kameo_remote::MessageType::Ask,
                0,
                4,
            );
            handle
                .write_header_and_payload_ask(
                    bytes::Bytes::copy_from_slice(&header),
                    bytes::Bytes::from_static(b"ping"),
                )
                .await?;
        }
        Ok::<(), kameo_remote::GossipError>(())
    })
    .await;

    assert!(send_result.is_ok());

    handle.shutdown();
    reader_task.abort();
}
