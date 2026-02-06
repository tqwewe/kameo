#![cfg(feature = "remote")]

use kameo::remote::type_hash::HasTypeHash;
use kameo::{Actor, distributed_actor};

#[derive(kameo::RemoteMessage, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct ArchivedTestMessage {
    value: u32,
}

#[derive(kameo::Actor)]
struct ArchivedTestActor;

impl ArchivedTestActor {
    async fn handle_archived(&mut self, _msg: &rkyv::Archived<ArchivedTestMessage>) -> u32 {
        42
    }
}

distributed_actor! {
    ArchivedTestActor {
        ArchivedTestMessage => handle_archived,
    }
}

#[tokio::test]
async fn archived_handler_rejects_invalid_payload() {
    let mut actor = ArchivedTestActor;

    let mut buf = rkyv::util::AlignedVec::new();
    buf.extend_from_slice(&[0u8; 1]);
    let aligned = kameo_remote::AlignedBytes::from_aligned_vec(buf);

    let result = actor
        .__handle_distributed_message(
            <ArchivedTestMessage as HasTypeHash>::TYPE_HASH.as_u32(),
            aligned,
        )
        .await;

    assert!(result.is_err(), "invalid payload should be rejected");
}
