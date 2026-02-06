use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

use crate::actor::ActorId;

use super::archived_access;

/// Location metadata inserted by `KameoTransport` during registration.
///
/// This enables:
/// - reconnect by `PeerId` (TLS-verified) rather than address-only
/// - remote link disconnect notifications keyed by `PeerId`
#[derive(Archive, RSerialize, RDeserialize, Debug, Clone)]
pub(crate) struct LocationMetadataV1 {
    pub(crate) actor_id: ActorId,
    pub(crate) peer_id: kameo_remote::PeerId,
}

/// Encode V1 metadata as bytes.
///
/// Note: the returned `Vec<u8>` is not guaranteed to be aligned for rkyv access, so callers
/// must use the decode helpers below which realign before validating.
pub(crate) fn encode_v1(
    actor_id: ActorId,
    peer_id: kameo_remote::PeerId,
) -> Result<Vec<u8>, rkyv::rancor::Error> {
    let meta = LocationMetadataV1 { actor_id, peer_id };
    Ok(rkyv::to_bytes::<rkyv::rancor::Error>(&meta)?.to_vec())
}

fn aligned_copy(bytes: &[u8]) -> kameo_remote::AlignedBytes {
    let mut aligned = rkyv::util::AlignedVec::<{ kameo_remote::PAYLOAD_ALIGNMENT }>::new();
    aligned.extend_from_slice(bytes);
    kameo_remote::AlignedBytes::from_aligned_vec(aligned)
}

/// Decode V1 metadata from an unaligned byte slice.
///
/// This copies into an aligned buffer before validating. This is not on the tell/ask hot path
/// (it runs on lookup/refresh/link/error paths).
pub(crate) fn decode_v1(
    bytes: &[u8],
) -> Result<LocationMetadataV1, archived_access::ArchivedAccessError> {
    let aligned = aligned_copy(bytes);
    let archived =
        archived_access::access_archived::<rkyv::Archived<LocationMetadataV1>>(&aligned)?;
    use rkyv::Deserialize as _;
    let mut pool = rkyv::de::Pool::new();
    let de = rkyv::rancor::Strategy::<_, rkyv::rancor::Error>::wrap(&mut pool);
    Ok(archived.deserialize(de)?)
}

/// Decode legacy ActorId-only metadata from an unaligned byte slice.
pub(crate) fn decode_legacy_actor_id(
    bytes: &[u8],
) -> Result<ActorId, archived_access::ArchivedAccessError> {
    let aligned = aligned_copy(bytes);
    let archived = archived_access::access_archived::<rkyv::Archived<ActorId>>(&aligned)?;
    use rkyv::Deserialize as _;
    let mut pool = rkyv::de::Pool::new();
    let de = rkyv::rancor::Strategy::<_, rkyv::rancor::Error>::wrap(&mut pool);
    Ok(archived.deserialize(de)?)
}
