use std::error;
use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::{fmt, sync::atomic::AtomicUsize};

#[cfg(feature = "remote")]
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

static ACTOR_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A globally unique identifier for an actor within a distributed system.
///
/// `ActorId` combines a locally sequential `sequence_id` with an optional `peer_id`
/// to uniquely identify actors across a distributed network.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "remote", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "remote", rkyv(derive(Debug)))]
pub struct ActorId {
    sequence_id: u64,
}

impl ActorId {
    /// Creates a new `ActorId` with the given `sequence_id`, using the local actor system.
    ///
    /// If the local actor system hasn't been bootstrapped, no `peer_id` will be associated,
    /// but the actor is still considered to be running locally.
    ///
    /// # Arguments
    ///
    /// * `sequence_id` - The sequential identifier for the actor.
    ///
    /// # Returns
    ///
    /// A new `ActorId` instance.
    pub fn new(sequence_id: u64) -> Self {
        ActorId { sequence_id }
    }

    /// Generates a new `ActorId` with an automatically incremented `sequence_id`.
    ///
    /// Uses an atomic counter to ensure unique `sequence_id` values across threads.
    ///
    /// # Returns
    ///
    /// A new `ActorId` instance with the next available `sequence_id`.
    pub fn generate() -> Self {
        ActorId::new(
            ACTOR_COUNTER
                .fetch_add(1, Ordering::Relaxed)
                .try_into()
                .unwrap(),
        )
    }

    /// Returns the sequential identifier of the actor.
    ///
    /// This `sequence_id` is a unique, locally-generated `u64` assigned to each actor
    /// in the order they are spawned. The first spawned actor gets id 0, the second 1, and so on.
    ///
    /// # Returns
    ///
    /// A `u64` representing the actor's `sequence_id`.
    pub fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    /// Convert ActorId to u64 for kameo_remote compatibility
    #[cfg(feature = "remote")]
    pub fn into_u64(self) -> u64 {
        self.sequence_id
    }

    /// Create ActorId from u64 for kameo_remote compatibility
    #[cfg(feature = "remote")]
    pub fn from_u64(id: u64) -> Self {
        Self::new(id)
    }

    /// Serializes the `ActorId` into a byte vector.
    ///
    /// The resulting vector contains the `sequence_id` followed by the `peer_id` (if present).
    ///
    /// # Returns
    ///
    /// A `Vec<u8>` containing the serialized `ActorId`.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.sequence_id.to_le_bytes().to_vec()
    }

    /// Deserializes an `ActorId` from a byte slice.
    ///
    /// # Arguments
    ///
    /// * `bytes` - A byte slice containing a serialized `ActorId`.
    ///
    /// # Returns
    ///
    /// A `Result` containing either the deserialized `ActorId` or an `ActorIdFromBytesError`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ActorIdFromBytesError> {
        if bytes.len() < 8 {
            return Err(ActorIdFromBytesError::MissingSequenceID);
        }

        let sequence_id = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| ActorIdFromBytesError::MissingSequenceID)?,
        );

        Ok(ActorId { sequence_id })
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorId({})", self.sequence_id)
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorId({:?})", self.sequence_id)
    }
}

// Serde implementations removed - using only rkyv for zero-copy serialization

/// Errors that can occur when deserializing an `ActorId` from bytes.
#[derive(Debug)]
pub enum ActorIdFromBytesError {
    /// The byte slice doesn't contain enough data for the `sequence_id`.
    MissingSequenceID,
}

impl fmt::Display for ActorIdFromBytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorIdFromBytesError::MissingSequenceID => write!(f, "missing instance ID"),
        }
    }
}

impl error::Error for ActorIdFromBytesError {}

#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hasher};

    use super::*;

    #[test]
    fn test_actor_id_partial_eq_local() {
        let id1 = ActorId { sequence_id: 0 };
        let id2 = ActorId { sequence_id: 0 };
        assert_eq!(id1, id2);

        let id1 = ActorId { sequence_id: 0 };
        let id2 = ActorId { sequence_id: 1 };
        assert_ne!(id1, id2);
    }

    fn hashes_eq(id1: &ActorId, id2: &ActorId) -> bool {
        let mut hasher = DefaultHasher::new();
        id1.hash(&mut hasher);
        let id1_hash = hasher.finish();

        let mut hasher = DefaultHasher::new();
        id2.hash(&mut hasher);
        let id2_hash = hasher.finish();

        id1_hash == id2_hash
    }

    #[test]
    fn test_actor_id_hash_local() {
        let id1 = ActorId { sequence_id: 0 };
        let id2 = ActorId { sequence_id: 0 };

        assert!(hashes_eq(&id1, &id2));

        let id1 = ActorId { sequence_id: 0 };
        let id2 = ActorId { sequence_id: 1 };

        assert!(!hashes_eq(&id1, &id2));
    }
}
