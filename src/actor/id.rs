use std::sync::atomic::Ordering;
use std::{fmt, sync::atomic::AtomicU64};

use serde::{Deserialize, Serialize};

use crate::error::ActorIDFromBytesError;
#[cfg(feature = "remote")]
use crate::remote::ActorSwarm;

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A globally unique identifier for an actor within a distributed system.
///
/// `ActorID` combines a locally sequential `sequence_id` with an optional `peer_id`
/// to uniquely identify actors across a distributed network.#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActorID {
    sequence_id: u64,
    #[cfg(feature = "remote")]
    peer_id: PeerIdKind,
}

impl ActorID {
    /// Creates a new `ActorID` with the given `sequence_id`, using the local actor swarm.
    ///
    /// If the local actor swarm hasn't been bootstrapped, no `peer_id` will be associated,
    /// but the actor is still considered to be running locally.
    ///
    /// # Arguments
    ///
    /// * `sequence_id` - The sequential identifier for the actor.
    ///
    /// # Returns
    ///
    /// A new `ActorID` instance.
    pub fn new(sequence_id: u64) -> Self {
        ActorID {
            sequence_id,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        }
    }

    /// Creates a new `ActorID` with a specific `sequence_id` and `peer_id`.
    ///
    /// # Arguments
    ///
    /// * `sequence_id` - The sequential identifier for the actor.
    /// * `peer_id` - The `PeerId` associated with this actor.
    ///
    /// # Returns
    ///
    /// A new `ActorID` instance.
    #[cfg(feature = "remote")]
    pub fn new_with_peer_id(sequence_id: u64, peer_id: libp2p::PeerId) -> Self {
        ActorID {
            sequence_id,
            peer_id: PeerIdKind::PeerId(peer_id),
        }
    }

    /// Generates a new `ActorID` with an automatically incremented `sequence_id`.
    ///
    /// Uses an atomic counter to ensure unique `sequence_id` values across threads.
    ///
    /// # Returns
    ///
    /// A new `ActorID` instance with the next available `sequence_id`.
    pub fn generate() -> Self {
        ActorID::new(ACTOR_COUNTER.fetch_add(1, Ordering::Relaxed))
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

    /// Returns the `PeerId` associated with the `ActorID`, if any.
    ///
    /// # Returns
    ///
    /// An `Option<PeerId>`. `None` is returned if the peer ID is local and no [`ActorSwarm`] has been bootstrapped.
    #[cfg(feature = "remote")]
    pub fn peer_id(&self) -> Option<&libp2p::PeerId> {
        self.peer_id.peer_id()
    }

    /// Serializes the `ActorID` into a byte vector.
    ///
    /// The resulting vector contains the `sequence_id` followed by the `peer_id` (if present).
    ///
    /// # Returns
    ///
    /// A `Vec<u8>` containing the serialized `ActorID`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 42);
        bytes.extend(&self.sequence_id.to_le_bytes());

        #[cfg(feature = "remote")]
        let peer_id_bytes = self
            .peer_id
            .peer_id()
            .map(|peer_id| peer_id.to_bytes())
            .or_else(|| ActorSwarm::get().map(|swarm| swarm.local_peer_id().to_bytes()));
        #[cfg(feature = "remote")]
        if let Some(peer_id_bytes) = peer_id_bytes {
            bytes.extend(peer_id_bytes);
        }

        bytes
    }

    /// Deserializes an `ActorID` from a byte slice.
    ///
    /// # Arguments
    ///
    /// * `bytes` - A byte slice containing a serialized `ActorID`.
    ///
    /// # Returns
    ///
    /// A `Result` containing either the deserialized `ActorID` or an `ActorIDFromBytesError`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ActorIDFromBytesError> {
        // Extract the ID
        let sequence_id = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| ActorIDFromBytesError::MissingSequenceID)?,
        );

        // Extract the peer id
        #[cfg(feature = "remote")]
        let peer_id = if bytes.len() > 8 {
            PeerIdKind::PeerId(libp2p::PeerId::from_bytes(&bytes[8..])?)
        } else {
            PeerIdKind::Local
        };

        Ok(ActorID {
            sequence_id,
            #[cfg(feature = "remote")]
            peer_id,
        })
    }
}

impl fmt::Display for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(not(feature = "remote"))]
        return write!(f, "ActorID({})", self.sequence_id);

        #[cfg(feature = "remote")]
        match self.peer_id.peer_id() {
            Some(peer_id) => write!(f, "ActorID({}, {peer_id})", self.sequence_id),
            None => write!(f, "ActorID({}, local)", self.sequence_id),
        }
    }
}

impl fmt::Debug for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(feature = "remote")]
        return write!(
            f,
            "ActorID({:?}, {:?})",
            self.sequence_id,
            self.peer_id.peer_id()
        );

        #[cfg(not(feature = "remote"))]
        return write!(f, "ActorID({:?})", self.sequence_id);
    }
}

impl Serialize for ActorID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.to_bytes())
    }
}

impl<'de> Deserialize<'de> for ActorID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = <&[u8]>::deserialize(deserializer)?;
        let bytes_len = bytes.len();
        ActorID::from_bytes(bytes).map_err(|err| match err {
            ActorIDFromBytesError::MissingSequenceID => {
                serde::de::Error::invalid_length(bytes_len, &"sequence ID")
            }
            #[cfg(feature = "remote")]
            err @ ActorIDFromBytesError::ParsePeerID(_) => serde::de::Error::custom(err),
        })
    }
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum PeerIdKind {
    Local,
    PeerId(libp2p::PeerId),
}

#[cfg(feature = "remote")]
impl PeerIdKind {
    fn peer_id(&self) -> Option<&libp2p::PeerId> {
        match self {
            PeerIdKind::Local => ActorSwarm::get().map(ActorSwarm::local_peer_id),
            PeerIdKind::PeerId(peer_id) => Some(peer_id),
        }
    }
}
