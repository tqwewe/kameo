use std::error;
use std::sync::atomic::Ordering;
use std::{fmt, sync::atomic::AtomicU64};

use internment::Intern;
use libp2p::identity::ParseError;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::remote::ActorSwarm;

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A globally unique identifier for an actor within a distributed system.
///
/// `ActorID` combines a locally sequential `sequence_id` with an optional `peer_id`
/// to uniquely identify actors across a distributed network.#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActorID {
    sequence_id: u64,
    /// None indicates a local actor; the local peer ID should be used in this case.
    peer_id: Option<Intern<PeerId>>,
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
            peer_id: ActorSwarm::get().map(|swarm| swarm.local_peer_id_intern().clone()),
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
    pub fn new_with_peer_id(sequence_id: u64, peer_id: PeerId) -> Self {
        ActorID {
            sequence_id,
            peer_id: Some(Intern::new(peer_id)),
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
    /// An `Option<PeerId>`. `None` indicates a local actor.
    pub fn peer_id(&self) -> Option<PeerId> {
        self.peer_id.map(|peer_id| *peer_id)
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
        let peer_id_bytes = self
            .peer_id
            .map(|peer_id| peer_id.to_bytes())
            .or_else(|| ActorSwarm::get().map(|swarm| swarm.local_peer_id().to_bytes()));
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
        let peer_id = if bytes.len() > 8 {
            Some(Intern::new(PeerId::from_bytes(&bytes[8..])?))
        } else {
            None
        };

        Ok(ActorID {
            sequence_id,
            peer_id,
        })
    }

    /// Returns the interned `PeerId` associated with this `ActorID`, if any.
    ///
    /// This method is primarily for internal use.
    ///
    /// # Returns
    ///
    /// An `Option<&Intern<PeerId>>`. `None` indicates a local actor.
    pub(crate) fn peer_id_intern(&self) -> Option<&Intern<PeerId>> {
        self.peer_id.as_ref()
    }

    /// Ensures the `ActorID` has an associated `peer_id`.
    ///
    /// If the `ActorID` doesn't have a `peer_id`, this method associates it with the local `PeerId`.
    /// This method is primarily for internal use.
    ///
    /// # Returns
    ///
    /// An `ActorID` with a guaranteed `peer_id`.
    pub(crate) fn with_hydrate_peer_id(mut self) -> ActorID {
        if self.peer_id.is_none() {
            self.peer_id = Some(ActorSwarm::get().unwrap().local_peer_id_intern().clone());
        }
        self
    }
}

impl fmt::Display for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorID({}, ", self.sequence_id)?;
        match self.peer_id {
            Some(peer_id) => write!(f, "{peer_id})"),
            None => write!(f, "local)"),
        }
    }
}

impl fmt::Debug for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorID({:?}, {:?})", self.sequence_id, self.peer_id)
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
            err @ ActorIDFromBytesError::ParsePeerID(_) => serde::de::Error::custom(err),
        })
    }
}

/// Errors that can occur when deserializing an `ActorID` from bytes.
#[derive(Debug)]
pub enum ActorIDFromBytesError {
    /// The byte slice doesn't contain enough data for the `sequence_id`.
    MissingSequenceID,
    /// An error occurred while parsing the `PeerId`.
    ParsePeerID(ParseError),
}

impl From<ParseError> for ActorIDFromBytesError {
    fn from(err: ParseError) -> Self {
        ActorIDFromBytesError::ParsePeerID(err)
    }
}

impl fmt::Display for ActorIDFromBytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorIDFromBytesError::MissingSequenceID => write!(f, "missing instance ID"),
            ActorIDFromBytesError::ParsePeerID(err) => err.fmt(f),
        }
    }
}

impl error::Error for ActorIDFromBytesError {}
