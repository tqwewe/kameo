use std::error;
use std::sync::atomic::Ordering;
use std::{fmt, sync::atomic::AtomicU64};

use internment::Intern;
use libp2p::identity::ParseError;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::remote::ActorSwarm;

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Represents an `ActorID` which is a wrapper around a `u64`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorID {
    id: u64,
    /// None indicates its local, and the local peer ID should be used
    peer_id: Option<Intern<PeerId>>,
}

impl ActorID {
    /// Creates a new `ActorID` from a `u64`.
    pub fn new(id: u64) -> Self {
        ActorID { id, peer_id: None }
    }

    /// Generates a new `ActorID`.
    ///
    /// If no `NODE_ID` is specified, it is defaulted to `0`.
    pub fn generate() -> Self {
        ActorID::new(ACTOR_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns the peer ID associated with the actor ID.
    pub fn peer_id(&self) -> Option<PeerId> {
        self.peer_id.map(|peer_id| *peer_id)
    }

    pub(crate) fn peer_id_intern(&self) -> Option<&Intern<PeerId>> {
        self.peer_id.as_ref()
    }

    pub(crate) fn with_hydrate_peer_id(mut self) -> ActorID {
        if self.peer_id.is_none() {
            self.peer_id = Some(ActorSwarm::get().unwrap().local_peer_id_intern().clone());
        }
        self
    }

    pub(crate) fn to_bytes(&self, local_peer_id: PeerId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 42);
        bytes.extend(&self.id.to_le_bytes());
        bytes.extend(
            &self
                .peer_id
                .as_deref()
                .cloned()
                .unwrap_or(local_peer_id)
                .to_bytes(),
        );
        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, ActorIDFromBytesError> {
        // Extract the ID
        let id = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| ActorIDFromBytesError::MissingInstanceID)?,
        );

        // Extract the peer id
        let peer_id = PeerId::from_bytes(&bytes[8..])?;

        Ok(ActorID {
            id,
            peer_id: Some(Intern::new(peer_id)),
        })
    }
}

impl fmt::Display for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorID({}, ", self.id)?;
        match self.peer_id {
            Some(peer_id) => write!(f, "{peer_id})"),
            None => write!(f, "local)"),
        }
    }
}

impl fmt::Debug for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorID({:?}, {:?})", self.id, self.peer_id)
    }
}

#[derive(Debug)]
pub(crate) enum ActorIDFromBytesError {
    MissingInstanceID,
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
            ActorIDFromBytesError::MissingInstanceID => write!(f, "missing instance ID"),
            ActorIDFromBytesError::ParsePeerID(err) => err.fmt(f),
        }
    }
}

impl error::Error for ActorIDFromBytesError {}
