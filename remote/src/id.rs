//! Identity types for nodes and remote actors.

use std::{fmt, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A cluster-unique node identity.
///
/// This is the gossip node id string, either configured via
/// [`RemoteNodeConfig::node_name`](crate::RemoteNodeConfig) or generated at bootstrap.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(Arc<str>);

impl NodeId {
    /// Returns the node id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId(Arc::from(s))
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId(Arc::from(s))
    }
}

impl From<Arc<str>> for NodeId {
    fn from(s: Arc<str>) -> Self {
        NodeId(s)
    }
}

impl Serialize for NodeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(NodeId::from)
    }
}

/// The identity of a remote actor: the owning node incarnation plus the actor's local
/// sequence id.
///
/// The `generation_id` identifies the node's incarnation (it increases on restart), so a
/// stale reference into a previous incarnation can never reach an unrelated actor that
/// happens to reuse the same sequence id.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RemoteActorId {
    /// The node the actor lives on.
    pub node_id: NodeId,
    /// The generation of the node's incarnation the actor was registered in.
    pub generation_id: u64,
    /// The actor's locally-unique sequence id on that node.
    pub sequence_id: u64,
}

impl fmt::Display for RemoteActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}@{}", self.sequence_id, self.node_id)
    }
}
