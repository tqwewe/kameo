//! The actor registry, stored as key-values in each node's own gossip state.
//!
//! Keys have the form `actor:<name>:<sequence_id>`; values are JSON
//! [`RegistrationValue`]s. Set-per-name semantics fall out of this schema: any
//! number of actors, across any number of nodes, can register the same name.

use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use chitchat::{Chitchat, ChitchatId, NodeState};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    dispatch::DispatchTable,
    error::RegistryError,
    id::{NodeId, RemoteActorId},
    messaging::transport::ConnectionPool,
    remote_actor::RemoteActor,
    remote_ref::RemoteActorRef,
};

pub(crate) const ACTOR_KEY_PREFIX: &str = "actor:";

/// The JSON value stored in the gossip state for one registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RegistrationValue {
    /// The actor type's `REMOTE_ID`, checked at lookup.
    pub actor_remote_id: String,
    /// The TCP messaging advertise address of the owning node.
    pub messaging_addr: SocketAddr,
}

pub(crate) fn validate_name(name: &str) -> Result<(), RegistryError> {
    if name.is_empty() || name.contains(':') {
        return Err(RegistryError::InvalidName(name.to_string()));
    }
    Ok(())
}

pub(crate) fn actor_key(name: &str, sequence_id: u64) -> String {
    format!("{ACTOR_KEY_PREFIX}{name}:{sequence_id}")
}

fn parse_actor_key(key: &str) -> Option<(&str, u64)> {
    let rest = key.strip_prefix(ACTOR_KEY_PREFIX)?;
    let (name, sequence_id) = rest.rsplit_once(':')?;
    Some((name, sequence_id.parse().ok()?))
}

/// Collects the providers of `name` from a set of live node states.
fn providers_from_states<'a, A: RemoteActor>(
    states: impl Iterator<Item = (&'a ChitchatId, &'a NodeState)>,
    name: &str,
    pool: &ConnectionPool,
    self_id: &ChitchatId,
    dispatch: &Arc<DispatchTable>,
    saw_mismatch: &mut bool,
) -> Vec<RemoteActorRef<A>> {
    let prefix = format!("{ACTOR_KEY_PREFIX}{name}:");
    let mut refs = Vec::new();
    for (chitchat_id, state) in states {
        for (key, versioned) in state.iter_prefix(&prefix) {
            let Some((parsed_name, sequence_id)) = parse_actor_key(key) else {
                continue;
            };
            if parsed_name != name {
                continue;
            }
            let value: RegistrationValue = match serde_json::from_str(&versioned.value) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!("skipping malformed registration {key:?}: {err}");
                    continue;
                }
            };
            if value.actor_remote_id != A::REMOTE_ID {
                *saw_mismatch = true;
                continue;
            }
            // Actors on this node incarnation are dispatched in-process, skipping TCP.
            let local_dispatch = (chitchat_id == self_id).then(|| dispatch.clone());
            refs.push(RemoteActorRef::new(
                RemoteActorId {
                    node_id: NodeId::from(chitchat_id.node_id.clone()),
                    generation_id: chitchat_id.generation_id,
                    sequence_id,
                },
                value.messaging_addr,
                pool.clone(),
                local_dispatch,
            ));
        }
    }
    refs
}

/// Collects the current live providers of `name` across the cluster.
pub(crate) async fn collect_providers<A: RemoteActor>(
    chitchat: &Arc<Mutex<Chitchat>>,
    pool: &ConnectionPool,
    dispatch: &Arc<DispatchTable>,
    name: &str,
) -> Result<Vec<RemoteActorRef<A>>, RegistryError> {
    let mut saw_mismatch = false;
    let refs = {
        // Everything is copied out under the guard; it is never held across I/O.
        let guard = chitchat.lock().await;
        let self_id = guard.self_chitchat_id().clone();
        let live: HashSet<ChitchatId> = guard.live_nodes().cloned().collect();
        let states = guard
            .node_states()
            .iter()
            .filter(|(chitchat_id, _)| live.contains(chitchat_id));
        providers_from_states::<A>(states, name, pool, &self_id, dispatch, &mut saw_mismatch)
    };
    if refs.is_empty() && saw_mismatch {
        return Err(RegistryError::BadActorType {
            name: name.to_string(),
            expected_remote_id: A::REMOTE_ID,
        });
    }
    Ok(refs)
}

/// Streams the live provider set of `name`, yielding on every change.
///
/// Built on the gossip live-nodes watcher rather than key listeners, so it observes
/// registrations, deregistrations, and node death uniformly. The current provider
/// set is yielded immediately.
pub(crate) async fn watch_providers<A: RemoteActor>(
    chitchat: &Arc<Mutex<Chitchat>>,
    pool: ConnectionPool,
    dispatch: Arc<DispatchTable>,
    name: String,
) -> impl Stream<Item = Vec<RemoteActorRef<A>>> + Send + 'static {
    let (stream, self_id) = {
        let guard = chitchat.lock().await;
        (
            guard.live_nodes_watch_stream(),
            guard.self_chitchat_id().clone(),
        )
    };
    stream
        .map(move |snapshot| {
            let mut saw_mismatch = false;
            providers_from_states::<A>(
                snapshot.iter(),
                &name,
                &pool,
                &self_id,
                &dispatch,
                &mut saw_mismatch,
            )
        })
        .scan(
            None::<Vec<RemoteActorId>>,
            |prev, refs: Vec<RemoteActorRef<A>>| {
                let mut ids: Vec<RemoteActorId> = refs
                    .iter()
                    .map(|actor_ref| actor_ref.id().clone())
                    .collect();
                ids.sort();
                let item = if prev.as_ref() == Some(&ids) {
                    // Deduplicate consecutive identical provider sets.
                    None
                } else {
                    *prev = Some(ids);
                    Some(refs)
                };
                futures::future::ready(Some(item))
            },
        )
        .filter_map(futures::future::ready)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_key_format() {
        assert_eq!(actor_key("incrementor", 42), "actor:incrementor:42");
    }

    #[test]
    fn parse_actor_key_round_trip() {
        assert_eq!(
            parse_actor_key("actor:incrementor:42"),
            Some(("incrementor", 42))
        );
        assert_eq!(parse_actor_key("actor:incrementor:"), None);
        assert_eq!(parse_actor_key("actor:incrementor"), None);
        assert_eq!(parse_actor_key("other:incrementor:42"), None);
        assert_eq!(parse_actor_key("actor:a:b:42"), Some(("a:b", 42)));
    }

    #[test]
    fn validate_name_rejects_invalid() {
        assert!(validate_name("incrementor").is_ok());
        assert_eq!(
            validate_name(""),
            Err(RegistryError::InvalidName(String::new()))
        );
        assert_eq!(
            validate_name("a:b"),
            Err(RegistryError::InvalidName("a:b".to_string()))
        );
    }
}
