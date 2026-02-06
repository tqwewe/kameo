use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{LazyLock, Mutex},
};

use crate::{
    actor::ActorId,
    error::ActorStopReason,
    mailbox::SignalMailbox,
};

type RemoteActorLinks = HashMap<ActorId, HashMap<ActorId, Box<dyn SignalMailbox>>>;

struct RemoteLinkRegistry {
    links_by_peer: Mutex<HashMap<SocketAddr, RemoteActorLinks>>,
    peer_id_to_addr: Mutex<HashMap<kameo_remote::PeerId, SocketAddr>>,
}

#[derive(Clone)]
struct LocalLinkIntent {
    local_actor_id: ActorId,
    mailbox: Box<dyn SignalMailbox>,
}

static LOCAL_LINK_INTENTS: LazyLock<Mutex<HashMap<String, Vec<LocalLinkIntent>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

impl RemoteLinkRegistry {
    fn link(
        &self,
        peer_addr: SocketAddr,
        remote_actor_id: ActorId,
        local_actor_id: ActorId,
        local_mailbox: Box<dyn SignalMailbox>,
    ) -> bool {
        let mut guard = self
            .links_by_peer
            .lock()
            .expect("remote link registry lock poisoned");
        let by_actor = guard.entry(peer_addr).or_insert_with(HashMap::new);
        let locals = by_actor.entry(remote_actor_id).or_insert_with(HashMap::new);
        if locals.contains_key(&local_actor_id) {
            return false;
        }
        locals.insert(local_actor_id, local_mailbox);
        tracing::info!(
            peer_addr = %peer_addr,
            remote_actor_id = ?remote_actor_id,
            local_actor_id = ?local_actor_id,
            "remote link registered"
        );
        true
    }

    fn link_with_peer_id(
        &self,
        peer_id: &kameo_remote::PeerId,
        peer_addr: SocketAddr,
        remote_actor_id: ActorId,
        local_actor_id: ActorId,
        local_mailbox: Box<dyn SignalMailbox>,
    ) -> bool {
        {
            let mut guard = self
                .peer_id_to_addr
                .lock()
                .expect("remote link peer_id_to_addr lock poisoned");
            guard.insert(peer_id.clone(), peer_addr);
        }
        self.link(peer_addr, remote_actor_id, local_actor_id, local_mailbox)
    }

    fn unlink(
        &self,
        peer_addr: SocketAddr,
        remote_actor_id: ActorId,
        local_actor_id: ActorId,
    ) {
        let mut guard = self
            .links_by_peer
            .lock()
            .expect("remote link registry lock poisoned");
        let Some(by_actor) = guard.get_mut(&peer_addr) else {
            return;
        };
        let Some(locals) = by_actor.get_mut(&remote_actor_id) else {
            return;
        };
        locals.remove(&local_actor_id);
        tracing::info!(
            peer_addr = %peer_addr,
            remote_actor_id = ?remote_actor_id,
            local_actor_id = ?local_actor_id,
            "remote link removed"
        );
        if locals.is_empty() {
            by_actor.remove(&remote_actor_id);
        }
        if by_actor.is_empty() {
            guard.remove(&peer_addr);
        }
    }

    fn take_peer_links_by_addr(&self, peer_addr: SocketAddr) -> Option<RemoteActorLinks> {
        let mut guard = self
            .links_by_peer
            .lock()
            .expect("remote link registry lock poisoned");
        guard.remove(&peer_addr)
    }

    fn take_peer_links_by_id(
        &self,
        peer_id: &kameo_remote::PeerId,
    ) -> Option<RemoteActorLinks> {
        let peer_addr = {
            let mut guard = self
                .peer_id_to_addr
                .lock()
                .expect("remote link peer_id_to_addr lock poisoned");
            guard.remove(peer_id)
        }?;
        self.take_peer_links_by_addr(peer_addr)
    }
}

static REMOTE_LINK_REGISTRY: LazyLock<RemoteLinkRegistry> = LazyLock::new(|| RemoteLinkRegistry {
    links_by_peer: Mutex::new(HashMap::new()),
    peer_id_to_addr: Mutex::new(HashMap::new()),
});

pub(crate) fn link(
    peer_addr: SocketAddr,
    remote_actor_id: ActorId,
    local_actor_id: ActorId,
    local_mailbox: Box<dyn SignalMailbox>,
) {
    let inserted = REMOTE_LINK_REGISTRY.link(
        peer_addr,
        remote_actor_id,
        local_actor_id,
        local_mailbox.clone(),
    );
    if inserted {
        signal_link_established(local_mailbox, remote_actor_id);
    }
}

pub(crate) fn link_with_peer_id(
    peer_id: &kameo_remote::PeerId,
    peer_addr: SocketAddr,
    remote_actor_id: ActorId,
    local_actor_id: ActorId,
    local_mailbox: Box<dyn SignalMailbox>,
) {
    let inserted = REMOTE_LINK_REGISTRY.link_with_peer_id(
        peer_id,
        peer_addr,
        remote_actor_id,
        local_actor_id,
        local_mailbox.clone(),
    );
    if inserted {
        signal_link_established(local_mailbox, remote_actor_id);
    }
}

pub(crate) fn register_local_actor(
    name: String,
    local_actor_id: ActorId,
    mailbox: Box<dyn SignalMailbox>,
) {
    let mut guard = LOCAL_LINK_INTENTS
        .lock()
        .expect("local link intents lock poisoned");
    let entry = guard.entry(name).or_insert_with(Vec::new);
    entry.push(LocalLinkIntent {
        local_actor_id,
        mailbox,
    });
}

pub(crate) fn auto_link_peer(
    peer_addr: SocketAddr,
    peer_id: &kameo_remote::PeerId,
) {
    let intents = {
        let guard = LOCAL_LINK_INTENTS
            .lock()
            .expect("local link intents lock poisoned");
        guard.clone()
    };

    if intents.is_empty() {
        return;
    }

    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(&peer_id.to_bytes()[..8]);
    let remote_actor_id = ActorId::from_u64(u64::from_le_bytes(id_bytes));

    for locals in intents.values() {
        for local in locals {
            let inserted = REMOTE_LINK_REGISTRY.link_with_peer_id(
                peer_id,
                peer_addr,
                remote_actor_id,
                local.local_actor_id,
                local.mailbox.clone(),
            );
            if inserted {
                signal_link_established(local.mailbox.clone(), remote_actor_id);
            }
        }
    }
}

pub(crate) fn unlink(
    peer_addr: SocketAddr,
    remote_actor_id: ActorId,
    local_actor_id: ActorId,
) {
    REMOTE_LINK_REGISTRY.unlink(peer_addr, remote_actor_id, local_actor_id);
}

pub(crate) async fn notify_peer_disconnected_by_id(peer_id: &kameo_remote::PeerId) {
    let Some(by_actor) = REMOTE_LINK_REGISTRY.take_peer_links_by_id(peer_id) else {
        return;
    };

    for (remote_actor_id, locals) in by_actor {
        tracing::warn!(
            peer_id = %peer_id,
            remote_actor_id = ?remote_actor_id,
            local_links = locals.len(),
            "remote link peer disconnected"
        );
        for (_local_actor_id, mailbox) in locals {
            tracing::info!(
                peer_id = %peer_id,
                remote_actor_id = ?remote_actor_id,
                local_actor_id = ?_local_actor_id,
                reason = ?ActorStopReason::PeerDisconnected,
                "dispatching on_link_died"
            );
            let _ = mailbox
                .signal_link_died(remote_actor_id, ActorStopReason::PeerDisconnected)
                .await;
        }
    }
}

fn signal_link_established(
    local_mailbox: Box<dyn SignalMailbox>,
    remote_actor_id: ActorId,
) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            if let Err(err) = local_mailbox.signal_link_established(remote_actor_id).await {
                tracing::warn!(
                    remote_actor_id = ?remote_actor_id,
                    error = %err,
                    "failed to signal remote link established"
                );
            }
        });
    } else {
        if let Err(err) = futures::executor::block_on(
            local_mailbox.signal_link_established(remote_actor_id),
        ) {
            tracing::warn!(
                remote_actor_id = ?remote_actor_id,
                error = %err,
                "failed to signal remote link established"
            );
        }
    }
}
