use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use kameo_remote::PeerId;

#[derive(Clone, Hash, PartialEq, Eq)]
enum CacheKey {
    Addr(SocketAddr),
    PeerId(PeerId),
}

static HANDLE_CACHE: LazyLock<Mutex<HashMap<CacheKey, Vec<Weak<AtomicBool>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn register_key(
    map: &mut HashMap<CacheKey, Vec<Weak<AtomicBool>>>,
    key: CacheKey,
    alive: &Arc<AtomicBool>,
) {
    let entry = map.entry(key).or_insert_with(Vec::new);
    entry.retain(|weak| weak.strong_count() > 0);
    entry.push(Arc::downgrade(alive));
}

fn invalidate_key(map: &mut HashMap<CacheKey, Vec<Weak<AtomicBool>>>, key: CacheKey) {
    let Some(entry) = map.get_mut(&key) else {
        return;
    };
    entry.retain(|weak| {
        if let Some(alive) = weak.upgrade() {
            alive.store(false, Ordering::Release);
            true
        } else {
            false
        }
    });
    if entry.is_empty() {
        map.remove(&key);
    }
}

/// Register a cached connection liveness token for a peer.
/// Returns an `Arc<AtomicBool>` that is `true` while the connection is valid.
pub(crate) fn register(
    peer_addr: SocketAddr,
    peer_id: Option<PeerId>,
) -> Arc<AtomicBool> {
    let alive = Arc::new(AtomicBool::new(true));
    let mut map = HANDLE_CACHE.lock().expect("handle cache lock poisoned");
    register_key(&mut map, CacheKey::Addr(peer_addr), &alive);
    if let Some(peer_id) = peer_id {
        register_key(&mut map, CacheKey::PeerId(peer_id), &alive);
    }
    alive
}

/// Invalidate all cached connections for a peer.
pub(crate) fn invalidate(
    peer_addr: SocketAddr,
    peer_id: Option<&PeerId>,
) {
    let mut map = HANDLE_CACHE.lock().expect("handle cache lock poisoned");
    invalidate_key(&mut map, CacheKey::Addr(peer_addr));
    if let Some(peer_id) = peer_id {
        invalidate_key(&mut map, CacheKey::PeerId(peer_id.clone()));
    }
}
