//! Per-actor monitoring state and the global registry the collector reads.
//!
//! Every actor registers an [`ActorMonitor`] when it is prepared (see
//! `PreparedActor::new_with`) and keeps it for the lifetime of the process, plus a short
//! grave window after it stops so the console can show it dying. The hot-path counters
//! (messages, mailbox depth) are cheap relaxed atomics; the current-handler and per-type
//! message tally use a short uncontended mutex bumped once per handled message. The registry
//! map itself is only touched on spawn and during snapshots, never per message.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use crate::{
    actor::{Actor, ActorId},
    error::ActorStopReason,
    links::Links,
    mailbox::{MailboxSender, Signal, WeakMailboxSender},
    supervision::{RestartPolicy, SupervisionStrategy},
};

use super::wire;

static REGISTRY: LazyLock<Mutex<HashMap<ActorId, Arc<ActorMonitor>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static START: LazyLock<Instant> = LazyLock::new(Instant::now);
static SEQ: AtomicU64 = AtomicU64::new(0);
static TOTAL_SPAWNED: AtomicU64 = AtomicU64::new(0);
static REAPED_STOPPED: AtomicU64 = AtomicU64::new(0);

const STARTING: u8 = 0;
const RUNNING: u8 = 1;
const RESTARTING: u8 = 2;
const STOPPING: u8 = 3;
const STOPPED: u8 = 4;

/// How often (in registrations) the spawn path sweeps long-dead monitors, and how long a
/// stopped monitor may linger before that sweep reaps it. The window is deliberately far
/// larger than any realistic display grave window so it only bounds memory and never affects
/// what an attached console sees.
const SAFETY_SWEEP_EVERY: u64 = 256;
const SAFETY_GRAVE_WINDOW: Duration = Duration::from_secs(300);

/// Reads liveness/ref-counts off a still-alive actor without keeping it alive.
trait LiveProbe: Send + Sync {
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
    /// The bounded mailbox's queued depth read live (`max_capacity - capacity`), or `None` for
    /// an unbounded or already-dropped mailbox. Lets a snapshot report true depth even while
    /// the actor is parked in a slow handler and hasn't looped back to refresh its cached len.
    fn live_mailbox_len(&self) -> Option<usize>;
}

impl<A: Actor> LiveProbe for WeakMailboxSender<A> {
    fn strong_count(&self) -> usize {
        WeakMailboxSender::strong_count(self)
    }

    fn weak_count(&self) -> usize {
        WeakMailboxSender::weak_count(self)
    }

    fn live_mailbox_len(&self) -> Option<usize> {
        let tx = self.upgrade()?;
        Some(tx.max_capacity()?.saturating_sub(tx.capacity()?))
    }
}

struct Stopped {
    since: Instant,
    at: SystemTime,
    reason: String,
}

struct CurrentHandler {
    message: &'static str,
    since: Instant,
}

struct Waiting {
    target: ActorId,
    kind: wire::WaitKind,
    since: Instant,
}

tokio::task_local! {
    /// The monitor of the actor running on the current task, so blocking `ask`/`tell` calls
    /// can record a wait-for edge against the right actor without a registry lookup.
    static CURRENT_MONITOR: Arc<ActorMonitor>;
}

/// Runs `fut` (an actor's whole lifecycle) with its monitor bound to the task, enabling
/// [`begin_wait`] to attribute wait-for edges to it.
pub(crate) fn with_monitor<F: std::future::Future>(
    monitor: Arc<ActorMonitor>,
    fut: F,
) -> impl std::future::Future<Output = F::Output> {
    CURRENT_MONITOR.scope(monitor, fut)
}

/// Records that the current actor (if any) is blocked waiting on `target`, clearing the edge
/// when the returned guard is dropped (i.e. when the blocking call completes or is cancelled).
pub(crate) fn begin_wait(target: ActorId, kind: wire::WaitKind) -> WaitGuard {
    match CURRENT_MONITOR.try_with(Arc::clone) {
        Ok(monitor) => {
            // `since` doubles as a unique token identifying this wait, so the guard only clears
            // the slot if it's still the one it set (see `WaitGuard::drop`).
            let since = Instant::now();
            *monitor.waiting_on.lock().unwrap() = Some(Waiting {
                target,
                kind,
                since,
            });
            WaitGuard(Some((monitor, since)))
        }
        Err(_) => WaitGuard(None),
    }
}

#[must_use]
pub(crate) struct WaitGuard(Option<(Arc<ActorMonitor>, Instant)>);

impl Drop for WaitGuard {
    fn drop(&mut self) {
        if let Some((monitor, since)) = &self.0 {
            // Only clear if this guard's wait is still the active one. If a second, concurrent
            // `begin_wait` (e.g. two asks awaited together in one handler) overwrote the slot,
            // this guard must not erase that newer edge — that would drop a live wait-for edge
            // and hide a real deadlock.
            let mut waiting = monitor.waiting_on.lock().unwrap();
            if waiting.as_ref().is_some_and(|w| w.since == *since) {
                *waiting = None;
            }
        }
    }
}

/// Live, per-instance monitoring state for a single actor.
pub(crate) struct ActorMonitor {
    id: ActorId,
    name: &'static str,
    strategy: SupervisionStrategy,
    spawned_at: SystemTime,
    mailbox_kind: wire::MailboxKind,
    mailbox_capacity: Option<usize>,
    links: Links,
    probe: Box<dyn LiveProbe>,
    status: AtomicU8,
    stopped: Mutex<Option<Stopped>>,
    current_handler: Mutex<Option<CurrentHandler>>,
    waiting_on: Mutex<Option<Waiting>>,
    message_types: Mutex<HashMap<&'static str, u64>>,
    mailbox_len: AtomicUsize,
    messages_received: AtomicU64,
    lifecycle_signals_received: AtomicU64,
    link_died_signals_received: AtomicU64,
    panics: AtomicU64,
    restarts: AtomicU64,
}

impl ActorMonitor {
    pub(crate) fn set_running(&self) {
        self.status.store(RUNNING, Ordering::Relaxed);
    }

    pub(crate) fn set_stopping(&self) {
        self.status.store(STOPPING, Ordering::Relaxed);
    }

    pub(crate) fn set_stopped(&self, reason: &ActorStopReason) {
        if matches!(reason, ActorStopReason::Panicked(_)) {
            self.panics.fetch_add(1, Ordering::Relaxed);
        }
        *self.stopped.lock().unwrap() = Some(Stopped {
            since: Instant::now(),
            at: SystemTime::now(),
            reason: reason.to_string(),
        });
        self.status.store(STOPPED, Ordering::Relaxed);
    }

    pub(crate) fn set_mailbox_len(&self, len: usize) {
        self.mailbox_len.store(len, Ordering::Relaxed);
    }

    pub(crate) fn begin_handler(&self, message: &'static str) {
        *self.current_handler.lock().unwrap() = Some(CurrentHandler {
            message,
            since: Instant::now(),
        });
        *self
            .message_types
            .lock()
            .unwrap()
            .entry(message)
            .or_insert(0) += 1;
    }

    pub(crate) fn end_handler(&self) {
        *self.current_handler.lock().unwrap() = None;
    }

    pub(crate) fn record_received<A: Actor>(&self, signal: &Signal<A>) {
        let counter = match signal {
            Signal::Message { .. } => &self.messages_received,
            Signal::StartupFinished | Signal::Stop | Signal::SupervisorRestart => {
                &self.lifecycle_signals_received
            }
            Signal::LinkDied { .. } => &self.link_died_signals_received,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    async fn to_wire(&self, monitors: &HashMap<ActorId, Arc<ActorMonitor>>) -> wire::ActorSnapshot {
        let (parent_id, children, siblings) = {
            let links = self.links.lock().await;
            (
                links.parent.as_ref().map(|(id, _)| *id),
                links
                    .children
                    .keys()
                    .copied()
                    .map(wire_id)
                    .collect::<Vec<_>>(),
                links
                    .sibblings
                    .keys()
                    .copied()
                    .map(wire_id)
                    .collect::<Vec<_>>(),
            )
        };

        // A supervised child's restart policy and live (windowed) restart count live on its
        // supervisor, in the child spec — read them from the parent's monitor.
        let supervision = match parent_id.and_then(|pid| monitors.get(&pid)) {
            Some(parent) => {
                let plinks = parent.links.lock().await;
                plinks
                    .children
                    .get(&self.id)
                    .map(|spec| wire::SupervisionInfo {
                        policy: wire_policy(spec.restart_policy),
                        max_restarts: spec.max_restarts,
                        restart_window: spec.restart_window,
                        restart_count: spec.restart_count,
                    })
            }
            None => None,
        };

        let message_types = {
            let map = self.message_types.lock().unwrap();
            let mut counts: Vec<wire::MessageCount> = map
                .iter()
                .map(|(name, count)| wire::MessageCount {
                    name: name.to_string(),
                    count: *count,
                })
                .collect();
            counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.name.cmp(&b.name)));
            counts
        };

        let is_supervisor = !Vec::is_empty(&children);
        let status = self.wire_status();

        let handling = (self.status.load(Ordering::Relaxed) == RUNNING)
            .then(|| {
                self.current_handler
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|h| wire::HandlerActivity {
                        message: h.message.to_string(),
                        elapsed: h.since.elapsed(),
                    })
            })
            .flatten();

        let waiting_on = (self.status.load(Ordering::Relaxed) == RUNNING)
            .then(|| {
                self.waiting_on
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|w| wire::WaitEdge {
                        target: wire_id(w.target),
                        kind: w.kind,
                        elapsed: w.since.elapsed(),
                    })
            })
            .flatten();

        wire::ActorSnapshot {
            id: wire_id(self.id),
            name: self.name.to_string(),
            status,
            handling,
            waiting_on,
            strategy: is_supervisor.then(|| wire_strategy(self.strategy)),
            spawned_at: self.spawned_at,
            mailbox: wire::MailboxStats {
                kind: self.mailbox_kind,
                // Prefer the live depth for bounded mailboxes (accurate even mid slow handler);
                // fall back to the cached value for unbounded or already-dropped ones.
                len: self
                    .probe
                    .live_mailbox_len()
                    .unwrap_or_else(|| self.mailbox_len.load(Ordering::Relaxed)),
                capacity: self.mailbox_capacity,
            },
            counters: self.wire_counters(),
            message_types,
            refs: wire::RefCounts {
                strong: self.probe.strong_count(),
                weak: self.probe.weak_count(),
            },
            links: wire::Links {
                parent: parent_id.map(wire_id),
                children,
                siblings,
            },
            supervision,
        }
    }

    fn wire_status(&self) -> wire::ActorStatus {
        match self.status.load(Ordering::Relaxed) {
            STARTING => wire::ActorStatus::Starting,
            RUNNING => wire::ActorStatus::Running,
            RESTARTING => wire::ActorStatus::Restarting,
            STOPPING => wire::ActorStatus::Stopping,
            _ => {
                let stopped = self.stopped.lock().unwrap();
                let (at, reason) = stopped
                    .as_ref()
                    .map(|s| (s.at, s.reason.clone()))
                    .unwrap_or_else(|| (SystemTime::now(), String::new()));
                wire::ActorStatus::Stopped { at, reason }
            }
        }
    }

    fn wire_counters(&self) -> wire::ActorCounters {
        wire::ActorCounters {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            lifecycle_signals_received: self.lifecycle_signals_received.load(Ordering::Relaxed),
            link_died_signals_received: self.link_died_signals_received.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
        }
    }
}

/// Registers a freshly prepared actor, or — when the id already exists — records a restart
/// of the same logical actor (its id and mailbox are reused across supervised restarts).
pub(crate) fn register_or_get<A: Actor>(
    id: ActorId,
    mailbox_tx: &MailboxSender<A>,
    links: &Links,
) -> Arc<ActorMonitor> {
    let mut registry = REGISTRY.lock().unwrap();
    if let Some(monitor) = registry.get(&id) {
        monitor.restarts.fetch_add(1, Ordering::Relaxed);
        *monitor.stopped.lock().unwrap() = None;
        *monitor.waiting_on.lock().unwrap() = None;
        // If the actor panicked inside a handler, `end_handler` never ran, so clear the stale
        // handler here — otherwise the restarted actor would show as forever handling the old
        // message (an ever-growing "Stuck" elapsed).
        *monitor.current_handler.lock().unwrap() = None;
        monitor.status.store(RESTARTING, Ordering::Relaxed);
        return Arc::clone(monitor);
    }

    LazyLock::force(&START);
    let total = TOTAL_SPAWNED.fetch_add(1, Ordering::Relaxed) + 1;

    // Safety valve: snapshots reap stopped actors, but a process with the feature enabled and
    // no console ever attached would otherwise retain a monitor per stopped actor forever.
    // Periodically sweep ones stopped longer than a generous window — far longer than any sane
    // display grave window, so an attached client's view is never affected.
    if total.is_multiple_of(SAFETY_SWEEP_EVERY) {
        reap_stopped(&mut registry, SAFETY_GRAVE_WINDOW);
    }

    let monitor = Arc::new(ActorMonitor {
        id,
        name: A::name(),
        strategy: A::supervision_strategy(),
        spawned_at: SystemTime::now(),
        mailbox_kind: match mailbox_tx.max_capacity() {
            Some(_) => wire::MailboxKind::Bounded,
            None => wire::MailboxKind::Unbounded,
        },
        mailbox_capacity: mailbox_tx.max_capacity(),
        links: links.clone(),
        probe: Box::new(mailbox_tx.downgrade()),
        status: AtomicU8::new(STARTING),
        stopped: Mutex::new(None),
        current_handler: Mutex::new(None),
        waiting_on: Mutex::new(None),
        message_types: Mutex::new(HashMap::new()),
        mailbox_len: AtomicUsize::new(0),
        messages_received: AtomicU64::new(0),
        lifecycle_signals_received: AtomicU64::new(0),
        link_died_signals_received: AtomicU64::new(0),
        panics: AtomicU64::new(0),
        restarts: AtomicU64::new(0),
    });
    registry.insert(id, Arc::clone(&monitor));
    monitor
}

/// Builds a full snapshot of the actor system, reaping actors that have been stopped for
/// longer than the grave window in the same pass.
pub(crate) async fn snapshot(grave_window: Duration) -> wire::Snapshot {
    let monitors: Vec<Arc<ActorMonitor>> = {
        let mut registry = REGISTRY.lock().unwrap();
        reap_stopped(&mut registry, grave_window);
        registry.values().map(Arc::clone).collect()
    };

    let by_id: HashMap<ActorId, Arc<ActorMonitor>> =
        monitors.iter().map(|m| (m.id, Arc::clone(m))).collect();

    let mut actors = Vec::with_capacity(monitors.len());
    let mut alive = 0;
    let mut stopped_now = 0;
    let mut messages_received = 0;
    for monitor in &monitors {
        if monitor.status.load(Ordering::Relaxed) == STOPPED {
            stopped_now += 1;
        } else {
            alive += 1;
        }
        messages_received += monitor.messages_received.load(Ordering::Relaxed);
        actors.push(monitor.to_wire(&by_id).await);
    }

    wire::Snapshot {
        seq: SEQ.fetch_add(1, Ordering::Relaxed),
        captured_at: SystemTime::now(),
        uptime: START.elapsed(),
        actors,
        totals: wire::Totals {
            alive,
            total_spawned: TOTAL_SPAWNED.load(Ordering::Relaxed),
            total_stopped: REAPED_STOPPED.load(Ordering::Relaxed) + stopped_now,
            messages_received,
        },
    }
}

/// Drops monitors that have been stopped for longer than `ttl`, counting them toward the
/// process-wide reaped total. Callers must hold the registry lock.
fn reap_stopped(registry: &mut HashMap<ActorId, Arc<ActorMonitor>>, ttl: Duration) {
    registry.retain(|_, monitor| {
        let reap = monitor.status.load(Ordering::Relaxed) == STOPPED
            && monitor
                .stopped
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|s| s.since.elapsed() > ttl);
        if reap {
            REAPED_STOPPED.fetch_add(1, Ordering::Relaxed);
        }
        !reap
    });
}

fn wire_id(id: ActorId) -> wire::ActorId {
    wire::ActorId(id.sequence_id())
}

fn wire_strategy(strategy: SupervisionStrategy) -> wire::SupervisorStrategy {
    match strategy {
        SupervisionStrategy::OneForOne => wire::SupervisorStrategy::OneForOne,
        SupervisionStrategy::OneForAll => wire::SupervisorStrategy::OneForAll,
        SupervisionStrategy::RestForOne => wire::SupervisorStrategy::RestForOne,
    }
}

fn wire_policy(policy: RestartPolicy) -> wire::RestartPolicy {
    match policy {
        RestartPolicy::Permanent => wire::RestartPolicy::Permanent,
        RestartPolicy::Transient => wire::RestartPolicy::Transient,
        RestartPolicy::Never => wire::RestartPolicy::Never,
    }
}
