//! Console wire protocol.
//!
//! **Unstable:** these types are the serialization contract between an
//! instrumented kameo app and a console client. The format may change in any
//! release. Enable via the `console` feature

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

/// One message on the wire. Snapshot-only for now; the variant keeps the door
/// open for an incremental event stream later without a protocol rewrite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Snapshot(Snapshot),
    // Update(Vec<Event>),   // added later: spawned / stopped / panicked …
}

/// A full picture of the actor system at one instant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Incremented once per produced snapshot. Resets to 0 when the source
    /// process restarts — the client uses a drop in `seq`/`uptime` to know the
    /// counters reset and avoid rendering bogus negative rates.
    pub seq: u64,
    /// Source-clock timestamp. The client diffs `captured_at` between two
    /// snapshots for an accurate `dt`, rather than trusting its own receive time.
    pub captured_at: SystemTime,
    /// How long the source process has been running.
    pub uptime: Duration,
    /// Every actor the collector currently tracks, including recently-stopped
    /// ones within the grave window (see `ActorStatus::Stopped`).
    pub actors: Vec<ActorSnapshot>,
    /// Process-wide rollups, so the client doesn't recompute them every frame.
    pub totals: Totals,
}

/// Local-only for now: just the sequence id. A newtype so a `peer_id` field can
/// be added later for remote monitoring without churning every call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorSnapshot {
    pub id: ActorId,
    /// `A::name()` — not unique; the id is the unique key.
    pub name: String,
    pub status: ActorStatus,
    /// `Some` while the actor is inside a message handler: the message type name and how long
    /// it has been running. Surfaces slow or stuck (potentially deadlocked) handlers.
    pub handling: Option<HandlerActivity>,
    /// `Some` while the actor is blocked inside a handler waiting on another actor (an `ask`
    /// reply). These edges form the wait-for graph the client walks to detect deadlocks.
    pub waiting_on: Option<WaitEdge>,
    /// `Some` for supervisors, describing how they restart children; `None` otherwise.
    pub strategy: Option<SupervisorStrategy>,
    pub spawned_at: SystemTime,
    pub mailbox: MailboxStats,
    /// Monotonic counters. The client diffs consecutive snapshots for rates;
    /// the source never sends pre-computed rates.
    pub counters: ActorCounters,
    /// Cumulative count of received messages broken down by message type name, descending by
    /// count. Empty for actors that handle no messages.
    pub message_types: Vec<MessageCount>,
    pub refs: RefCounts,
    pub links: Links,
    /// `Some` for supervised children, describing their restart policy and limits.
    pub supervision: Option<SupervisionInfo>,
}

/// A message type name and how many of that message the actor has received.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageCount {
    pub name: String,
    pub count: u64,
}

/// The restart configuration of a supervised child, read from its supervisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisionInfo {
    pub policy: RestartPolicy,
    pub max_restarts: u32,
    pub restart_window: Duration,
    /// Restarts within the current window. `restart_count >= max_restarts` means the
    /// supervisor has stopped restarting this child ("at limit").
    pub restart_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestartPolicy {
    Permanent,
    Transient,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupervisorStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

/// The message a handler is currently processing, and how long it has been running. A large
/// `elapsed` is the primary "slow/stuck handler" signal the console surfaces.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerActivity {
    /// The message type name (`Message`'s type), not its contents.
    pub message: String,
    /// How long the handler has been running, as of `Snapshot::captured_at`.
    pub elapsed: Duration,
}

/// An edge in the wait-for graph: an actor blocked inside a handler waiting on `target`. A
/// cycle of these edges is a deadlock.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitEdge {
    /// The actor being waited on.
    pub target: ActorId,
    pub kind: WaitKind,
    /// How long the wait has been blocked, as of `Snapshot::captured_at`.
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitKind {
    /// Blocked awaiting an `ask` reply.
    Ask,
    /// Blocked awaiting mailbox capacity on a bounded `tell`.
    Tell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActorStatus {
    Starting,
    Running,
    Restarting,
    Stopping,
    /// Kept in the snapshot for a short grave window after death so the actor
    /// fades out instead of blinking away, and so very short-lived actors are
    /// still visible for at least one poll.
    Stopped {
        at: SystemTime,
        /// `ActorStopReason` rendered to a string on the source side.
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailboxStats {
    pub kind: MailboxKind,
    /// Messages queued right now (derived: `max_capacity - capacity`).
    pub len: usize,
    /// `Some` for bounded mailboxes, `None` for unbounded.
    pub capacity: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailboxKind {
    Bounded,
    Unbounded,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActorCounters {
    pub messages_received: u64,
    pub lifecycle_signals_received: u64,
    pub link_died_signals_received: u64,
    pub panics: u64,
    pub restarts: u64,
}

/// `ActorRef::strong_count` / `weak_count`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RefCounts {
    pub strong: usize,
    pub weak: usize,
}

/// Topology. Supervision (`parent`/`children`) is distinct from non-supervising
/// peer `links`, mirroring kameo's `Links` (children carry a restart policy).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Links {
    pub parent: Option<ActorId>,
    pub children: Vec<ActorId>,
    pub siblings: Vec<ActorId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Totals {
    pub alive: usize,
    pub total_spawned: u64,
    pub total_stopped: u64,
    pub messages_received: u64,
}
