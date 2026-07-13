use std::{
    any::Any,
    collections::HashMap,
    fmt, mem,
    ops::{self, ControlFlow},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{
    FutureExt, StreamExt,
    future::{BoxFuture, join_all},
    stream::FuturesUnordered,
};
use tokio::sync::Mutex;

use crate::{
    actor::{Actor, ActorId},
    error::ActorStopReason,
    mailbox::{MailboxReceiver, SignalMailbox},
    supervision::RestartPolicy,
};

pub type BoxMailboxReceiver = Box<dyn Any + Send>;
pub type SpawnFactory = Box<dyn Fn(BoxMailboxReceiver) -> BoxFuture<'static, ()> + Send + Sync>;
pub type ShutdownFn = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// A collection of links to other actors that are notified when the actor dies.
///
/// Links are used for parent-child or sibling relationships, allowing actors to observe each other's lifecycle.
#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
pub struct Links(Arc<Mutex<LinksInner>>);

impl Links {
    /// Creates links for a supervised child, pre-populated with the restart configuration its
    /// supervisor shares with it: the `restart_policy`, the shared `restart_tracker` (so the child
    /// can predict its own restarts in [`LinksInner::will_restart`]), and the shared
    /// `parent_shutdown` flag. Building the inner value up front avoids locking it right after
    /// construction.
    pub fn supervised(
        restart_policy: RestartPolicy,
        restart_tracker: Arc<RestartTracker>,
        parent_shutdown: Arc<AtomicBool>,
    ) -> Self {
        Links(Arc::new(Mutex::new(LinksInner {
            restart_policy: Some(restart_policy),
            restart_tracker: Some(restart_tracker),
            parent_shutdown,
            ..Default::default()
        })))
    }

    /// Sets the `parent_shutdown` flag on all supervised children.
    ///
    /// This must be called before the parent stops processing its mailbox so that any child
    /// exiting independently after this point will drop its `mailbox_rx` immediately in
    /// `notify_links`, preventing it from being queued in the parent's mailbox where it would
    /// deadlock the `shutdown_children` wait.
    pub async fn set_children_parent_shutdown(&self) {
        let inner = self.lock().await;
        for spec in inner.children.values() {
            spec.parent_shutdown.store(true, Ordering::Release);
        }
    }

    pub async fn send_children_shutdown(&self) {
        let shutdowns: Vec<_> = {
            let inner = self.lock().await;
            inner
                .children
                .values()
                .map(|c| c.shutdown.clone())
                .collect()
        };

        join_all(shutdowns.iter().map(|s| s())).await;
    }

    pub async fn wait_children_closed(&self) {
        let mailboxes: Vec<_> = {
            let inner = self.lock().await;
            inner
                .children
                .values()
                .map(|c| c.signal_mailbox.clone())
                .collect()
        };

        join_all(mailboxes.iter().map(|m| m.closed())).await;
    }

    pub(crate) async fn clear_children(&self) {
        self.lock().await.children.clear();
    }
}

#[derive(Default)]
pub struct LinksInner {
    /// Parent actor supervising us.
    pub parent: Option<(ActorId, Link)>,
    /// Sibbling linked actors.
    pub sibblings: HashMap<ActorId, Link>,
    /// Child actors we are supervising.
    pub children: HashMap<ActorId, ErasedChildSpec>,
    /// Set by the parent supervisor (via `ErasedChildSpec`) before sending `SupervisorRestart`
    /// during final shutdown. When `true`, `notify_links` drops `mailbox_rx` immediately
    /// instead of queuing it in the parent's mailbox, which would deadlock the parent's
    /// `shutdown_children` wait.
    pub parent_shutdown: Arc<AtomicBool>,
    /// The restart policy our supervisor will apply to us, `Some` only when supervised. Lets the
    /// actor determine, during its own teardown, whether it will be restarted (see
    /// [`Self::will_restart`]).
    pub restart_policy: Option<RestartPolicy>,
    /// The restart intensity bookkeeping, shared by `Arc` with our supervisor's [`ErasedChildSpec`].
    /// `Some` only when supervised. Lets [`Self::will_restart`] apply the same restart-limit check
    /// the supervisor uses, instead of guessing.
    pub restart_tracker: Option<Arc<RestartTracker>>,
}

impl LinksInner {
    /// Determines, during the actor's own teardown, whether its supervisor will restart it.
    /// Mirrors [`ErasedChildSpec::should_restart`], reading the same shared [`RestartTracker`] so
    /// the restart-limit check matches the supervisor's decision. `run_actor_lifecycle` uses this
    /// both to route a dropped `ask`'s message (`ActorRestarting` vs `ActorNotRunning`) and to
    /// decide whether leftover tells go to `Actor::on_undelivered`, so it must not report a restart
    /// that won't happen.
    ///
    /// The child reads the tracker slightly before the supervisor decides. Because the restart
    /// count only changes on the supervisor's restart path (which hasn't run yet for this death)
    /// and `now - last_restart` is monotonic, the only possible disagreement is at a restart-window
    /// boundary, where the child predicts no restart but the supervisor restarts: the leftover
    /// tells reach `on_undelivered` instead of the new incarnation. No message is dropped either
    /// way. The opposite (predict restart, supervisor declines) cannot occur.
    pub fn will_restart(&self, reason: &ActorStopReason) -> bool {
        let Some(policy) = self.restart_policy else {
            return false; // unsupervised: nobody will restart us
        };
        if self.parent_shutdown.load(Ordering::Acquire) {
            return false; // our supervisor is going away too
        }
        if matches!(reason, ActorStopReason::Shutdown) {
            return false; // explicit graceful shutdown is terminal
        }
        match policy {
            RestartPolicy::Never => false,
            // a coordinator-initiated restart bypasses the intensity check, same as the supervisor
            _ if matches!(reason, ActorStopReason::SupervisorRestart) => true,
            RestartPolicy::Permanent => self.restart_intensity_allows(),
            RestartPolicy::Transient => !reason.is_normal() && self.restart_intensity_allows(),
        }
    }

    /// Whether the shared restart budget still permits a restart. Falls back to `true`
    /// if no tracker is set, which shouldn't happen while supervised.
    fn restart_intensity_allows(&self) -> bool {
        self.restart_tracker
            .as_ref()
            .is_none_or(|tracker| tracker.predict_restart())
    }

    /// Notify parent or sibblings, depending on supervision status.
    pub fn notify_links<A: Actor>(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        mailbox_rx: MailboxReceiver<A>,
    ) {
        match self.parent.clone() {
            Some((parent_id, parent_link)) => {
                let sibblings = mem::take(&mut self.sibblings);
                if self.parent_shutdown.load(Ordering::Acquire) {
                    // Parent is in shutdown_children — drop mailbox_rx immediately so the
                    // child's channel closes and the parent's mailbox.closed() wait resolves.
                    // Passing it to the parent's queue would deadlock since the parent is not
                    // processing its mailbox while blocked in shutdown_children.
                    tokio::spawn(parent_link.notify(parent_id, id, reason, None, None));
                } else {
                    // Supervised normal path — pass mailbox_rx so the parent can restart us.
                    tokio::spawn(parent_link.notify(
                        parent_id,
                        id,
                        reason,
                        Some(Box::new(mailbox_rx)),
                        Some(sibblings),
                    ));
                }
            }
            None => {
                // Unsupervised, notify sibblings directly
                self.notify_sibblings(id, &reason);
            }
        }
    }

    /// Notify sibbling links.
    pub fn notify_sibblings(&mut self, id: ActorId, reason: &ActorStopReason) {
        let mut notify_futs: FuturesUnordered<_> = self
            .sibblings
            .drain()
            .map(|(sibbling_actor_id, link)| {
                link.notify(sibbling_actor_id, id, reason.clone(), None, None)
                    .boxed()
            })
            .collect();
        tokio::spawn(async move { while let Some(()) = notify_futs.next().await {} });
    }
}

impl ops::Deref for Links {
    type Target = Mutex<LinksInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub enum Link {
    Local(Box<dyn SignalMailbox>),
    #[cfg(feature = "remote")]
    Remote(std::borrow::Cow<'static, str>),
}

impl Link {
    #[cfg_attr(not(feature = "remote"), allow(unused_variables))]
    pub async fn notify(
        self,
        link_actor_id: ActorId,
        dead_actor_id: ActorId,
        reason: ActorStopReason,
        mailbox_rx: Option<BoxMailboxReceiver>,
        dead_actor_sibblings: Option<HashMap<ActorId, Link>>,
    ) {
        match self {
            Link::Local(mailbox) => {
                #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                if let Err(err) = mailbox
                    .signal_link_died(dead_actor_id, reason, mailbox_rx, dead_actor_sibblings)
                    .await
                {
                    #[cfg(feature = "tracing")]
                    match err {
                        crate::error::SendError::ActorNotRunning(_) => {}
                        _ => {
                            tracing::error!("failed to notify actor a link died: {err}");
                        }
                    }
                }
            }
            #[cfg(feature = "remote")]
            Link::Remote(notified_actor_remote_id) => {
                if let Some(swarm) = crate::remote::ActorSwarm::get() {
                    let res = swarm
                        .signal_link_died(
                            dead_actor_id,
                            link_actor_id,
                            notified_actor_remote_id,
                            reason,
                        )
                        .await;
                    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                    if let Err(err) = res {
                        #[cfg(feature = "tracing")]
                        tracing::error!("failed to notify actor a link died: {err}");
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ErasedChildSpec {
    pub factory: Arc<SpawnFactory>,
    pub shutdown: Arc<ShutdownFn>,
    pub signal_mailbox: Box<dyn SignalMailbox>,
    /// Shared with the child's `LinksInner::parent_shutdown`. Set to `true` by
    /// `shutdown_children` before sending `SupervisorRestart`, so the child knows to drop
    /// its `mailbox_rx` immediately rather than forwarding it to the parent's queue.
    pub parent_shutdown: Arc<AtomicBool>,
    pub restart_policy: RestartPolicy,
    /// Restart intensity bookkeeping, shared by `Arc` with the child's
    /// [`LinksInner::restart_tracker`] so the child can predict this same decision.
    pub restart_tracker: Arc<RestartTracker>,
}

impl ErasedChildSpec {
    pub fn should_restart(&self, reason: &ActorStopReason) -> ControlFlow<NoRestartReason> {
        // Never policy takes precedence over everything, including coordinator-initiated restarts
        if matches!(self.restart_policy, RestartPolicy::Never) {
            return ControlFlow::Break(NoRestartReason::NeverPolicy);
        }

        if matches!(reason, ActorStopReason::Shutdown) {
            return ControlFlow::Break(NoRestartReason::Shutdown);
        }

        // Always restart if supervisor initiated the shutdown for coordination
        if matches!(reason, ActorStopReason::SupervisorRestart) {
            return ControlFlow::Continue(());
        }

        // Policy check
        match self.restart_policy {
            RestartPolicy::Permanent => {}
            RestartPolicy::Transient if reason.is_normal() => {
                return ControlFlow::Break(NoRestartReason::NormalExitUnderTransientPolicy);
            }
            RestartPolicy::Transient => {}
            RestartPolicy::Never => unreachable!(),
        }

        self.restart_tracker.record_restart()
    }
}

/// Restart intensity bookkeeping for a supervised child, shared by `Arc` between the supervisor's
/// [`ErasedChildSpec`] and the child's [`LinksInner`]. The supervisor mutates it authoritatively
/// via [`Self::record_restart`]; the child reads it via [`Self::predict_restart`] to decide, during
/// its own teardown, whether it is about to be restarted.
#[derive(Debug)]
pub struct RestartTracker {
    max_restarts: u32,
    restart_window: Duration,
    // std Mutex: only ever locked for the short, non-async body of the methods below, and
    // independent of the async `Links` mutexes, so it can't deadlock against them.
    state: std::sync::Mutex<RestartTrackerState>,
}

#[derive(Debug)]
struct RestartTrackerState {
    restart_count: u32,
    last_restart: Instant,
}

impl RestartTracker {
    pub fn new(max_restarts: u32, restart_window: Duration) -> Self {
        RestartTracker {
            max_restarts,
            restart_window,
            state: std::sync::Mutex::new(RestartTrackerState {
                restart_count: 0,
                last_restart: Instant::now(),
            }),
        }
    }

    /// Authoritative check made by the supervisor. Applies the restart-window reset and intensity
    /// limit, recording the restart when it is allowed.
    pub fn record_restart(&self) -> ControlFlow<NoRestartReason> {
        let mut state = self.state.lock().unwrap();

        // Reset count if outside time window
        let now = Instant::now();
        if now.duration_since(state.last_restart) > self.restart_window {
            state.restart_count = 0;
        }

        // Intensity check
        if state.restart_count >= self.max_restarts {
            return ControlFlow::Break(NoRestartReason::MaxRestartsExceeded {
                restart_count: state.restart_count,
                max_restarts: self.max_restarts,
            });
        }

        state.restart_count += 1;
        state.last_restart = now;

        ControlFlow::Continue(())
    }

    /// Read-only mirror of [`Self::record_restart`]'s intensity decision, used by the child to
    /// predict whether the supervisor will restart it. Does not mutate the count.
    pub fn predict_restart(&self) -> bool {
        let state = self.state.lock().unwrap();
        let count = if Instant::now().duration_since(state.last_restart) > self.restart_window {
            0
        } else {
            state.restart_count
        };
        count < self.max_restarts
    }

    #[cfg(feature = "console")]
    pub fn max_restarts(&self) -> u32 {
        self.max_restarts
    }

    #[cfg(feature = "console")]
    pub fn restart_window(&self) -> Duration {
        self.restart_window
    }

    #[cfg(any(feature = "tracing", feature = "console"))]
    pub fn current_count(&self) -> u32 {
        self.state.lock().unwrap().restart_count
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoRestartReason {
    /// Explicit graceful shutdown is terminal
    Shutdown,
    /// Transient policy but stop was normal
    NormalExitUnderTransientPolicy,
    /// Restart intensity threshold exceeded
    MaxRestartsExceeded {
        restart_count: u32,
        max_restarts: u32,
    },
    /// Never policy — this child is configured to never restart
    NeverPolicy,
}

impl fmt::Display for NoRestartReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoRestartReason::Shutdown => write!(f, "graceful shutdown"),
            NoRestartReason::NormalExitUnderTransientPolicy => {
                write!(f, "normal exit under transient policy")
            }
            NoRestartReason::MaxRestartsExceeded {
                restart_count,
                max_restarts,
            } => {
                write!(
                    f,
                    "max restarts exceeded ({restart_count} >= {max_restarts})"
                )
            }
            NoRestartReason::NeverPolicy => write!(f, "never restart policy"),
        }
    }
}
