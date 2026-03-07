use std::{
    any::Any,
    collections::HashMap,
    fmt,
    ops::{self, ControlFlow},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::sync::Mutex;

use crate::{
    actor::{Actor, ActorId},
    error::ActorStopReason,
    mailbox::{MailboxReceiver, SignalMailbox},
    supervision::RestartPolicy,
};

pub type BoxMailboxReceiver = Box<dyn Any + Send>;
pub type BoxActorRef = Box<dyn Any + Send>;
pub type SpawnFactory =
    Box<dyn Fn(BoxMailboxReceiver) -> BoxFuture<'static, BoxActorRef> + Send + Sync>;
pub type ShutdownFn = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// A collection of links to other actors that are notified when the actor dies.
///
/// Links are used for parent-child or sibling relationships, allowing actors to observe each other's lifecycle.
#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
pub struct Links(Arc<Mutex<LinksInner>>);

#[derive(Default)]
pub struct LinksInner {
    /// Parent actor supervising us.
    pub parent: Option<(ActorId, Link)>,
    /// Sibbling linked actors.
    pub sibblings: HashMap<ActorId, Link>,
    /// Child actors we are supervising.
    pub children: HashMap<ActorId, ErasedChildSpec>,
}

impl LinksInner {
    /// Notify parent or sibblings, depending on supervision status.
    pub fn notify_links<A: Actor>(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        mailbox_rx: MailboxReceiver<A>,
    ) {
        match self.parent.clone() {
            Some((parent_id, parent_link)) => {
                // Supervised, notify parent, it will be responsible for notifying sibblings if it decides not to restart
                tokio::spawn(parent_link.notify(parent_id, id, reason, Some(Box::new(mailbox_rx))));
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
                link.notify(sibbling_actor_id, id, reason.clone(), None)
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
    ) {
        match self {
            Link::Local(mailbox) => {
                if let Err(err) = mailbox
                    .signal_link_died(dead_actor_id, reason, mailbox_rx)
                    .await
                {
                    #[cfg(feature = "tracing")]
                    tracing::error!("failed to notify actor a link died: {err}");
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
                    if let Err(err) = res {
                        #[cfg(feature = "tracing")]
                        tracing::error!("failed to notify actor a link died: {err}");
                    }
                }
            }
        }
    }
}

pub struct ErasedChildSpec {
    pub factory: Arc<SpawnFactory>,
    pub shutdown: Arc<ShutdownFn>,
    pub restart_policy: RestartPolicy,
    pub restart_count: u32,
    pub last_restart: Instant,
    pub max_restarts: u32,
    pub restart_window: Duration,
}

impl ErasedChildSpec {
    pub fn should_restart(&mut self, reason: &ActorStopReason) -> ControlFlow<NoRestartReason> {
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
        }

        // Reset count if outside time window
        let now = Instant::now();
        if now.duration_since(self.last_restart) > self.restart_window {
            self.restart_count = 0;
        }

        // Intensity check
        if self.restart_count >= self.max_restarts {
            return ControlFlow::Break(NoRestartReason::MaxRestartsExceeded {
                restart_count: self.restart_count,
                max_restarts: self.max_restarts,
            });
        }

        self.restart_count += 1;
        self.last_restart = now;

        ControlFlow::Continue(())
    }
}

pub enum NoRestartReason {
    /// Transient policy but stop was normal
    NormalExitUnderTransientPolicy,
    /// Restart intensity threshold exceeded
    MaxRestartsExceeded {
        restart_count: u32,
        max_restarts: u32,
    },
}

impl fmt::Display for NoRestartReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoRestartReason::NormalExitUnderTransientPolicy => {
                write!(f, "normal exit under transient policy")
            }
            NoRestartReason::MaxRestartsExceeded {
                restart_count,
                max_restarts,
            } => {
                write!(
                    f,
                    "max restarts exceeded ({} >= {})",
                    restart_count, max_restarts
                )
            }
        }
    }
}
