use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kameo::{error::Infallible, prelude::*, supervision::RestartPolicy};
use tokio::sync::mpsc;

const EVENT_TIMEOUT: Duration = Duration::from_secs(2);
const CHURN_CHILDREN: usize = 64;

struct Supervisor;

impl Actor for Supervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

struct FactoryAnchor {
    live: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    dropped_tx: mpsc::UnboundedSender<()>,
}

impl FactoryAnchor {
    fn new(
        live: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        dropped_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        live.fetch_add(1, Ordering::AcqRel);
        Self {
            live,
            drops,
            dropped_tx,
        }
    }
}

impl Drop for FactoryAnchor {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::AcqRel);
        self.drops.fetch_add(1, Ordering::AcqRel);
        let _ = self.dropped_tx.send(());
    }
}

struct TerminalArgs {
    ready_tx: mpsc::UnboundedSender<usize>,
    stopped_tx: mpsc::UnboundedSender<usize>,
    child: usize,
    factory_anchor: Arc<FactoryAnchor>,
    clones: Arc<AtomicUsize>,
}

impl Clone for TerminalArgs {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::AcqRel);
        Self {
            ready_tx: self.ready_tx.clone(),
            stopped_tx: self.stopped_tx.clone(),
            child: self.child,
            factory_anchor: Arc::clone(&self.factory_anchor),
            clones: Arc::clone(&self.clones),
        }
    }
}

struct TerminalChild {
    child: usize,
    stopped_tx: mpsc::UnboundedSender<usize>,
}

impl Actor for TerminalChild {
    type Args = TerminalArgs;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        let TerminalArgs {
            ready_tx,
            stopped_tx,
            child,
            factory_anchor,
            clones: _,
        } = args;

        // The initial cloned args have been consumed before readiness is published. The only
        // remaining anchor is the original args retained by the supervisor-owned restart factory.
        drop(factory_anchor);
        let _ = ready_tx.send(child);

        Ok(Self { child, stopped_tx })
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        let _ = self.stopped_tx.send(self.child);
        Ok(())
    }
}

async fn recv<T>(receiver: &mut mpsc::UnboundedReceiver<T>, waiting_for: &str) -> T {
    tokio::time::timeout(EVENT_TIMEOUT, receiver.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {waiting_for}"))
        .unwrap_or_else(|| panic!("channel closed while waiting for {waiting_for}"))
}

async fn shutdown_supervisor(supervisor: &ActorRef<Supervisor>) -> Result<(), &'static str> {
    supervisor.kill();
    tokio::time::timeout(EVENT_TIMEOUT, supervisor.wait_for_shutdown())
        .await
        .map_err(|_| "timed out waiting for supervisor shutdown")
}

fn terminal_args(
    child: usize,
    ready_tx: mpsc::UnboundedSender<usize>,
    stopped_tx: mpsc::UnboundedSender<usize>,
    live: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    dropped_tx: mpsc::UnboundedSender<()>,
    clones: Arc<AtomicUsize>,
) -> (TerminalArgs, Weak<FactoryAnchor>) {
    let factory_anchor = Arc::new(FactoryAnchor::new(live, drops, dropped_tx));
    let weak_factory_anchor = Arc::downgrade(&factory_anchor);
    let args = TerminalArgs {
        ready_tx,
        stopped_tx,
        child,
        factory_anchor,
        clones,
    };
    (args, weak_factory_anchor)
}

#[tokio::test]
async fn terminal_transient_child_is_removed_once() {
    let supervisor = Supervisor::spawn(Supervisor);
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
    let (stopped_tx, mut stopped_rx) = mpsc::unbounded_channel();
    let (factory_dropped_tx, mut factory_dropped_rx) = mpsc::unbounded_channel();
    let live_factory_anchors = Arc::new(AtomicUsize::new(0));
    let factory_drops = Arc::new(AtomicUsize::new(0));
    let args_cloned = Arc::new(AtomicUsize::new(0));
    let (args, weak_factory_anchor) = terminal_args(
        0,
        ready_tx,
        stopped_tx,
        Arc::clone(&live_factory_anchors),
        Arc::clone(&factory_drops),
        factory_dropped_tx,
        Arc::clone(&args_cloned),
    );

    let child = TerminalChild::supervise(&supervisor, args)
        .restart_policy(RestartPolicy::Transient)
        .spawn()
        .await;
    let weak_child = child.downgrade();

    assert_eq!(recv(&mut ready_rx, "child startup").await, 0);
    assert_eq!(
        args_cloned.load(Ordering::Acquire),
        1,
        "supervision must create exactly one initial cloned args value"
    );
    assert_eq!(
        live_factory_anchors.load(Ordering::Acquire),
        1,
        "after startup only the supervisor factory should own the original args anchor"
    );

    child
        .stop_gracefully()
        .await
        .expect("terminal transient child accepts graceful stop");
    assert_eq!(recv(&mut stopped_rx, "child terminal stop").await, 0);
    child.wait_for_shutdown().await;
    drop(child);

    let released_before_parent_shutdown =
        tokio::time::timeout(EVENT_TIMEOUT, factory_dropped_rx.recv()).await;
    let factory_drops_before_parent_shutdown = factory_drops.load(Ordering::Acquire);
    let anchors_before_parent_shutdown = live_factory_anchors.load(Ordering::Acquire);
    let child_ref_released_before_parent_shutdown = weak_child.upgrade().is_none();
    let child_ref_strong_count_before_parent_shutdown = weak_child.strong_count();
    let factory_anchor_released_before_parent_shutdown = weak_factory_anchor.upgrade().is_none();
    let cleanup = shutdown_supervisor(&supervisor).await;
    let factory_drops_after_parent_shutdown = factory_drops.load(Ordering::Acquire);

    assert!(
        matches!(released_before_parent_shutdown, Ok(Some(()))),
        "terminal child factory must release before its supervisor shuts down"
    );
    assert_eq!(
        factory_drops_before_parent_shutdown, 1,
        "terminal child factory anchor must drop exactly once"
    );
    assert_eq!(
        factory_drops_after_parent_shutdown, 1,
        "parent shutdown must not release a terminal child factory a second time"
    );
    assert_eq!(
        anchors_before_parent_shutdown, 0,
        "terminal child must release the factory-held cloned args anchor"
    );
    assert!(
        factory_anchor_released_before_parent_shutdown,
        "the factory closure must not retain the original cloned args"
    );
    assert!(
        child_ref_released_before_parent_shutdown,
        "terminal child must release the supervisor-held logical ActorRef and signal sender"
    );
    assert_eq!(
        child_ref_strong_count_before_parent_shutdown, 0,
        "no supervisor-owned strong logical ActorRef may remain after terminal removal"
    );
    assert!(cleanup.is_ok(), "supervisor cleanup must finish");
}

#[tokio::test]
async fn dynamic_terminal_child_churn_returns_to_baseline() {
    let supervisor = Supervisor::spawn(Supervisor);
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
    let (stopped_tx, mut stopped_rx) = mpsc::unbounded_channel();
    let (factory_dropped_tx, mut factory_dropped_rx) = mpsc::unbounded_channel();
    let live_factory_anchors = Arc::new(AtomicUsize::new(0));
    let factory_drops = Arc::new(AtomicUsize::new(0));
    let args_cloned = Arc::new(AtomicUsize::new(0));
    let baseline_live_factory_anchors = live_factory_anchors.load(Ordering::Acquire);
    let baseline_factory_drops = factory_drops.load(Ordering::Acquire);
    let mut weak_children = Vec::with_capacity(CHURN_CHILDREN);
    let mut weak_factory_anchors = Vec::with_capacity(CHURN_CHILDREN);

    for child in 0..CHURN_CHILDREN {
        let (args, weak_factory_anchor) = terminal_args(
            child,
            ready_tx.clone(),
            stopped_tx.clone(),
            Arc::clone(&live_factory_anchors),
            Arc::clone(&factory_drops),
            factory_dropped_tx.clone(),
            Arc::clone(&args_cloned),
        );
        let child_ref = TerminalChild::supervise(&supervisor, args)
            .restart_policy(RestartPolicy::Transient)
            .spawn()
            .await;

        assert_eq!(recv(&mut ready_rx, "dynamic child startup").await, child);
        child_ref
            .stop_gracefully()
            .await
            .expect("dynamic terminal child accepts graceful stop");
        assert_eq!(
            recv(&mut stopped_rx, "dynamic child terminal stop").await,
            child
        );
        child_ref.wait_for_shutdown().await;
        weak_children.push(child_ref.downgrade());
        weak_factory_anchors.push(weak_factory_anchor);
        drop(child_ref);
    }

    for _ in 0..CHURN_CHILDREN {
        recv(&mut factory_dropped_rx, "terminal child factory release").await;
    }

    let live_after_churn = live_factory_anchors.load(Ordering::Acquire);
    let drops_after_churn = factory_drops.load(Ordering::Acquire);
    let retained_child_refs = weak_children
        .iter()
        .filter(|weak_child| weak_child.upgrade().is_some())
        .count();
    let retained_factory_anchors = weak_factory_anchors
        .iter()
        .filter(|weak_factory_anchor| weak_factory_anchor.upgrade().is_some())
        .count();
    let cleanup = shutdown_supervisor(&supervisor).await;

    assert_eq!(
        args_cloned.load(Ordering::Acquire),
        CHURN_CHILDREN,
        "every dynamic child must build exactly one initial cloned args value"
    );
    assert_eq!(
        live_after_churn, baseline_live_factory_anchors,
        "dynamic terminal churn must return factory-owned args to baseline"
    );
    assert_eq!(
        drops_after_churn,
        baseline_factory_drops + CHURN_CHILDREN,
        "each terminal child factory must drop exactly once"
    );
    assert_eq!(
        retained_factory_anchors, 0,
        "dynamic terminal churn must not retain factory or cloned-args anchors"
    );
    assert_eq!(
        retained_child_refs, 0,
        "dynamic terminal churn must not retain logical ActorRefs or signal senders"
    );
    assert!(cleanup.is_ok(), "supervisor cleanup must finish");
}
