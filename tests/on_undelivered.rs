use std::sync::Arc;
use std::time::Duration;

use kameo::error::{Infallible, SendError};
use kameo::prelude::*;
use kameo::supervision::RestartPolicy;
use tokio::sync::{Notify, mpsc, oneshot};

const TIMEOUT: Duration = Duration::from_secs(5);

async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(TIMEOUT, rx.recv())
        .await
        .expect("timed out waiting for value")
        .expect("channel closed")
}

// A message the hook downcasts back to.
struct Work(u32);

// ==================== DrainActor (terminal-stop tests) ====================

struct DrainActor {
    undelivered_tx: mpsc::UnboundedSender<(ActorStopReason, Vec<u32>)>,
    entered_tx: mpsc::UnboundedSender<()>,
    gate: Option<oneshot::Receiver<()>>,
}

impl Actor for DrainActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }

    async fn on_undelivered(
        &mut self,
        reason: ActorStopReason,
        undelivered: Vec<BoxMessage<Self>>,
    ) -> Result<(), Self::Error> {
        let mut values = Vec::new();
        for msg in undelivered {
            if let Ok(work) = msg.as_any().downcast::<Work>() {
                values.push(work.0);
            }
        }
        let _ = self.undelivered_tx.send((reason, values));
        Ok(())
    }
}

// Occupies the actor until the gate is released, so a backlog can build behind it. It signals once
// it starts running (which proves startup has finished, so later messages land in the mailbox
// rather than the startup buffer) and then blocks.
struct Block;

impl Message<Block> for DrainActor {
    type Reply = ();

    async fn handle(&mut self, _: Block, _: &mut Context<Self, Self::Reply>) {
        let _ = self.entered_tx.send(());
        if let Some(gate) = self.gate.take() {
            let _ = gate.await;
        }
    }
}

impl Message<Work> for DrainActor {
    type Reply = ();

    async fn handle(&mut self, _: Work, _: &mut Context<Self, Self::Reply>) {}
}

// Leftover tells are handed to the hook on a terminal stop, with the stop reason.
#[tokio::test]
async fn undelivered_tells_delivered_on_terminal_stop() {
    let (undelivered_tx, mut undelivered_rx) = mpsc::unbounded_channel();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let (gate_tx, gate_rx) = oneshot::channel();
    let actor = DrainActor::spawn(DrainActor {
        undelivered_tx,
        entered_tx,
        gate: Some(gate_rx),
    });

    actor.tell(Block).await.unwrap();
    recv(&mut entered_rx).await; // Block is now running and about to block

    actor.tell(Work(1)).await.unwrap();
    actor.tell(Work(2)).await.unwrap();
    actor.tell(Work(3)).await.unwrap();

    actor.kill();
    actor.wait_for_shutdown().await;

    let (reason, values) = recv(&mut undelivered_rx).await;
    assert!(matches!(reason, ActorStopReason::Killed));
    assert_eq!(values, vec![1, 2, 3]);

    let _ = gate_tx; // keep the gate un-fired until after kill
}

// The hook is not called when the mailbox is empty at stop.
#[tokio::test]
async fn undelivered_not_called_when_mailbox_empty() {
    let (undelivered_tx, mut undelivered_rx) = mpsc::unbounded_channel();
    let (entered_tx, _entered_rx) = mpsc::unbounded_channel();
    let actor = DrainActor::spawn(DrainActor {
        undelivered_tx,
        entered_tx,
        gate: None,
    });

    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;

    assert!(undelivered_rx.try_recv().is_err());
}

// Pending asks are returned to their caller with the message; only tells reach the hook.
#[tokio::test]
async fn undelivered_asks_bounce_to_caller_not_hook() {
    let (undelivered_tx, mut undelivered_rx) = mpsc::unbounded_channel();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let (gate_tx, gate_rx) = oneshot::channel();
    let actor = DrainActor::spawn(DrainActor {
        undelivered_tx,
        entered_tx,
        gate: Some(gate_rx),
    });

    actor.tell(Block).await.unwrap();
    recv(&mut entered_rx).await; // Block is now running and about to block

    actor.tell(Work(1)).await.unwrap();
    // Enqueue an ask synchronously so it's queued before the kill.
    let pending = actor.ask(Work(2)).try_enqueue().unwrap();

    actor.kill();
    actor.wait_for_shutdown().await;

    // The ask caller gets its message back.
    let ask_result = pending.await;
    assert!(matches!(&ask_result, Err(SendError::ActorNotRunning(work)) if work.0 == 2));

    // The hook received only the tell.
    let (_reason, values) = recv(&mut undelivered_rx).await;
    assert_eq!(values, vec![1]);

    let _ = gate_tx;
}

// ==================== Supervised restart test ====================

struct Supervisor;

impl Actor for Supervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

#[derive(Clone)]
struct RestartWorker {
    undelivered_tx: mpsc::UnboundedSender<Vec<u32>>,
    processed_tx: mpsc::UnboundedSender<u32>,
    started_tx: mpsc::UnboundedSender<()>,
    entered_tx: mpsc::UnboundedSender<()>,
    // Shared (Clone-friendly) so it survives being cloned into each restarted incarnation. It is
    // never fired; the exhausted-budget test kills the actor while a Block handler waits on it.
    gate: Arc<Notify>,
}

impl Actor for RestartWorker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        this.started_tx.send(()).unwrap();
        Ok(this)
    }

    async fn on_undelivered(
        &mut self,
        _: ActorStopReason,
        undelivered: Vec<BoxMessage<Self>>,
    ) -> Result<(), Self::Error> {
        let mut values = Vec::new();
        for msg in undelivered {
            if let Ok(work) = msg.as_any().downcast::<Work>() {
                values.push(work.0);
            }
        }
        let _ = self.undelivered_tx.send(values);
        Ok(())
    }
}

struct PanicMsg;

impl Message<PanicMsg> for RestartWorker {
    type Reply = ();

    async fn handle(&mut self, _: PanicMsg, _: &mut Context<Self, Self::Reply>) {
        // Sleep so the following tells are queued before we panic.
        tokio::time::sleep(Duration::from_millis(30)).await;
        panic!("intentional panic for testing");
    }
}

impl Message<Work> for RestartWorker {
    type Reply = ();

    async fn handle(&mut self, Work(n): Work, _: &mut Context<Self, Self::Reply>) {
        let _ = self.processed_tx.send(n);
    }
}

impl Message<Block> for RestartWorker {
    type Reply = ();

    async fn handle(&mut self, _: Block, _: &mut Context<Self, Self::Reply>) {
        let _ = self.entered_tx.send(());
        self.gate.notified().await;
    }
}

// On a supervisor restart the pending tells are preserved for the next incarnation, so the hook
// must not fire.
#[tokio::test]
async fn undelivered_not_called_on_restart() {
    let supervisor = Supervisor::spawn(Supervisor);
    let (undelivered_tx, mut undelivered_rx) = mpsc::unbounded_channel();
    let (processed_tx, mut processed_rx) = mpsc::unbounded_channel();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let worker = RestartWorker::supervise(
        &supervisor,
        RestartWorker {
            undelivered_tx,
            processed_tx,
            started_tx,
            entered_tx: mpsc::unbounded_channel().0,
            gate: Arc::new(Notify::new()),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    .restart_limit(5, Duration::from_secs(10))
    .spawn()
    .await;

    recv(&mut started_rx).await; // initial startup

    worker.tell(PanicMsg).await.unwrap();
    worker.tell(Work(10)).await.unwrap();
    worker.tell(Work(11)).await.unwrap();

    recv(&mut started_rx).await; // restart startup

    // Both tells are processed by the restarted incarnation, proving they survived the restart.
    assert_eq!(recv(&mut processed_rx).await, 10);
    assert_eq!(recv(&mut processed_rx).await, 11);

    // The hook was never called during the restart.
    assert!(undelivered_rx.try_recv().is_err());

    supervisor.kill();
    supervisor.wait_for_shutdown().await;
}

// A supervised actor that has exhausted its restart budget is not actually restarted, so its
// leftover tells must reach on_undelivered rather than being dropped when the supervisor declines
// the restart. Regression test for the restart-exhaustion leak.
#[tokio::test]
async fn undelivered_called_when_restart_budget_exhausted() {
    let supervisor = Supervisor::spawn(Supervisor);
    let (undelivered_tx, mut undelivered_rx) = mpsc::unbounded_channel();
    let (processed_tx, _processed_rx) = mpsc::unbounded_channel();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();

    let worker = RestartWorker::supervise(
        &supervisor,
        RestartWorker {
            undelivered_tx,
            processed_tx,
            started_tx,
            entered_tx,
            gate: Arc::new(Notify::new()),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    // A budget of zero means the first death already exceeds the limit, so the supervisor never
    // restarts it.
    .restart_limit(0, Duration::from_secs(10))
    .spawn()
    .await;

    recv(&mut started_rx).await;

    worker.tell(Block).await.unwrap();
    recv(&mut entered_rx).await; // Block is running (startup finished) and now blocked

    worker.tell(Work(1)).await.unwrap();
    worker.tell(Work(2)).await.unwrap();
    worker.tell(Work(3)).await.unwrap();

    worker.kill();
    worker.wait_for_shutdown().await;

    // The leftover tells reach the hook instead of being dropped by the declining supervisor.
    assert_eq!(recv(&mut undelivered_rx).await, vec![1, 2, 3]);

    // It was not restarted (no second startup).
    assert!(started_rx.try_recv().is_err());

    supervisor.kill();
    supervisor.wait_for_shutdown().await;
}

// Like the above, but the budget is spent by a genuine restart first, proving the child reads the
// same live restart count the supervisor mutates.
#[tokio::test]
async fn undelivered_called_after_restart_then_exhausted() {
    let supervisor = Supervisor::spawn(Supervisor);
    let (undelivered_tx, mut undelivered_rx) = mpsc::unbounded_channel();
    let (processed_tx, _processed_rx) = mpsc::unbounded_channel();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let worker = RestartWorker::supervise(
        &supervisor,
        RestartWorker {
            undelivered_tx,
            processed_tx,
            started_tx,
            entered_tx: mpsc::unbounded_channel().0,
            gate: Arc::new(Notify::new()),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    .restart_limit(1, Duration::from_secs(10))
    .spawn()
    .await;

    recv(&mut started_rx).await; // incarnation 1

    // Spend the single allowed restart with a panic (nothing else queued, so nothing undelivered).
    worker.tell(PanicMsg).await.unwrap();
    recv(&mut started_rx).await; // incarnation 2, budget now exhausted

    // Queue tells behind another panic. `PanicMsg` sleeps before panicking, so the tells are in the
    // mailbox when incarnation 2 dies. Because the budget is exhausted it is not restarted, so the
    // tells must reach on_undelivered rather than being preserved or dropped.
    worker.tell(PanicMsg).await.unwrap();
    worker.tell(Work(1)).await.unwrap();
    worker.tell(Work(2)).await.unwrap();
    worker.tell(Work(3)).await.unwrap();

    assert_eq!(recv(&mut undelivered_rx).await, vec![1, 2, 3]);
    assert!(started_rx.try_recv().is_err()); // not restarted a second time

    supervisor.kill();
    supervisor.wait_for_shutdown().await;
}
