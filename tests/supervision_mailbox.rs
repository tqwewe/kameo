//! Regression tests for https://github.com/tqwewe/kameo/issues/335
//!
//! When a supervised actor stops or panics, the messages still pending in its mailbox must
//! survive the restart and be processed afterwards, in order. Previously they could be
//! randomly dropped because the shutdown path drained and discarded the mailbox while
//! waiting for children to close.
//!
//! Each test loops the scenario many times because the bug was non-deterministic: a single
//! run could pass by luck. With enough iterations the probability of all passing on the
//! buggy code is effectively zero.

use std::time::Duration;

use kameo::error::Infallible;
use kameo::prelude::*;
use kameo::supervision::RestartPolicy;
use tokio::sync::mpsc;

const ITERATIONS: usize = 50;
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Receive the next item, failing the test (rather than hanging forever) if nothing arrives.
async fn recv_or_fail<T>(rx: &mut mpsc::UnboundedReceiver<T>, what: &str) -> T {
    match tokio::time::timeout(RECV_TIMEOUT, rx.recv()).await {
        Ok(Some(v)) => v,
        Ok(None) => panic!("channel closed before receiving {what} — message was dropped"),
        Err(_) => panic!("timed out waiting for {what} — message was dropped from the mailbox"),
    }
}

// ==================== Supervisor ====================

struct Supervisor;

impl Actor for Supervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

// ==================== Worker ====================

#[derive(Clone)]
struct Worker {
    on_start_tx: mpsc::UnboundedSender<()>,
    on_msg_tx: mpsc::UnboundedSender<Msg>,
}

impl Actor for Worker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        this.on_start_tx.send(()).unwrap();
        Ok(this)
    }
}

/// Makes the worker panic, but only after a short delay so that messages sent afterwards
/// are guaranteed to be sitting in the mailbox when the panic occurs.
struct Panic;

impl Message<Panic> for Worker {
    type Reply = ();

    async fn handle(&mut self, _msg: Panic, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        tokio::time::sleep(Duration::from_millis(30)).await;
        panic!("intentional panic for testing");
    }
}

/// Stops the worker normally, again after a short delay.
struct StopAfterDelay;

impl Message<StopAfterDelay> for Worker {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: StopAfterDelay,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tokio::time::sleep(Duration::from_millis(30)).await;
        ctx.stop();
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Msg(usize);

impl Message<Msg> for Worker {
    type Reply = ();

    async fn handle(&mut self, msg: Msg, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.on_msg_tx.send(msg).unwrap();
    }
}

#[tokio::test]
async fn messages_survive_supervised_panic_restart() {
    for i in 0..ITERATIONS {
        let supervisor = Supervisor::spawn(Supervisor);

        let (on_start_tx, mut on_start_rx) = mpsc::unbounded_channel();
        let (on_msg_tx, mut on_msg_rx) = mpsc::unbounded_channel();
        let worker = Worker::supervise(
            &supervisor,
            Worker {
                on_start_tx,
                on_msg_tx,
            },
        )
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(5, Duration::from_secs(10))
        .spawn()
        .await;

        // Wait for the initial startup.
        recv_or_fail(&mut on_start_rx, "initial startup").await;

        // Trigger the panic, then enqueue two messages behind it.
        worker.tell(Panic).await.unwrap();
        worker.tell(Msg(0)).await.unwrap();
        worker.tell(Msg(1)).await.unwrap();

        // Wait for the actor to restart after the panic.
        recv_or_fail(&mut on_start_rx, "restart startup").await;

        // The remaining messages must be processed, in order.
        assert_eq!(
            recv_or_fail(&mut on_msg_rx, "Msg(0)").await,
            Msg(0),
            "iteration {i}: first pending message lost or reordered after restart"
        );
        assert_eq!(
            recv_or_fail(&mut on_msg_rx, "Msg(1)").await,
            Msg(1),
            "iteration {i}: second pending message lost or reordered after restart"
        );

        supervisor.kill();
        supervisor.wait_for_shutdown().await;
    }
}

#[tokio::test]
async fn messages_survive_supervised_normal_stop_restart() {
    for i in 0..ITERATIONS {
        let supervisor = Supervisor::spawn(Supervisor);

        let (on_start_tx, mut on_start_rx) = mpsc::unbounded_channel();
        let (on_msg_tx, mut on_msg_rx) = mpsc::unbounded_channel();
        let worker = Worker::supervise(
            &supervisor,
            Worker {
                on_start_tx,
                on_msg_tx,
            },
        )
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(5, Duration::from_secs(10))
        .spawn()
        .await;

        recv_or_fail(&mut on_start_rx, "initial startup").await;

        // Stop normally, then enqueue two messages behind the stop.
        worker.tell(StopAfterDelay).await.unwrap();
        worker.tell(Msg(0)).await.unwrap();
        worker.tell(Msg(1)).await.unwrap();

        // Permanent policy restarts even on a normal exit.
        recv_or_fail(&mut on_start_rx, "restart startup").await;

        assert_eq!(
            recv_or_fail(&mut on_msg_rx, "Msg(0)").await,
            Msg(0),
            "iteration {i}: first pending message lost or reordered after restart"
        );
        assert_eq!(
            recv_or_fail(&mut on_msg_rx, "Msg(1)").await,
            Msg(1),
            "iteration {i}: second pending message lost or reordered after restart"
        );

        supervisor.kill();
        supervisor.wait_for_shutdown().await;
    }
}
