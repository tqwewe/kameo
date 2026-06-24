use std::time::Duration;

use kameo::error::{Infallible, SendError};
use kameo::prelude::*;
use kameo::supervision::RestartPolicy;
use tokio::sync::mpsc;

const ITERATIONS: usize = 50;
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

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

struct Panic;

impl Message<Panic> for Worker {
    type Reply = ();

    async fn handle(&mut self, _msg: Panic, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        tokio::time::sleep(Duration::from_millis(30)).await;
        panic!("intentional panic for testing");
    }
}

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

        recv_or_fail(&mut on_start_rx, "initial startup").await;

        worker.tell(Panic).await.unwrap();
        worker.tell(Msg(0)).await.unwrap();
        worker.tell(Msg(1)).await.unwrap();

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

        worker.tell(StopAfterDelay).await.unwrap();
        worker.tell(Msg(0)).await.unwrap();
        worker.tell(Msg(1)).await.unwrap();

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

// ==================== ask-to-dying-supervisor deadlock (supervised) ====================

struct PingSupervisor;

impl Actor for PingSupervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

struct Ping;

impl Message<Ping> for PingSupervisor {
    type Reply = ();

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

#[derive(Clone)]
struct AskBackChild {
    supervisor: ActorRef<PingSupervisor>,
    result_tx: mpsc::UnboundedSender<Result<(), SendError<Ping, Infallible>>>,
}

impl Actor for AskBackChild {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

struct TriggerAsk;

impl Message<TriggerAsk> for AskBackChild {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: TriggerAsk,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = self.supervisor.ask(Ping).await;
        let _ = self.result_tx.send(result);
    }
}

#[tokio::test]
async fn supervised_child_ask_to_stopping_supervisor_does_not_deadlock() {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let supervisor = PingSupervisor::spawn(PingSupervisor);
    let child = AskBackChild::supervise(
        &supervisor,
        AskBackChild {
            supervisor: supervisor.clone(),
            result_tx,
        },
    )
    .spawn()
    .await;

    child.tell(TriggerAsk).await.unwrap();
    supervisor.stop_gracefully().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), supervisor.wait_for_shutdown()).await;

    assert!(
        result.is_ok(),
        "supervisor deadlocked — child's ask() to the stopping supervisor prevented the child \
         from finishing and its mailbox from closing"
    );

    let ask_result = recv_or_fail(&mut result_rx, "ask result from child").await;
    assert!(
        ask_result.is_err(),
        "expected Err from ask to stopping supervisor — drain discards messages; got: {ask_result:?}"
    );
}

// ==================== ask-to-dying actor (sibling link) ====================

struct SiblingParent;

impl Actor for SiblingParent {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

struct PingSibling;

impl Message<PingSibling> for SiblingParent {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: PingSibling,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
    }
}

#[derive(Clone)]
struct SiblingChild {
    parent: ActorRef<SiblingParent>,
}

impl Actor for SiblingChild {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

struct TriggerAskSibling;

impl Message<TriggerAskSibling> for SiblingChild {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: TriggerAskSibling,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = self.parent.ask(PingSibling).await;
    }
}

#[tokio::test]
async fn sibling_linked_child_ask_to_killed_parent_does_not_deadlock() {
    let parent = SiblingParent::spawn(SiblingParent);
    let child = SiblingChild::spawn_link(
        &parent,
        SiblingChild {
            parent: parent.clone(),
        },
    )
    .await;

    child.tell(TriggerAskSibling).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    parent.kill();

    let result = tokio::time::timeout(Duration::from_secs(2), child.wait_for_shutdown()).await;

    assert!(
        result.is_ok(),
        "child deadlocked — ask() to the killed parent hung instead of returning promptly"
    );
}

// ==================== Multiple children asking supervisor during drain ====================

#[tokio::test]
async fn two_children_asking_supervisor_during_drain_both_get_actor_stopped() {
    let (result1_tx, mut result1_rx) = mpsc::unbounded_channel();
    let (result2_tx, mut result2_rx) = mpsc::unbounded_channel();
    let supervisor = PingSupervisor::spawn(PingSupervisor);

    let child1 = AskBackChild::supervise(
        &supervisor,
        AskBackChild {
            supervisor: supervisor.clone(),
            result_tx: result1_tx,
        },
    )
    .spawn()
    .await;

    let child2 = AskBackChild::supervise(
        &supervisor,
        AskBackChild {
            supervisor: supervisor.clone(),
            result_tx: result2_tx,
        },
    )
    .spawn()
    .await;

    child1.tell(TriggerAsk).await.unwrap();
    child2.tell(TriggerAsk).await.unwrap();
    supervisor.stop_gracefully().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), supervisor.wait_for_shutdown()).await;
    assert!(
        result.is_ok(),
        "supervisor deadlocked with two children both asking during drain"
    );

    let r1 = recv_or_fail(&mut result1_rx, "child1 ask result").await;
    let r2 = recv_or_fail(&mut result2_rx, "child2 ask result").await;
    assert!(r1.is_err(), "child1 expected ActorStopped, got: {r1:?}");
    assert!(r2.is_err(), "child2 expected ActorStopped, got: {r2:?}");
}
