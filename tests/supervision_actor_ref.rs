use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kameo::{
    error::Infallible,
    prelude::*,
    supervision::{RestartPolicy, SupervisionStrategy},
};
use tokio::sync::{Mutex, mpsc};

const TIMEOUT: Duration = Duration::from_secs(5);

struct Supervisor;

impl Actor for Supervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct OneForAllSupervisor;

impl Actor for OneForAllSupervisor {
    type Args = Self;
    type Error = Infallible;

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForAll
    }

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

#[derive(Clone)]
struct WorkerArgs {
    starts: Arc<AtomicUsize>,
    started_tx: mpsc::UnboundedSender<usize>,
}

struct Worker {
    generation: usize,
}

impl Actor for Worker {
    type Args = WorkerArgs;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        let generation = args.starts.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = args.started_tx.send(generation);
        Ok(Worker { generation })
    }
}

struct GetGeneration;
struct Ping;
struct Boom;
struct ContextWeakUpgrades;

impl Message<GetGeneration> for Worker {
    type Reply = usize;

    async fn handle(
        &mut self,
        _: GetGeneration,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.generation
    }
}

impl Message<Ping> for Worker {
    type Reply = ();

    async fn handle(&mut self, _: Ping, _: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

impl Message<Boom> for Worker {
    type Reply = ();

    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("boom")
    }
}

impl Message<ContextWeakUpgrades> for Worker {
    type Reply = bool;

    async fn handle(
        &mut self,
        _: ContextWeakUpgrades,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.actor_ref().downgrade().upgrade().is_some()
    }
}

#[derive(Clone)]
struct SlowStartArgs {
    starts: Arc<AtomicUsize>,
    entered_tx: mpsc::UnboundedSender<usize>,
    ready_tx: mpsc::UnboundedSender<usize>,
    permits_rx: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,
}

struct SlowStartWorker {
    generation: usize,
}

impl Actor for SlowStartWorker {
    type Args = SlowStartArgs;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        let generation = args.starts.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = args.entered_tx.send(generation);
        let _ = args.permits_rx.lock().await.recv().await;
        let _ = args.ready_tx.send(generation);
        Ok(SlowStartWorker { generation })
    }
}

impl Message<GetGeneration> for SlowStartWorker {
    type Reply = usize;

    async fn handle(
        &mut self,
        _: GetGeneration,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.generation
    }
}

impl Message<Boom> for SlowStartWorker {
    type Reply = ();

    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("boom")
    }
}

fn worker_args() -> (WorkerArgs, mpsc::UnboundedReceiver<usize>) {
    let (started_tx, started_rx) = mpsc::unbounded_channel();
    (
        WorkerArgs {
            starts: Arc::new(AtomicUsize::new(0)),
            started_tx,
        },
        started_rx,
    )
}

fn slow_start_args() -> (
    SlowStartArgs,
    mpsc::UnboundedSender<()>,
    mpsc::UnboundedReceiver<usize>,
    mpsc::UnboundedReceiver<usize>,
) {
    let (entered_tx, entered_rx) = mpsc::unbounded_channel();
    let (ready_tx, ready_rx) = mpsc::unbounded_channel();
    let (permits_tx, permits_rx) = mpsc::unbounded_channel();
    (
        SlowStartArgs {
            starts: Arc::new(AtomicUsize::new(0)),
            entered_tx,
            ready_tx,
            permits_rx: Arc::new(Mutex::new(permits_rx)),
        },
        permits_tx,
        entered_rx,
        ready_rx,
    )
}

async fn recv_start(started_rx: &mut mpsc::UnboundedReceiver<usize>) -> usize {
    tokio::time::timeout(TIMEOUT, started_rx.recv())
        .await
        .expect("timed out waiting for actor startup")
        .expect("startup channel closed")
}

async fn spawn_worker(
    policy: RestartPolicy,
) -> (
    ActorRef<Supervisor>,
    ActorRef<Worker>,
    mpsc::UnboundedReceiver<usize>,
) {
    let supervisor = Supervisor::spawn(Supervisor);
    let (args, started_rx) = worker_args();
    let worker = Worker::supervise(&supervisor, args)
        .restart_policy(policy)
        .restart_limit(10, Duration::from_secs(10))
        .spawn()
        .await;
    (supervisor, worker, started_rx)
}

async fn spawn_worker_under<S: Actor>(
    supervisor: &ActorRef<S>,
) -> (ActorRef<Worker>, mpsc::UnboundedReceiver<usize>) {
    let (args, started_rx) = worker_args();
    let worker = Worker::supervise(supervisor, args)
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(10, Duration::from_secs(10))
        .spawn()
        .await;
    (worker, started_rx)
}

#[tokio::test]
async fn old_refs_and_recipients_survive_supervised_restart() {
    let (supervisor, worker, mut started_rx) = spawn_worker(RestartPolicy::Permanent).await;
    assert_eq!(recv_start(&mut started_rx).await, 1);

    let weak = worker.downgrade();
    let recipient = worker.clone().recipient::<Ping>();

    let _ = worker.ask(Boom).await;
    assert_eq!(recv_start(&mut started_rx).await, 2);

    assert_eq!(worker.ask(GetGeneration).await.unwrap(), 2);
    assert!(weak.upgrade().is_some());
    weak.upgrade().unwrap().ask(Ping).await.unwrap();
    recipient.tell(Ping).await.unwrap();
    assert!(worker.ask(ContextWeakUpgrades).await.unwrap());
    assert!(worker.get_shutdown_result().is_none());

    worker.kill();
    assert_eq!(recv_start(&mut started_rx).await, 3);
    assert_eq!(worker.ask(GetGeneration).await.unwrap(), 3);

    supervisor.stop_gracefully().await.unwrap();
    supervisor.wait_for_shutdown().await;
}

#[tokio::test]
async fn supervisor_owns_child_ref_until_terminal_stop() {
    let (supervisor, worker, mut started_rx) = spawn_worker(RestartPolicy::Never).await;
    assert_eq!(recv_start(&mut started_rx).await, 1);

    let weak = worker.downgrade();
    drop(worker);

    let worker = weak
        .upgrade()
        .expect("supervisor should own the supervised child ref");
    worker.ask(Ping).await.unwrap();
    worker.stop_gracefully().await.unwrap();
    worker.wait_for_shutdown_result().await.unwrap();
    drop(worker);

    assert!(weak.upgrade().is_none());

    supervisor.stop_gracefully().await.unwrap();
    supervisor.wait_for_shutdown().await;
}

#[tokio::test]
async fn stop_gracefully_is_terminal_under_permanent_policy() {
    let (supervisor, worker, mut started_rx) = spawn_worker(RestartPolicy::Permanent).await;
    assert_eq!(recv_start(&mut started_rx).await, 1);

    worker.stop_gracefully().await.unwrap();
    let reason = worker.wait_for_shutdown_result().await.unwrap();
    assert!(matches!(reason, ActorStopReason::Shutdown));

    if let Ok(Some(generation)) =
        tokio::time::timeout(Duration::from_millis(100), started_rx.recv()).await
    {
        panic!("worker restarted after graceful shutdown: {generation}");
    }

    supervisor.stop_gracefully().await.unwrap();
    supervisor.wait_for_shutdown().await;
}

#[tokio::test]
async fn coordinated_restart_does_not_publish_terminal_shutdown_result() {
    let supervisor = OneForAllSupervisor::spawn(OneForAllSupervisor);
    let (worker1, mut worker1_started_rx) = spawn_worker_under(&supervisor).await;
    let (worker2, mut worker2_started_rx) = spawn_worker_under(&supervisor).await;
    assert_eq!(recv_start(&mut worker1_started_rx).await, 1);
    assert_eq!(recv_start(&mut worker2_started_rx).await, 1);

    let _ = worker1.ask(Boom).await;
    assert_eq!(recv_start(&mut worker1_started_rx).await, 2);
    assert_eq!(recv_start(&mut worker2_started_rx).await, 2);

    assert_eq!(worker2.ask(GetGeneration).await.unwrap(), 2);
    assert!(worker2.get_shutdown_result().is_none());

    worker2.stop_gracefully().await.unwrap();
    let reason = worker2.wait_for_shutdown_result().await.unwrap();
    assert!(matches!(reason, ActorStopReason::Shutdown));

    supervisor.stop_gracefully().await.unwrap();
    supervisor.wait_for_shutdown().await;
}

#[tokio::test]
async fn wait_for_startup_during_restart_observes_initial_startup_result() {
    let supervisor = Supervisor::spawn(Supervisor);
    let (args, permits_tx, mut entered_rx, mut ready_rx) = slow_start_args();
    let worker = SlowStartWorker::supervise(&supervisor, args)
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(10, Duration::from_secs(10))
        .spawn()
        .await;

    assert_eq!(recv_start(&mut entered_rx).await, 1);
    permits_tx.send(()).unwrap();
    assert_eq!(recv_start(&mut ready_rx).await, 1);
    worker.wait_for_startup().await;

    let _ = worker.ask(Boom).await;
    assert_eq!(recv_start(&mut entered_rx).await, 2);

    tokio::time::timeout(Duration::from_millis(100), worker.wait_for_startup())
        .await
        .expect("wait_for_startup should observe the initial startup result during restart");

    assert!(worker.get_shutdown_result().is_none());
    permits_tx.send(()).unwrap();
    assert_eq!(recv_start(&mut ready_rx).await, 2);
    assert_eq!(worker.ask(GetGeneration).await.unwrap(), 2);

    worker.stop_gracefully().await.unwrap();
    worker.wait_for_shutdown_result().await.unwrap();
    supervisor.stop_gracefully().await.unwrap();
    supervisor.wait_for_shutdown().await;
}

#[cfg(not(feature = "remote"))]
#[tokio::test]
async fn registry_entry_survives_restart_and_is_removed_on_terminal_stop() {
    let (supervisor, worker, mut started_rx) = spawn_worker(RestartPolicy::Permanent).await;
    assert_eq!(recv_start(&mut started_rx).await, 1);

    let name = format!("stable-restart-{}", worker.id());
    worker.register(name.clone()).unwrap();

    let _ = worker.ask(Boom).await;
    assert_eq!(recv_start(&mut started_rx).await, 2);

    let looked_up = ActorRef::<Worker>::lookup(name.as_str())
        .unwrap()
        .expect("registered worker");
    assert_eq!(looked_up.ask(GetGeneration).await.unwrap(), 2);

    worker.stop_gracefully().await.unwrap();
    worker.wait_for_shutdown_result().await.unwrap();
    assert!(ActorRef::<Worker>::lookup(name.as_str()).unwrap().is_none());

    supervisor.stop_gracefully().await.unwrap();
    supervisor.wait_for_shutdown().await;
}
