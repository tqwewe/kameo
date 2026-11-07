//! Handles taken before a supervised restart keep naming the same logical actor
//! afterwards: they upgrade, kill and wait on the incarnation that is running now,
//! not on the one that already stopped (#392).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use kameo::{error::Infallible, prelude::*, supervision::RestartPolicy};
use tokio::sync::{Semaphore, mpsc};

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Starting(u32),
    Started(u32),
    Stopped(u32),
}

struct Supervisor;

impl Actor for Supervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

#[derive(Clone)]
struct WorkerArgs {
    starts: Arc<AtomicU32>,
    events: mpsc::UnboundedSender<Event>,
    /// The `on_start` of incarnation `gated` waits for a permit here, so a test can act while
    /// the new incarnation exists but has not finished starting.
    gate: Arc<Semaphore>,
    gated: u32,
}

struct Worker {
    incarnation: u32,
    events: mpsc::UnboundedSender<Event>,
    /// Taken in `on_start`, the way an actor hands itself to the tasks it spawns.
    me: WeakActorRef<Self>,
}

impl Actor for Worker {
    type Args = WorkerArgs;
    type Error = Infallible;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let incarnation = args.starts.fetch_add(1, Ordering::SeqCst) + 1;
        args.events.send(Event::Starting(incarnation)).unwrap();
        if incarnation == args.gated {
            args.gate.acquire().await.unwrap().forget();
        }
        args.events.send(Event::Started(incarnation)).unwrap();
        Ok(Worker {
            incarnation,
            events: args.events,
            me: actor_ref.downgrade(),
        })
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.events.send(Event::Stopped(self.incarnation)).unwrap();
        Ok(())
    }
}

struct Crash;

impl Message<Crash> for Worker {
    type Reply = ();

    async fn handle(&mut self, _: Crash, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("worker crashed on request");
    }
}

/// Which incarnation handled the request.
struct Which;

impl Message<Which> for Worker {
    type Reply = u32;

    async fn handle(&mut self, _: Which, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.incarnation
    }
}

/// Whether the worker's own `on_start` handle still upgrades.
struct SelfUpgrades;

impl Message<SelfUpgrades> for Worker {
    type Reply = bool;

    async fn handle(&mut self, _: SelfUpgrades, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.me.upgrade().is_some()
    }
}

struct Fixture {
    _supervisor: ActorRef<Supervisor>,
    worker: ActorRef<Worker>,
    events: mpsc::UnboundedReceiver<Event>,
    gate: Arc<Semaphore>,
}

/// Spawns a supervised worker whose incarnation `gated` blocks in `on_start` until
/// `Fixture::gate` gets a permit (0: none is gated).
async fn spawn_worker(gated: u32) -> Fixture {
    let supervisor = Supervisor::spawn(Supervisor);
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    let args = WorkerArgs {
        starts: Arc::new(AtomicU32::new(0)),
        events: events_tx,
        gate: gate.clone(),
        gated,
    };
    let worker = Worker::supervise_with(&supervisor, move || args.clone())
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(10, Duration::from_secs(60))
        .spawn()
        .await;
    assert_eq!(next(&mut events).await, Event::Starting(1));
    assert_eq!(next(&mut events).await, Event::Started(1));
    Fixture {
        _supervisor: supervisor,
        worker,
        events,
        gate,
    }
}

async fn next(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    match tokio::time::timeout(TIMEOUT, events.recv()).await {
        Ok(Some(event)) => event,
        Ok(None) => panic!("event channel closed"),
        Err(_) => panic!("timed out waiting for a lifecycle event"),
    }
}

/// Crash the first incarnation and wait until the second is in `on_start`.
async fn crash_once(f: &mut Fixture) {
    f.worker.tell(Crash).await.unwrap();
    assert_eq!(next(&mut f.events).await, Event::Stopped(1));
    assert_eq!(next(&mut f.events).await, Event::Starting(2));
}

/// Crash the first incarnation and wait until the second has started.
async fn restart_once(f: &mut Fixture) {
    crash_once(f).await;
    assert_eq!(next(&mut f.events).await, Event::Started(2));
    f.worker.wait_for_startup().await;
}

#[tokio::test]
async fn a_weak_ref_taken_in_on_start_upgrades_after_a_restart() {
    let mut f = spawn_worker(0).await;
    assert!(f.worker.ask(SelfUpgrades).await.unwrap());
    restart_once(&mut f).await;
    assert!(
        f.worker.ask(SelfUpgrades).await.unwrap(),
        "the restarted worker's own handle must upgrade while the original ref is alive"
    );
}

#[tokio::test]
async fn old_actor_ref_kill_targets_current_incarnation() {
    let mut f = spawn_worker(0).await;
    restart_once(&mut f).await;
    f.worker.kill();
    assert_eq!(
        next(&mut f.events).await,
        Event::Stopped(2),
        "kill through the original ref stops the incarnation that is running"
    );
    assert_eq!(next(&mut f.events).await, Event::Starting(3));
}

#[tokio::test]
async fn old_weak_ref_kill_targets_current_incarnation() {
    let mut f = spawn_worker(0).await;
    let weak = f.worker.downgrade();
    restart_once(&mut f).await;
    weak.kill();
    assert_eq!(next(&mut f.events).await, Event::Stopped(2));
}

#[tokio::test]
async fn old_recipient_kill_targets_current_incarnation() {
    let mut f = spawn_worker(0).await;
    let recipient = f.worker.clone().recipient::<Crash>();
    restart_once(&mut f).await;
    assert!(recipient.is_alive());
    recipient.kill();
    assert_eq!(next(&mut f.events).await, Event::Stopped(2));
}

#[tokio::test]
async fn old_refs_see_the_restarted_actor_alive() {
    let mut f = spawn_worker(0).await;
    let weak = f.worker.downgrade();
    restart_once(&mut f).await;
    assert!(weak.is_alive());
    assert!(f.worker.get_shutdown_result().is_none());
}

#[tokio::test]
async fn old_weak_ref_waits_for_the_current_incarnation_to_stop() {
    let mut f = spawn_worker(0).await;
    let weak = f.worker.downgrade();
    restart_once(&mut f).await;
    let wait = tokio::time::timeout(Duration::from_millis(100), weak.wait_for_shutdown());
    assert!(
        wait.await.is_err(),
        "the actor is running again, so waiting for its shutdown must not return"
    );
}

/// The next incarnation's controls are published before its task starts, so an old handle
/// waits for the new `on_start` instead of reading the stopped incarnation's result.
#[tokio::test]
async fn old_ref_waits_for_the_new_incarnation_to_start() {
    let mut f = spawn_worker(2).await;
    crash_once(&mut f).await;

    let wait = tokio::time::timeout(Duration::from_millis(100), f.worker.wait_for_startup());
    assert!(
        wait.await.is_err(),
        "wait_for_startup returned while the new incarnation is still in on_start"
    );
    assert!(f.worker.get_startup_result().is_none());

    f.gate.add_permits(1);
    assert_eq!(next(&mut f.events).await, Event::Started(2));
    tokio::time::timeout(TIMEOUT, f.worker.wait_for_startup())
        .await
        .expect("wait_for_startup resolves once the new incarnation started");
    assert_eq!(f.worker.ask(Which).await.unwrap(), 2);
}

#[tokio::test]
async fn old_ref_kill_during_the_new_on_start_stops_the_new_incarnation() {
    let mut f = spawn_worker(2).await;
    crash_once(&mut f).await;

    f.worker.kill();
    f.gate.add_permits(1);
    assert_eq!(next(&mut f.events).await, Event::Started(2));
    assert_eq!(next(&mut f.events).await, Event::Stopped(2));
    assert_eq!(next(&mut f.events).await, Event::Starting(3));
}

/// With no strong handle left at the restart, an old weak ref does not upgrade to the new
/// incarnation (as for any actor without strong handles), but it still sees it and kills it.
#[tokio::test]
async fn old_weak_ref_reaches_an_incarnation_restarted_without_strong_handles() {
    let mut f = spawn_worker(0).await;
    let weak = f.worker.downgrade();
    f.worker.tell(Crash).await.unwrap();
    drop(f.worker);
    assert_eq!(next(&mut f.events).await, Event::Stopped(1));
    assert_eq!(next(&mut f.events).await, Event::Starting(2));
    assert_eq!(next(&mut f.events).await, Event::Started(2));

    assert!(weak.upgrade().is_none());
    assert!(weak.is_alive());
    weak.kill();
    assert_eq!(next(&mut f.events).await, Event::Stopped(2));
}
