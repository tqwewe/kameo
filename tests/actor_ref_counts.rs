//! Handle-count and liveness semantics of `ActorRef`/`WeakActorRef`, which are backed by a
//! single shared `Arc` so cloning is cheap.

use std::time::Duration;

use kameo::error::Infallible;
use kameo::prelude::*;
use tokio::sync::mpsc;

const TIMEOUT: Duration = Duration::from_secs(5);

struct CountActor {
    stopped_tx: mpsc::UnboundedSender<()>,
}

struct Ping;

impl Actor for CountActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        let _ = self.stopped_tx.send(());
        Ok(())
    }
}

impl Message<Ping> for CountActor {
    type Reply = ();

    async fn handle(&mut self, _: Ping, _: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

fn new_actor() -> (CountActor, mpsc::UnboundedReceiver<()>) {
    let (stopped_tx, stopped_rx) = mpsc::unbounded_channel();
    (CountActor { stopped_tx }, stopped_rx)
}

async fn wait_stopped(stopped_rx: &mut mpsc::UnboundedReceiver<()>) {
    tokio::time::timeout(TIMEOUT, stopped_rx.recv())
        .await
        .expect("timed out waiting for the actor to stop")
        .expect("stopped channel closed");
}

// A prepared (not yet spawned) actor has no lifecycle task touching counts, so they are exact.
#[tokio::test]
async fn prepared_actor_counts_are_exact() {
    let prepared = CountActor::prepare();
    let actor_ref = prepared.actor_ref().clone();

    // The prepared actor holds one handle, plus ours.
    assert_eq!(actor_ref.strong_count(), 2);

    let clone = actor_ref.clone();
    assert_eq!(actor_ref.strong_count(), 3);
    drop(clone);
    assert_eq!(actor_ref.strong_count(), 2);

    // Under the console feature the monitor's probe holds a weak handle too, so assert deltas.
    let baseline_weak = actor_ref.weak_count();
    let weak = actor_ref.downgrade();
    assert_eq!(actor_ref.weak_count(), baseline_weak + 1);
    assert_eq!(weak.strong_count(), 2);
    drop(weak);
    assert_eq!(actor_ref.weak_count(), baseline_weak);
}

// Once running, the actor's own task holds only weak handles, so the user's refs are the only
// strong counts.
#[tokio::test]
async fn clone_and_drop_tracks_strong_count() {
    let (actor, _stopped_rx) = new_actor();
    let actor_ref = CountActor::spawn(actor);
    actor_ref.wait_for_startup().await;

    assert_eq!(actor_ref.strong_count(), 1);
    let clone1 = actor_ref.clone();
    let clone2 = actor_ref.clone();
    assert_eq!(actor_ref.strong_count(), 3);
    drop(clone1);
    drop(clone2);
    assert_eq!(actor_ref.strong_count(), 1);
}

// A recipient boxes an ActorRef, so it counts as (and acts as) a strong handle.
#[tokio::test]
async fn recipient_counts_as_strong_handle() {
    let (actor, mut stopped_rx) = new_actor();
    let actor_ref = CountActor::spawn(actor);
    actor_ref.wait_for_startup().await;

    let recipient = actor_ref.clone().recipient::<Ping>();
    assert_eq!(actor_ref.strong_count(), 2);

    // The recipient alone keeps the actor alive and usable.
    drop(actor_ref);
    assert_eq!(recipient.strong_count(), 1);
    recipient.tell(Ping).await.expect("tell through recipient");

    // Dropping the last handle stops the actor.
    drop(recipient);
    wait_stopped(&mut stopped_rx).await;
}

#[tokio::test]
async fn actor_stops_when_last_ref_dropped() {
    let (actor, mut stopped_rx) = new_actor();
    let actor_ref = CountActor::spawn(actor);
    actor_ref.wait_for_startup().await;

    let weak = actor_ref.downgrade();

    // While a strong handle exists, the weak ref upgrades and the upgraded handle works.
    let upgraded = weak.upgrade().expect("upgrade while actor ref alive");
    upgraded.ask(Ping).await.expect("ask through upgraded ref");
    drop(upgraded);

    drop(actor_ref);

    // The last strong handle is gone: no resurrection, and the actor stops.
    assert!(weak.upgrade().is_none());
    assert_eq!(weak.strong_count(), 0);
    wait_stopped(&mut stopped_rx).await;
}
