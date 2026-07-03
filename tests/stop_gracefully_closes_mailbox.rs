use std::time::Duration;

use kameo::error::{Infallible, SendError};
use kameo::prelude::*;
use tokio::sync::{mpsc, oneshot};

const TIMEOUT: Duration = Duration::from_secs(5);

struct TestActor {
    handled_tx: mpsc::UnboundedSender<u32>,
    gate: Option<oneshot::Receiver<()>>,
}

impl Actor for TestActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

// Occupies the actor until the gate is released, so a backlog can build up behind it.
struct Block;

impl Message<Block> for TestActor {
    type Reply = ();

    async fn handle(&mut self, _: Block, _: &mut Context<Self, Self::Reply>) {
        if let Some(gate) = self.gate.take() {
            let _ = gate.await;
        }
    }
}

// Reports that it was handled by forwarding its value.
struct Record(u32);

impl Message<Record> for TestActor {
    type Reply = ();

    async fn handle(&mut self, Record(n): Record, _: &mut Context<Self, Self::Reply>) {
        let _ = self.handled_tx.send(n);
    }
}

fn spawn_gated() -> (
    ActorRef<TestActor>,
    mpsc::UnboundedReceiver<u32>,
    oneshot::Sender<()>,
) {
    let (handled_tx, handled_rx) = mpsc::unbounded_channel();
    let (gate_tx, gate_rx) = oneshot::channel();
    let actor = TestActor::spawn(TestActor {
        handled_tx,
        gate: Some(gate_rx),
    });
    (actor, handled_rx, gate_tx)
}

async fn recv(rx: &mut mpsc::UnboundedReceiver<u32>) -> u32 {
    tokio::time::timeout(TIMEOUT, rx.recv())
        .await
        .expect("timed out waiting for handled message")
        .expect("channel closed before message was handled")
}

// Messages already queued before the stop signal are still processed.
#[tokio::test]
async fn queued_messages_are_processed_before_graceful_stop() {
    let (actor, mut handled_rx, gate_tx) = spawn_gated();

    actor.tell(Block).await.unwrap();
    // Queued while the actor is still accepting.
    actor.tell(Record(0)).await.unwrap();
    actor.tell(Record(1)).await.unwrap();
    actor.tell(Record(2)).await.unwrap();
    // Enqueues Stop behind the records and flips the actor to not-accepting.
    actor.stop_gracefully().await.unwrap();
    // Let the actor drain the backlog.
    gate_tx.send(()).unwrap();

    assert_eq!(recv(&mut handled_rx).await, 0);
    assert_eq!(recv(&mut handled_rx).await, 1);
    assert_eq!(recv(&mut handled_rx).await, 2);

    tokio::time::timeout(TIMEOUT, actor.wait_for_shutdown())
        .await
        .expect("actor did not stop");
}

// A `tell` after graceful stop is rejected via the flag while the actor is still draining.
#[tokio::test]
async fn tell_after_graceful_stop_is_rejected() {
    let (actor, _handled_rx, gate_tx) = spawn_gated();

    actor.tell(Block).await.unwrap();
    actor.stop_gracefully().await.unwrap();

    let err = actor.tell(Record(9)).await.unwrap_err();
    assert!(matches!(err, SendError::ActorNotRunning(Record(9))));

    gate_tx.send(()).unwrap();
}

// An `ask` after graceful stop is rejected and returns the message.
#[tokio::test]
async fn ask_after_graceful_stop_is_rejected() {
    let (actor, _handled_rx, gate_tx) = spawn_gated();

    actor.tell(Block).await.unwrap();
    actor.stop_gracefully().await.unwrap();

    let err = actor.ask(Record(9)).await.unwrap_err();
    assert!(matches!(err, SendError::ActorNotRunning(Record(9))));

    gate_tx.send(()).unwrap();
}

// `try_send` and `blocking_send` are also gated (the flag short-circuits before the channel call,
// so `blocking_send` never actually blocks here).
#[tokio::test]
async fn try_and_blocking_sends_after_graceful_stop_are_rejected() {
    let (actor, _handled_rx, gate_tx) = spawn_gated();

    actor.tell(Block).await.unwrap();
    actor.stop_gracefully().await.unwrap();

    let try_err = actor.tell(Record(1)).try_send().unwrap_err();
    assert!(matches!(try_err, SendError::ActorNotRunning(Record(1))));

    let blocking_err = actor.tell(Record(2)).blocking_send().unwrap_err();
    assert!(matches!(
        blocking_err,
        SendError::ActorNotRunning(Record(2))
    ));

    gate_tx.send(()).unwrap();
}

// `is_alive` flips to false immediately, even while the actor is still draining.
#[tokio::test]
async fn is_alive_is_false_after_graceful_stop() {
    let (actor, _handled_rx, gate_tx) = spawn_gated();
    assert!(actor.is_alive());

    actor.tell(Block).await.unwrap();
    actor.stop_gracefully().await.unwrap();
    assert!(!actor.is_alive());

    gate_tx.send(()).unwrap();
}

// Graceful stop still actually stops the actor, and sends after full stop still map to
// `ActorNotRunning` (the closed-channel path).
#[tokio::test]
async fn graceful_stop_still_stops_the_actor() {
    let (handled_tx, _handled_rx) = mpsc::unbounded_channel();
    let actor = TestActor::spawn(TestActor {
        handled_tx,
        gate: None,
    });

    actor.stop_gracefully().await.unwrap();
    tokio::time::timeout(TIMEOUT, actor.wait_for_shutdown())
        .await
        .expect("actor did not stop");
    assert!(!actor.is_alive());

    let err = actor.tell(Record(0)).await.unwrap_err();
    assert!(matches!(err, SendError::ActorNotRunning(Record(0))));
}

// The gate flip propagates through the type-erased `Recipient` (it delegates to the same actor).
#[tokio::test]
async fn recipient_graceful_stop_rejects_sends() {
    let (actor, _handled_rx, gate_tx) = spawn_gated();
    actor.tell(Block).await.unwrap();

    let recipient: Recipient<Record> = actor.clone().recipient();
    recipient.stop_gracefully().await.unwrap();

    assert!(!recipient.is_alive());
    let err = recipient.tell(Record(5)).await.unwrap_err();
    assert!(matches!(err, SendError::ActorNotRunning(Record(5))));

    gate_tx.send(()).unwrap();
}
