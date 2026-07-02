use std::time::Duration;

use kameo::error::Infallible;
use kameo::prelude::*;
use tokio::sync::mpsc;

const RECV_TIMEOUT: Duration = Duration::from_secs(5);

struct PipeActor {
    value: u32,
    done_tx: mpsc::UnboundedSender<u32>,
}

impl Actor for PipeActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

// Applies a piped future's result to the actor's state.
struct Trigger;

impl Message<Trigger> for PipeActor {
    type Reply = ();

    async fn handle(&mut self, _: Trigger, ctx: &mut Context<Self, Self::Reply>) {
        ctx.pipe_with(async { 40 + 2 }, |actor, _ctx, out| {
            Box::pin(async move {
                actor.value = out;
                actor.done_tx.send(actor.value).unwrap();
            })
        });
    }
}

// The piped future and the continuation both await; the continuation still holds `&mut self`.
struct TriggerDelayed;

impl Message<TriggerDelayed> for PipeActor {
    type Reply = ();

    async fn handle(&mut self, _: TriggerDelayed, ctx: &mut Context<Self, Self::Reply>) {
        ctx.pipe_with(
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                42
            },
            |actor, _ctx, out| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    actor.value = out;
                    actor.done_tx.send(actor.value).unwrap();
                })
            },
        );
    }
}

// The continuation stops the actor via `ctx.stop()`.
struct TriggerStop;

impl Message<TriggerStop> for PipeActor {
    type Reply = ();

    async fn handle(&mut self, _: TriggerStop, ctx: &mut Context<Self, Self::Reply>) {
        ctx.pipe_with(async { 7 }, |actor, ctx, out| {
            Box::pin(async move {
                actor.value = out;
                actor.done_tx.send(actor.value).unwrap();
                ctx.stop();
            })
        });
    }
}

// Pipes a future whose output is delivered back as a message the actor already handles.
struct TriggerMessage;

impl Message<TriggerMessage> for PipeActor {
    type Reply = ();

    async fn handle(&mut self, _: TriggerMessage, ctx: &mut Context<Self, Self::Reply>) {
        ctx.pipe(async { Fetched(99) });
    }
}

struct Fetched(u32);

impl Message<Fetched> for PipeActor {
    type Reply = ();

    async fn handle(&mut self, Fetched(value): Fetched, _: &mut Context<Self, Self::Reply>) {
        self.value = value;
        self.done_tx.send(self.value).unwrap();
    }
}

struct GetValue;

impl Message<GetValue> for PipeActor {
    type Reply = u32;

    async fn handle(&mut self, _: GetValue, _: &mut Context<Self, Self::Reply>) -> u32 {
        self.value
    }
}

async fn recv(rx: &mut mpsc::UnboundedReceiver<u32>) -> u32 {
    tokio::time::timeout(RECV_TIMEOUT, rx.recv())
        .await
        .expect("timed out waiting for pipe continuation")
        .expect("channel closed before pipe continuation ran")
}

#[tokio::test]
async fn pipe_with_applies_result_to_state() {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let actor = PipeActor::spawn(PipeActor { value: 0, done_tx });

    actor.tell(Trigger).await.unwrap();

    assert_eq!(recv(&mut done_rx).await, 42);
    assert_eq!(actor.ask(GetValue).await.unwrap(), 42);
}

#[tokio::test]
async fn pipe_with_continuation_may_await() {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let actor = PipeActor::spawn(PipeActor { value: 0, done_tx });

    actor.tell(TriggerDelayed).await.unwrap();

    assert_eq!(recv(&mut done_rx).await, 42);
    assert_eq!(actor.ask(GetValue).await.unwrap(), 42);
}

#[tokio::test]
async fn pipe_with_continuation_can_stop_actor() {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let actor = PipeActor::spawn(PipeActor { value: 0, done_tx });

    actor.tell(TriggerStop).await.unwrap();

    assert_eq!(recv(&mut done_rx).await, 7);
    tokio::time::timeout(RECV_TIMEOUT, actor.wait_for_shutdown())
        .await
        .expect("actor did not stop after continuation called ctx.stop()");
    assert!(!actor.is_alive());
}

#[tokio::test]
async fn pipe_delivers_result_as_message() {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let actor = PipeActor::spawn(PipeActor { value: 0, done_tx });

    actor.tell(TriggerMessage).await.unwrap();

    assert_eq!(recv(&mut done_rx).await, 99);
    assert_eq!(actor.ask(GetValue).await.unwrap(), 99);
}

#[tokio::test]
async fn pipe_with_keeps_actor_alive_until_future_resolves() {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let actor = PipeActor::spawn(PipeActor { value: 0, done_tx });

    actor.tell(TriggerDelayed).await.unwrap();
    // Drop the only external ref. The pipe task holds a strong ref, so the actor must stay
    // alive long enough for the continuation to run.
    drop(actor);

    assert_eq!(recv(&mut done_rx).await, 42);
}
