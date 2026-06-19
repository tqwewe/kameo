#![cfg(feature = "console")]

use std::time::Duration;

use kameo::{
    console::{
        Console,
        wire::{ActorStatus, Message as WireMessage, RestartPolicy},
    },
    error::Infallible,
    prelude::*,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Clone)]
struct Echo;

impl Actor for Echo {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct Hi;

impl Message<Hi> for Echo {
    type Reply = ();

    async fn handle(&mut self, _: Hi, _: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

async fn request_snapshot(addr: std::net::SocketAddr) -> kameo::console::wire::Snapshot {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&[0]).await.unwrap();

    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.unwrap();
    let len = u32::from_be_bytes(len) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.unwrap();

    let WireMessage::Snapshot(snapshot) = rmp_serde::from_slice(&buf).unwrap();
    snapshot
}

#[tokio::test]
async fn serves_live_snapshot_over_tcp() {
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    actor.ask(Hi).await.unwrap();

    let console = kameo::console::serve("127.0.0.1:0").await.unwrap();
    let snapshot = request_snapshot(console.local_addr()).await;

    let echo = snapshot
        .actors
        .iter()
        .find(|a| a.id.0 == actor.id().sequence_id())
        .expect("spawned actor should be in the snapshot");

    assert!(matches!(echo.status, ActorStatus::Running));
    assert!(echo.counters.messages_received >= 1);
    assert!(snapshot.totals.alive >= 1);

    console.shutdown();
}

#[derive(Clone)]
struct Sup;

impl Actor for Sup {
    type Args = ();
    type Error = Infallible;

    async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Sup)
    }
}

struct SpawnChild(std::sync::Arc<std::sync::Mutex<Option<ActorRef<Echo>>>>);

impl Message<SpawnChild> for Sup {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SpawnChild,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let child = Echo::supervise(ctx.actor_ref(), Echo).spawn().await;
        *msg.0.lock().unwrap() = Some(child);
    }
}

struct Boom;

impl Message<Boom> for Echo {
    type Reply = ();

    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("boom");
    }
}

// A pair of actors that ask each other, forming a deadlock cycle.
struct Knot {
    peer: Option<ActorRef<Knot>>,
}

impl Actor for Knot {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct Tie(ActorRef<Knot>);

impl Message<Tie> for Knot {
    type Reply = ();

    async fn handle(&mut self, msg: Tie, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.peer = Some(msg.0);
    }
}

struct Tangle;

impl Message<Tangle> for Knot {
    type Reply = ();

    async fn handle(&mut self, _: Tangle, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(peer) = self.peer.clone() {
            let _ = peer.ask(Tangle).await;
        }
    }
}

#[tokio::test]
async fn deadlock_shows_as_a_wait_for_cycle() {
    let a = Knot::spawn(Knot { peer: None });
    let b = Knot::spawn(Knot { peer: None });
    a.tell(Tie(b.clone())).await.unwrap();
    b.tell(Tie(a.clone())).await.unwrap();
    a.tell(Tangle).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let console = kameo::console::serve("127.0.0.1:0").await.unwrap();
    let snapshot = request_snapshot(console.local_addr()).await;

    let edge = |id: u64| {
        snapshot
            .actors
            .iter()
            .find(|x| x.id.0 == id)
            .unwrap()
            .waiting_on
            .as_ref()
            .map(|w| w.target.0)
    };
    // Each knot is blocked waiting on the other: a mutual wait-for cycle.
    assert_eq!(edge(a.id().sequence_id()), Some(b.id().sequence_id()));
    assert_eq!(edge(b.id().sequence_id()), Some(a.id().sequence_id()));

    console.shutdown();
}

struct Slow;

impl Message<Slow> for Echo {
    type Reply = ();

    async fn handle(&mut self, _: Slow, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn handler_activity_is_reported_while_handling() {
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    let id = actor.id().sequence_id();

    // Fire a slow message without awaiting it, so the actor is mid-handler at snapshot time.
    let task = tokio::spawn({
        let actor = actor.clone();
        async move {
            let _ = actor.ask(Slow).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let console = kameo::console::serve("127.0.0.1:0").await.unwrap();
    let snapshot = request_snapshot(console.local_addr()).await;

    let echo = snapshot.actors.iter().find(|a| a.id.0 == id).unwrap();
    let handling = echo
        .handling
        .as_ref()
        .expect("actor should be reported as handling a message");
    assert!(
        handling.message.contains("Slow"),
        "got {}",
        handling.message
    );
    assert!(handling.elapsed >= Duration::from_millis(50));

    task.await.unwrap();
    console.shutdown();
}

#[tokio::test]
async fn supervised_restart_increments_restarts() {
    let sup = Sup::spawn(());
    let slot = std::sync::Arc::new(std::sync::Mutex::new(None));
    sup.ask(SpawnChild(slot.clone())).await.unwrap();
    let child = slot.lock().unwrap().clone().unwrap();
    let id = child.id().sequence_id();

    // Panic the child; its Permanent policy restarts it under the same id.
    let _ = child.tell(Boom).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let console = kameo::console::serve("127.0.0.1:0").await.unwrap();
    let snapshot = request_snapshot(console.local_addr()).await;

    let child = snapshot
        .actors
        .iter()
        .find(|a| a.id.0 == id)
        .expect("restarted child keeps its id and stays in the snapshot");
    assert!(child.counters.restarts >= 1, "restart should be counted");
    assert!(child.counters.panics >= 1, "panic should be counted");
    // The child panicked inside the Boom handler (so end_handler never ran). After the restart
    // it must not still report handling that stale message — otherwise it looks forever-stuck.
    assert!(
        child.handling.is_none(),
        "restarted child should not report a stale handler, got {:?}",
        child.handling
    );

    console.shutdown();
}

#[tokio::test]
async fn message_types_and_supervision_are_reported() {
    let sup = Sup::spawn(());
    let slot = std::sync::Arc::new(std::sync::Mutex::new(None));
    sup.ask(SpawnChild(slot.clone())).await.unwrap();
    let child = slot.lock().unwrap().clone().unwrap();
    let id = child.id().sequence_id();
    child.ask(Hi).await.unwrap();
    child.ask(Hi).await.unwrap();

    let console = kameo::console::serve("127.0.0.1:0").await.unwrap();
    let snapshot = request_snapshot(console.local_addr()).await;
    let child = snapshot.actors.iter().find(|a| a.id.0 == id).unwrap();

    let total: u64 = child.message_types.iter().map(|m| m.count).sum();
    assert!(
        total >= 2,
        "message types should count the handled messages"
    );
    assert!(
        child.message_types.iter().any(|m| m.name.contains("Hi")),
        "the Hi message type should appear"
    );

    let supervision = child
        .supervision
        .as_ref()
        .expect("a supervised child reports its restart policy");
    assert!(matches!(supervision.policy, RestartPolicy::Permanent));
    assert_eq!(supervision.max_restarts, 5);

    console.shutdown();
}

#[tokio::test]
async fn dead_actor_appears_then_is_reaped() {
    let actor = Echo::spawn(Echo);
    let id = actor.id().sequence_id();
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;

    let console = Console::builder()
        .grave_window(Duration::from_millis(300))
        .serve("127.0.0.1:0")
        .await
        .unwrap();
    let addr = console.local_addr();

    let snapshot = request_snapshot(addr).await;
    let dead = snapshot.actors.iter().find(|a| a.id.0 == id);
    assert!(
        matches!(dead.map(|a| &a.status), Some(ActorStatus::Stopped { .. })),
        "stopped actor should linger within the grave window"
    );

    tokio::time::sleep(Duration::from_millis(600)).await;
    let snapshot = request_snapshot(addr).await;
    assert!(
        snapshot.actors.iter().all(|a| a.id.0 != id),
        "stopped actor should be reaped after the grave window"
    );

    console.shutdown();
}
