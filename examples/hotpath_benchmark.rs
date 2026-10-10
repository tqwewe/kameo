//! Fixed-workload port of the kameo actor cases in `benches/overhead.rs` for hotpath profiling.
//!
//! Each Criterion benchmark becomes a measured function running a fixed number of
//! iterations, so two runs of one commit report the same allocations. Actor mailboxes
//! show up as instrumented channels labelled with the actor name.
//!
//! Run with:
//!   cargo run --release --example hotpath_benchmark --features hotpath,hotpath-alloc

use kameo::prelude::*;
use tokio::runtime::{Builder, Runtime};

const ITERATIONS: u32 = 100_000;

#[derive(Actor)]
struct MyActor;

impl Message<u32> for MyActor {
    type Reply = u32;

    async fn handle(&mut self, msg: u32, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        msg
    }
}

async fn spawn_actor(
    mailbox: (MailboxSender<MyActor>, MailboxReceiver<MyActor>),
) -> ActorRef<MyActor> {
    let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox);
    actor_ref.ask(0).send().await.unwrap(); // Ask an initial message to make sure the actor is ready
    actor_ref
}

#[hotpath::measure]
async fn bounded_ask(actor_ref: &ActorRef<MyActor>) {
    for _ in 0..ITERATIONS {
        actor_ref.ask(0).send().await.unwrap();
    }
}

#[hotpath::measure]
async fn unbounded_ask(actor_ref: &ActorRef<MyActor>) {
    for _ in 0..ITERATIONS {
        actor_ref.ask(0).send().await.unwrap();
    }
}

// Like the Criterion benches, each benchmark gets its own current-thread runtime with no IO
// or time driver. `#[tokio::main]` enables both, and polling the IO driver on every wakeup
// dominates a round trip.
fn runtime() -> Runtime {
    Builder::new_current_thread().build().unwrap()
}

#[hotpath::main]
fn main() {
    runtime().block_on(async {
        let actor_ref = spawn_actor(mailbox::bounded(10)).await;
        bounded_ask(&actor_ref).await;
    });

    runtime().block_on(async {
        let actor_ref = spawn_actor(mailbox::unbounded()).await;
        unbounded_ask(&actor_ref).await;
    });
}
