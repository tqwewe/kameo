use std::time::Duration;

use kameo::{
    actor::pool::{ActorPool, BroadcastMsg, WorkerMsg},
    prelude::*,
};

#[derive(Actor, Default)]
struct MyActor;

struct PrintActorID;

impl Message<PrintActorID> for MyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorID,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("Hello from {}", ctx.actor_ref().id());
    }
}

#[derive(Clone)]
struct ForceStop;

impl Message<ForceStop> for MyActor {
    type Reply = ();

    async fn handle(&mut self, _: ForceStop, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        ctx.actor_ref().kill();
        ctx.actor_ref().wait_for_stop().await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = kameo::spawn(ActorPool::new(5, || kameo::spawn(MyActor)));

    // Print IDs from 0..=4
    for _ in 0..5 {
        pool.tell(WorkerMsg(PrintActorID)).await?;
    }

    // Force all workers to stop, causing them to be restarted
    pool.ask(BroadcastMsg(ForceStop)).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Restarted all workers");

    // New IDs from 6..=10 will be printed
    for _ in 0..5 {
        pool.tell(WorkerMsg(PrintActorID)).await?;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok(())
}
