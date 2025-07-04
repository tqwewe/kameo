use std::time::Duration;

use kameo::prelude::*;
use kameo_actors::pool::{ActorPool, Broadcast, Dispatch};

#[derive(Actor, Default)]
struct MyActor;

struct PrintActorId;

impl Message<PrintActorId> for MyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorId,
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
        ctx.actor_ref().wait_for_shutdown().await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = ActorPool::spawn(ActorPool::new(5, || MyActor::spawn(MyActor)));

    // Print IDs 0, 2, 4, 6, 8
    for _ in 0..5 {
        pool.tell(Dispatch(PrintActorId)).await?;
    }

    // Force all workers to stop, causing them to be restarted
    pool.ask(Broadcast(ForceStop)).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Restarted all workers");

    // New IDs 11, 13, 15, 17, 19 will be printed
    for _ in 0..5 {
        pool.tell(Dispatch(PrintActorId)).await?;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok(())
}
