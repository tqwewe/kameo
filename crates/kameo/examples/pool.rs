use std::time::Duration;

use kameo::{
    actor::{ActorPool, BroadcastMsg, WorkerMsg},
    message::{Context, Message},
    Actor,
};
use tracing_subscriber::EnvFilter;

#[derive(Actor, Default)]
struct MyActor;

struct PrintActorID;

impl Message<PrintActorID> for MyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorID,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("{}", ctx.actor_ref().id());
    }
}

#[derive(Clone)]
struct ForceStop;

impl Message<ForceStop> for MyActor {
    type Reply = ();

    async fn handle(&mut self, _: ForceStop, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        ctx.actor_ref().kill();
        ctx.actor_ref().wait_for_stop().await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let pool = kameo::spawn(ActorPool::new(5, || kameo::spawn(MyActor)));

    // Print IDs from 0..=4
    for _ in 0..5 {
        pool.ask(WorkerMsg(PrintActorID)).send().await?;
    }

    // Force all workers to stop, causing them to be restarted
    pool.ask(BroadcastMsg(ForceStop)).send().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Restarted all workers");

    // New IDs from 6..=10 will be printed
    for _ in 0..5 {
        pool.ask(WorkerMsg(PrintActorID)).send().await?;
    }

    Ok(())
}
