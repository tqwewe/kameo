use std::time::Duration;

use kameo::{
    actor::ActorPool,
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

    let mut pool = ActorPool::new(5, || kameo::spawn(MyActor));
    for _ in 0..10 {
        pool.send(PrintActorID).await?;
    }

    pool.broadcast(ForceStop).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    for _ in 0..10 {
        pool.send(PrintActorID).await?;
    }

    Ok(())
}
