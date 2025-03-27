use kameo::{
    actor::pubsub::{PubSub, Publish, Subscribe},
    prelude::*,
};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct PrintActorID;

#[derive(Actor, Default)]
struct ActorA;

impl Message<PrintActorID> for ActorA {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorID,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorA: {}", ctx.actor_ref().id());
    }
}

#[derive(Actor, Default)]
struct ActorB;

impl Message<PrintActorID> for ActorB {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorID,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorB: {}", ctx.actor_ref().id());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let pubsub = kameo::spawn(PubSub::<PrintActorID>::new(), mailbox::unbounded());
    let actor_a = kameo::spawn(ActorA, mailbox::unbounded());
    let actor_b = kameo::spawn(ActorB, mailbox::unbounded());
    pubsub.ask(Subscribe(actor_a)).await?;
    pubsub.ask(Subscribe(actor_b)).await?;
    pubsub.ask(Publish(PrintActorID)).await?;

    Ok(())
}
