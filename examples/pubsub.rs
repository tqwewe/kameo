use kameo::prelude::*;
use kameo_actors::pubsub::{PubSub, Publish, Subscribe};

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
    let pubsub = PubSub::spawn(PubSub::<PrintActorID>::new());

    let actor_a = ActorA::spawn(ActorA);
    let actor_b = ActorB::spawn(ActorB);

    pubsub.ask(Subscribe(actor_a)).await?;
    pubsub.ask(Subscribe(actor_b)).await?;
    pubsub.ask(Publish(PrintActorID)).await?;

    Ok(())
}
