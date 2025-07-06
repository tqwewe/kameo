use kameo::prelude::*;
use kameo_actors::{
    pubsub::{PubSub, Publish, Subscribe},
    DeliveryStrategy,
};

#[derive(Clone)]
struct PrintActorId;

#[derive(Actor, Default)]
struct ActorA;

impl Message<PrintActorId> for ActorA {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorId,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorA: {}", ctx.actor_ref().id());
    }
}

#[derive(Actor, Default)]
struct ActorB;

impl Message<PrintActorId> for ActorB {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorId,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorB: {}", ctx.actor_ref().id());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pubsub = PubSub::spawn(PubSub::<PrintActorId>::new(DeliveryStrategy::Guaranteed));

    let actor_a = ActorA::spawn(ActorA);
    let actor_b = ActorB::spawn(ActorB);

    pubsub.ask(Subscribe(actor_a)).await?;
    pubsub.ask(Subscribe(actor_b)).await?;
    pubsub.ask(Publish(PrintActorId)).await?;

    Ok(())
}
