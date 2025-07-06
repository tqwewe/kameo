use kameo::prelude::*;
use kameo_actors::{
    pubsub::{PubSub, Publish, Subscribe, SubscribeFilter},
    DeliveryStrategy,
};

#[derive(Clone)]
struct PrintActorId(String);

#[derive(Actor, Default)]
struct ActorA;

impl Message<PrintActorId> for ActorA {
    type Reply = ();

    async fn handle(
        &mut self,
        PrintActorId(msg): PrintActorId,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorA: {} - {msg}", ctx.actor_ref().id());
    }
}

#[derive(Actor, Default)]
struct ActorB;

impl Message<PrintActorId> for ActorB {
    type Reply = ();

    async fn handle(
        &mut self,
        PrintActorId(msg): PrintActorId,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorB: {} - {msg}", ctx.actor_ref().id());
    }
}

#[derive(Actor, Default)]
struct ActorC;

impl Message<PrintActorId> for ActorC {
    type Reply = ();

    async fn handle(
        &mut self,
        PrintActorId(msg): PrintActorId,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorC: {} - {msg}", ctx.actor_ref().id());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pubsub = PubSub::spawn(PubSub::<PrintActorId>::new(DeliveryStrategy::Guaranteed));

    let actor_a = ActorA::spawn(ActorA);
    let actor_b = ActorB::spawn(ActorB);
    let actor_c = ActorC::spawn(ActorC);

    pubsub
        .ask(SubscribeFilter(actor_a, |m: &PrintActorId| {
            m.0.starts_with("TopicA:")
        }))
        .await?;
    pubsub
        .ask(SubscribeFilter(actor_b, |m: &PrintActorId| {
            m.0.starts_with("TopicB:")
        }))
        .await?;
    pubsub.ask(Subscribe(actor_c)).await?;

    pubsub
        .ask(Publish(PrintActorId(
            "TopicA: Some important note".to_string(),
        )))
        .await?;
    pubsub
        .ask(Publish(PrintActorId(
            "TopicB: Some very important note".to_string(),
        )))
        .await?;

    Ok(())
}
