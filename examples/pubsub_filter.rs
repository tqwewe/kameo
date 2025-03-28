use kameo::prelude::*;
use kameo_actors::pubsub::{PubSub, Publish, Subscribe, SubscribeFilter};

#[derive(Clone)]
struct PrintActorID(String);

#[derive(Actor, Default)]
struct ActorA;

impl Message<PrintActorID> for ActorA {
    type Reply = ();

    async fn handle(
        &mut self,
        PrintActorID(msg): PrintActorID,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorA: {} - {msg}", ctx.actor_ref().id());
    }
}

#[derive(Actor, Default)]
struct ActorB;

impl Message<PrintActorID> for ActorB {
    type Reply = ();

    async fn handle(
        &mut self,
        PrintActorID(msg): PrintActorID,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorB: {} - {msg}", ctx.actor_ref().id());
    }
}

#[derive(Actor, Default)]
struct ActorC;

impl Message<PrintActorID> for ActorC {
    type Reply = ();

    async fn handle(
        &mut self,
        PrintActorID(msg): PrintActorID,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorC: {} - {msg}", ctx.actor_ref().id());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pubsub = kameo::spawn(PubSub::<PrintActorID>::new());

    let actor_a = kameo::spawn(ActorA);
    let actor_b = kameo::spawn(ActorB);
    let actor_c = kameo::spawn(ActorC);

    pubsub
        .ask(SubscribeFilter(actor_a, |m: &PrintActorID| {
            m.0.starts_with("TopicA:")
        }))
        .await?;
    pubsub
        .ask(SubscribeFilter(actor_b, |m: &PrintActorID| {
            m.0.starts_with("TopicB:")
        }))
        .await?;
    pubsub.ask(Subscribe(actor_c)).await?;

    pubsub
        .ask(Publish(PrintActorID(
            "TopicA: Some important note".to_string(),
        )))
        .await?;
    pubsub
        .ask(Publish(PrintActorID(
            "TopicB: Some very important note".to_string(),
        )))
        .await?;

    Ok(())
}
