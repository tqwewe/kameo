use kameo::{
    actor::pubsub::{PubSub, Publish, Subscribe, SubscribeFilter},
    message::{Context, Message},
    Actor,
};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Debug)]
struct PrintActorID(String);

#[derive(Actor, Default)]
struct ActorA;

impl Message<PrintActorID> for ActorA {
    type Reply = ();

    async fn handle(
        &mut self,
        m: PrintActorID,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorA: {} - {:?}", ctx.actor_ref().id(), m);
    }
}

#[derive(Actor, Default)]
struct ActorB;

impl Message<PrintActorID> for ActorB {
    type Reply = ();

    async fn handle(
        &mut self,
        m: PrintActorID,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorB: {} - {:?}", ctx.actor_ref().id(), m);
    }
}

#[derive(Actor, Default)]
struct ActorC;

impl Message<PrintActorID> for ActorC {
    type Reply = ();

    async fn handle(
        &mut self,
        m: PrintActorID,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorC: {} - {:?}", ctx.actor_ref().id(), m);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

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
