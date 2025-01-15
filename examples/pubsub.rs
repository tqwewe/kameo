use kameo::{
    actor::pubsub::{PubSub, Publish, Subscribe, SubscribeFilter},
    message::{Context, Message},
    Actor,
};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct PrintActorID(bool);

#[derive(Actor, Default)]
struct ActorA;

impl Message<PrintActorID> for ActorA {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorID,
        ctx: Context<'_, Self, Self::Reply>,
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
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorB: {}", ctx.actor_ref().id());
    }
}

#[derive(Actor, Default)]
struct ActorC;

impl Message<PrintActorID> for ActorC {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorID,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("ActorC: {}", ctx.actor_ref().id());
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
    pubsub.ask(Subscribe(actor_a)).await?;
    pubsub.ask(Subscribe(actor_b)).await?;
    pubsub
        .ask(SubscribeFilter(actor_c, |m: &PrintActorID| m.0))
        .await?;
    pubsub.ask(Publish(PrintActorID(false))).await?;
    pubsub.ask(Publish(PrintActorID(true))).await?;

    Ok(())
}
