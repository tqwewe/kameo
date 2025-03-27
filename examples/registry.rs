use kameo::prelude::*;

#[derive(Actor)]
#[cfg_attr(feature = "remote", derive(kameo::RemoteActor))]
pub struct MyActor;

#[cfg(not(feature = "remote"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    actor_ref.register("my awesome actor")?;

    let other_actor_ref = ActorRef::<MyActor>::lookup("my awesome actor")?.unwrap();

    assert_eq!(actor_ref.id(), other_actor_ref.id());

    Ok(())
}

#[cfg(feature = "remote")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _swarm = kameo::remote::ActorSwarm::bootstrap()?;

    let actor_ref = kameo::spawn(MyActor, mailbox::unbounded());
    actor_ref.register("my awesome actor").await?;

    let other_actor_ref = ActorRef::<MyActor>::lookup("my awesome actor")
        .await?
        .unwrap();

    assert_eq!(actor_ref.id(), other_actor_ref.id());

    Ok(())
}
