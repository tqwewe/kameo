use kameo::{actor::ActorRef, Actor};

#[derive(Actor)]
pub struct MyActor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actor_ref = kameo::spawn(MyActor);
    actor_ref.register("my awesome actor")?;

    let other_actor_ref = ActorRef::<MyActor>::lookup("my awesome actor")?.unwrap();

    assert_eq!(actor_ref.id(), other_actor_ref.id());

    Ok(())
}
