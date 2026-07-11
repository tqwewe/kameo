use kameo::prelude::*;

#[derive(Actor)]
pub struct MyActor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actor_ref = MyActor::spawn(MyActor);
    actor_ref.register("my awesome actor")?;

    let other_actor_ref = ActorRef::<MyActor>::lookup("my awesome actor")?.unwrap();

    assert_eq!(actor_ref.id(), other_actor_ref.id());
    println!("Registered and looked up actor");

    Ok(())
}
