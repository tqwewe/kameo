use kameo::prelude::*;

#[derive(Actor)]
#[cfg_attr(feature = "remote", derive(kameo::RemoteActor))]
pub struct MyActor;

#[cfg(not(feature = "remote"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actor_ref = MyActor::spawn(MyActor);
    actor_ref.register("my awesome actor")?;

    let other_actor_ref = ActorRef::<MyActor>::lookup("my awesome actor")?.unwrap();

    assert_eq!(actor_ref.id(), other_actor_ref.id());
    println!("Registered and looked up actor");

    Ok(())
}

#[cfg(feature = "remote")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt;
    use kameo::remote::messaging;
    use libp2p::{noise, tcp, yamux};

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let actors = kameo::remote::Behaviour::new(
                key.public().to_peer_id(),
                messaging::Config::default(),
            );

            Ok(actors)
        })?
        .build();
    tokio::spawn(async move {
        // Run the swarm by progressing the stream
        loop {
            swarm.select_next_some().await;
        }
    });

    let actor_ref = MyActor::spawn(MyActor);
    actor_ref.register("my awesome actor").await?;

    let other_actor_ref = ActorRef::<MyActor>::lookup("my awesome actor")
        .await?
        .unwrap();

    assert_eq!(actor_ref.id(), other_actor_ref.id());
    println!("Registered and looked up actor");

    Ok(())
}
