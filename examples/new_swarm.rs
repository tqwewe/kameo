use std::time::Duration;

use futures::StreamExt;
use kameo::{
    actor::ActorId,
    remote::{ActorRegistration, Behaviour, Event},
};
use libp2p::{
    mdns, noise, request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use tracing_subscriber::EnvFilter;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    actors: Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>()?)
        .without_time()
        .with_target(false)
        .init();

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let actors = Behaviour::new(
                key.public().to_peer_id(),
                request_response::Config::default(),
            );

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;

            Ok(MyBehaviour { actors, mdns })
        })?
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let registration = ActorRegistration::new(
        ActorId::new_with_peer_id(0, *swarm.local_peer_id()),
        "hiii".into(),
    );
    swarm
        .behaviour_mut()
        .actors
        .register("foobar".to_string(), registration)
        .unwrap();

    loop {
        let to = tokio::time::timeout(Duration::from_secs(3), swarm.select_next_some()).await;
        match to {
            Ok(event) => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .actors
                            .kademlia
                            .add_address(&peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, multiaddr) in list {
                        println!("mDNS discover peer has expired: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .actors
                            .kademlia
                            .remove_address(&peer_id, &multiaddr);
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Actors(actors_event)) => match actors_event
                {
                    Event::LookupProgressed { result, .. } => {
                        println!("Lookup Progressed: {result:?}");
                    }
                    Event::LookupTimeout { .. } => {
                        println!("Lookup Timedout");
                    }
                    Event::LookupCompleted { .. } => {
                        println!("Lookup Completed");
                    }
                    Event::RegistrationFailed { error, .. } => {
                        println!("Registration Failed: {error}");
                    }
                    Event::RegisteredActor { .. } => {
                        println!("Registered Actor");
                    }
                    _ => {}
                },
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                }
                _ => {}
            },
            Err(_) => {
                swarm.behaviour_mut().actors.lookup("foobar".to_string());
            }
        }
    }
}
