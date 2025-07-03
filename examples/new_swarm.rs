use std::time::Duration;

use futures::StreamExt;
use kameo::{
    actor::ActorId,
    remote::{
        messaging,
        registry::{self, ActorRegistration},
        Behaviour, Event,
    },
};
use libp2p::{
    mdns, noise,
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
            let local_peer_id = key.public().to_peer_id();
            let actors = Behaviour::new(local_peer_id, messaging::Config::default());

            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

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
        .registry
        .register("foobar".to_string(), registration)
        .unwrap();

    let mut interval = tokio::time::interval(Duration::from_secs(3));
    let mut is_looking_up = false;

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, multiaddr) in list {
                            println!("mDNS discovered a new peer: {peer_id}");
                            swarm.add_peer_address(peer_id, multiaddr);
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, _multiaddr) in list {
                            println!("mDNS discover peer has expired: {peer_id}");
                            let _ = swarm.disconnect_peer_id(peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Actors(ev)) => match ev {
                        Event::Registry(ev) => match ev {
                            registry::Event::LookupProgressed { result, .. } => match result {
                                Ok(ActorRegistration {
                                    actor_id,
                                    remote_id,
                                }) => {
                                    println!("Found actor registration for {actor_id} with remote id {remote_id}");
                                }
                                Err(err) => {
                                    println!("Registry error: {err}");
                                }
                            },
                            registry::Event::LookupTimeout { .. } => {
                                println!("Lookup Timedout");
                            }
                            registry::Event::LookupCompleted { .. } => {
                                println!("Lookup Completed");
                                is_looking_up = false;
                            }
                            registry::Event::RegistrationFailed { error, .. } => {
                                println!("Registration Failed: {error}");
                            }
                            registry::Event::RegisteredActor { .. } => {
                                println!("Registered Actor");
                            }
                            _ => {}
                        },
                        Event::Messaging(_) => {}
                    },
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("Local node is listening on {address}");
                    }
                    _ => {}
                }
            }
            _ = interval.tick() => {
                if !is_looking_up {
                    is_looking_up = true;
                    println!("starting lookup...");
                    swarm
                        .behaviour_mut()
                        .actors
                        .registry
                        .lookup("foobar".to_string());

                }
            }
        }
    }
}
