use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use kameo::prelude::*;
use libp2p::{
    mdns, noise, request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Actor, RemoteActor)]
pub struct MyActor {
    count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct Inc {
    amount: u32,
    from: PeerId,
}

#[remote_message("3b9128f1-0593-44a0-b83a-f4188baa05bf")]
impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        info!(
            "<-- recv inc message from peer {}",
            &msg.from.to_base58()[46..]
        );
        self.count += msg.amount as i64;
        self.count
    }
}

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    actors: kameo::remote::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>()?)
        .without_time()
        .with_target(false)
        .init();

    let local_peer_id = spawn_swarm()?;

    // Register a local actor as "incrementor"
    let actor_ref = MyActor::spawn(MyActor { count: 0 });
    actor_ref.register("incrementor").await?;
    info!("registered local actor");

    loop {
        // Find all "incrementor" actors
        let mut incrementors = RemoteActorRef::<MyActor>::lookup_all("incrementor");
        while let Some(incrementor) = incrementors.try_next().await? {
            // Skip our local actor
            if incrementor.id().peer_id() == Some(&local_peer_id) {
                continue;
            }

            // Send a remote ask request
            match incrementor
                .ask(&Inc {
                    amount: 10,
                    from: local_peer_id,
                })
                .await
            {
                Ok(count) => info!("--> send inc: count is {count}"),
                Err(err) => error!("failed to increment actor: {err}"),
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn spawn_swarm() -> Result<PeerId, Box<dyn std::error::Error>> {
    // Create libp2p swarm
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            // Instantiate kameo's behaviour
            let actors = kameo::remote::Behaviour::new(
                key.public().to_peer_id(),
                request_response::Config::default(),
            );

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;

            Ok(MyBehaviour { actors, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // Listen on OS assigned IP and port
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let local_peer_id = *swarm.local_peer_id();
    tokio::spawn(async move {
        // Run the swarm by progressing the stream
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, multiaddr) in list {
                        info!("mDNS discovered a new peer: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .actors
                            .kademlia
                            .add_address(&peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, multiaddr) in list {
                        warn!("mDNS discover peer has expired: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .actors
                            .kademlia
                            .remove_address(&peer_id, &multiaddr);
                    }
                }
                _ => {}
            }
        }
    });

    Ok(local_peer_id)
}
