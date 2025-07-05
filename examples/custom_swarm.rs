use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use kameo::prelude::*;
use libp2p::{
    mdns, noise,
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

// Custom behaviour combining Kameo with other libp2p protocols
#[derive(NetworkBehaviour)]
struct MyBehaviour {
    // Kameo remote actor capabilities
    kameo: remote::Behaviour,
    // mDNS for local peer discovery
    mdns: mdns::tokio::Behaviour,
    // You could add other behaviours here:
    // gossipsub: gossipsub::Behaviour,
    // kademlia: kad::Behaviour<kad::store::MemoryStore>,
    // etc.
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>()?)
        .without_time()
        .with_target(false)
        .init();

    // Create a custom libp2p swarm with full control
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        // Configure transports - you have full control here
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic() // Enable QUIC transport
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();

            // Create Kameo behaviour with custom messaging config
            let kameo = remote::Behaviour::new(
                local_peer_id,
                remote::messaging::Config::default()
                    .with_request_timeout(Duration::from_secs(30))
                    .with_max_concurrent_streams(100),
            );

            // Create mDNS behaviour for local discovery
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

            Ok(MyBehaviour { kameo, mdns })
        })?
        .with_swarm_config(|c| {
            c.with_idle_connection_timeout(Duration::from_secs(300)) // Custom timeout
        })
        .build();

    // Initialize the global Kameo swarm
    swarm.behaviour().kameo.init_global();

    // Listen on specific addresses - you have full control
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    // You could listen on additional addresses:
    // swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    let local_peer_id = *swarm.local_peer_id();

    // Spawn the swarm task
    tokio::spawn(async move {
        loop {
            match swarm.select_next_some().await {
                // Handle mDNS discovery
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, multiaddr) in list {
                        info!("mDNS discovered peer: {peer_id}");
                        swarm.add_peer_address(peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _) in list {
                        warn!("mDNS peer expired: {peer_id}");
                        let _ = swarm.disconnect_peer_id(peer_id);
                    }
                }
                // Handle Kameo events (optional - for monitoring)
                SwarmEvent::Behaviour(MyBehaviourEvent::Kameo(remote::Event::Registry(
                    registry_event,
                ))) => {
                    info!("Registry event: {:?}", registry_event);
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Kameo(remote::Event::Messaging(
                    messaging_event,
                ))) => {
                    info!("Messaging event: {:?}", messaging_event);
                }
                // Handle other swarm events
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Listening on {address}");
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    info!("Connected to {peer_id}");
                }
                SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                    warn!("Disconnected from {peer_id}: {cause:?}");
                }
                _ => {}
            }
        }
    });

    // Register a local actor as "incrementor"
    let actor_ref = MyActor::spawn(MyActor { count: 0 });
    actor_ref.register("incrementor").await?;
    info!("registered local actor");

    // Main application loop (same as bootstrap example)
    loop {
        let mut incrementors = RemoteActorRef::<MyActor>::lookup_all("incrementor");
        while let Some(incrementor) = incrementors.try_next().await? {
            if incrementor.id().peer_id() == Some(&local_peer_id) {
                continue;
            }

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
