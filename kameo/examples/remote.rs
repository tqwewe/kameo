use std::time::Duration;

use kameo::{
    actor::RemoteActorRef,
    message::{Context, Message},
    register_actor, register_message,
    remote::ActorSwarm,
    Actor,
};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Actor, Default, Serialize, Deserialize)]
pub struct MyActor {
    count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct Inc {
    amount: u32,
}

impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        println!("incrementing");
        self.count += msg.amount as i64;
        self.count
    }
}

register_actor!(MyActor);
register_message!(MyActor, Inc);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let is_host = match std::env::args().nth(1).as_deref() {
        Some("guest") => false,
        Some("host") => true,
        Some(_) | None => {
            error!("expected either 'host' or 'guest' argument");
            return Ok(());
        }
    };

    // Bootstrap the actor swarm
    if is_host {
        ActorSwarm::bootstrap("0.0.0.0:8020".parse().unwrap())?;
    } else {
        let swarm = ActorSwarm::bootstrap("0.0.0.0:8022".parse().unwrap())?;
        swarm
            .add_peer_address(PeerId::random(), "0.0.0.0:8020".parse().unwrap())
            .await;
    }

    if is_host {
        let actor_ref = kameo::spawn(MyActor { count: 0 });
        info!("registering actor");
        actor_ref.register("my_actor").await?;
    } else {
        // Wait for registry to sync
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    loop {
        if !is_host {
            let remote_actor_ref = RemoteActorRef::<MyActor>::lookup("my_actor").await?;
            match remote_actor_ref {
                Some(remote_actor_ref) => {
                    let count = remote_actor_ref.ask(&Inc { amount: 10 }).send().await?;
                    println!("Incremented! Count is {count}");
                }
                None => {
                    println!("actor not found");
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
