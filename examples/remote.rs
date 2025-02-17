use std::time::Duration;

use kameo::{
    actor::RemoteActorRef,
    message::{Context, Message},
    remote::{dial_opts::DialOpts, ActorSwarm},
    Actor,
};
use kameo_macros::{remote_message, RemoteActor};
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Actor, RemoteActor)]
pub struct MyActor {
    count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct Inc {
    amount: u32,
}

#[remote_message("3b9128f1-0593-44a0-b83a-f4188baa05bf")]
impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        println!("incrementing");
        self.count += msg.amount as i64;
        self.count
    }
}

#[derive(Serialize, Deserialize)]
pub struct Dec {
    amount: u32,
}

#[remote_message("20185b42-8645-47d2-8d65-2d1c68d26823")]
impl Message<Dec> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Dec, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        println!("decrementing");
        self.count -= msg.amount as i64;
        self.count
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>()?)
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
        ActorSwarm::bootstrap()?
            .listen_on("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?)
            .await?;
    } else {
        ActorSwarm::bootstrap()?.dial(
            DialOpts::unknown_peer_id()
                .address("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?)
                .build(),
        );
    }

    if is_host {
        let actor_ref = kameo::spawn(MyActor { count: 0 });
        info!("registering actor");
        actor_ref.register("my_actor").await?;
    } else {
        // Wait for registry to sync
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    loop {
        if !is_host {
            let remote_actor_ref = RemoteActorRef::<MyActor>::lookup("my_actor").await?;
            match remote_actor_ref {
                Some(remote_actor_ref) => {
                    let count = remote_actor_ref.ask(&Inc { amount: 10 }).await?;
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
