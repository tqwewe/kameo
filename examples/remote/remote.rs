use std::time::Duration;

use futures::TryStreamExt;
use kameo::prelude::*;
use libp2p::PeerId;
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
    from: PeerId,
}

#[remote_message]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>()?)
        .without_time()
        .with_target(false)
        .init();

    // Starts the swarm, and listens on an OS assigned IP and port.
    let local_peer_id = remote::bootstrap()?;

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
