use std::time::Duration;

use kameo::prelude::*;
use kameo::distributed_actor;
use kameo_remote::KeyPair;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Actor)]
pub struct MyActor {
    count: i64,
}

#[derive(RemoteMessage)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Inc {
    amount: u32,
    from: String,
}

impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        info!("<-- recv inc message from peer {}", msg.from);
        self.count += msg.amount as i64;
        self.count
    }
}

distributed_actor! {
    MyActor {
        Inc,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>()?)
        .without_time()
        .with_target(false)
        .init();

    // Starts the node with an explicit keypair (TLS required).
    let keypair = KeyPair::new_for_testing("remote-example-node");
    remote::bootstrap_with_keypair("127.0.0.1:0".parse()?, keypair).await?;

    // Register a local actor as "incrementor"
    let actor_ref = <MyActor as Actor>::spawn(MyActor { count: 0 });
    actor_ref.register("incrementor").await?;
    info!("registered local actor");

    loop {
        // Find all "incrementor" actors
        if let Some(incrementor) = DistributedActorRef::<MyActor>::lookup("incrementor").await? {
            // Send a remote ask request
            match incrementor
                .ask(Inc {
                    amount: 10,
                    from: "local".to_string(),
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
