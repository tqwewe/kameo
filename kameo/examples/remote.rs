use std::time::Duration;

use kameo::{
    actor::{
        remote::{ActorServiceClient, ActorServiceServer, DefaultActorService},
        spawn_remote,
    },
    message::{Context, Message},
    register_actor, register_message, Actor,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Server;
use tracing::info;
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

    let server_addr = std::env::args()
        .nth(1)
        .unwrap_or("[::1]:50051".to_string())
        .parse()?;
    let client_addr = std::env::args().nth(2).unwrap_or("[::1]:50052".to_string());

    info!("listening on {server_addr}");

    tokio::spawn(async move {
        let client = loop {
            let res = ActorServiceClient::connect(format!("http://{client_addr}")).await;
            match res {
                Ok(client) => break client,
                Err(_) => {
                    // error!("failed to connect to client http://{client_addr}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
        };

        let remote_actor_ref = spawn_remote(client, &MyActor { count: 0 }).await?;
        let count = remote_actor_ref.ask(&Inc { amount: 10 }).send().await?;

        dbg!(count);

        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    Server::builder()
        .add_service(ActorServiceServer::new(DefaultActorService::default()))
        .serve(server_addr)
        .await?;

    Ok(())
}
