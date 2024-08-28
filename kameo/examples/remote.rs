use std::time::Duration;

use kameo::{
    message::{Context, Message},
    register_actor, register_message,
    registry::ActorRegistry,
    Actor,
};
use serde::{Deserialize, Serialize};
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

    let is_main = std::env::args().count() >= 2;
    if !is_main {
        println!("sleeping...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    println!("bootstrapping");
    let reg = ActorRegistry::bootstrap();
    if is_main {
        println!("sleeping...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    if is_main {
        let actor_ref = kameo::spawn(MyActor { count: 0 });
        println!("registering");
        reg.register::<MyActor>(actor_ref, "hi".to_string()).await;
    } else {
        println!("sleeping...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        if !is_main {
            println!("looking up...");
            let remote_actor_ref = reg.lookup::<MyActor>("hi".to_string()).await;
            match remote_actor_ref {
                Some(remote_actor_ref) => {
                    let count = remote_actor_ref
                        .ask(&Inc { amount: 10 })
                        .send()
                        .await
                        .unwrap();
                    println!("Incremented! Count is {count}");
                }

                None => {
                    println!("None");
                }
            }
        }
    }
}
