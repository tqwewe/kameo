use std::time::Duration;

use futures::future::BoxFuture;
use kameo::{Actor, Message, Query, Reply, ReplyFuture};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Default)]
pub struct MyActor {
    count: i64,
}

impl Actor for MyActor {
    fn name() -> &'static str {
        "MyActor"
    }
}

// A simple increment message, returning the new count
pub struct Inc {
    amount: u32,
}

impl Message<MyActor> for Inc {
    type Reply = i64;

    async fn handle(state: &mut MyActor, msg: Inc) -> Self::Reply {
        state.count += msg.amount as i64;
        state.count
    }
}

// A simple increment message, returning the new count
pub struct DelayedReply;

impl Message<MyActor> for DelayedReply {
    type Reply = ReplyFuture<MyActor, Result<String, String>>;

    async fn handle(state: &mut MyActor, _msg: DelayedReply) -> Self::Reply {
        ReplyFuture::new(state.actor_ref(), async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            println!("done sleeping");
            Ok("Hello!".to_string())
            // Err("Nope!".to_string())
        })
    }
}

// Always returns an error
pub struct ForceErr;

impl Message<MyActor> for ForceErr {
    type Reply = Result<(), i32>;

    async fn handle(_state: &mut MyActor, _msg: ForceErr) -> Self::Reply {
        Err(3)
    }
}

// Queries the current count
pub struct Count;

impl Query<MyActor> for Count {
    type Reply = i64;

    async fn handle(state: &MyActor, _msg: Count) -> Self::Reply {
        state.count
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let my_actor_ref = kameo::spawn(MyActor::default());

    let res = my_actor_ref.send(DelayedReply).await?;
    // info!("{res}");

    // Increment the count by 3
    let count = my_actor_ref.send(Inc { amount: 3 }).await?;
    info!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.send_async(Inc { amount: 50 })?;

    // Increment the count by 2
    let count = my_actor_ref.send(Inc { amount: 2 }).await?;
    info!("Count is {count}");

    // Async messages that return an Err will cause the actor to panic
    // my_actor_ref.send_async(ForceErr)?;

    // Actor should be stopped, so we cannot send more messages to it
    // assert!(my_actor_ref.send(Inc { amount: 2 }).await.is_err());
    tokio::time::sleep(Duration::from_secs(2)).await;

    Ok(())
}
