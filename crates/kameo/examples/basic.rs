use kameo::{Actor, Message, Query, Spawn};
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

impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc) -> Self::Reply {
        self.count += msg.amount as i64;
        self.count
    }
}

// Always returns an error
pub struct ForceErr;

impl Message<ForceErr> for MyActor {
    type Reply = Result<(), i32>;

    async fn handle(&mut self, _msg: ForceErr) -> Self::Reply {
        Err(3)
    }
}

// Queries the current count
pub struct Count;

impl Query<Count> for MyActor {
    type Reply = i64;

    async fn handle(&self, _msg: Count) -> Self::Reply {
        self.count
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let my_actor_ref = MyActor::default().spawn();

    // Increment the count by 3
    let count = my_actor_ref.send(Inc { amount: 3 }).await?;
    info!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.send_async(Inc { amount: 50 })?;

    // Increment the count by 2
    let count = my_actor_ref.send(Inc { amount: 2 }).await?;
    info!("Count is {count}");

    // Async messages that return an Err will cause the actor to panic
    my_actor_ref.send_async(ForceErr)?;

    // Actor should be stopped, so we cannot send more messages to it
    assert!(my_actor_ref.send(Inc { amount: 2 }).await.is_err());

    Ok(())
}
