use std::borrow::Cow;

use async_trait::async_trait;
use kameo::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Default)]
pub struct MyActor {
    count: i64,
}

impl Actor for MyActor {
    fn name(&self) -> Cow<'_, str> {
        "my_actor".into()
    }
}

// A simple increment message, returning the new count
pub struct Inc {
    amount: u32,
}

#[async_trait]
impl Message<MyActor> for Inc {
    type Reply = i64;

    async fn handle(self, state: &mut MyActor) -> Self::Reply {
        state.count += self.amount as i64;
        state.count
    }
}

// Always returns an error
pub struct ForceErr;

#[async_trait]
impl Message<MyActor> for ForceErr {
    type Reply = Result<(), i32>;

    async fn handle(self, _state: &mut MyActor) -> Self::Reply {
        Err(3)
    }
}

// Queries the current count
pub struct Count;

#[async_trait]
impl Query<MyActor> for Inc {
    type Reply = i64;

    async fn handle(self, state: &MyActor) -> Self::Reply {
        state.count
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let my_actor_ref = MyActor::default().start();

    // Increment the count by 3
    let count = my_actor_ref.send(Inc { amount: 3 }).await.unwrap();
    info!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.send_async(Inc { amount: 50 }).unwrap();

    // Increment the count by 2
    let count = my_actor_ref.send(Inc { amount: 2 }).await.unwrap();
    info!("Count is {count}");

    // Async messages that return an Err will cause the actor to panic
    my_actor_ref.send_async(ForceErr).unwrap();

    // Actor should be stopped, so we cannot send more messages to it
    assert!(my_actor_ref.send(Inc { amount: 2 }).await.is_err());
}
