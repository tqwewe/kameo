use std::{convert::Infallible, fmt};

use kameo::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Actor)]
pub struct MyActor {
    count: i64,
}

#[actor]
impl MyActor {
    fn new() -> Self {
        MyActor { count: 0 }
    }

    #[message(derive(Clone))]
    fn inc(&mut self, amount: u32) -> Result<i64, Infallible> {
        self.count += amount as i64;
        Ok(self.count)
    }

    #[message]
    fn force_err(&self) -> Result<(), i32> {
        Err(3)
    }

    #[query]
    fn count(&self) -> Result<i64, Infallible> {
        Ok(self.count)
    }

    /// Prints a message
    #[message]
    pub fn print<T>(
        &self,
        /// Message to print
        msg: T,
    ) where
        T: fmt::Display + Send + 'static,
    {
        info!("{msg}");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let my_actor_ref = MyActor::new().spawn();

    // Increment the count by 3
    let count = my_actor_ref.send(Inc { amount: 3 }).await??;
    info!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.send_async(Inc { amount: 50 })?;

    // Query the count
    let count = my_actor_ref.query(Count).await??;
    info!("Count is {count}");

    // Generic message
    my_actor_ref
        .send(Print {
            msg: "Generics work!",
        })
        .await??;

    // Async messages that return an Err will cause the actor to panic
    my_actor_ref.send_async(ForceErr)?;

    // Actor should be stopped, so we cannot send more messages to it
    assert!(my_actor_ref.send(Inc { amount: 2 }).await.is_err());

    Ok(())
}
