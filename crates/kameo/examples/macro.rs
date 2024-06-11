use std::fmt;

use kameo::{messages, Actor};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Actor)]
pub struct MyActor {
    count: i64,
}

#[messages]
impl MyActor {
    fn new() -> Self {
        MyActor { count: 0 }
    }

    #[message(derive(Clone))]
    fn inc(&mut self, amount: u32) -> i64 {
        self.count += amount as i64;
        self.count
    }

    #[message]
    fn force_err(&self) -> Result<(), i32> {
        Err(3)
    }

    #[query]
    fn count(&self) -> i64 {
        self.count
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

    let my_actor_ref = kameo::spawn(MyActor::new());

    // Increment the count by 3
    let count = my_actor_ref.ask(Inc { amount: 3 }).send().await?;
    info!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.tell(Inc { amount: 50 }).send()?;

    // Query the count
    let count = my_actor_ref.query(Count).send().await?;
    info!("Count is {count}");

    // Generic message
    my_actor_ref
        .ask(Print {
            msg: "Generics work!",
        })
        .send()
        .await?;

    // Async messages that return an Err will cause the actor to panic
    my_actor_ref.tell(ForceErr).send()?;

    // Actor should be stopped, so we cannot send more messages to it
    assert!(my_actor_ref.ask(Inc { amount: 2 }).send().await.is_err());

    Ok(())
}
