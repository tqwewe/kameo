use kameo::prelude::*;

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

    #[message]
    pub fn print<T>(&self, msg: T)
    where
        T: std::fmt::Display + Send + 'static,
    {
        println!("{msg}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let my_actor_ref = MyActor::spawn(MyActor::new());

    // Increment the count by 3
    let count = my_actor_ref.ask(Inc { amount: 3 }).await?;
    println!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.tell(Inc { amount: 50 }).await?;

    // Generic message
    my_actor_ref
        .ask(Print {
            msg: "Generics work!",
        })
        .await?;

    // Async messages that return an Err will cause the actor to panic
    my_actor_ref.tell(ForceErr).await?;

    // Actor should be stopped, so we cannot send more messages to it
    assert!(my_actor_ref.ask(Inc { amount: 2 }).await.is_err());

    Ok(())
}
