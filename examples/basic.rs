use kameo::prelude::*;

#[derive(Actor, Default)]
pub struct MyActor {
    count: i64,
}

// A simple increment message, returning the new count
pub struct Inc {
    amount: u32,
}

impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("Incrementing count by {}", msg.amount);
        self.count += msg.amount as i64;
        self.count
    }
}

// Always returns an error
pub struct ForceErr;

impl Message<ForceErr> for MyActor {
    type Reply = Result<(), i32>;

    async fn handle(
        &mut self,
        _msg: ForceErr,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Err(3)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let my_actor_ref = MyActor::spawn(MyActor::default());

    // Increment the count by 3
    let count = my_actor_ref.ask(Inc { amount: 3 }).await?;
    println!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.tell(Inc { amount: 50 }).await?;

    // Increment the count by 2
    let count = my_actor_ref.ask(Inc { amount: 2 }).await?;
    println!("Count is {count}");

    // Async messages that return an Err will cause the actor to panic
    my_actor_ref.tell(ForceErr).await?;

    // Actor should be stopped, so we cannot send more messages to it
    assert!(my_actor_ref.ask(Inc { amount: 2 }).await.is_err());

    Ok(())
}
