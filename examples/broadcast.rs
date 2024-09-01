use std::time::Duration;

use kameo::{
    actor::BroadcastMailbox,
    message::{Context, Message},
    Actor,
};

#[derive(Default)]
pub struct MyActor {
    count: i64,
}

impl Actor for MyActor {
    type Mailbox = BroadcastMailbox<Self>;
}

#[derive(Clone)]
pub struct Inc {
    amount: u32,
}

impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        println!(
            "incrmenting by {} from actor {}",
            msg.amount,
            ctx.actor_ref().id()
        );
        self.count += msg.amount as i64;
        self.count
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mailbox = BroadcastMailbox::new(64);

    // Spawn 3 actors
    mailbox.spawn(MyActor::default());
    mailbox.spawn(MyActor::default());
    mailbox.spawn(MyActor::default());

    // Increment all by 10
    mailbox.send(Inc { amount: 10 })?;

    // Spawn another actor
    mailbox.spawn(MyActor::default());

    // Increment all by 20
    mailbox.send(Inc { amount: 20 })?;

    // Final actor counts will be [30, 30, 30, 20]

    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}
