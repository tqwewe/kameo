use std::time::Duration;

use kameo::{
    error::Infallible,
    mailbox::unbounded::UnboundedMailbox,
    message::{Context, Message},
    request::MessageSendSync,
    Actor,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Default)]
pub struct MyActor {
    count: i64,
}

impl Actor for MyActor {
    type Mailbox = UnboundedMailbox<Self>;
    type Error = Infallible;

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

    async fn handle(&mut self, msg: Inc, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.count += msg.amount as i64;
        self.count
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let my_actor_ref = kameo::spawn(MyActor::default());

    my_actor_ref.tell(Inc { amount: 3 }).send_sync()?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Increment the count by 3
    let count = my_actor_ref
        .ask(Inc { amount: 3 })
        .reply_timeout(Duration::from_millis(10))
        .await?;
    info!("Count is {count}");

    Ok(())
}
