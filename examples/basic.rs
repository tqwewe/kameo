use kameo::{
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

// Always returns an error
pub struct ForceErr;

impl Message<ForceErr> for MyActor {
    type Reply = Result<(), i32>;

    async fn handle(
        &mut self,
        _msg: ForceErr,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        Err(3)
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

    // Increment the count by 3
    let count = my_actor_ref.ask(Inc { amount: 3 }).await?;
    info!("Count is {count}");

    // Increment the count by 50 in the background
    my_actor_ref.tell(Inc { amount: 50 }).await?;

    // Increment the count by 2
    let count = my_actor_ref.ask(Inc { amount: 2 }).await?;
    info!("Count is {count}");

    // Async messages that return an Err will cause the actor to panic
    // send_sync is possible since the mailbox is unbounded, so we don't need to wait for any capacity
    my_actor_ref.tell(ForceErr).send_sync()?;

    // Actor should be stopped, so we cannot send more messages to it
    assert!(my_actor_ref.ask(Inc { amount: 2 }).await.is_err());

    Ok(())
}
