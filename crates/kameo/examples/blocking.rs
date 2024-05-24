use kameo::{
    message::{BlockingMessage, Context},
    Actor,
};
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

impl BlockingMessage<Inc> for MyActor {
    type Reply = i64;

    fn handle(&mut self, msg: Inc, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.count += msg.amount as i64;
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

    let my_actor_ref = kameo::spawn(MyActor::default());
    let res = my_actor_ref.send_blocking(Inc { amount: 10 }).await?;
    dbg!(res);

    Ok(())
}
