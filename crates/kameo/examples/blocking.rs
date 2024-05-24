use kameo::{
    message::{Context, Message},
    reply::DelegatedReply,
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

impl Message<Inc> for MyActor {
    type Reply = DelegatedReply<i64>;

    async fn handle(&mut self, msg: Inc, mut ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        ctx.blocking(self, move |state| {
            state.count += msg.amount as i64;
            state.count
        })
        .await
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
    let res = my_actor_ref.send(Inc { amount: 10 }).await?;
    dbg!(res);

    Ok(())
}
