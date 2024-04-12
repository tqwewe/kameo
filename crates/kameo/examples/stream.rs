use std::{future::pending, time};

use futures::stream;
use kameo::{
    actor::ActorRef,
    error::BoxError,
    message::{Context, StreamMessage},
    Actor,
};
use tokio_stream::StreamExt;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Default)]
pub struct MyActor {
    count: i64,
}

impl Actor for MyActor {
    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        let stream = Box::pin(
            stream::repeat(1)
                .take(5)
                .throttle(time::Duration::from_secs(1)),
        );
        actor_ref.attach_stream(stream);

        let stream = stream::repeat(1).take(5);
        actor_ref.attach_stream(stream);

        Ok(())
    }
}

impl StreamMessage<i64> for MyActor {
    async fn handle(&mut self, msg: i64, _ctx: Context<'_, Self, ()>) {
        self.count += msg;
        info!("Count is {}", self.count);
    }

    async fn finished(&mut self, _ctx: Context<'_, Self, ()>) {
        info!("Finished");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    kameo::spawn(MyActor::default());

    pending().await
}
