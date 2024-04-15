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
        actor_ref.attach_stream(stream, "1st stream", "1st stream");

        let stream = stream::repeat(1).take(5);
        actor_ref.attach_stream(stream, "2nd stream", "2nd stream");

        Ok(())
    }
}

impl StreamMessage<i64, &'static str, &'static str> for MyActor {
    async fn handle(&mut self, msg: i64, _ctx: Context<'_, Self, ()>) {
        self.count += msg;
        info!("Count is {}", self.count);
    }

    async fn on_start(&mut self, s: &'static str, _ctx: Context<'_, Self, ()>) {
        info!("Started {s}");
    }

    async fn on_finish(&mut self, s: &'static str, _ctx: Context<'_, Self, ()>) {
        info!("Finished {s}");
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
