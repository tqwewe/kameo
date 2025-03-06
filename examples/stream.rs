use std::{future::pending, time};

use futures::stream;
use kameo::{
    actor::ActorRef,
    error::Infallible,
    mailbox::unbounded::UnboundedMailbox,
    message::{Context, Message, StreamMessage},
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
    type Mailbox = UnboundedMailbox<Self>;
    type Error = Infallible;

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), Self::Error> {
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

impl Message<StreamMessage<i64, &'static str, &'static str>> for MyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StreamMessage<i64, &'static str, &'static str>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        match msg {
            StreamMessage::Next(amount) => {
                self.count += amount;
                info!("Count is {}", self.count);
            }
            StreamMessage::Started(s) => {
                info!("Started {s}");
            }
            StreamMessage::Finished(s) => {
                info!("Finished {s}");
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("trace".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    kameo::spawn(MyActor::default());

    pending().await
}
