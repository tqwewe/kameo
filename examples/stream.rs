use std::time::Duration;

use futures::stream;
use kameo::{error::Infallible, message::StreamMessage, prelude::*};
use tokio_stream::StreamExt;

#[derive(Default)]
pub struct MyActor {
    count: i64,
    streams_complete: u8,
}

impl Actor for MyActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let stream = Box::pin(stream::repeat(1).take(5).throttle(Duration::from_secs(1)));
        actor_ref.attach_stream(stream, "1st stream", "1st stream");

        let stream = stream::repeat(1).take(5);
        actor_ref.attach_stream(stream, "2nd stream", "2nd stream");

        Ok(state)
    }
}

impl Message<StreamMessage<i64, &'static str, &'static str>> for MyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StreamMessage<i64, &'static str, &'static str>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match msg {
            StreamMessage::Next(amount) => {
                self.count += amount;
                println!("Count is {}", self.count);
            }
            StreamMessage::Started(s) => {
                println!("Started {s}");
            }
            StreamMessage::Finished(s) => {
                println!("Finished {s}");
                self.streams_complete += 1;
                if self.streams_complete == 2 {
                    ctx.actor_ref().stop_gracefully().await.unwrap();
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let actor_ref = MyActor::spawn(MyActor::default());

    actor_ref.wait_for_shutdown().await;
}
