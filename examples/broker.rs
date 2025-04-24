use kameo::prelude::*;
use kameo_actors::{
    broker::{Broker, Publish, Subscribe},
    DeliveryStrategy,
};

#[derive(Actor)]
struct MyActor;

#[derive(Clone)]
struct Echo {
    message: String,
}

impl Message<Echo> for MyActor {
    type Reply = ();

    async fn handle(&mut self, msg: Echo, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("Actor {} says {}", ctx.actor_ref().id(), msg.message);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker_ref = Broker::spawn(Broker::new(DeliveryStrategy::Guaranteed));

    // Subscribe
    let my_actor_ref = MyActor::spawn(MyActor);
    broker_ref
        .tell(Subscribe {
            topic: "my-topic".parse()?,
            recipient: my_actor_ref.clone().recipient(),
        })
        .await?;

    let my_actor_ref2 = MyActor::spawn(MyActor);
    broker_ref
        .tell(Subscribe {
            topic: "my-*".parse()?,
            recipient: my_actor_ref2.clone().recipient(),
        })
        .await?;

    // Publish
    broker_ref
        .tell(Publish {
            topic: "my-topic".to_string(),
            message: Echo {
                message: "Hola".to_string(),
            },
        })
        .await?;

    // Shutdown everything
    broker_ref.stop_gracefully().await?;
    broker_ref.wait_for_shutdown().await;

    my_actor_ref.stop_gracefully().await?;
    my_actor_ref2.stop_gracefully().await?;
    my_actor_ref.wait_for_shutdown().await;
    my_actor_ref2.wait_for_shutdown().await;

    Ok(())
}
