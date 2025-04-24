use kameo::prelude::*;
use kameo_actors::{
    message_bus::{MessageBus, Publish, Register},
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
    let message_bus_ref = MessageBus::spawn(MessageBus::new(DeliveryStrategy::Guaranteed));

    // Subscribe
    let my_actor_ref = MyActor::spawn(MyActor);
    message_bus_ref
        .tell(Register(my_actor_ref.clone().recipient()))
        .await?;

    let my_actor_ref2 = MyActor::spawn(MyActor);
    message_bus_ref
        .tell(Register(my_actor_ref2.clone().recipient()))
        .await?;

    // Publish
    message_bus_ref
        .tell(Publish(Echo {
            message: "Hola".to_string(),
        }))
        .await?;

    // Shutdown everything
    message_bus_ref.stop_gracefully().await?;
    message_bus_ref.wait_for_shutdown().await;

    my_actor_ref.stop_gracefully().await?;
    my_actor_ref2.stop_gracefully().await?;
    my_actor_ref.wait_for_shutdown().await;
    my_actor_ref2.wait_for_shutdown().await;

    Ok(())
}
