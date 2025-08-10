use kameo::prelude::*;
use kameo_actors::message_queue::{
    BasicConsume, BasicPublish, ExchangeDeclare, ExchangeType, MessageQueue, QueueBind,
    QueueDeclare,
};
use kameo_actors::DeliveryStrategy;

#[derive(Clone)]
struct TemperatureUpdate(f32);

#[derive(Actor)]
struct TemperatureDisplay;

impl Message<TemperatureUpdate> for TemperatureDisplay {
    type Reply = ();

    async fn handle(&mut self, msg: TemperatureUpdate, _ctx: &mut Context<Self, Self::Reply>) {
        println!("Temperature updated: {}", msg.0);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let amqp = MessageQueue::spawn(MessageQueue::new(DeliveryStrategy::BestEffort));

    amqp.tell(ExchangeDeclare {
        exchange: "sensors".to_string(),
        kind: ExchangeType::Topic,
        ..Default::default()
    })
    .await?;

    amqp.tell(QueueDeclare {
        queue: "temperature".to_string(),
        ..Default::default()
    })
    .await?;

    amqp.tell(QueueBind {
        queue: "temperature".to_string(),
        exchange: "sensors".to_string(),
        routing_key: "temperature.*".to_string(),
        ..Default::default()
    })
    .await?;

    let display = TemperatureDisplay::spawn(TemperatureDisplay);
    amqp.tell(BasicConsume {
        queue: "temperature".to_string(),
        recipient: display.clone().recipient(),
        tags: Default::default(),
    })
    .await?;

    amqp.tell(BasicPublish {
        exchange: "sensors".to_string(),
        routing_key: "temperature.kitchen".to_string(),
        message: TemperatureUpdate(22.5),
        properties: Default::default(),
    })
    .await?;
    amqp.stop_gracefully().await?;
    amqp.wait_for_shutdown().await;
    display.stop_gracefully().await?;
    display.wait_for_shutdown().await;
    Ok(())
}
