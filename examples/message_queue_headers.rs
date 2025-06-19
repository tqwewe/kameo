use kameo::prelude::*;
use kameo_actors::{
    message_queue::{
        BasicConsume, BasicPublish, ExchangeDeclare, ExchangeType, MessageProperties, MessageQueue,
        QueueBind, QueueDeclare,
    },
    DeliveryStrategy,
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Clone, PartialEq)]
    struct TestMessage(&'static str);

    #[derive(Actor, Default)]
    struct TestConsumer;

    impl Message<TestMessage> for TestConsumer {
        type Reply = ();
        async fn handle(&mut self, msg: TestMessage, ctx: &mut Context<Self, Self::Reply>) {
            println!(
                "Actor id: {}, Received message: {:?}",
                ctx.actor_ref().id(),
                msg
            );
        }
    }

    let message_queue = MessageQueue::new(DeliveryStrategy::BestEffort);
    let mq_ref = MessageQueue::spawn(message_queue);

    mq_ref
        .tell(ExchangeDeclare {
            exchange: "headers-exchange".to_string(),
            kind: ExchangeType::Headers,
            ..Default::default()
        })
        .await?;

    mq_ref
        .tell(QueueDeclare {
            queue: "queue1".to_string(),
            ..Default::default()
        })
        .await?;

    mq_ref
        .tell(QueueDeclare {
            queue: "queue2".to_string(),
            ..Default::default()
        })
        .await?;

    let consumer1 = TestConsumer::spawn(TestConsumer);
    let consumer2 = TestConsumer::spawn(TestConsumer);

    //  format=json and priority=high (x-match=all)
    let mut bind_args1 = HashMap::new();
    bind_args1.insert("format".to_string(), "json".to_string());
    bind_args1.insert("priority".to_string(), "high".to_string());
    bind_args1.insert("x-match".to_string(), "all".to_string());

    mq_ref
        .tell(QueueBind {
            queue: "queue1".to_string(),
            exchange: "headers-exchange".to_string(),
            routing_key: "".to_string(),
            arguments: bind_args1,
        })
        .await?;

    mq_ref
        .tell(BasicConsume {
            queue: "queue1".to_string(),
            recipient: consumer1.clone().recipient::<TestMessage>(),
            tags: Default::default(),
        })
        .await?;

    //  format=xml or priority=low  (x-match=any)
    let mut bind_args2 = HashMap::new();
    bind_args2.insert("format".to_string(), "xml".to_string());
    bind_args2.insert("priority".to_string(), "low".to_string());
    bind_args2.insert("x-match".to_string(), "any".to_string());

    mq_ref
        .tell(QueueBind {
            queue: "queue2".to_string(),
            exchange: "headers-exchange".to_string(),
            routing_key: "".to_string(),
            arguments: bind_args2,
        })
        .await?;

    mq_ref
        .tell(BasicConsume {
            queue: "queue2".to_string(),
            recipient: consumer2.clone().recipient::<TestMessage>(),
            tags: Default::default(),
        })
        .await?;

    // match all
    let mut headers1 = HashMap::new();
    headers1.insert("format".to_string(), "json".to_string());
    headers1.insert("priority".to_string(), "high".to_string());

    mq_ref
        .tell(BasicPublish {
            exchange: "headers-exchange".to_string(),
            routing_key: "".to_string(),
            message: TestMessage("msg1"),
            properties: MessageProperties {
                headers: Some(headers1),
                filter: Default::default(),
            },
        })
        .await?;

    // match priority=low
    let mut headers2 = HashMap::new();
    headers2.insert("priority".to_string(), "low".to_string());

    mq_ref
        .tell(BasicPublish {
            exchange: "headers-exchange".to_string(),
            routing_key: "".to_string(),
            message: TestMessage("msg2"),
            properties: MessageProperties {
                headers: Some(headers2),
                filter: Default::default(),
            },
        })
        .await?;

    // no match
    let mut headers3 = HashMap::new();
    headers3.insert("format".to_string(), "csv".to_string());
    headers3.insert("priority".to_string(), "medium".to_string());

    mq_ref
        .tell(BasicPublish {
            exchange: "headers-exchange".to_string(),
            routing_key: "".to_string(),
            message: TestMessage("msg3"),
            properties: MessageProperties {
                headers: Some(headers3),
                filter: Default::default(),
            },
        })
        .await?;

    mq_ref.stop_gracefully().await?;
    mq_ref.wait_for_shutdown().await;
    consumer1.stop_gracefully().await?;
    consumer1.wait_for_shutdown().await;
    consumer2.stop_gracefully().await?;
    consumer2.wait_for_shutdown().await;

    Ok(())
}
