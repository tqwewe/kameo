//! Simplified server to test message reception

#![allow(dead_code, unused_variables)]

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// Simplified message without HashMap
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct SimpleMessage {
    pub symbol_base: String,
    pub symbol_quote: String,
    pub candle_count: u32,
    pub test_value: f64,
}

#[derive(Debug)]
struct SimpleActor {
    messages_received: u64,
}

impl Actor for SimpleActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("📊 SimpleActor started");
        Ok(Self {
            messages_received: 0,
        })
    }
}

impl Message<SimpleMessage> for SimpleActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SimpleMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.messages_received += 1;

        println!("\n🎉 === MESSAGE RECEIVED ===");
        println!("📝 Message #{}", self.messages_received);
        println!("   Symbol: {}/{}", msg.symbol_base, msg.symbol_quote);
        println!("   Candle count: {}", msg.candle_count);
        println!("   Test value: {}", msg.test_value);
        println!("✅ Successfully deserialized SimpleMessage!");
    }
}

distributed_actor! {
    SimpleActor {
        SimpleMessage,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🚀 === SIMPLE MESSAGE TEST SERVER ===");

    let server_keypair = kameo_remote::KeyPair::new_for_testing("simple_server_test_key");

    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9342".parse()?,
        server_keypair,
    )
    .await?;

    println!("✅ Server listening on {}", transport.local_addr());

    // Spawn actor
    let simple_actor = SimpleActor::spawn(());

    // DO NOT call register_handlers - tell_concrete doesn't use it
    // SimpleActor::register_handlers(&simple_actor);

    // Register with transport
    let actor_id = simple_actor.id();
    transport
        .register_actor("simple_actor".to_string(), actor_id)
        .await?;

    println!("✅ SimpleActor registered");
    println!("📡 Waiting for messages...");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
