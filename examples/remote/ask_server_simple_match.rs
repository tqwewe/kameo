//! Kameo ask server - simple stateless version matching Theta
//!
//! Run this first:
//! cargo run --example ask_server_simple_match --features remote

#![allow(dead_code, unused_variables)]

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// Simple calculator actor - stateless like Theta version
#[derive(Debug)]
struct CalculatorActor;

impl Actor for CalculatorActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

// Request message for addition
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Add {
    a: i32,
    b: i32,
}

// Response for addition - EXACTLY matching Theta (no operation_count)
#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
struct AddResult {
    result: i32,
}

impl kameo::reply::Reply for AddResult {
    type Ok = Self;
    type Error = kameo::error::Infallible;
    type Value = Self;

    fn to_result(self) -> Result<Self, kameo::error::Infallible> {
        Ok(self)
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

// Request message for multiplication
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Multiply {
    a: i32,
    b: i32,
}

// Response for multiplication - EXACTLY matching Theta (no operation_count)
#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
struct MultiplyResult {
    result: i32,
}

impl kameo::reply::Reply for MultiplyResult {
    type Ok = Self;
    type Error = kameo::error::Infallible;
    type Value = Self;

    fn to_result(self) -> Result<Self, kameo::error::Infallible> {
        Ok(self)
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl Message<Add> for CalculatorActor {
    type Reply = AddResult;

    async fn handle(&mut self, msg: Add, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let result = msg.a + msg.b;
        println!("➕ Computing {} + {} = {}", msg.a, msg.b, result);
        AddResult { result }
    }
}

impl Message<Multiply> for CalculatorActor {
    type Reply = MultiplyResult;

    async fn handle(
        &mut self,
        msg: Multiply,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result = msg.a * msg.b;
        println!("✖️ Computing {} × {} = {}", msg.a, msg.b, result);
        MultiplyResult { result }
    }
}

// Register with distributed actor macro for ask/reply support
distributed_actor! {
    CalculatorActor {
        Add,
        Multiply,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Disable verbose logging for clean output
    // let _ = tracing_subscriber::fmt()
    //     .with_env_filter("kameo_remote=info,kameo=info")
    //     .try_init();

    println!("\n🚀 === KAMEO ASK SERVER (SIMPLE) ===");

    // Bootstrap on port 9330 with a deterministic test keypair
    let server_keypair = kameo_remote::KeyPair::new_for_testing("ask_server_test_key");
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9330".parse()?,
        server_keypair,
    )
    .await?;
    println!("✅ Server listening on {}", transport.local_addr());

    // Create actor using regular spawn
    let actor_ref = CalculatorActor::spawn(());

    // Register with transport - automatically handles distributed ask/reply
    transport
        .register_distributed_actor("calculator".to_string(), &actor_ref)
        .await?;
    let actor_id = actor_ref.id();

    println!("✅ Calculator actor spawned with ID: {:?}", actor_id);
    println!("✅ Calculator bound to 'calculator' name for remote access");

    // Add client as peer
    if let Some(handle) = transport.handle() {
        let _peer = handle
            .add_peer(&kameo_remote::PeerId::new("ask_client_test_key"))
            .await;
        println!("✅ Added client node as peer");
    }

    println!("\n📡 Server ready for remote ask operations!");
    println!("💤 Server running. Press Ctrl+C to stop...\n");

    // Keep server running with heartbeat like Theta
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        println!("💓 Server heartbeat");
    }
}
