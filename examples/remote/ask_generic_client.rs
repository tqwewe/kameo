//! Simplified concrete actor ask client (previously generic)
//!
//! Run after starting the server:
//! cargo run --example ask_generic_client --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::{distributed_actor_ref::DistributedActorRef, transport::RemoteTransport};
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// We need to define the same actor and message types to get the type hashes
#[derive(Debug)]
struct CalculatorActor {
    _phantom: std::marker::PhantomData<()>,
}

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};

impl Actor for CalculatorActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            _phantom: std::marker::PhantomData,
        })
    }
}

// Same message types as server
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Add {
    a: i32,
    b: i32,
}

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Multiply {
    a: i32,
    b: i32,
}

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct GetLastResult;

#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
struct LastResult {
    value: Option<i32>,
    operation_count: u32,
}

impl kameo::reply::Reply for LastResult {
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
    type Reply = i32;

    async fn handle(&mut self, _msg: Add, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

impl Message<Multiply> for CalculatorActor {
    type Reply = i32;

    async fn handle(
        &mut self,
        _msg: Multiply,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

impl Message<GetLastResult> for CalculatorActor {
    type Reply = LastResult;

    async fn handle(
        &mut self,
        _msg: GetLastResult,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

// Handler methods for distributed actor (just for type hash generation)
impl CalculatorActor {
    async fn handle_add(&mut self, _msg: &rkyv::Archived<Add>) -> i32 {
        panic!("Client should not handle messages")
    }

    async fn handle_multiply(&mut self, _msg: &rkyv::Archived<Multiply>) -> i32 {
        panic!("Client should not handle messages")
    }

    async fn handle_get_last(&mut self, _msg: &rkyv::Archived<GetLastResult>) -> LastResult {
        panic!("Client should not handle messages")
    }
}

use kameo::distributed_actor;

// Register to generate type hashes
distributed_actor! {
    CalculatorActor {
        Add => handle_add,
        Multiply => handle_multiply,
        GetLastResult => handle_get_last,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=info")
        .try_init();

    println!("\n🚀 === SIMPLIFIED CALCULATOR ASK CLIENT ===");

    // Bootstrap on port 9341
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9341".parse()?).await?;
    println!("✅ Client listening on {}", transport.local_addr());

    // Connect to server
    println!("\n📡 Connecting to server at 127.0.0.1:9340...");
    if let Some(handle) = transport.handle() {
        let peer = handle
            .add_peer(&kameo_remote::PeerId::new("kameo_node_9340"))
            .await;
        peer.connect(&"127.0.0.1:9340".parse()?).await?;
        println!("✅ Connected to server");
    }

    // Wait for connection to stabilize
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Look up remote actor
    println!("\n🔍 Looking up remote CalculatorActor...");
    let calc_ref = match DistributedActorRef::lookup("calculator").await? {
        Some(ref_) => {
            println!("✅ Found CalculatorActor on server");
            ref_
        }
        None => {
            println!("❌ CalculatorActor not found on server");
            return Err("Calculator not found".into());
        }
    };

    // Send ask messages and verify responses
    println!("\n📤 Sending ask messages to remote actor...");

    // Test 1: Addition
    println!("\n🧪 Test 1: Addition 42 + 58");
    let result: i32 = calc_ref.ask(Add { a: 42, b: 58 }).send().await?;
    println!("✅ Got response: {}", result);
    assert_eq!(result, 100);

    // Test 2: Multiplication
    println!("\n🧪 Test 2: Multiplication 12 × 12");
    let result: i32 = calc_ref.ask(Multiply { a: 12, b: 12 }).send().await?;
    println!("✅ Got response: {}", result);
    assert_eq!(result, 144);

    // Test 3: Get last result
    println!("\n🧪 Test 3: Getting last result");
    let result: LastResult = calc_ref.ask(GetLastResult).send().await?;
    println!(
        "✅ Got response: value={:?}, operation_count={}",
        result.value, result.operation_count
    );
    assert_eq!(result.value, Some(144));
    assert_eq!(result.operation_count, 2);

    // Test 4: More operations
    println!("\n🧪 Test 4: Addition 1000 + 2000");
    let result: i32 = calc_ref.ask(Add { a: 1000, b: 2000 }).send().await?;
    println!("✅ Got response: {}", result);
    assert_eq!(result, 3000);

    // Test 5: Final status
    println!("\n🧪 Test 5: Getting final status");
    let result: LastResult = calc_ref.ask(GetLastResult).send().await?;
    println!(
        "✅ Got response: value={:?}, operation_count={}",
        result.value, result.operation_count
    );
    assert_eq!(result.value, Some(3000));
    assert_eq!(result.operation_count, 3);

    println!("\n🎉 All tests passed! Check the server output for the operation logs.");

    Ok(())
}
