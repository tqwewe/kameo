//! Concrete actor ask client
//!
//! Run after starting the server:
//! cargo run --example ask_concrete_client --features remote

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

#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
struct AddResult {
    result: i32,
    operation_count: u32,
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

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Multiply {
    a: i32,
    b: i32,
}

#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
struct MultiplyResult {
    result: i32,
    operation_count: u32,
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

    async fn handle(&mut self, _msg: Add, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

impl Message<Multiply> for CalculatorActor {
    type Reply = MultiplyResult;

    async fn handle(
        &mut self,
        _msg: Multiply,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

use kameo::distributed_actor;

// Register with distributed actor macro to generate type hashes
distributed_actor! {
    CalculatorActor {
        Add,
        Multiply,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=info")
        .try_init();

    println!("\n🚀 === CONCRETE ACTOR ASK CLIENT ===");

    // Use a deterministic keypair for testing (consistent peer ID)
    let client_keypair = kameo_remote::KeyPair::new_for_testing("ask_client_test_key");
    println!("🔐 Client using Ed25519 keypair for TLS encryption");

    // Bootstrap on port 9331 with TLS enabled using keypair
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9331".parse()?,
        client_keypair,
    )
    .await?;
    println!(
        "✅ Client listening on {} with TLS encryption",
        transport.local_addr()
    );

    // Connect to server with TLS encryption
    println!("\n📡 Connecting to server at 127.0.0.1:9330 with TLS...");
    if let Some(handle) = transport.handle() {
        // Add the server as a trusted peer using its keypair-based PeerId
        let server_peer_id = kameo_remote::PeerId::new("ask_server_test_key");

        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9330".parse()?).await?;
        println!("✅ Connected to server with TLS encryption and mutual authentication");
    }

    // Wait for connection to stabilize and gossip to propagate
    println!("⏳ Waiting for gossip to propagate actor registration...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // Look up remote actor (with connection caching)
    println!("\n🔍 Looking up remote CalculatorActor...");
    let calc_ref = match DistributedActorRef::lookup("calculator").await? {
        Some(ref_) => {
            println!("✅ Found CalculatorActor on server with cached connection");
            ref_
        }
        None => {
            println!("❌ CalculatorActor not found on server");
            return Err("Actor not found".into());
        }
    };

    // Send ask messages and verify responses
    println!("\n📤 Sending ask messages to remote actor...");
    let all_tests_start = std::time::Instant::now();

    // Test 1: Add operation
    println!("\n🧪 Test 1: Asking to add 10 + 20");
    let start = std::time::Instant::now();
    let result: AddResult = calc_ref.ask(Add { a: 10, b: 20 }).send().await?;
    let duration = start.elapsed();
    println!(
        "✅ Got response: {} (operation count: {}) in {:?}",
        result.result, result.operation_count, duration
    );
    assert_eq!(result.result, 30);
    assert_eq!(result.operation_count, 1);

    // Test 2: Multiply operation
    println!("\n🧪 Test 2: Asking to multiply 5 × 7");
    let start = std::time::Instant::now();
    let result: MultiplyResult = calc_ref.ask(Multiply { a: 5, b: 7 }).send().await?;
    let duration = start.elapsed();
    println!(
        "✅ Got response: {} (operation count: {}) in {:?}",
        result.result, result.operation_count, duration
    );
    assert_eq!(result.result, 35);
    assert_eq!(result.operation_count, 2);

    // Test 3: Another add operation
    println!("\n🧪 Test 3: Asking to add 100 + 200");
    let start = std::time::Instant::now();
    let result: AddResult = calc_ref.ask(Add { a: 100, b: 200 }).send().await?;
    let duration = start.elapsed();
    println!(
        "✅ Got response: {} (operation count: {}) in {:?}",
        result.result, result.operation_count, duration
    );
    assert_eq!(result.result, 300);
    assert_eq!(result.operation_count, 3);

    // Test 4: With timeout
    println!("\n🧪 Test 4: Asking with explicit timeout");
    let start = std::time::Instant::now();
    let result: MultiplyResult = calc_ref
        .ask(Multiply { a: 12, b: 12 })
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await?;
    let duration = start.elapsed();
    println!(
        "✅ Got response: {} (operation count: {}) in {:?}",
        result.result, result.operation_count, duration
    );
    assert_eq!(result.result, 144);
    assert_eq!(result.operation_count, 4);

    // Test 5: Multiple rapid asks
    println!("\n🧪 Test 5: Sending 5 rapid ask requests");
    let batch_start = std::time::Instant::now();
    for i in 1..=5 {
        let start = std::time::Instant::now();
        let result: AddResult = calc_ref.ask(Add { a: i, b: i * 10 }).send().await?;
        let duration = start.elapsed();
        println!(
            "   Request {}: {} + {} = {} (operation count: {}) in {:?}",
            i,
            i,
            i * 10,
            result.result,
            result.operation_count,
            duration
        );
        assert_eq!(result.result, i + i * 10);
        assert_eq!(result.operation_count, 4 + i as u32);
    }
    let total_duration = batch_start.elapsed();
    println!(
        "   Total time for 5 requests: {:?} (avg: {:?}/request)",
        total_duration,
        total_duration / 5
    );

    let all_tests_duration = all_tests_start.elapsed();
    println!("\n🎉 All tests passed! Check the server output for the operation logs.");
    println!("⏱️  Total time for all tests: {:?}", all_tests_duration);

    Ok(())
}
