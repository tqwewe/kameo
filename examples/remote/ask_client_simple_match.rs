//! Kameo ask client - identical test to Theta for fair comparison
//!
//! Run after starting the server:
//! cargo run --example ask_client_simple_match --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::{distributed_actor_ref::DistributedActorRef, transport::RemoteTransport};
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use std::time::Instant;

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

// Same message types as server - EXACTLY matching Theta's structure
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Add {
    a: i32,
    b: i32,
}

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

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Multiply {
    a: i32,
    b: i32,
}

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
    // Disable logging for clean output like Theta
    // let _ = tracing_subscriber::fmt()
    //     .with_env_filter("kameo_remote=info,kameo=info")
    //     .try_init();

    println!("\n🚀 === KAMEO ASK CLIENT (SIMPLE) ===");

    // Use a deterministic keypair for testing (consistent peer ID)
    let client_keypair = kameo_remote::KeyPair::new_for_testing("ask_client_test_key");

    // Bootstrap on port 9331 with TLS enabled using keypair
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9331".parse()?,
        client_keypair,
    )
    .await?;
    println!(
        "✅ Client initialized with address: {}",
        transport.local_addr()
    );

    // Connect to server with TLS encryption
    println!("\n📡 Connecting to server at 127.0.0.1:9330...");
    if let Some(handle) = transport.handle() {
        // Add the server as a trusted peer using its keypair-based PeerId
        let server_peer_id = kameo_remote::PeerId::new("ask_server_test_key");

        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9330".parse()?).await?;
        println!("✅ Connected to server");
    }

    // Wait for connection to stabilize and gossip to propagate
    println!("⏳ Waiting for gossip to propagate actor registration...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // Look up remote actor (with connection caching)
    println!("🔍 Looking up remote calculator...");
    let calc_ref = match DistributedActorRef::lookup("calculator").await? {
        Some(ref_) => {
            println!("✅ Found remote Calculator actor!");
            ref_
        }
        None => {
            println!("❌ Failed to find Calculator actor");
            println!("💡 Make sure the server is running.");
            return Ok(());
        }
    };

    println!("\n📤 Running Kameo ask performance tests...");
    let all_tests_start = Instant::now();

    // Test 1: Add operation - EXACTLY like Theta
    println!("\n🧪 Test 1: Asking to add 10 + 20");
    let start = Instant::now();
    let result: AddResult = calc_ref.ask(Add { a: 10, b: 20 }).send().await?;
    let duration = start.elapsed();
    println!("✅ Got response: {} in {:?}", result.result, duration);
    assert_eq!(result.result, 30);

    // Test 2: Multiply operation - EXACTLY like Theta
    println!("\n🧪 Test 2: Asking to multiply 5 × 7");
    let start = Instant::now();
    let result: MultiplyResult = calc_ref.ask(Multiply { a: 5, b: 7 }).send().await?;
    let duration = start.elapsed();
    println!("✅ Got response: {} in {:?}", result.result, duration);
    assert_eq!(result.result, 35);

    // Test 3: Multiple operations - EXACTLY like Theta (10 samples)
    println!("\n🧪 Test 3: Multiple rapid operations (10 samples)");
    let mut times = Vec::new();
    let rapid_start = Instant::now();

    for i in 1..=10 {
        let start = Instant::now();
        let result: AddResult = calc_ref.ask(Add { a: i, b: i * 2 }).send().await?;
        let duration = start.elapsed();
        times.push(duration);
        println!(
            "   Sample {}: {} + {} = {} in {:?}",
            i,
            i,
            i * 2,
            result.result,
            duration
        );
        assert_eq!(result.result, i + i * 2);
    }

    let total_duration = rapid_start.elapsed();
    let avg_duration = times.iter().sum::<std::time::Duration>() / times.len() as u32;

    // IDENTICAL stats format to Theta
    println!("📊 Performance Stats:");
    println!("   Successful operations: {}", times.len());
    println!("   Total time: {:?}", total_duration);
    println!("   Average time: {:?}", avg_duration);
    if !times.is_empty() {
        times.sort();
        println!("   Min time: {:?}", times[0]);
        println!("   Max time: {:?}", times[times.len() - 1]);
        println!(
            "   Throughput: {:.2} ops/sec",
            times.len() as f64 / total_duration.as_secs_f64()
        );
    }

    let all_tests_duration = all_tests_start.elapsed();
    println!("\n🎉 All Kameo ask tests completed!");
    println!("⏱️ Total time for all tests: {:?}", all_tests_duration);
    println!("📊 Framework: Kameo (TCP/TLS with connection pooling)");

    Ok(())
}
