//! Enhanced Kameo ask client with detailed performance benchmarking
//!
//! Run after starting the server:
//! cargo run --example ask_benchmark_client --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::{distributed_actor_ref::DistributedActorRef, transport::RemoteTransport};
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use std::time::{Duration, Instant};

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

struct BenchmarkStats {
    times: Vec<Duration>,
    min: Duration,
    max: Duration,
    avg: Duration,
    p95: Duration,
    p99: Duration,
}

impl BenchmarkStats {
    fn new(mut times: Vec<Duration>) -> Self {
        times.sort();
        let len = times.len();
        
        let min = times[0];
        let max = times[len - 1];
        let avg = times.iter().sum::<Duration>() / len as u32;
        
        let p95_idx = ((len as f64 * 0.95) as usize).saturating_sub(1);
        let p99_idx = ((len as f64 * 0.99) as usize).saturating_sub(1);
        let p95 = times[p95_idx];
        let p99 = times[p99_idx];
        
        Self { times, min, max, avg, p95, p99 }
    }
    
    fn print(&self, operation: &str) {
        println!("📊 {} Performance Stats:", operation);
        println!("   Min:  {:?}", self.min);
        println!("   Avg:  {:?}", self.avg);
        println!("   P95:  {:?}", self.p95);
        println!("   P99:  {:?}", self.p99);
        println!("   Max:  {:?}", self.max);
        println!("   Ops:  {} samples", self.times.len());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=info")
        .try_init();

    println!("\n🚀 === KAMEO ASK BENCHMARK CLIENT ===");

    // Use a deterministic keypair for testing (consistent peer ID)
    let client_keypair = kameo_remote::KeyPair::new_for_testing("ask_client_test_key");
    println!("🔐 Client using Ed25519 keypair for TLS encryption");
    
    // Bootstrap on port 9331 with TLS enabled using keypair
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9331".parse()?,
        client_keypair
    ).await?;
    println!("✅ Client listening on {} with TLS encryption", transport.local_addr());

    // Connect to server with TLS encryption
    println!("\n📡 Connecting to server at 127.0.0.1:9330 with TLS...");
    if let Some(handle) = transport.handle() {
        // Add the server as a trusted peer using its keypair-based PeerId
        let server_peer_id = kameo_remote::PeerId::new("ask_server_test_key");
        
        let peer = handle
            .add_peer(&server_peer_id)
            .await;
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

    println!("\n📤 Running Kameo ask performance benchmarks...");
    let all_tests_start = Instant::now();

    // Warm up with a few operations
    println!("\n🏃 Warming up connection...");
    for i in 1..=3 {
        let _: AddResult = calc_ref.ask(Add { a: i, b: i }).send().await?;
    }
    println!("✅ Warm-up complete");

    // Benchmark 1: Single operations latency test
    println!("\n🧪 Benchmark 1: Single operation latency (10 samples)");
    let mut single_times = Vec::new();
    for i in 1..=10 {
        let start = Instant::now();
        let result: AddResult = calc_ref.ask(Add { a: i, b: i * 2 }).send().await?;
        let duration = start.elapsed();
        single_times.push(duration);
        println!("   Sample {}: {} in {:?}", i, result.result, duration);
    }
    BenchmarkStats::new(single_times).print("Single Add Operations");

    // Benchmark 2: Rapid-fire operations (measuring sustained throughput)
    println!("\n🧪 Benchmark 2: Rapid-fire operations (50 samples)");
    let rapid_start = Instant::now();
    let mut rapid_times = Vec::new();
    
    for i in 1..=50 {
        let start = Instant::now();
        let _: AddResult = calc_ref.ask(Add { a: i, b: i + 10 }).send().await?;
        let duration = start.elapsed();
        rapid_times.push(duration);
    }
    
    let total_rapid_time = rapid_start.elapsed();
    let throughput = 50.0 / total_rapid_time.as_secs_f64();
    
    BenchmarkStats::new(rapid_times).print("Rapid-fire Add Operations");
    println!("🚀 Throughput: {:.2} ops/sec", throughput);

    // Benchmark 3: Mixed operations (Add vs Multiply)
    println!("\n🧪 Benchmark 3: Mixed operations - Add vs Multiply (20 each)");
    let mut add_times = Vec::new();
    let mut multiply_times = Vec::new();
    
    for i in 1..=20 {
        // Add operation
        let start = Instant::now();
        let _: AddResult = calc_ref.ask(Add { a: i, b: i * 3 }).send().await?;
        let duration = start.elapsed();
        add_times.push(duration);
        
        // Multiply operation  
        let start = Instant::now();
        let _: MultiplyResult = calc_ref.ask(Multiply { a: i, b: i + 1 }).send().await?;
        let duration = start.elapsed();
        multiply_times.push(duration);
    }
    
    BenchmarkStats::new(add_times).print("Mixed - Add Operations");
    BenchmarkStats::new(multiply_times).print("Mixed - Multiply Operations");

    // Benchmark 4: Concurrent operations
    println!("\n🧪 Benchmark 4: Concurrent operations (20 parallel)");
    let concurrent_start = Instant::now();
    
    let mut handles = Vec::new();
    for i in 1..=20 {
        let calc_ref_clone = calc_ref.clone();
        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let _: AddResult = calc_ref_clone.ask(Add { a: i, b: i * 4 }).send().await.unwrap();
            start.elapsed()
        });
        handles.push(handle);
    }
    
    let mut concurrent_times = Vec::new();
    for handle in handles {
        let duration = handle.await?;
        concurrent_times.push(duration);
    }
    
    let total_concurrent_time = concurrent_start.elapsed();
    let concurrent_throughput = 20.0 / total_concurrent_time.as_secs_f64();
    
    BenchmarkStats::new(concurrent_times).print("Concurrent Add Operations");
    println!("🚀 Concurrent throughput: {:.2} ops/sec", concurrent_throughput);

    let all_tests_duration = all_tests_start.elapsed();
    println!("\n🎉 Kameo benchmarks completed!");
    println!("📊 Overall Summary:");
    println!("   Framework: Kameo (TCP/TLS with connection pooling)");
    println!("   Total operations: 103");
    println!("   Total time: {:?}", all_tests_duration);
    println!("   Overall throughput: {:.2} ops/sec", 103.0 / all_tests_duration.as_secs_f64());

    Ok(())
}