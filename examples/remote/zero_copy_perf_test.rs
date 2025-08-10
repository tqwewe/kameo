//! Performance test for zero-copy optimizations
//!
//! This test measures the performance difference between the current implementation
//! and a truly zero-copy implementation.
//!
//! Run with: cargo run --example zero_copy_perf_test --features remote --release

use kameo::actor::{ActorRef, WeakActorRef};
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use kameo::{message, Actor};
use std::time::{Duration, Instant};

// Test message of various sizes
#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TestMessage {
    id: u64,
    payload: Vec<u8>,
}

// Simple actor that counts messages
struct CounterActor {
    count: u64,
    start_time: Option<Instant>,
}

impl Actor for CounterActor {
    type Mailbox = kameo::mailbox::unbounded::UnboundedMailbox<Self>;

    async fn on_start(&mut self, _: WeakActorRef<Self>) -> Result<(), kameo::error::ActorStopReason> {
        println!("CounterActor started");
        Ok(())
    }
}

#[message]
impl CounterActor {
    fn handle_test_message(&mut self, _msg: TestMessage) {
        if self.count == 0 {
            self.start_time = Some(Instant::now());
        }
        self.count += 1;
    }
    
    fn get_stats(&self) -> (u64, Option<Duration>) {
        (self.count, self.start_time.map(|start| start.elapsed()))
    }
    
    fn reset(&mut self) {
        self.count = 0;
        self.start_time = None;
    }
}

// Enable distributed actor
kameo::distributed_actor! {
    CounterActor {
        TestMessage => handle_test_message,
    }
}

async fn run_benchmark(
    actor_ref: &DistributedActorRef,
    message_size: usize,
    num_messages: usize,
    test_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n📊 {} - Message size: {} bytes, Count: {}", test_name, message_size, num_messages);
    
    // Create test message with specified payload size
    let payload = vec![0xAB; message_size];
    let test_msg = TestMessage {
        id: 0,
        payload,
    };
    
    // Measure serialization time
    let serialize_start = Instant::now();
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&test_msg)?;
    let serialize_duration = serialize_start.elapsed();
    println!("  Serialization time: {:?} for {} bytes", serialize_duration, serialized.len());
    
    // Warm up - send a few messages to establish connection
    println!("  Warming up...");
    for _ in 0..10 {
        actor_ref.tell(test_msg.clone()).send().await?;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Benchmark: Send messages and measure time
    println!("  Running benchmark...");
    let bench_start = Instant::now();
    
    for i in 0..num_messages {
        let mut msg = test_msg.clone();
        msg.id = i as u64;
        actor_ref.tell(msg).send().await?;
    }
    
    let send_duration = bench_start.elapsed();
    let messages_per_second = num_messages as f64 / send_duration.as_secs_f64();
    let avg_latency_us = send_duration.as_micros() as f64 / num_messages as f64;
    
    println!("  ✅ Results:");
    println!("     Total time: {:?}", send_duration);
    println!("     Throughput: {:.2} messages/second", messages_per_second);
    println!("     Avg latency: {:.2} µs/message", avg_latency_us);
    println!("     Total data: {:.2} MB", (serialized.len() * num_messages) as f64 / 1_048_576.0);
    println!("     Bandwidth: {:.2} MB/s", (serialized.len() * num_messages) as f64 / 1_048_576.0 / send_duration.as_secs_f64());
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo=warn,kameo_remote=warn,zero_copy_perf_test=info")
        .try_init();

    println!("🚀 === ZERO-COPY PERFORMANCE TEST ===");
    println!("This test measures the current performance to establish a baseline.");
    println!("After implementing zero-copy optimizations, run this again to compare.\n");

    // Start server
    let server_transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9320".parse()?).await?;
    println!("✅ Server listening on {}", server_transport.local_addr());
    
    // Spawn local actor
    let counter = kameo::spawn(CounterActor { count: 0, start_time: None });
    
    // Register with distributed handler
    let handler = kameo::remote::v2_bootstrap::get_distributed_handler();
    handler.registry().register(counter.id(), counter.clone());
    
    // Register with transport
    server_transport.register_actor("perf_counter".to_string(), counter.id()).await?;
    println!("✅ CounterActor registered as 'perf_counter'");
    
    // Start client
    let client_transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9321".parse()?).await?;
    println!("✅ Client listening on {}", client_transport.local_addr());
    
    // Connect to server
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&kameo_remote::PeerId::new("kameo_node_9320")).await;
        peer.connect(&"127.0.0.1:9320".parse()?).await?;
        println!("✅ Connected to server");
    }
    
    // Wait for connection to stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Look up remote actor
    let remote_counter = match DistributedActorRef::lookup("perf_counter", client_transport).await? {
        Some(ref_) => {
            println!("✅ Found remote CounterActor");
            ref_
        }
        None => {
            return Err("Remote actor not found".into());
        }
    };
    
    // Run benchmarks with different message sizes
    let test_configs = vec![
        // (message_size, num_messages, test_name)
        (64, 10000, "Small messages (64B)"),
        (256, 10000, "Medium messages (256B)"),
        (1024, 5000, "Large messages (1KB)"),
        (4096, 2000, "XL messages (4KB)"),
        (16384, 500, "XXL messages (16KB)"),
        (65536, 100, "Huge messages (64KB)"),
    ];
    
    println!("\n📈 Running performance benchmarks...");
    println!("=" .repeat(60));
    
    for (size, count, name) in test_configs {
        // Reset counter
        counter.tell(CounterActor::Reset).send()?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Run benchmark
        run_benchmark(&remote_counter, size, count, name).await?;
        
        // Wait for messages to be processed
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Check stats on server side
        let (received_count, duration) = counter.ask(CounterActor::GetStats).send().await?;
        println!("  📊 Server stats: Received {} messages in {:?}", received_count, duration);
        
        if received_count < count as u64 {
            println!("  ⚠️  Warning: Only {}/{} messages received", received_count, count);
        }
    }
    
    println!("\n" + &"=".repeat(60));
    println!("🏁 Benchmark complete!");
    println!("\nSave these results and run again after implementing zero-copy optimizations.");
    println!("Look for improvements in:");
    println!("  - Throughput (messages/second)");
    println!("  - Latency (µs/message)");
    println!("  - CPU usage (run with `time` command)");
    println!("  - Memory allocations (run with heaptrack or valgrind)");
    
    Ok(())
}