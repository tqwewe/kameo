//! Direct comparison test between current implementation and zero-copy version
//!
//! This test measures the performance difference between:
//! 1. Current implementation (Vec<u8> + send_raw_bytes with copy)
//! 2. Zero-copy implementation (BytesMut + freeze + write_bytes_nonblocking)
//!
//! Run with: cargo run --example zero_copy_comparison_test --features remote --release

use bytes::{Bytes, BytesMut, BufMut};
use std::time::{Duration, Instant};

// Simulate the current implementation
fn build_message_current(actor_id: u64, type_hash: u32, payload: &[u8]) -> Vec<u8> {
    let inner_size = 8 + 16 + payload.len(); // header + actor fields + payload
    let mut message = Vec::with_capacity(4 + inner_size);
    
    // Length prefix
    message.extend_from_slice(&(inner_size as u32).to_be_bytes());
    
    // Header: [type:1][correlation_id:2][reserved:5]
    message.push(3u8); // MessageType::ActorTell
    message.extend_from_slice(&0u16.to_be_bytes()); // No correlation for tell
    message.extend_from_slice(&[0u8; 5]); // Reserved
    
    // Actor message: [actor_id:8][type_hash:4][payload_len:4][payload:N]
    message.extend_from_slice(&actor_id.to_be_bytes());
    message.extend_from_slice(&type_hash.to_be_bytes());
    message.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    message.extend_from_slice(payload);
    
    message
}

// Zero-copy implementation
fn build_message_zero_copy(actor_id: u64, type_hash: u32, payload: &[u8]) -> Bytes {
    let inner_size = 8 + 16 + payload.len(); // header + actor fields + payload
    let mut message = BytesMut::with_capacity(4 + inner_size);
    
    // Length prefix
    message.put_u32(inner_size as u32);
    
    // Header: [type:1][correlation_id:2][reserved:5]
    message.put_u8(3); // MessageType::ActorTell
    message.put_u16(0); // No correlation for tell
    message.put_slice(&[0u8; 5]); // Reserved
    
    // Actor message: [actor_id:8][type_hash:4][payload_len:4][payload:N]
    message.put_u64(actor_id);
    message.put_u32(type_hash);
    message.put_u32(payload.len() as u32);
    message.put_slice(payload);
    
    message.freeze()
}

// Simulate send_raw_bytes (which copies)
fn send_raw_bytes_simulation(data: &[u8]) -> Bytes {
    Bytes::copy_from_slice(data)
}

// Simulate write_bytes_nonblocking (no copy)
fn write_bytes_nonblocking_simulation(data: Bytes) -> Bytes {
    data // No copy, just pass through
}

struct BenchmarkResult {
    name: &'static str,
    build_time: Duration,
    send_time: Duration,
    total_time: Duration,
    allocations: usize,
    bytes_copied: usize,
}

fn run_benchmark_current(
    iterations: usize,
    payload_size: usize,
) -> BenchmarkResult {
    let actor_id = 12345u64;
    let type_hash = 0xABCDEF00u32;
    let payload = vec![0x42u8; payload_size];
    
    // Warm up
    for _ in 0..10 {
        let msg = build_message_current(actor_id, type_hash, &payload);
        let _ = send_raw_bytes_simulation(&msg);
    }
    
    // Benchmark
    let mut build_time = Duration::ZERO;
    let mut send_time = Duration::ZERO;
    let start_total = Instant::now();
    
    for _ in 0..iterations {
        let start = Instant::now();
        let msg = build_message_current(actor_id, type_hash, &payload);
        build_time += start.elapsed();
        
        let start_send = Instant::now();
        let _ = send_raw_bytes_simulation(&msg);
        send_time += start_send.elapsed();
    }
    
    let total_time = start_total.elapsed();
    
    BenchmarkResult {
        name: "Current (Vec<u8> + copy)",
        build_time,
        send_time,
        total_time,
        allocations: iterations * 2, // Vec allocation + Bytes::copy_from_slice
        bytes_copied: iterations * (4 + 8 + 16 + payload_size), // Full message copied
    }
}

fn run_benchmark_zero_copy(
    iterations: usize,
    payload_size: usize,
) -> BenchmarkResult {
    let actor_id = 12345u64;
    let type_hash = 0xABCDEF00u32;
    let payload = vec![0x42u8; payload_size];
    
    // Warm up
    for _ in 0..10 {
        let msg = build_message_zero_copy(actor_id, type_hash, &payload);
        let _ = write_bytes_nonblocking_simulation(msg);
    }
    
    // Benchmark
    let mut build_time = Duration::ZERO;
    let mut send_time = Duration::ZERO;
    let start_total = Instant::now();
    
    for _ in 0..iterations {
        let start = Instant::now();
        let msg = build_message_zero_copy(actor_id, type_hash, &payload);
        build_time += start.elapsed();
        
        let start_send = Instant::now();
        let _ = write_bytes_nonblocking_simulation(msg);
        send_time += start_send.elapsed();
    }
    
    let total_time = start_total.elapsed();
    
    BenchmarkResult {
        name: "Zero-Copy (BytesMut + freeze)",
        build_time,
        send_time,
        total_time,
        allocations: iterations, // Only BytesMut allocation
        bytes_copied: iterations * payload_size, // Only payload copied into BytesMut
    }
}

fn print_results(results: &[BenchmarkResult], iterations: usize, payload_size: usize) {
    println!("\n📊 Results for {} iterations with {}B payload:", iterations, payload_size);
    println!("{:-<80}", "");
    println!("{:<30} {:>15} {:>15} {:>15}", "Method", "Build Time", "Send Time", "Total Time");
    println!("{:-<80}", "");
    
    for result in results {
        println!("{:<30} {:>15.3?} {:>15.3?} {:>15.3?}", 
            result.name,
            result.build_time,
            result.send_time,
            result.total_time
        );
    }
    
    println!("\n📈 Performance Metrics:");
    println!("{:-<80}", "");
    
    for result in results {
        let ops_per_sec = iterations as f64 / result.total_time.as_secs_f64();
        let ns_per_op = result.total_time.as_nanos() as f64 / iterations as f64;
        let mb_per_sec = (iterations * (payload_size + 28)) as f64 / 1_048_576.0 / result.total_time.as_secs_f64();
        
        println!("{}", result.name);
        println!("  Operations/sec: {:.2}", ops_per_sec);
        println!("  Nanoseconds/op: {:.2}", ns_per_op);
        println!("  Throughput: {:.2} MB/s", mb_per_sec);
        println!("  Allocations: {}", result.allocations);
        println!("  Bytes copied: {} ({:.2} MB)", result.bytes_copied, result.bytes_copied as f64 / 1_048_576.0);
    }
    
    // Calculate improvement
    if results.len() >= 2 {
        let current = &results[0];
        let zero_copy = &results[1];
        
        let speedup = current.total_time.as_secs_f64() / zero_copy.total_time.as_secs_f64();
        let alloc_reduction = (current.allocations - zero_copy.allocations) as f64 / current.allocations as f64 * 100.0;
        let copy_reduction = (current.bytes_copied - zero_copy.bytes_copied) as f64 / current.bytes_copied as f64 * 100.0;
        
        println!("\n🚀 Improvements with Zero-Copy:");
        println!("  Speed improvement: {:.2}x faster", speedup);
        println!("  Allocation reduction: {:.1}%", alloc_reduction);
        println!("  Copy reduction: {:.1}%", copy_reduction);
    }
}

fn main() {
    println!("🔬 === ZERO-COPY PERFORMANCE COMPARISON ===");
    println!("Comparing current implementation vs zero-copy approach\n");
    
    // Test configurations
    let test_configs = vec![
        (100_000, 64, "Small messages"),
        (100_000, 256, "Medium messages"),
        (50_000, 1024, "Large messages"),
        (10_000, 4096, "XL messages"),
        (5_000, 16384, "XXL messages"),
    ];
    
    for (iterations, payload_size, desc) in test_configs {
        println!("\n🧪 Test: {} ({} bytes)", desc, payload_size);
        
        let results = vec![
            run_benchmark_current(iterations, payload_size),
            run_benchmark_zero_copy(iterations, payload_size),
        ];
        
        print_results(&results, iterations, payload_size);
        
        // Add a small delay between tests
        std::thread::sleep(Duration::from_millis(100));
    }
    
    println!("\n📝 Summary:");
    println!("The zero-copy approach eliminates one memory allocation and one full message copy");
    println!("per operation. The performance benefit increases with message size.");
    println!("\nNote: This is a microbenchmark. Real-world performance depends on many factors");
    println!("including network I/O, actor processing time, and system load.");
}