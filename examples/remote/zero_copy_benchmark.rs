//! Direct benchmark comparing old vs new message sending implementation
//!
//! Run with: cargo run --example zero_copy_benchmark --features remote --release

use bytes::{Bytes, BytesMut, BufMut};
use std::time::{Duration, Instant};

// Mock connection handle to test both implementations
struct MockConnection {
    bytes_sent: std::cell::RefCell<Vec<Bytes>>,
}

impl MockConnection {
    fn new() -> Self {
        Self {
            bytes_sent: std::cell::RefCell::new(Vec::new()),
        }
    }
    
    // Old method - copies data
    fn send_raw_bytes(&self, data: &[u8]) -> Result<(), String> {
        let bytes = Bytes::copy_from_slice(data);
        self.bytes_sent.borrow_mut().push(bytes);
        Ok(())
    }
    
    // New method - zero copy
    fn send_bytes_zero_copy(&self, data: Bytes) -> Result<(), String> {
        self.bytes_sent.borrow_mut().push(data);
        Ok(())
    }
    
    fn clear(&self) {
        self.bytes_sent.borrow_mut().clear();
    }
}

// Old implementation (using Vec<u8>)
fn send_message_old(conn: &MockConnection, actor_id: u64, type_hash: u32, payload: &[u8]) -> Result<(), String> {
    let inner_size = 8 + 16 + payload.len();
    let mut message = Vec::with_capacity(4 + inner_size);
    
    message.extend_from_slice(&(inner_size as u32).to_be_bytes());
    message.push(3u8);
    message.extend_from_slice(&0u16.to_be_bytes());
    message.extend_from_slice(&[0u8; 5]);
    message.extend_from_slice(&actor_id.to_be_bytes());
    message.extend_from_slice(&type_hash.to_be_bytes());
    message.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    message.extend_from_slice(payload);
    
    conn.send_raw_bytes(&message)
}

// New implementation (using BytesMut)
fn send_message_new(conn: &MockConnection, actor_id: u64, type_hash: u32, payload: &[u8]) -> Result<(), String> {
    let inner_size = 8 + 16 + payload.len();
    let mut message = BytesMut::with_capacity(4 + inner_size);
    
    message.put_u32(inner_size as u32);
    message.put_u8(3);
    message.put_u16(0);
    message.put_slice(&[0u8; 5]);
    message.put_u64(actor_id);
    message.put_u32(type_hash);
    message.put_u32(payload.len() as u32);
    message.put_slice(payload);
    
    conn.send_bytes_zero_copy(message.freeze())
}

fn benchmark_implementation<F>(
    name: &str,
    iterations: usize,
    payload_size: usize,
    send_fn: F,
) -> Duration
where
    F: Fn(&MockConnection, &[u8]) -> Result<(), String>,
{
    let conn = MockConnection::new();
    let payload = vec![0x42u8; payload_size];
    
    // Warm up
    for _ in 0..100 {
        send_fn(&conn, &payload).unwrap();
        conn.clear();
    }
    
    // Benchmark
    let start = Instant::now();
    for _ in 0..iterations {
        send_fn(&conn, &payload).unwrap();
    }
    let duration = start.elapsed();
    
    println!("{} - {} iterations, {}B payload: {:?} ({:.2} ns/op)", 
        name, 
        iterations, 
        payload_size,
        duration,
        duration.as_nanos() as f64 / iterations as f64
    );
    
    duration
}

fn main() {
    println!("🔬 === ZERO-COPY MESSAGE SENDING BENCHMARK ===\n");
    
    let test_configs = vec![
        (1_000_000, 64, "Small payload"),
        (1_000_000, 256, "Medium payload"),
        (500_000, 1024, "Large payload"),
        (100_000, 4096, "XL payload"),
        (50_000, 16384, "XXL payload"),
    ];
    
    for (iterations, payload_size, desc) in test_configs {
        println!("\n📊 {} ({} bytes):", desc, payload_size);
        println!("{}", "-".repeat(60));
        
        let old_duration = benchmark_implementation(
            "OLD (Vec + copy)",
            iterations,
            payload_size,
            |conn, payload| send_message_old(conn, 12345, 0xABCDEF00, payload),
        );
        
        let new_duration = benchmark_implementation(
            "NEW (BytesMut + zero-copy)",
            iterations,
            payload_size,
            |conn, payload| send_message_new(conn, 12345, 0xABCDEF00, payload),
        );
        
        let improvement = old_duration.as_secs_f64() / new_duration.as_secs_f64();
        let percent_faster = (improvement - 1.0) * 100.0;
        
        println!("\n🚀 Improvement: {:.2}x faster ({:.1}% reduction in time)", improvement, percent_faster);
    }
    
    println!("\n\n📝 Analysis:");
    println!("The zero-copy implementation eliminates:");
    println!("1. One allocation (Vec vs reusing BytesMut)");
    println!("2. One full message copy in send_raw_bytes()");
    println!("\nFor small messages, the benefit is minimal due to:");
    println!("- CPU cache efficiency with small copies");
    println!("- Overhead of method calls dominating");
    println!("\nFor large messages, the benefit is significant due to:");
    println!("- Reduced memory bandwidth usage");
    println!("- Fewer allocations and deallocations");
}