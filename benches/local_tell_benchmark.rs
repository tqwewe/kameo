use kameo::actor::ActorRef;
use kameo::request::Tell;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct CountingAlloc;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

#[derive(Debug, Clone)]
struct EchoMessage {
    payload: Vec<u8>,
}

impl Tell for EchoMessage {}

struct EchoActor;

impl kameo::actor::Actor for EchoActor {
    type Msg = EchoMessage;

    async fn pre_start(&mut self, _ctx: kameo::context::Context<Self::Msg>) -> Result<(), kameo::error::BoxError> {
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: Self::Msg,
        _ctx: kameo::context::Context<Self::Msg>,
    ) -> Result<(), kameo::error::BoxError> {
        // Simulate minimal work - just read the payload
        let _ = msg.payload.len();
        Ok(())
    }
}

/// Local tell benchmark for kameo actors.
///
/// Usage:
///   cargo bench --bench local_tell_benchmark -- [tell_count]
///
/// Example:
///   cargo bench --bench local_tell_benchmark -- 100000
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let tell_count: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(100_000);

    println!("🔸 Local Tell Benchmark");
    println!("======================");
    println!("Tell count: {}\n", tell_count);

    // Create the runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    rt.block_on(async {
        // Spawn the actor
        let actor_ref: ActorRef<EchoActor> =
            kameo::actor::Actor::spawn(None, EchoActor, ())
                .await
                .expect("Failed to spawn actor");

        println!("✅ Actor spawned: {:?}", actor_ref.id());
        println!("Warming up...\n");

        // Warmup
        let warmup_count = 1000.min(tell_count);
        for _ in 0..warmup_count {
            let msg = EchoMessage {
                payload: b"hello".to_vec(),
            };
            let _ = actor_ref.tell(msg).await;
        }
        tokio::task::yield_now().await;

        println!("Starting benchmark...\n");

        // Benchmark
        let before = alloc_snapshot();
        let mut latencies = Vec::with_capacity(tell_count);
        let start = Instant::now();

        for _ in 0..tell_count {
            let msg = EchoMessage {
                payload: b"hello".to_vec(),
            };
            let instant_start = Instant::now();
            let _ = actor_ref.tell(msg).await;
            latencies.push(instant_start.elapsed());
        }

        let total = start.elapsed();
        let after = alloc_snapshot();

        // Calculate results
        let rps = tell_count as f64 / total.as_secs_f64();

        // Print results
        print_latency_stats("local tell()", &latencies);
        println!(
            "local tell() throughput: {:.0} msgs/sec (total {:.3}s)",
            rps,
            total.as_secs_f64()
        );
        print_alloc_delta("local tell()", before, after);

        // Cleanup
        let _ = actor_ref.stop().await;
        println!("\n✅ Benchmark complete");
    });
}

#[derive(Clone, Copy)]
struct AllocSnapshot {
    allocs: u64,
    deallocs: u64,
    alloc_bytes: u64,
    dealloc_bytes: u64,
}

fn alloc_snapshot() -> AllocSnapshot {
    AllocSnapshot {
        allocs: ALLOC_COUNT.load(Ordering::Relaxed),
        deallocs: DEALLOC_COUNT.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
    }
}

fn print_alloc_delta(label: &str, before: AllocSnapshot, after: AllocSnapshot) {
    let allocs = after.allocs.saturating_sub(before.allocs);
    let deallocs = after.deallocs.saturating_sub(before.deallocs);
    let alloc_bytes = after.alloc_bytes.saturating_sub(before.alloc_bytes);
    let dealloc_bytes = after.dealloc_bytes.saturating_sub(before.dealloc_bytes);
    let live_bytes = alloc_bytes.saturating_sub(dealloc_bytes);

    println!(
        "{} allocations: allocs {} ({} bytes) | deallocs {} ({} bytes) | net {} bytes",
        label, allocs, alloc_bytes, deallocs, dealloc_bytes, live_bytes
    );
}

fn print_latency_stats(label: &str, latencies: &[Duration]) {
    if latencies.is_empty() {
        return;
    }

    let mut nanos: Vec<u128> = latencies.iter().map(|d| d.as_nanos()).collect();
    nanos.sort_unstable();

    let len = nanos.len() as u128;
    let sum: u128 = nanos.iter().sum();
    let mean = sum / len;

    let p50 = percentile(&nanos, 50);
    let p95 = percentile(&nanos, 95);
    let p99 = percentile(&nanos, 99);
    let min = *nanos.first().unwrap();
    let max = *nanos.last().unwrap();

    println!(
        "{} latency (µs): min {:.2} | p50 {:.2} | p95 {:.2} | p99 {:.2} | max {:.2} | avg {:.2}",
        label,
        nanos_to_micros(min),
        nanos_to_micros(p50),
        nanos_to_micros(p95),
        nanos_to_micros(p99),
        nanos_to_micros(max),
        nanos_to_micros(mean)
    );
}

fn percentile(sorted_nanos: &[u128], pct: u128) -> u128 {
    if sorted_nanos.is_empty() {
        return 0;
    }
    let idx = ((pct * (sorted_nanos.len() as u128 - 1)) / 100) as usize;
    sorted_nanos[idx]
}

fn nanos_to_micros(nanos: u128) -> f64 {
    nanos as f64 / 1000.0
}
