use kameo_remote::connection_pool::BufferConfig;
use kameo_remote::{ChannelId, LockFreeStreamHandle};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::AsyncReadExt;

struct CountingAlloc;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

fn reset_alloc_counters() {
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    DEALLOC_COUNT.store(0, Ordering::Relaxed);
}

fn allocation_delta() -> (usize, usize) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        DEALLOC_COUNT.load(Ordering::Relaxed),
    )
}

async fn run_bench(
    label: &str,
    handle: &LockFreeStreamHandle,
    payload_size: usize,
    count: usize,
    use_ask_lane: bool,
) {
    let payload = bytes::Bytes::from(vec![0u8; payload_size]);
    let header = bytes::Bytes::copy_from_slice(&(payload_size as u32).to_be_bytes());

    // Warm up to avoid counting one-time allocations in the send path.
    for _ in 0..16 {
        if use_ask_lane {
            handle
                .write_header_and_payload_ask(header.clone(), payload.clone())
                .await
                .unwrap();
        } else {
            handle
                .write_header_and_payload_control(header.clone(), payload.clone())
                .await
                .unwrap();
        }
    }

    reset_alloc_counters();
    let start = Instant::now();
    for _ in 0..count {
        if use_ask_lane {
            handle
                .write_header_and_payload_ask(header.clone(), payload.clone())
                .await
                .unwrap();
        } else {
            handle
                .write_header_and_payload_control(header.clone(), payload.clone())
                .await
                .unwrap();
        }
    }
    let elapsed = start.elapsed();
    let (allocs, deallocs) = allocation_delta();

    let throughput = count as f64 / elapsed.as_secs_f64();

    println!(
        "[{}] size={}B count={} throughput={:.0} msg/s allocs={} deallocs={}",
        label, payload_size, count, throughput, allocs, deallocs
    );
}

#[tokio::test]
async fn test_pooled_send_benchmarks() {
    let (writer, mut reader) = tokio::io::duplex(4 * 1024 * 1024);
    let handle = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
    );

    let reader_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let sizes = [128usize, 4 * 1024, 64 * 1024];
    for size in sizes {
        run_bench("tell", &handle, size, 1000, false).await;
        run_bench("ask", &handle, size, 1000, true).await;
    }

    handle.shutdown();
    reader_task.abort();
}
