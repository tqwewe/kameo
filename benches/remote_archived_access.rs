use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use kameo::remote::archived_access::{
    access_archived, set_trusted_archived, trusted_archived_supported,
};
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::util::AlignedVec;

#[derive(Archive, Serialize, Deserialize)]
struct BenchPayload {
    data: [u8; 256],
}

fn make_payload() -> kameo_remote::AlignedBytes {
    let value = BenchPayload { data: [42u8; 256] };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value).expect("serialize payload");
    let mut aligned = AlignedVec::<{ kameo_remote::PAYLOAD_ALIGNMENT }>::new();
    aligned.extend_from_slice(bytes.as_ref());
    kameo_remote::AlignedBytes::from_aligned_vec(aligned)
}

fn bench_archived_access(c: &mut Criterion) {
    let payload = make_payload();
    let mut latency = c.benchmark_group("remote_archived_access_latency");

    latency.bench_function("validated", |b| {
        set_trusted_archived(false);
        b.iter(|| {
            let archived = access_archived::<rkyv::Archived<BenchPayload>>(&payload)
                .expect("validated access");
            black_box(archived);
        });
    });

    if trusted_archived_supported() {
        latency.bench_function("trusted", |b| {
            set_trusted_archived(true);
            b.iter(|| {
                let archived = access_archived::<rkyv::Archived<BenchPayload>>(&payload)
                    .expect("trusted access");
                black_box(archived);
            });
        });
        set_trusted_archived(false);
    }

    latency.finish();

    let mut throughput = c.benchmark_group("remote_archived_access_throughput");
    let batch = 1024usize;
    throughput.throughput(Throughput::Bytes((payload.len() * batch) as u64));

    throughput.bench_function("validated", |b| {
        set_trusted_archived(false);
        b.iter(|| {
            for _ in 0..batch {
                let archived = access_archived::<rkyv::Archived<BenchPayload>>(&payload)
                    .expect("validated access");
                black_box(archived);
            }
        });
    });

    if trusted_archived_supported() {
        throughput.bench_function("trusted", |b| {
            set_trusted_archived(true);
            b.iter(|| {
                for _ in 0..batch {
                    let archived = access_archived::<rkyv::Archived<BenchPayload>>(&payload)
                        .expect("trusted access");
                    black_box(archived);
                }
            });
        });
        set_trusted_archived(false);
    }

    throughput.finish();
}

criterion_group!(benches, bench_archived_access);
criterion_main!(benches);
