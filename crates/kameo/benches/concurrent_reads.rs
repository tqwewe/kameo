use std::convert::Infallible;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kameo::{Actor, Query, Spawn};

struct FibActor {}

impl Actor for FibActor {}

struct Fib(u64);

impl Query<FibActor> for Fib {
    type Reply = Result<u64, Infallible>;

    async fn handle(self, _state: &FibActor) -> Self::Reply {
        Ok(fibonacci(self.0))
    }
}

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn concurrent_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let size: usize = 1024;
    let actor_ref = FibActor {}.spawn();

    c.bench_with_input(BenchmarkId::new("concurrent_reads", size), &size, |b, _| {
        b.to_async(&rt)
            .iter(|| async { actor_ref.query(Fib(20)).await.unwrap() });
    });

    c.bench_with_input(
        BenchmarkId::new("concurrent_reads_without_actor", size),
        &size,
        |b, _| b.iter(|| fibonacci(20)),
    );
}

criterion_group!(benches, concurrent_reads);
criterion_main!(benches);
