use async_trait::async_trait;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kameo::{Actor, Query};

struct FibActor {}

impl Actor for FibActor {}

struct Fib(u64);

#[async_trait]
impl Query<FibActor> for Fib {
    type Reply = u64;

    async fn handle(self, _state: &FibActor) -> Self::Reply {
        fibonacci(self.0)
    }
}

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn from_elem(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let size: usize = 1024;
    let actor_ref = FibActor {}.start();
    c.bench_with_input(BenchmarkId::new("concurrent_reads", size), &size, |b, _| {
        b.to_async(&rt)
            .iter(|| async { actor_ref.query(Fib(20)).await.unwrap() });
    });
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
