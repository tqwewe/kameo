use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kameo::actor::BoundedMailbox;
use kameo::{
    message::{Context, Message, Query},
    Actor,
};

struct FibActor {}

impl Actor for FibActor {
    type Mailbox = BoundedMailbox<Self>;
}

struct Fib(u64);

impl Message<Fib> for FibActor {
    type Reply = u64;

    async fn handle(&mut self, msg: Fib, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        fibonacci(msg.0)
    }
}

impl Query<Fib> for FibActor {
    type Reply = u64;

    async fn handle(&self, msg: Fib, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        fibonacci(msg.0)
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let _guard = rt.enter();
    let size: usize = 1024;
    let unsync_actor_ref = kameo::actor::spawn_unsync(FibActor {});
    let sync_actor_ref = kameo::spawn(FibActor {});

    c.bench_with_input(
        BenchmarkId::new("unsync_messages_instant", size),
        &size,
        |b, _| {
            b.to_async(&rt)
                .iter(|| async { unsync_actor_ref.ask(Fib(0)).send().await.unwrap() });
        },
    );

    c.bench_with_input(
        BenchmarkId::new("sync_messages_instant", size),
        &size,
        |b, _| {
            b.to_async(&rt)
                .iter(|| async { sync_actor_ref.ask(Fib(0)).send().await.unwrap() });
        },
    );

    c.bench_with_input(BenchmarkId::new("queries_instant", size), &size, |b, _| {
        b.to_async(&rt)
            .iter(|| async { sync_actor_ref.query(Fib(0)).send().await.unwrap() });
    });

    c.bench_with_input(
        BenchmarkId::new("unsync_messages_fast", size),
        &size,
        |b, _| {
            b.to_async(&rt)
                .iter(|| async { unsync_actor_ref.ask(Fib(20)).send().await.unwrap() });
        },
    );

    c.bench_with_input(
        BenchmarkId::new("sync_messages_fast", size),
        &size,
        |b, _| {
            b.to_async(&rt)
                .iter(|| async { sync_actor_ref.ask(Fib(20)).send().await.unwrap() });
        },
    );

    c.bench_with_input(BenchmarkId::new("queries_fast", size), &size, |b, _| {
        b.to_async(&rt)
            .iter(|| async { sync_actor_ref.query(Fib(20)).send().await.unwrap() });
    });

    c.bench_with_input(
        BenchmarkId::new("unsync_messages_slow", size),
        &size,
        |b, _| {
            b.to_async(&rt)
                .iter(|| async { unsync_actor_ref.ask(Fib(30)).send().await.unwrap() });
        },
    );

    c.bench_with_input(
        BenchmarkId::new("sync_messages_slow", size),
        &size,
        |b, _| {
            b.to_async(&rt)
                .iter(|| async { sync_actor_ref.ask(Fib(30)).send().await.unwrap() });
        },
    );

    c.bench_with_input(BenchmarkId::new("queries_slow", size), &size, |b, _| {
        b.to_async(&rt)
            .iter(|| async { sync_actor_ref.query(Fib(30)).send().await.unwrap() });
    });
}

criterion_group!(benches, concurrent_reads);
criterion_main!(benches);
