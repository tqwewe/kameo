use criterion::{Criterion, criterion_group, criterion_main};
use kameo::prelude::*;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[derive(Actor)]
struct MyActor;

impl Message<u32> for MyActor {
    type Reply = u32;

    async fn handle(&mut self, msg: u32, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        msg
    }
}

fn actor_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Kameo Actor");

    // Bounded actor benchmarks
    group.bench_function("bounded_ask", |b| {
        let rt = Builder::new_current_thread().build().unwrap();
        let _guard = rt.enter();
        let actor_ref = rt.block_on(async {
            let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::bounded(10));
            actor_ref.ask(0).send().await.unwrap(); // Ask an initial message to make sure the actor is ready
            actor_ref
        });
        b.to_async(&rt).iter(|| async {
            actor_ref.ask(0).send().await.unwrap();
        });
    });

    // Unbounded actor benchmarks
    group.bench_function("unbounded_ask", |b| {
        let rt = Builder::new_current_thread().build().unwrap();
        let _guard = rt.enter();
        let actor_ref = rt.block_on(async {
            let actor_ref = MyActor::spawn_with_mailbox(MyActor, mailbox::unbounded());
            actor_ref.ask(0).send().await.unwrap(); // Ask an initial message to make sure the actor is ready
            actor_ref
        });
        b.to_async(&rt).iter(|| async {
            actor_ref.ask(0).send().await.unwrap();
        });
    });

    group.finish();
}

fn plain_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Plain Tokio Task");

    // Bounded channel benchmarks
    group.bench_function("bounded_ask", |b| {
        let rt = Builder::new_current_thread().build().unwrap();
        let (tx, mut rx) = mpsc::channel::<(u32, oneshot::Sender<u32>)>(10);
        rt.spawn(async move {
            while let Some((msg, tx)) = rx.recv().await {
                tx.send(msg).unwrap();
            }
        });

        b.to_async(&rt).iter(|| async {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send((0, reply_tx)).await.unwrap();
            reply_rx.await.unwrap();
        });
    });

    // Unbounded channel benchmarks
    group.bench_function("unbounded_ask", |b| {
        let rt = Builder::new_current_thread().build().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel::<(u32, oneshot::Sender<u32>)>();
        rt.spawn(async move {
            while let Some((msg, tx)) = rx.recv().await {
                tx.send(msg).unwrap();
            }
        });

        b.to_async(&rt).iter(|| async {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send((0, reply_tx)).unwrap();
            reply_rx.await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, actor_benchmarks, plain_benchmarks);
criterion_main!(benches);
