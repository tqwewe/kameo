use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kameo::{
    message::{Context, Message},
    Actor,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;

fn actor(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let _guard = rt.enter();

    struct BenchActor;

    impl Actor for BenchActor {}

    impl Message<u32> for BenchActor {
        type Reply = u32;

        async fn handle(&mut self, msg: u32, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
            msg
        }
    }
    let actor_ref = kameo::actor::spawn(BenchActor {});

    c.bench_function("actor_sync_messages", |b| {
        b.to_async(&rt).iter(|| async {
            actor_ref.send(0).await.unwrap();
        });
    });
}

fn plain(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let _guard = rt.enter();

    // Echo task - pendant to the actor.
    let (tx, mut rx) = mpsc::unbounded_channel::<(u32, oneshot::Sender<u32>)>();
    let handle = task::spawn(async move {
        while let Some((msg, tx)) = rx.recv().await {
            tx.send(msg).unwrap();
        }
    });

    c.bench_function("plain_sync_messages_unbounded", |b| {
        b.to_async(&rt).iter(|| async {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send((0, reply_tx)).unwrap();
            reply_rx.await.unwrap();
        });
    });
    handle.abort();

    // Echo task bounded - pendant to the actor.
    let (tx, mut rx) = mpsc::channel::<(u32, oneshot::Sender<u32>)>(10);
    let handle = task::spawn(async move {
        while let Some((msg, tx)) = rx.recv().await {
            tx.send(msg).unwrap();
        }
    });

    // Echo task unbounded boxed - pendant to the actor.
    c.bench_function("plain_sync_messages_bounded", |b| {
        b.to_async(&rt).iter(|| async {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send((0, reply_tx)).await.unwrap();
            reply_rx.await.unwrap();
        });
    });
    handle.abort();

    let (tx, mut rx) = mpsc::unbounded_channel::<(
        Box<dyn std::any::Any + Send>,
        oneshot::Sender<Box<dyn std::any::Any + Send>>,
    )>();
    let handle = task::spawn(async move {
        while let Some((msg, tx)) = rx.recv().await {
            let num: u32 = *msg.downcast().unwrap();
            tx.send(Box::new(num)).unwrap();
        }
    });

    c.bench_function("plain_sync_messages_unbounded_boxed", |b| {
        b.to_async(&rt).iter(|| async {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send((Box::new(0u32), reply_tx)).unwrap();
            let _num: u32 = *reply_rx.await.unwrap().downcast().unwrap();
        });
    });
    handle.abort();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = actor, plain
}

criterion_main!(benches);
