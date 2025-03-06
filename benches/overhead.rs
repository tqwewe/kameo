use criterion::{criterion_group, criterion_main, Criterion};
use kameo::error::Infallible;
use kameo::mailbox::bounded::BoundedMailboxReceiver;
use kameo::mailbox::{bounded::BoundedMailbox, unbounded::UnboundedMailbox};
use kameo::request::MessageSend;
use kameo::{
    message::{Context, Message},
    Actor,
};
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

// Define a bounded actor
struct BoundedActor;

impl Actor for BoundedActor {
    type Mailbox = BoundedMailbox<Self>;
    type Error = Infallible;

    fn new_mailbox() -> (Self::Mailbox, BoundedMailboxReceiver<Self>) {
        BoundedMailbox::new(10)
    }
}

impl Message<u32> for BoundedActor {
    type Reply = u32;

    async fn handle(&mut self, msg: u32, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        msg
    }
}

// Define an unbounded actor
struct UnboundedActor;

impl Actor for UnboundedActor {
    type Mailbox = UnboundedMailbox<Self>;
    type Error = Infallible;
}

impl Message<u32> for UnboundedActor {
    type Reply = u32;

    async fn handle(&mut self, msg: u32, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
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
            let actor_ref = kameo::actor::spawn(BoundedActor);
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
            let actor_ref = kameo::actor::spawn(UnboundedActor);
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
