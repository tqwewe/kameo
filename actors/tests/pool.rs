use kameo::{
    actor::{Actor, ActorRef, Spawn},
    error::{Infallible, SendError},
    mailbox,
    message::{Context, Message},
};
use kameo_actors::pool::{ActorPool, Dispatch};

#[derive(Debug, PartialEq, Eq)]
struct Job(u32);

struct Worker;

impl Actor for Worker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Job> for Worker {
    type Reply = u32;

    async fn handle(&mut self, Job(value): Job, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        value
    }
}

#[tokio::test]
async fn dispatch_propagates_worker_send_error() {
    let unavailable_worker = Worker::spawn(Worker);
    unavailable_worker.kill();
    unavailable_worker.wait_for_shutdown().await;

    let pool = ActorPool::spawn(ActorPool::new(1, {
        let unavailable_worker = unavailable_worker.clone();
        move || unavailable_worker.clone()
    }));

    let result = pool.ask(Dispatch(Job(42))).await;

    assert!(matches!(
        result,
        Err(SendError::HandlerError(SendError::ActorNotRunning(Job(42))))
    ));
}

/// An actor that never finishes starting, so its mailbox loop never runs and nothing it is sent
/// is ever drained. That lets the test pin the number of free mailbox slots exactly, instead of
/// racing the actor's own progress.
struct NeverStarts;

impl Actor for NeverStarts {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        std::future::pending::<()>().await;
        Ok(state)
    }
}

impl Message<Job> for NeverStarts {
    type Reply = u32;

    async fn handle(&mut self, Job(value): Job, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        value
    }
}

/// Building a pool around a worker whose bounded mailbox is momentarily full must not panic.
///
/// The pool gives each worker a wrapper actor and sizes that wrapper's mailbox from the worker's.
/// Reading the worker's *free slots* rather than its total size makes that sizing depend on
/// whatever happened to be queued at that instant, and for a mailbox of one a full queue means
/// zero — which `mailbox::bounded` rejects outright. A freshly spawned actor briefly holds a
/// startup signal in its mailbox, so this was reachable in ordinary use, not just when a caller
/// deliberately saturates a worker as this test does.
#[tokio::test]
async fn pool_construction_tolerates_a_saturated_worker_mailbox() {
    for size in [1, 2, 8] {
        let worker = NeverStarts::spawn_with_mailbox(NeverStarts, mailbox::bounded(size));
        while worker.tell(Job(0)).try_send().is_ok() {}
        assert_eq!(
            worker.mailbox_sender().capacity(),
            Some(0),
            "test setup: the worker's mailbox should be full"
        );

        let _pool = ActorPool::spawn(ActorPool::new(1, {
            let worker = worker.clone();
            move || worker.clone()
        }));
    }
}

/// The async constructor sizes worker mailboxes through the same path, so it has the same
/// exposure.
#[tokio::test]
async fn async_pool_construction_tolerates_a_saturated_worker_mailbox() {
    let worker = NeverStarts::spawn_with_mailbox(NeverStarts, mailbox::bounded(1));
    while worker.tell(Job(0)).try_send().is_ok() {}
    assert_eq!(worker.mailbox_sender().capacity(), Some(0));

    let _pool = ActorPool::spawn(
        ActorPool::new_async(1, {
            let worker = worker.clone();
            move || {
                let worker = worker.clone();
                async move { worker }
            }
        })
        .await,
    );
}
