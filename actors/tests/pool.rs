use kameo::{
    actor::{Actor, ActorRef, Spawn},
    error::{Infallible, SendError},
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
