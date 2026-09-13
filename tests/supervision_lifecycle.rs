use std::time::Duration;

use kameo::{error::Infallible, prelude::*, supervision::RestartPolicy};
use tokio::sync::mpsc;

const TIMEOUT: Duration = Duration::from_secs(5);
const ON_STOP_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Started,
    Stopping,
    Stopped,
}

struct Supervisor;

impl Actor for Supervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(this)
    }
}

#[derive(Clone)]
struct Worker {
    events: mpsc::UnboundedSender<Event>,
}

impl Actor for Worker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(this: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        this.events.send(Event::Started).unwrap();
        Ok(this)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.events.send(Event::Stopping).unwrap();
        tokio::time::sleep(ON_STOP_DELAY).await;
        self.events.send(Event::Stopped).unwrap();
        Ok(())
    }
}

struct Stop;

impl Message<Stop> for Worker {
    type Reply = ();

    async fn handle(&mut self, _: Stop, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        ctx.stop();
    }
}

async fn recv_event(events: &mut mpsc::UnboundedReceiver<Event>, expected: &str) -> Event {
    match tokio::time::timeout(TIMEOUT, events.recv()).await {
        Ok(Some(event)) => event,
        Ok(None) => panic!("lifecycle event channel closed before {expected}"),
        Err(_) => panic!("timed out waiting for {expected}"),
    }
}

#[tokio::test(start_paused = true)]
async fn supervised_restart_waits_for_previous_on_stop() {
    let supervisor = Supervisor::spawn(Supervisor);
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let worker = Worker::supervise(&supervisor, Worker { events: events_tx })
        .restart_policy(RestartPolicy::Permanent)
        .spawn()
        .await;

    assert_eq!(
        recv_event(&mut events, "initial startup").await,
        Event::Started
    );

    worker.tell(Stop).await.unwrap();

    let actual = [
        recv_event(&mut events, "on_stop start").await,
        recv_event(&mut events, "on_stop completion").await,
        recv_event(&mut events, "restart startup").await,
    ];

    assert_eq!(
        actual,
        [Event::Stopping, Event::Stopped, Event::Started],
        "next incarnation started before previous on_stop completed"
    );

    supervisor.kill();
    tokio::time::timeout(TIMEOUT, supervisor.wait_for_shutdown())
        .await
        .expect("timed out waiting for supervisor shutdown");
}
