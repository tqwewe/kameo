use std::{ops::ControlFlow, time::Duration};

use kameo::{error::Infallible, prelude::*, supervision::SupervisionStrategy};
use tracing::{Level, info};

#[derive(Default)]
pub struct MySupervisor;

impl Actor for MySupervisor {
    type Args = ();
    type Error = Infallible;

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForAll
    }

    async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let brother = actor_ref
            .supervise::<BrotherActor>(BrotherActor)
            .spawn()
            .await;
        let worker = actor_ref
            .supervise::<MyActor>(MyActor::default())
            .restart_limit(2, Duration::from_secs(5))
            .spawn()
            .await;

        worker.link(&brother).await;

        tokio::spawn({
            let worker = worker.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    worker.tell(ForceErr).await.unwrap();
                }
            }
        });
        worker.tell(Inc { amount: 10 }).await.unwrap();

        Box::leak(Box::new(brother));

        Ok(MySupervisor)
    }
}

#[derive(Clone, Default)]
pub struct MyActor {
    count: i64,
}

impl Actor for MyActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        info!("my actor started");
        Ok(state)
    }
}

// A simple increment message, returning the new count
pub struct Inc {
    amount: u32,
}

impl Message<Inc> for MyActor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("Incrementing count by {}", msg.amount);
        self.count += msg.amount as i64;
        self.count
    }
}

// Always returns an error
pub struct ForceErr;

impl Message<ForceErr> for MyActor {
    type Reply = Result<(), i32>;

    async fn handle(
        &mut self,
        _msg: ForceErr,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Err(3)
    }
}

#[derive(Clone, Default)]
pub struct BrotherActor;

impl Actor for BrotherActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        info!("brother actor started");
        Ok(state)
    }

    async fn on_link_died(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _id: ActorId,
        _reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        info!("brother actor received link died");
        Ok(ControlFlow::Continue(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let supervisor = MySupervisor::spawn_default();

    supervisor.wait_for_shutdown().await;

    Ok(())
}
