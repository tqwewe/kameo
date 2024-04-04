use std::time::Duration;

use kameo::{actor, Actor, ActorPool, Spawn};
use tracing_subscriber::EnvFilter;

#[derive(Actor, Default)]
struct MyActor;

#[actor]
impl MyActor {
    #[message]
    fn print_actor_id(&self) {
        println!("{}", self.actor_ref().id());
    }

    #[message(derive(Clone))]
    async fn force_stop(&self) {
        let self_ref = self.actor_ref();
        self_ref.kill();
        self_ref.wait_for_stop().await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let mut pool = ActorPool::new(5, || MyActor.spawn());
    for _ in 0..10 {
        pool.send(PrintActorId).await?;
    }

    pool.broadcast(ForceStop).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    for _ in 0..10 {
        pool.send(PrintActorId).await?;
    }

    Ok(())
}
