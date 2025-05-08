use std::{ops::ControlFlow, time::Duration};

use kameo::{error::Infallible, prelude::*};
use libp2p::swarm::dial_opts::DialOpts;
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(RemoteActor)]
pub struct Supervisor;

impl Actor for Supervisor {
    type Error = Infallible;
    type Args = ();

    async fn on_link_died(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        id: ActorID,
        reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        info!("A supervised actor died: {id} - {reason}");
        Ok(ControlFlow::Continue(()))
    }

    async fn on_start(_args: (), actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        info!("supervisor started");

        actor_ref
            .register("supervisor")
            .await
            .expect("could not register supervisor");

        Ok(Supervisor)
    }
}

#[derive(RemoteActor)]
pub struct Panicker {
    actor_registry_name: String,
}

impl Actor for Panicker {
    type Error = Infallible;
    type Args = String;

    async fn on_start(
        actor_registry_name: String,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        info!("panicker started");

        actor_ref
            .register(&actor_registry_name)
            .await
            .expect("could not register panicker");

        let supervisor_ref = RemoteActorRef::<Supervisor>::lookup("supervisor")
            .await
            .expect("could not lookup supervisor")
            .expect("supervisor not found");

        supervisor_ref
            .tell(&LinkMe {
                actor_registry_name: actor_registry_name.clone(),
            })
            .await
            .expect("could not tell supervisor");

        Ok(Panicker {
            actor_registry_name,
        })
    }
}

pub struct Panic;

impl Message<Panic> for Panicker {
    type Reply = ();

    async fn handle(&mut self, _msg: Panic, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("panicker {} panicked", self.actor_registry_name);
    }
}

#[derive(Serialize, Deserialize)]
pub struct LinkMe {
    actor_registry_name: String,
}

#[remote_message("0cc2d8bc-1e9d-4ae2-b4a4-97e3220b7ac0")]
impl Message<LinkMe> for Supervisor {
    type Reply = Result<(), String>;

    async fn handle(&mut self, msg: LinkMe, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let panicker = RemoteActorRef::<Panicker>::lookup(&msg.actor_registry_name)
            .await
            .expect("could not lookup panicker")
            .expect("panicker not found");

        let self_remote_ref = RemoteActorRef::<Supervisor>::lookup("supervisor")
            .await
            .expect("could not lookup supervisor")
            .expect("supervisor not found");

        // This panics
        // self_remote_ref
        //     .link_remote(&panicker)
        //     .await
        //     .expect("could not link supervisor and panicker");

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info".parse::<EnvFilter>()?)
        .without_time()
        .with_target(false)
        .init();

    let actor_name = std::env::args().nth(1).expect("expected actor name");

    let mut supervisor_ref = None;

    let swarm = ActorSwarm::bootstrap()?;

    if swarm
        .listen_on("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?)
        .await
        .is_ok()
    {
        info!("starting actor {actor_name} as supervisor");
        let supervisor = Supervisor::spawn(());
        supervisor.wait_for_startup().await;
        supervisor_ref = Some(supervisor);
    } else {
        swarm
            .dial(
                DialOpts::unknown_peer_id()
                    .address("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?)
                    .build(),
            )
            .await
            .expect("could not dial");
    }

    loop {
        let supervisor_ref = RemoteActorRef::<Supervisor>::lookup("supervisor").await;

        if let Ok(Some(supervisor)) = supervisor_ref {
            info!("supervisor found {supervisor:?}");
            break;
        }

        info!("waiting for supervisor to start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    info!("starting actor {actor_name}");
    let panicker = Panicker::spawn(actor_name.clone());
    panicker.wait_for_startup().await;

    {
        let remote_panicker = RemoteActorRef::<Panicker>::lookup(&actor_name)
            .await
            .unwrap()
            .unwrap();

        let remote_supervisor = RemoteActorRef::<Supervisor>::lookup("supervisor")
            .await
            .unwrap()
            .unwrap();

        // This panics with a DialFailure
        remote_supervisor
            .link_remote(&remote_panicker)
            .await
            .unwrap();
    }

    // panicker
    //     .ask(Panic)
    //     .await
    //     .expect_err("panicker should have panicked");

    panicker.wait_for_shutdown().await;

    if let Some(supervisor) = supervisor_ref {
        supervisor.wait_for_shutdown().await;
    }

    Ok(())
}
