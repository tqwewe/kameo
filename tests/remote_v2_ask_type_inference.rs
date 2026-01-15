//! Test for type inference in remote ask calls using the v2 API.

use std::time::Duration;

use kameo::{
    actor::{Actor, ActorRef},
    distributed_actor,
    message::{Context, Message},
    remote::{
        transport::RemoteTransport,
        distributed_actor_ref::DistributedActorRef,
        v2_bootstrap,
    },
    RemoteMessage,
};
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use kameo_remote::KeyPair;

#[derive(Debug)]
struct TypeInferenceActor;

impl Actor for TypeInferenceActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

#[derive(RemoteMessage, Archive, RSerialize, RDeserialize, Debug, Clone, Copy)]
struct Ping;

#[derive(Archive, RSerialize, RDeserialize, Debug, PartialEq)]
struct PingResult;

impl kameo::reply::Reply for PingResult {
    type Ok = Self;
    type Error = kameo::error::Infallible;
    type Value = Self;

    fn to_result(self) -> Result<Self, kameo::error::Infallible> {
        Ok(self)
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl Message<Ping> for TypeInferenceActor {
    type Reply = PingResult;

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        PingResult
    }
}

distributed_actor! {
    TypeInferenceActor {
        Ping,
    }
}

#[tokio::test]
async fn test_ask_type_inference() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Start server
    let server_keypair = KeyPair::new_for_testing("server-type-inference");
    let server_peer_id = server_keypair.peer_id();
    let server_transport =
        v2_bootstrap::bootstrap_with_keypair("127.0.0.1:0".parse()?, server_keypair).await?;
    let server_addr = server_transport.local_addr();

    // Spawn actor on server
    let actor_ref = TypeInferenceActor::spawn(());
    server_transport.register_actor("my-actor".to_string(), actor_ref.id()).await?;


    // Start client
    let client_keypair = KeyPair::new_for_testing("client-type-inference");
    let client_transport = v2_bootstrap::bootstrap_with_keypair("127.0.0.1:0".parse()?, client_keypair).await?;
    if let Some(handle) = client_transport.handle() {
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&server_addr).await?;
    }
    tokio::time::sleep(Duration::from_secs(6)).await;


    // Lookup from client
    let remote_ref = DistributedActorRef::<TypeInferenceActor>::lookup("my-actor").await?.unwrap();

    // Send ask - should infer the type
    let result = remote_ref.ask(Ping).send().await?;

    assert_eq!(result, PingResult);

    Ok(())
}
