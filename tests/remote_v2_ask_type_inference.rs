#![cfg(feature = "remote")]
//! Test for type inference in remote ask calls using the v2 API.

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use kameo::{
    actor::{Actor, ActorId, ActorRef},
    distributed_actor,
    message::{Context, Message},
    remote::{
        distributed_actor_ref::DistributedActorRef,
        transport::{
            MessageHandler, RemoteActorLocation, RemoteTransport, TransportError, TransportFuture,
        },
    },
    RemoteMessage,
};
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

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
    // Create a deterministic remote actor location (no network needed for type inference test)
    let location = RemoteActorLocation {
        peer_addr: "127.0.0.1:0".parse()?,
        actor_id: ActorId::from_u64(42),
        metadata: Vec::new(),
    };

    // Use a deterministic transport that always returns PingResult bytes
    let transport = TestTransport;

    let remote_ref: DistributedActorRef<TypeInferenceActor, TestTransport> =
        DistributedActorRef::__new_without_connection_for_tests(
            location.actor_id,
            location,
            transport,
        );

    // Send ask - should infer the type
    let result: PingResult = remote_ref.ask(Ping).send().await?;

    assert_eq!(result, PingResult);

    Ok(())
}

#[derive(Clone, Copy)]
struct TestTransport;

impl RemoteTransport for TestTransport {
    fn start(&mut self) -> TransportFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> TransportFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn local_addr(&self) -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    fn register_actor(&self, _name: String, _actor_id: ActorId) -> TransportFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn register_actor_sync(
        &self,
        _name: String,
        _actor_id: ActorId,
        _timeout: Duration,
    ) -> TransportFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn unregister_actor(&self, _name: &str) -> TransportFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn lookup_actor(&self, _name: &str) -> TransportFuture<'_, Option<RemoteActorLocation>> {
        Box::pin(async { Ok(None) })
    }

    fn send_tell<M>(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _message: M,
    ) -> TransportFuture<'_, ()>
    where
        M: Archive
            + for<'a> RSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        &'a mut [u8],
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            > + Send
            + 'static,
    {
        Box::pin(async { Ok(()) })
    }

    fn send_ask<A, M>(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _message: M,
        _timeout: Duration,
    ) -> TransportFuture<'_, <A as Message<M>>::Reply>
    where
        A: Actor + Message<M>,
        M: Archive
            + for<'a> RSerialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        &'a mut [u8],
                        rkyv::ser::allocator::ArenaHandle<'a>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            > + Send
            + 'static,
        <A as Message<M>>::Reply: Archive
            + for<'a> rkyv::Deserialize<
                <A as Message<M>>::Reply,
                rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
            > + Send,
    {
        Box::pin(async {
            Err(TransportError::Other(
                "send_ask not used in test transport".into(),
            ))
        })
    }

    fn send_tell_typed(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _type_hash: u32,
        _payload: Bytes,
    ) -> TransportFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn send_ask_typed(
        &self,
        _actor_id: ActorId,
        _location: &RemoteActorLocation,
        _type_hash: u32,
        _payload: Bytes,
        _timeout: Duration,
    ) -> TransportFuture<'_, Bytes> {
        Box::pin(async {
            let reply = PingResult;
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&reply)
                .map_err(|e| TransportError::SerializationFailed(e.to_string()))?;
            Ok(Bytes::from(bytes.into_vec()))
        })
    }

    fn set_message_handler(&mut self, _handler: Box<dyn MessageHandler>) {}
}
