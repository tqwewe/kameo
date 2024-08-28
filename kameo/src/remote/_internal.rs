use std::time::Duration;

use futures::future::BoxFuture;
pub use linkme::distributed_slice;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::actor::{ActorID, ActorRef};
use crate::error::{RemoteSendError, RemoteSpawnError};
use crate::message::Message;
use crate::{Actor, Reply};

use super::REMOTE_REGISTRY;

#[distributed_slice]
pub static REMOTE_ACTORS: [(&'static str, RemoteSpawnFn)];

#[distributed_slice]
pub static REMOTE_MESSAGES: [(
    RemoteMessageRegistrationID<'static>,
    (AskRemoteMessageFn, TellRemoteMessageFn),
)];

pub type RemoteSpawnFn =
    fn(actor: Vec<u8>) -> BoxFuture<'static, Result<ActorID, RemoteSpawnError>>;

pub type AskRemoteMessageFn = fn(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>>;

pub type TellRemoteMessageFn = fn(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> BoxFuture<'static, Result<(), RemoteSendError<Vec<u8>>>>;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct RemoteMessageRegistrationID<'a> {
    pub actor_name: &'a str,
    pub message_name: &'a str,
}

pub async fn spawn_remote<A>(actor: Vec<u8>) -> Result<ActorID, RemoteSpawnError>
where
    A: Actor + DeserializeOwned + Send + Sync + 'static,
{
    let actor: A = rmp_serde::decode::from_slice(&actor)
        .map_err(|err| RemoteSpawnError::DeserializeActor(err.to_string()))?;
    let actor_ref = crate::spawn(actor);
    let actor_id = actor_ref.id();
    REMOTE_REGISTRY
        .lock()
        .await
        .insert(actor_id, Box::new(actor_ref));

    Ok(actor_id)
}

pub async fn ask_remote_message<A, M>(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned,
    ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
    <A::Reply as Reply>::Ok: Serialize,
    <A::Reply as Reply>::Error: Serialize,
{
    let res =
        ask_remote_message_inner::<A, M>(actor_id, msg, mailbox_timeout, reply_timeout, immediate)
            .await;
    match res {
        Ok(reply) => Ok(rmp_serde::to_vec_named(&reply)
            .map_err(|err| RemoteSendError::SerializeReply(err.to_string()))?),
        Err(err) => Err(err
            .map_err(|err| match rmp_serde::to_vec_named(&err) {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
            })
            .flatten()),
    }
}

async fn ask_remote_message_inner<A, M>(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned,
    ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };
    let msg: M = rmp_serde::decode::from_slice(&msg)
        .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

    let reply =
        crate::request::Request::ask(&actor_ref, msg, mailbox_timeout, reply_timeout, immediate)
            .await?;

    Ok(reply)
}

pub async fn tell_remote_message<A, M>(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned,
    ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
    <A::Reply as Reply>::Error: Serialize,
{
    let res = tell_remote_message_inner::<A, M>(actor_id, msg, mailbox_timeout, immediate).await;
    match res {
        Ok(()) => Ok(()),
        Err(err) => Err(err
            .map_err(|err| match rmp_serde::to_vec_named(&err) {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
            })
            .flatten()),
    }
}

async fn tell_remote_message_inner<A, M>(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned,
    ActorRef<A>: crate::request::Request<A, M, A::Mailbox>,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };
    let msg: M = rmp_serde::decode::from_slice(&msg)
        .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

    crate::request::Request::tell(&actor_ref, msg, mailbox_timeout, immediate).await?;

    Ok(())
}
