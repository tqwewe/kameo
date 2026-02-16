use std::borrow::Cow;
use std::time::Duration;

use crate::actor::{ActorId, Link};
use crate::error::{ActorStopReason, Infallible, RemoteSendError};
use crate::message::Message;
use crate::{Actor, Reply};
pub use const_fnv1a_hash;
pub use const_str;
use futures::future::BoxFuture;
pub use linkme;

use super::codec::{Decode, Encode};

use super::REMOTE_REGISTRY;

#[linkme::distributed_slice]
pub static REMOTE_ACTORS: [(&'static str, RemoteActorFns)];

#[linkme::distributed_slice]
pub static REMOTE_MESSAGES: [(RemoteMessageRegistrationID<'static>, RemoteMessageFns)];

#[derive(Clone, Copy, Debug)]
pub struct RemoteActorFns {
    pub link: RemoteLinkFn,
    pub unlink: RemoteUnlinkFn,
    pub signal_link_died: RemoteSignalLinkDiedFn,
}

#[derive(Clone, Copy, Debug)]
pub struct RemoteMessageFns {
    pub ask: RemoteAskFn,
    pub try_ask: RemoteTryAskFn,
    pub tell: RemoteTellFn,
    pub try_tell: RemoteTryTellFn,
}

pub type RemoteAskFn = fn(
    actor_id: ActorId,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>>;

pub type RemoteTryAskFn = fn(
    actor_id: ActorId,
    msg: Vec<u8>,
    reply_timeout: Option<Duration>,
) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>>;

pub type RemoteTellFn = fn(
    actor_id: ActorId,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
) -> BoxFuture<'static, Result<(), RemoteSendError>>;

pub type RemoteTryTellFn =
    fn(actor_id: ActorId, msg: Vec<u8>) -> BoxFuture<'static, Result<(), RemoteSendError>>;

pub type RemoteLinkFn = fn(
    actor_id: ActorId,
    sibling_id: ActorId,
    sibling_remote_id: Cow<'static, str>,
) -> BoxFuture<'static, Result<(), RemoteSendError<Infallible>>>;

pub type RemoteUnlinkFn = fn(
    actor_id: ActorId,
    sibling_id: ActorId,
) -> BoxFuture<'static, Result<(), RemoteSendError<Infallible>>>;

pub type RemoteSignalLinkDiedFn = fn(
    dead_actor_id: ActorId,
    notified_actor_id: ActorId,
    stop_reason: ActorStopReason,
)
    -> BoxFuture<'static, Result<(), RemoteSendError<Infallible>>>;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct RemoteMessageRegistrationID<'a> {
    pub actor_remote_id: &'a str,
    pub message_remote_id: &'a str,
}

pub async fn ask<A, M>(
    actor_id: ActorId,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: Decode,
    <A::Reply as Reply>::Ok: Encode,
    <A::Reply as Reply>::Error: Encode,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast::<A>()?
    };
    let msg: M = M::decode(&msg).map_err(RemoteSendError::DeserializeMessage)?;

    let res = actor_ref
        .ask(msg)
        .mailbox_timeout_opt(mailbox_timeout)
        .reply_timeout_opt(reply_timeout)
        .send()
        .await;
    match res {
        Ok(reply) => Ok(reply.encode().map_err(RemoteSendError::SerializeReply)?),
        Err(err) => Err(RemoteSendError::from(err)
            .map_err(|err| match err.encode() {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err),
            })
            .flatten()),
    }
}

pub async fn try_ask<A, M>(
    actor_id: ActorId,
    msg: Vec<u8>,
    reply_timeout: Option<Duration>,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: Decode,
    <A::Reply as Reply>::Ok: Encode,
    <A::Reply as Reply>::Error: Encode,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast::<A>()?
    };
    let msg: M = M::decode(&msg).map_err(RemoteSendError::DeserializeMessage)?;

    let res = actor_ref
        .ask(msg)
        .reply_timeout_opt(reply_timeout)
        .try_send()
        .await;
    match res {
        Ok(reply) => Ok(reply.encode().map_err(RemoteSendError::SerializeReply)?),
        Err(err) => Err(RemoteSendError::from(err)
            .map_err(|err| match err.encode() {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err),
            })
            .flatten()),
    }
}

pub async fn tell<A, M>(
    actor_id: ActorId,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
) -> Result<(), RemoteSendError>
where
    A: Actor + Message<M>,
    M: Decode,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast::<A>()?
    };
    let msg: M = M::decode(&msg).map_err(RemoteSendError::DeserializeMessage)?;

    let res = actor_ref
        .tell(msg)
        .mailbox_timeout_opt(mailbox_timeout)
        .send()
        .await;
    match res {
        Ok(()) => Ok(()),
        Err(err) => Err(RemoteSendError::from(err)),
    }
}

pub async fn try_tell<A, M>(actor_id: ActorId, msg: Vec<u8>) -> Result<(), RemoteSendError>
where
    A: Actor + Message<M>,
    M: Decode,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast::<A>()?
    };
    let msg: M = M::decode(&msg).map_err(RemoteSendError::DeserializeMessage)?;

    let res = actor_ref.tell(msg).try_send();
    match res {
        Ok(()) => Ok(()),
        Err(err) => Err(RemoteSendError::from(err)),
    }
}

pub async fn link<A>(
    actor_id: ActorId,
    sibling_id: ActorId,
    sibling_remote_id: Cow<'static, str>,
) -> Result<(), RemoteSendError<Infallible>>
where
    A: Actor,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast::<A>()?
    };

    actor_ref
        .links
        .lock()
        .await
        .insert(sibling_id, Link::Remote(sibling_remote_id));

    Ok(())
}

pub async fn unlink<A>(
    actor_id: ActorId,
    sibling_id: ActorId,
) -> Result<(), RemoteSendError<Infallible>>
where
    A: Actor,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast::<A>()?
    };

    actor_ref.links.lock().await.remove(&sibling_id);

    Ok(())
}

pub async fn signal_link_died<A>(
    dead_actor_id: ActorId,
    notified_actor_id: ActorId,
    stop_reason: ActorStopReason,
) -> Result<(), RemoteSendError<Infallible>>
where
    A: Actor,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&notified_actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .downcast::<A>()?
    };

    actor_ref
        .weak_signal_mailbox()
        .signal_link_died(dead_actor_id, stop_reason)
        .await?;

    Ok(())
}
