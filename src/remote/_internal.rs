use std::borrow::Cow;
use std::time::Duration;

use futures::future::BoxFuture;
pub use linkme;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::actor::{ActorID, ActorRef, Link};
use crate::error::{ActorStopReason, Infallible, RemoteSendError, SendError};
use crate::message::Message;
use crate::request::{
    AskRequest, LocalAskRequest, LocalTellRequest, MaybeRequestTimeout, MessageSend, TellRequest,
    TryMessageSend,
};
use crate::{Actor, Reply};

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
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>>;

pub type RemoteTryAskFn = fn(
    actor_id: ActorID,
    msg: Vec<u8>,
    reply_timeout: Option<Duration>,
) -> BoxFuture<'static, Result<Vec<u8>, RemoteSendError<Vec<u8>>>>;

pub type RemoteTellFn = fn(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
) -> BoxFuture<'static, Result<(), RemoteSendError<Vec<u8>>>>;

pub type RemoteTryTellFn =
    fn(actor_id: ActorID, msg: Vec<u8>) -> BoxFuture<'static, Result<(), RemoteSendError<Vec<u8>>>>;

pub type RemoteLinkFn = fn(
    actor_id: ActorID,
    sibbling_id: ActorID,
    sibbling_remote_id: Cow<'static, str>,
) -> BoxFuture<'static, Result<(), RemoteSendError<Infallible>>>;

pub type RemoteUnlinkFn = fn(
    actor_id: ActorID,
    sibbling_id: ActorID,
) -> BoxFuture<'static, Result<(), RemoteSendError<Infallible>>>;

pub type RemoteSignalLinkDiedFn = fn(
    dead_actor_id: ActorID,
    notified_actor_id: ActorID,
    stop_reason: ActorStopReason,
)
    -> BoxFuture<'static, Result<(), RemoteSendError<Infallible>>>;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct RemoteMessageRegistrationID<'a> {
    pub actor_remote_id: &'a str,
    pub message_remote_id: &'a str,
}

pub async fn ask<A, M>(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned + Send + 'static,
    <A::Reply as Reply>::Ok: Serialize,
    <A::Reply as Reply>::Error: Serialize,
    for<'a> AskRequest<
        LocalAskRequest<'a, A, A::Mailbox>,
        A::Mailbox,
        M,
        MaybeRequestTimeout,
        MaybeRequestTimeout,
    >: MessageSend<Ok = <A::Reply as Reply>::Ok, Error = SendError<M, <A::Reply as Reply>::Error>>,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .actor_ref
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };
    let msg: M = rmp_serde::decode::from_slice(&msg)
        .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

    let res = actor_ref
        .ask(msg)
        .into_maybe_timeouts(mailbox_timeout.into(), reply_timeout.into())
        .send()
        .await;
    match res {
        Ok(reply) => Ok(rmp_serde::to_vec_named(&reply)
            .map_err(|err| RemoteSendError::SerializeReply(err.to_string()))?),
        Err(err) => Err(RemoteSendError::from(err)
            .map_err(|err| match rmp_serde::to_vec_named(&err) {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
            })
            .flatten()),
    }
}

pub async fn try_ask<A, M>(
    actor_id: ActorID,
    msg: Vec<u8>,
    reply_timeout: Option<Duration>,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned + Send + 'static,
    <A::Reply as Reply>::Ok: Serialize,
    <A::Reply as Reply>::Error: Serialize,
    for<'a> AskRequest<
        LocalAskRequest<'a, A, A::Mailbox>,
        A::Mailbox,
        M,
        MaybeRequestTimeout,
        MaybeRequestTimeout,
    >: TryMessageSend<
        Ok = <A::Reply as Reply>::Ok,
        Error = SendError<M, <A::Reply as Reply>::Error>,
    >,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .actor_ref
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };
    let msg: M = rmp_serde::decode::from_slice(&msg)
        .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

    let res = actor_ref
        .ask(msg)
        .into_maybe_timeouts(None.into(), reply_timeout.into())
        .try_send()
        .await;
    match res {
        Ok(reply) => Ok(rmp_serde::to_vec_named(&reply)
            .map_err(|err| RemoteSendError::SerializeReply(err.to_string()))?),
        Err(err) => Err(RemoteSendError::from(err)
            .map_err(|err| match rmp_serde::to_vec_named(&err) {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
            })
            .flatten()),
    }
}

pub async fn tell<A, M>(
    actor_id: ActorID,
    msg: Vec<u8>,
    mailbox_timeout: Option<Duration>,
) -> Result<(), RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned + Send + 'static,
    <A::Reply as Reply>::Error: Serialize,
    for<'a> TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, MaybeRequestTimeout>:
        MessageSend<Ok = (), Error = SendError<M, <A::Reply as Reply>::Error>>,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .actor_ref
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };
    let msg: M = rmp_serde::decode::from_slice(&msg)
        .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

    let res = actor_ref
        .tell(msg)
        .into_maybe_timeouts(mailbox_timeout.into())
        .send()
        .await;
    match res {
        Ok(()) => Ok(()),
        Err(err) => Err(RemoteSendError::from(err)
            .map_err(|err| match rmp_serde::to_vec_named(&err) {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
            })
            .flatten()),
    }
}

pub async fn try_tell<A, M>(actor_id: ActorID, msg: Vec<u8>) -> Result<(), RemoteSendError<Vec<u8>>>
where
    A: Actor + Message<M>,
    M: DeserializeOwned + Send + 'static,
    <A::Reply as Reply>::Error: Serialize,
    for<'a> TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, MaybeRequestTimeout>:
        TryMessageSend<Ok = (), Error = SendError<M, <A::Reply as Reply>::Error>>,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .actor_ref
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };
    let msg: M = rmp_serde::decode::from_slice(&msg)
        .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?;

    let res = actor_ref
        .tell(msg)
        .into_maybe_timeouts(None.into())
        .try_send()
        .await;
    match res {
        Ok(()) => Ok(()),
        Err(err) => Err(RemoteSendError::from(err)
            .map_err(|err| match rmp_serde::to_vec_named(&err) {
                Ok(payload) => RemoteSendError::HandlerError(payload),
                Err(err) => RemoteSendError::SerializeHandlerError(err.to_string()),
            })
            .flatten()),
    }
}

pub async fn link<A>(
    actor_id: ActorID,
    sibbling_id: ActorID,
    sibbling_remote_id: Cow<'static, str>,
) -> Result<(), RemoteSendError<Infallible>>
where
    A: Actor,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .actor_ref
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };

    actor_ref
        .links
        .lock()
        .await
        .insert(sibbling_id, Link::Remote(sibbling_remote_id));

    Ok(())
}

pub async fn unlink<A>(
    actor_id: ActorID,
    sibbling_id: ActorID,
) -> Result<(), RemoteSendError<Infallible>>
where
    A: Actor,
{
    let actor_ref = {
        let remote_actors = REMOTE_REGISTRY.lock().await;
        remote_actors
            .get(&actor_id)
            .ok_or(RemoteSendError::ActorNotRunning)?
            .actor_ref
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };

    actor_ref.links.lock().await.remove(&sibbling_id);

    Ok(())
}

pub async fn signal_link_died<A>(
    dead_actor_id: ActorID,
    notified_actor_id: ActorID,
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
            .actor_ref
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RemoteSendError::BadActorType)?
            .clone()
    };

    actor_ref
        .weak_signal_mailbox()
        .signal_link_died(dead_actor_id, stop_reason)
        .await?;

    Ok(())
}
