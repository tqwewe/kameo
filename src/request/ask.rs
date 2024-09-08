use std::{borrow::Cow, marker::PhantomData, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{sync::oneshot, time::timeout};

use crate::{
    actor::{ActorRef, RemoteActorRef},
    error::{BoxSendError, RemoteSendError, SendError},
    mailbox::{bounded::BoundedMailbox, unbounded::UnboundedMailbox, Signal},
    message::{BoxReply, Message},
    remote::{ActorSwarm, RemoteActor, RemoteMessage, SwarmCommand, SwarmReq, SwarmResp},
    reply::ReplySender,
    Actor, Reply,
};

use super::{
    BlockingMessageSend, ForwardMessageSend, MaybeRequestTimeout, MessageSend,
    TryBlockingMessageSend, TryMessageSend, WithRequestTimeout, WithoutRequestTimeout,
};

/// A request to send a message to an actor, waiting for a reply.
#[allow(missing_debug_implementations)]
pub struct AskRequest<L, Mb, M, Tm, Tr> {
    location: L,
    mailbox_timeout: Tm,
    reply_timeout: Tr,
    phantom: PhantomData<(Mb, M)>,
}

/// A request to a local actor.
#[allow(missing_debug_implementations)]
pub struct LocalAskRequest<A, Mb>
where
    A: Actor<Mailbox = Mb>,
{
    mailbox: Mb,
    signal: Signal<A>,
    rx: oneshot::Receiver<Result<BoxReply, BoxSendError>>,
}

/// A request to a remote actor.
#[allow(missing_debug_implementations)]
pub struct RemoteAskRequest<'a, A, M>
where
    A: Actor,
{
    actor_ref: &'a RemoteActorRef<A>,
    msg: &'a M,
}

impl<A, M>
    AskRequest<
        LocalAskRequest<A, A::Mailbox>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor,
{
    pub(crate) fn new(actor_ref: &ActorRef<A>, msg: M) -> Self
    where
        A: Message<M>,
        M: Send + 'static,
    {
        let (reply, rx) = oneshot::channel();

        AskRequest {
            location: LocalAskRequest {
                mailbox: actor_ref.mailbox().clone(),
                signal: Signal::Message {
                    message: Box::new(msg),
                    actor_ref: actor_ref.clone(),
                    reply: Some(reply),
                    sent_within_actor: actor_ref.is_current(),
                },
                rx,
            },
            mailbox_timeout: WithoutRequestTimeout,
            reply_timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<'a, A, M>
    AskRequest<
        RemoteAskRequest<'a, A, M>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor,
{
    pub(crate) fn new_remote(actor_ref: &'a RemoteActorRef<A>, msg: &'a M) -> Self {
        AskRequest {
            location: RemoteAskRequest { actor_ref, msg },
            mailbox_timeout: WithoutRequestTimeout,
            reply_timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<L, A, M, Tm, Tr> AskRequest<L, BoundedMailbox<A>, M, Tm, Tr>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<L, BoundedMailbox<A>, M, WithRequestTimeout, Tr> {
        AskRequest {
            location: self.location,
            mailbox_timeout: WithRequestTimeout(duration),
            reply_timeout: self.reply_timeout,
            phantom: PhantomData,
        }
    }
}

impl<L, Mb, M, Tm, Tr> AskRequest<L, Mb, M, Tm, Tr> {
    /// Sets the timeout for waiting for a reply from the actor.
    pub fn reply_timeout(self, duration: Duration) -> AskRequest<L, Mb, M, Tm, WithRequestTimeout> {
        AskRequest {
            location: self.location,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

impl<L, Mb, M, Tm, Tr> AskRequest<L, Mb, M, Tm, Tr> {
    pub(crate) fn into_maybe_timeouts(
        self,
        mailbox_timeout: MaybeRequestTimeout,
        reply_timeout: MaybeRequestTimeout,
    ) -> AskRequest<L, Mb, M, MaybeRequestTimeout, MaybeRequestTimeout> {
        AskRequest {
            location: self.location,
            mailbox_timeout,
            reply_timeout,
            phantom: PhantomData,
        }
    }
}

/////////////////////////
// === MessageSend === //
/////////////////////////
macro_rules! impl_message_send {
    (local, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<A, M> MessageSend
            for AskRequest<
                LocalAskRequest<A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: Send + 'static,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = SendError<M, <A::Reply as Reply>::Error>;

            async fn send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
    (remote, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| ($mailbox_timeout_body:expr, $reply_timeout_body:expr)) => {
        impl<'a, A, M> MessageSend
            for AskRequest<
                RemoteAskRequest<'a, A, M>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            AskRequest<
                LocalAskRequest<A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >: MessageSend,
            A: Actor<Mailbox = $mailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
            M: Serialize + Send + Sync,
            <A::Reply as Reply>::Ok: DeserializeOwned,
            <A::Reply as Reply>::Error: DeserializeOwned,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = RemoteSendError<<A::Reply as Reply>::Error>;

            async fn send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                remote_ask(
                    $req.location.actor_ref,
                    &$req.location.msg,
                    $mailbox_timeout_body,
                    $reply_timeout_body,
                    false
                ).await
            }
        }
    };
}

impl_message_send!(
    local,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal).await?;
        match req.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);
impl_message_send!(
    local,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal).await?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);
impl_message_send!(
    local,
    BoundedMailbox,
    WithRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location
            .mailbox
            .0
            .send_timeout(req.location.signal, req.mailbox_timeout.0)
            .await?;
        match req.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);
impl_message_send!(
    local,
    BoundedMailbox,
    WithRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location
            .mailbox
            .0
            .send_timeout(req.location.signal, req.mailbox_timeout.0)
            .await?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

impl_message_send!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match req.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);
impl_message_send!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

impl_message_send!(
    remote,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| (None, None)
);
impl_message_send!(
    remote,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);
impl_message_send!(
    remote,
    BoundedMailbox,
    WithRequestTimeout,
    WithoutRequestTimeout,
    |req| (Some(req.mailbox_timeout.0), None)
);
impl_message_send!(
    remote,
    BoundedMailbox,
    WithRequestTimeout,
    WithRequestTimeout,
    |req| (Some(req.mailbox_timeout.0), Some(req.reply_timeout.0))
);

impl_message_send!(
    remote,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| (None, None)
);
impl_message_send!(
    remote,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);

/////////////////////////
// === MessageSend === //
/////////////////////////
macro_rules! impl_try_message_send {
    (local, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<A, M> TryMessageSend
            for AskRequest<
                LocalAskRequest<A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: Send + 'static,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = SendError<M, <A::Reply as Reply>::Error>;

            async fn try_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
    (remote, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| ($mailbox_timeout_body:expr, $reply_timeout_body:expr)) => {
        impl<'a, A, M> TryMessageSend
            for AskRequest<
                RemoteAskRequest<'a, A, M>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            AskRequest<
                LocalAskRequest<A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >: TryMessageSend,
            A: Actor<Mailbox = $mailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
            M: Serialize + Send + Sync,
            <A::Reply as Reply>::Ok: DeserializeOwned,
            <A::Reply as Reply>::Error: DeserializeOwned,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = RemoteSendError<<A::Reply as Reply>::Error>;

            async fn try_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                remote_ask(
                    $req.location.actor_ref,
                    &$req.location.msg,
                    $mailbox_timeout_body,
                    $reply_timeout_body,
                    true
                ).await
            }
        }
    };
}

impl_try_message_send!(
    local,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.try_send(req.location.signal)?;
        match req.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);
impl_try_message_send!(
    local,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.try_send(req.location.signal)?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

impl_try_message_send!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match req.location.rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);
impl_try_message_send!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

impl_try_message_send!(
    remote,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| (None, None)
);
impl_try_message_send!(
    remote,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);

impl_try_message_send!(
    remote,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| (None, None)
);
impl_try_message_send!(
    remote,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);

/////////////////////////////////
// === BlockingMessageSend === //
/////////////////////////////////
macro_rules! impl_blocking_message_send {
    (local, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<A, M> BlockingMessageSend
            for AskRequest<
                LocalAskRequest<A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: 'static,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = SendError<M, <A::Reply as Reply>::Error>;

            fn blocking_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
}

impl_blocking_message_send!(
    local,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.blocking_send(req.location.signal)?;
        match req.location.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

impl_blocking_message_send!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match req.location.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

/////////////////////////////////
// === BlockingMessageSend === //
/////////////////////////////////
macro_rules! impl_try_blocking_message_send {
    (local, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<A, M> TryBlockingMessageSend
            for AskRequest<
                LocalAskRequest<A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: 'static,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = SendError<M, <A::Reply as Reply>::Error>;

            fn try_blocking_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
}

impl_try_blocking_message_send!(
    local,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.try_send(req.location.signal)?;
        match req.location.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

impl_try_blocking_message_send!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match req.location.rx.blocking_recv()? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }
);

////////////////////////////////
// === ForwardMessageSend === //
////////////////////////////////
macro_rules! impl_forward_message {
    (local, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident, $tx:ident| $($body:tt)*) => {
        impl<A, M> ForwardMessageSend<A::Reply, M>
            for AskRequest<
                LocalAskRequest<A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: Send + 'static,
        {
            async fn forward(self, $tx: ReplySender<<A::Reply as Reply>::Value>)
                -> Result<(), SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>>
            {
                let mut $req = self;
                $($body)*
            }
        }
    };
}
impl_forward_message!(
    local,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req, tx| {
        match &mut req.location.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.box_sender()),
            _ => unreachable!("ask requests only support messages"),
        }

        req.location
            .mailbox
            .0
            .send(req.location.signal)
            .await
            .map_err(|err| match err.0 {
                Signal::Message {
                    message, mut reply, ..
                } => SendError::ActorNotRunning((
                    message.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                    ReplySender::new(reply.take().unwrap()),
                )),
                _ => unreachable!("ask requests only support messages"),
            })
    }
);

impl_forward_message!(
    local,
    BoundedMailbox,
    WithRequestTimeout,
    WithoutRequestTimeout,
    |req, tx| {
        match &mut req.location.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.box_sender()),
            _ => unreachable!("ask requests only support messages"),
        }

        req.location
            .mailbox
            .0
            .send_timeout(req.location.signal, req.mailbox_timeout.0)
            .await?;

        Ok(())
    }
);

impl_forward_message!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req, tx| {
        match &mut req.location.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.box_sender()),
            _ => unreachable!("ask requests only support messages"),
        }

        req.location
            .mailbox
            .0
            .send(req.location.signal)
            .map_err(|err| match err.0 {
                Signal::Message {
                    message, mut reply, ..
                } => SendError::ActorNotRunning((
                    message.as_any().downcast::<M>().ok().map(|v| *v).unwrap(),
                    ReplySender::new(reply.take().unwrap()),
                )),
                _ => unreachable!("ask requests only support messages"),
            })
    }
);

impl<A, M> MessageSend
    for AskRequest<
        LocalAskRequest<A, BoundedMailbox<A>>,
        BoundedMailbox<A>,
        M,
        MaybeRequestTimeout,
        MaybeRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: Send + 'static,
{
    type Ok = <A::Reply as Reply>::Ok;
    type Error = SendError<M, <A::Reply as Reply>::Error>;

    async fn send(self) -> Result<Self::Ok, Self::Error> {
        match (self.mailbox_timeout, self.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (MaybeRequestTimeout::Timeout(mailbox_timeout), MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithRequestTimeout(mailbox_timeout),
                    reply_timeout: WithoutRequestTimeout,
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (
                MaybeRequestTimeout::Timeout(mailbox_timeout),
                MaybeRequestTimeout::Timeout(reply_timeout),
            ) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithRequestTimeout(mailbox_timeout),
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    phantom: PhantomData,
                }
                .send()
                .await
            }
        }
    }
}

impl<A, M> MessageSend
    for AskRequest<
        LocalAskRequest<A, UnboundedMailbox<A>>,
        UnboundedMailbox<A>,
        M,
        MaybeRequestTimeout,
        MaybeRequestTimeout,
    >
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M>,
    M: Send + 'static,
{
    type Ok = <A::Reply as Reply>::Ok;
    type Error = SendError<M, <A::Reply as Reply>::Error>;

    async fn send(self) -> Result<Self::Ok, Self::Error> {
        match (self.mailbox_timeout, self.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (MaybeRequestTimeout::Timeout(_), MaybeRequestTimeout::NoTimeout) => {
                panic!("send is not available with a mailbox timeout on unbounded mailboxes")
            }
            (MaybeRequestTimeout::Timeout(_), MaybeRequestTimeout::Timeout(_)) => {
                panic!("send is not available with a mailbox timeout on unbounded mailboxes")
            }
        }
    }
}

impl<A, M> TryMessageSend
    for AskRequest<
        LocalAskRequest<A, BoundedMailbox<A>>,
        BoundedMailbox<A>,
        M,
        MaybeRequestTimeout,
        MaybeRequestTimeout,
    >
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: Send + 'static,
{
    type Ok = <A::Reply as Reply>::Ok;
    type Error = SendError<M, <A::Reply as Reply>::Error>;

    async fn try_send(self) -> Result<Self::Ok, Self::Error> {
        match (self.mailbox_timeout, self.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    phantom: PhantomData,
                }
                .try_send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    phantom: PhantomData,
                }
                .try_send()
                .await
            }
            (MaybeRequestTimeout::Timeout(_), MaybeRequestTimeout::NoTimeout) => {
                panic!("try_send is not available when a mailbox timeout is set")
            }
            (MaybeRequestTimeout::Timeout(_), MaybeRequestTimeout::Timeout(_)) => {
                panic!("try_send is not available when a mailbox timeout is set")
            }
        }
    }
}

impl<A, M> TryMessageSend
    for AskRequest<
        LocalAskRequest<A, UnboundedMailbox<A>>,
        UnboundedMailbox<A>,
        M,
        MaybeRequestTimeout,
        MaybeRequestTimeout,
    >
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M>,
    M: Send + 'static,
{
    type Ok = <A::Reply as Reply>::Ok;
    type Error = SendError<M, <A::Reply as Reply>::Error>;

    async fn try_send(self) -> Result<Self::Ok, Self::Error> {
        match (self.mailbox_timeout, self.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    phantom: PhantomData,
                }
                .try_send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: self.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    phantom: PhantomData,
                }
                .try_send()
                .await
            }
            (MaybeRequestTimeout::Timeout(_), MaybeRequestTimeout::NoTimeout) => {
                panic!("try_send is not available when a mailbox timeout is set")
            }
            (MaybeRequestTimeout::Timeout(_), MaybeRequestTimeout::Timeout(_)) => {
                panic!("try_send is not available when a mailbox timeout is set")
            }
        }
    }
}

async fn remote_ask<'a, A, M>(
    actor_ref: &'a RemoteActorRef<A>,
    msg: &'a M,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<<A::Reply as Reply>::Ok, RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Serialize,
    <A::Reply as Reply>::Ok: DeserializeOwned,
    <A::Reply as Reply>::Error: DeserializeOwned,
{
    let actor_id = actor_ref.id();
    let (reply_tx, reply_rx) = oneshot::channel();
    actor_ref
        .send_to_swarm(SwarmCommand::Req {
            peer_id: actor_id
                .peer_id()
                .unwrap_or_else(|| *ActorSwarm::get().unwrap().local_peer_id()),
            req: SwarmReq::Ask {
                actor_id,
                actor_remote_id: Cow::Borrowed(<A as RemoteActor>::REMOTE_ID),
                message_remote_id: Cow::Borrowed(<A as RemoteMessage<M>>::REMOTE_ID),
                payload: rmp_serde::to_vec_named(msg)
                    .map_err(|err| RemoteSendError::SerializeMessage(err.to_string()))?,
                mailbox_timeout,
                reply_timeout,
                immediate,
            },
            reply: reply_tx,
        })
        .await;

    match reply_rx.await.unwrap() {
        SwarmResp::Ask(res) => match res {
            Ok(payload) => Ok(rmp_serde::decode::from_slice(&payload)
                .map_err(|err| RemoteSendError::DeserializeMessage(err.to_string()))?),
            Err(err) => Err(err
                .map_err(|err| match rmp_serde::decode::from_slice(&err) {
                    Ok(err) => RemoteSendError::HandlerError(err),
                    Err(err) => RemoteSendError::DeserializeHandlerError(err.to_string()),
                })
                .flatten()),
        },
        SwarmResp::OutboundFailure(err) => {
            Err(err.map_err(|_| unreachable!("outbound failure doesn't contain handler errors")))
        }
        _ => panic!("unexpected response"),
    }
}
