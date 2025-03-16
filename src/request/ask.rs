use futures::{future::BoxFuture, FutureExt};
use std::{future::IntoFuture, marker::PhantomData, time::Duration};
use tokio::{sync::oneshot, time::timeout};

#[cfg(feature = "remote")]
use crate::remote::{RemoteActor, RemoteMessage, SwarmCommand, SwarmResponse};

use crate::{
    actor,
    error::{self, SendError},
    mailbox::{bounded::BoundedMailbox, unbounded::UnboundedMailbox, Mailbox, Signal},
    message::{BoxReply, Message},
    reply::ReplySender,
    Actor, Reply,
};

use super::{
    BlockingMessageSend, ForwardMessageSend, ForwardMessageSendSync, MaybeRequestTimeout,
    MessageSend, TryBlockingMessageSend, TryMessageSend, WithRequestTimeout, WithoutRequestTimeout,
};

/// A request to send a message to an actor, waiting for a reply.
#[allow(missing_debug_implementations)]
pub struct AskRequest<L, Mb, M, Tm, Tr> {
    location: L,
    mailbox_timeout: Tm,
    reply_timeout: Tr,
    #[cfg(debug_assertions)]
    called_at: &'static std::panic::Location<'static>,
    phantom: PhantomData<(Mb, M)>,
}

/// A request to a local actor.
#[allow(missing_debug_implementations)]
pub struct LocalAskRequest<'a, A, Mb>
where
    A: Actor<Mailbox = Mb>,
{
    mailbox: &'a Mb,
    signal: Signal<A>,
    rx: oneshot::Receiver<Result<BoxReply, error::BoxSendError>>,
}

/// A request to a remote actor.
#[allow(missing_debug_implementations)]
#[cfg(feature = "remote")]
pub struct RemoteAskRequest<'a, A, M>
where
    A: Actor,
{
    actor_ref: &'a actor::RemoteActorRef<A>,
    msg: &'a M,
}

impl<'a, A, M>
    AskRequest<
        LocalAskRequest<'a, A, A::Mailbox>,
        A::Mailbox,
        M,
        WithoutRequestTimeout,
        WithoutRequestTimeout,
    >
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new(
        actor_ref: &'a actor::ActorRef<A>,
        msg: M,
        #[cfg(debug_assertions)] called_at: &'static std::panic::Location<'static>,
    ) -> Self
    where
        A: Message<M>,
        M: Send + 'static,
    {
        let (reply, rx) = oneshot::channel();

        AskRequest {
            location: LocalAskRequest {
                mailbox: actor_ref.mailbox(),
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
            #[cfg(debug_assertions)]
            called_at,
            phantom: PhantomData,
        }
    }
}

impl<A, M, Tm, Tr> AskRequest<LocalAskRequest<'_, A, A::Mailbox>, A::Mailbox, M, Tm, Tr>
where
    A: Actor,
{
    #[cfg(all(debug_assertions, feature = "tracing"))]
    fn warn_deadlock(&self, msg: &'static str) {
        use tracing::warn;

        if let Signal::Message { actor_ref, .. } = &self.location.signal {
            if actor_ref.is_current() {
                warn!("At {}, {msg}", self.called_at);
            }
        }
    }
    #[cfg(not(all(debug_assertions, feature = "tracing")))]
    fn warn_deadlock(&self, _msg: &'static str) {}
}

#[cfg(feature = "remote")]
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
    #[inline]
    pub(crate) fn new_remote(
        actor_ref: &'a actor::RemoteActorRef<A>,
        msg: &'a M,
        #[cfg(debug_assertions)] called_at: &'static std::panic::Location<'static>,
    ) -> Self {
        AskRequest {
            location: RemoteAskRequest { actor_ref, msg },
            mailbox_timeout: WithoutRequestTimeout,
            reply_timeout: WithoutRequestTimeout,
            #[cfg(debug_assertions)]
            called_at,
            phantom: PhantomData,
        }
    }
}

impl<L, A, M, Tm, Tr> AskRequest<L, BoundedMailbox<A>, M, Tm, Tr>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    #[inline]
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> AskRequest<L, BoundedMailbox<A>, M, WithRequestTimeout, Tr> {
        AskRequest {
            location: self.location,
            mailbox_timeout: WithRequestTimeout(duration),
            reply_timeout: self.reply_timeout,
            #[cfg(debug_assertions)]
            called_at: self.called_at,
            phantom: PhantomData,
        }
    }
}

impl<L, Mb, M, Tm, Tr> AskRequest<L, Mb, M, Tm, Tr> {
    /// Sets the timeout for waiting for a reply from the actor.
    #[inline]
    pub fn reply_timeout(self, duration: Duration) -> AskRequest<L, Mb, M, Tm, WithRequestTimeout> {
        AskRequest {
            location: self.location,
            mailbox_timeout: self.mailbox_timeout,
            reply_timeout: WithRequestTimeout(duration),
            #[cfg(debug_assertions)]
            called_at: self.called_at,
            phantom: PhantomData,
        }
    }
}

#[cfg(feature = "remote")]
impl<L, Mb, M, Tm, Tr> AskRequest<L, Mb, M, Tm, Tr> {
    #[inline]
    pub(crate) fn into_maybe_timeouts(
        self,
        mailbox_timeout: MaybeRequestTimeout,
        reply_timeout: MaybeRequestTimeout,
    ) -> AskRequest<L, Mb, M, MaybeRequestTimeout, MaybeRequestTimeout> {
        AskRequest {
            location: self.location,
            mailbox_timeout,
            reply_timeout,
            #[cfg(debug_assertions)]
            called_at: self.called_at,
            phantom: PhantomData,
        }
    }
}

impl<'a, A, M, Tm, Tr> IntoFuture
    for AskRequest<LocalAskRequest<'a, A, A::Mailbox>, A::Mailbox, M, Tm, Tr>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: 'static,
    Tr: 'static,
    AskRequest<LocalAskRequest<'a, A, A::Mailbox>, A::Mailbox, M, Tm, Tr>: MessageSend<
        Ok = <A::Reply as Reply>::Ok,
        Error = error::SendError<M, <A::Reply as Reply>::Error>,
    >,
{
    type Output = Result<<A::Reply as Reply>::Ok, error::SendError<M, <A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        MessageSend::send(self).boxed()
    }
}

#[cfg(feature = "remote")]
impl<'a, A, M, Tm, Tr> IntoFuture for AskRequest<RemoteAskRequest<'a, A, M>, A::Mailbox, M, Tm, Tr>
where
    A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
    M: Send + 'static,
    Tm: 'static,
    Tr: 'static,
    AskRequest<RemoteAskRequest<'a, A, M>, A::Mailbox, M, Tm, Tr>: MessageSend<
        Ok = <A::Reply as Reply>::Ok,
        Error = error::RemoteSendError<<A::Reply as Reply>::Error>,
    >,
{
    type Output =
        Result<<A::Reply as Reply>::Ok, error::RemoteSendError<<A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        MessageSend::send(self).boxed()
    }
}

macro_rules! impl_message_trait {
    (local, $($async:ident)? => $trait:ident :: $method:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> $trait
            for AskRequest<
                LocalAskRequest<'a, A, A::Mailbox>,
                A::Mailbox,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor + Message<M>,
            M: Send + 'static,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
                self.warn_deadlock("An actor is sending an `ask` request to itself, which will likely lead to a deadlock. To avoid this, use a `tell` request instead.");

                let $req = self;
                $($body)*
            }
        }
    };
    (local, $($async:ident)? => $trait:ident :: $method:ident, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> $trait
            for AskRequest<
                LocalAskRequest<'a, A, $mailbox<A>>,
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
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
    (remote, $($async:ident)? => $trait:ident :: $method:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| ($mailbox_timeout_body:expr, $reply_timeout_body:expr)) => {
        impl<'a, A, M> $trait
            for AskRequest<RemoteAskRequest<'a, A, M>, A::Mailbox, M, $mailbox_timeout, $reply_timeout>
        where
            AskRequest<
                LocalAskRequest<'a, A, A::Mailbox>,
                A::Mailbox,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >: $trait,
            A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
            M: serde::Serialize + Send + Sync + 'static,
            <A::Reply as Reply>::Ok: for<'de> serde::Deserialize<'de>,
            <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = error::RemoteSendError<<A::Reply as Reply>::Error>;

            #[inline]
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
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
    (remote, $($async:ident)? => $trait:ident :: $method:ident, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident| ($mailbox_timeout_body:expr, $reply_timeout_body:expr)) => {
        impl<'a, A, M> $trait
            for AskRequest<RemoteAskRequest<'a, A, M>, $mailbox<A>, M, $mailbox_timeout, $reply_timeout>
        where
            AskRequest<
                LocalAskRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >: $trait,
            A: Actor<Mailbox = $mailbox<A>> + Message<M> + RemoteActor + RemoteMessage<M>,
            M: serde::Serialize + Send + Sync + 'static,
            <A::Reply as Reply>::Ok: for<'de> serde::Deserialize<'de>,
            <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
        {
            type Ok = <A::Reply as Reply>::Ok;
            type Error = error::RemoteSendError<<A::Reply as Reply>::Error>;

            #[inline]
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
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

/////////////////////////
// === MessageSend === //
/////////////////////////
impl_message_trait!(
    local,
    async => MessageSend::send,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.send(req.location.signal).await?;
        match req.location.rx.await? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);
impl_message_trait!(
    local,
    async => MessageSend::send,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal).await?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);
impl_message_trait!(
    local,
    async => MessageSend::send,
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
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);
impl_message_trait!(
    local,
    async => MessageSend::send,
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
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);

impl_message_trait!(
    local,
    async => MessageSend::send,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);

#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => MessageSend::send,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| (None, None)
);
#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => MessageSend::send,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);
#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => MessageSend::send,
    BoundedMailbox,
    WithRequestTimeout,
    WithoutRequestTimeout,
    |req| (Some(req.mailbox_timeout.0), None)
);
#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => MessageSend::send,
    BoundedMailbox,
    WithRequestTimeout,
    WithRequestTimeout,
    |req| (Some(req.mailbox_timeout.0), Some(req.reply_timeout.0))
);

#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => MessageSend::send,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);

impl_message_trait!(
    local,
    async => MessageSend::send,
    BoundedMailbox,
    MaybeRequestTimeout,
    MaybeRequestTimeout,
    |req| {
        match (req.mailbox_timeout, req.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (MaybeRequestTimeout::Timeout(mailbox_timeout), MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithRequestTimeout(mailbox_timeout),
                    reply_timeout: WithoutRequestTimeout,
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
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
                    location: req.location,
                    mailbox_timeout: WithRequestTimeout(mailbox_timeout),
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
                    phantom: PhantomData,
                }
                .send()
                .await
            }
        }
    }
);

impl_message_trait!(
    local,
    async => MessageSend::send,
    UnboundedMailbox,
    MaybeRequestTimeout,
    MaybeRequestTimeout,
    |req| {
        match (req.mailbox_timeout, req.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
                    phantom: PhantomData,
                }
                .send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
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
);

////////////////////////////
// === TryMessageSend === //
////////////////////////////
impl_message_trait!(
    local,
    async => TryMessageSend::try_send,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.try_send(req.location.signal)?;
        match req.location.rx.await? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);
impl_message_trait!(
    local,
    async => TryMessageSend::try_send,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.try_send(req.location.signal)?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);

impl_message_trait!(
    local,
    async => TryMessageSend::try_send,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        match timeout(req.reply_timeout.0, req.location.rx).await?? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);

#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => TryMessageSend::try_send,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| (None, None)
);
#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => TryMessageSend::try_send,
    BoundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);

#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => TryMessageSend::try_send,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithRequestTimeout,
    |req| (None, Some(req.reply_timeout.0))
);

impl_message_trait!(
    local,
    async => TryMessageSend::try_send,
    BoundedMailbox,
    MaybeRequestTimeout,
    MaybeRequestTimeout,
    |req| {
        match (req.mailbox_timeout, req.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
                    phantom: PhantomData,
                }
                .try_send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
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
);

impl_message_trait!(
    local,
    async => TryMessageSend::try_send,
    UnboundedMailbox,
    MaybeRequestTimeout,
    MaybeRequestTimeout,
    |req| {
        match (req.mailbox_timeout, req.reply_timeout) {
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::NoTimeout) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithoutRequestTimeout,
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
                    phantom: PhantomData,
                }
                .try_send()
                .await
            }
            (MaybeRequestTimeout::NoTimeout, MaybeRequestTimeout::Timeout(reply_timeout)) => {
                AskRequest {
                    location: req.location,
                    mailbox_timeout: WithoutRequestTimeout,
                    reply_timeout: WithRequestTimeout(reply_timeout),
                    #[cfg(debug_assertions)]
                    called_at: req.called_at,
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
);

/////////////////////////////////
// === BlockingMessageSend === //
/////////////////////////////////
impl_message_trait!(
    local,
    => BlockingMessageSend::blocking_send,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.blocking_send(req.location.signal)?;
        match req.location.rx.blocking_recv()? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);

////////////////////////////////////
// === TryBlockingMessageSend === //
////////////////////////////////////
impl_message_trait!(
    local,
    => TryBlockingMessageSend::try_blocking_send,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.try_send(req.location.signal)?;
        match req.location.rx.blocking_recv()? {
            Ok(val) => Ok(<A::Reply as Reply>::downcast_ok(val)),
            Err(err) => Err(<A::Reply as Reply>::downcast_err(err)),
        }
    }
);

////////////////////////////////
// === ForwardMessageSend === //
////////////////////////////////
macro_rules! impl_forward_message {
    (local, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident, $tx:ident| $($body:tt)*) => {
        impl<'a, A, M> ForwardMessageSend<A::Reply, M>
            for AskRequest<
                LocalAskRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: Send + 'static,
        {
            #[inline]
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
            Signal::Message { reply, .. } => *reply = Some(tx.boxed()),
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
            Signal::Message { reply, .. } => *reply = Some(tx.boxed()),
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
            Signal::Message { reply, .. } => *reply = Some(tx.boxed()),
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

////////////////////////////////////
// === ForwardMessageSendSync === //
////////////////////////////////////
macro_rules! impl_forward_message_sync {
    (local, $mailbox:ident, $mailbox_timeout:ident, $reply_timeout:ident, |$req:ident, $tx:ident| $($body:tt)*) => {
        impl<'a, A, M> ForwardMessageSendSync<A::Reply, M>
            for AskRequest<
                LocalAskRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $mailbox_timeout,
                $reply_timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: Send + 'static,
        {
            #[inline]
            fn forward_sync(self, $tx: ReplySender<<A::Reply as Reply>::Value>)
                -> Result<(), SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>>
            {
                let mut $req = self;
                $($body)*
            }
        }
    };
}

impl_forward_message_sync!(
    local,
    UnboundedMailbox,
    WithoutRequestTimeout,
    WithoutRequestTimeout,
    |req, tx| {
        match &mut req.location.signal {
            Signal::Message { reply, .. } => *reply = Some(tx.boxed()),
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

#[cfg(feature = "remote")]
async fn remote_ask<'a, A, M>(
    actor_ref: &'a actor::RemoteActorRef<A>,
    msg: &'a M,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<<A::Reply as Reply>::Ok, error::RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M> + RemoteActor + RemoteMessage<M>,
    M: serde::Serialize + Send + 'static,
    <A::Reply as Reply>::Ok: for<'de> serde::Deserialize<'de>,
    <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
{
    use std::borrow::Cow;

    let actor_id = actor_ref.id();
    let (reply_tx, reply_rx) = oneshot::channel();
    actor_ref.send_to_swarm(SwarmCommand::Ask {
        peer_id: *actor_id
            .peer_id()
            .expect("actor swarm should be bootstrapped"),
        actor_id,
        actor_remote_id: Cow::Borrowed(<A as RemoteActor>::REMOTE_ID),
        message_remote_id: Cow::Borrowed(<A as RemoteMessage<M>>::REMOTE_ID),
        payload: rmp_serde::to_vec_named(msg)
            .map_err(|err| error::RemoteSendError::SerializeMessage(err.to_string()))?,
        mailbox_timeout,
        reply_timeout,
        immediate,
        reply: reply_tx,
    });

    match reply_rx.await.unwrap() {
        SwarmResponse::Ask(res) => match res {
            Ok(payload) => Ok(rmp_serde::decode::from_slice(&payload)
                .map_err(|err| error::RemoteSendError::DeserializeMessage(err.to_string()))?),
            Err(err) => Err(err
                .map_err(|err| match rmp_serde::decode::from_slice(&err) {
                    Ok(err) => error::RemoteSendError::HandlerError(err),
                    Err(err) => error::RemoteSendError::DeserializeHandlerError(err.to_string()),
                })
                .flatten()),
        },
        SwarmResponse::OutboundFailure(err) => {
            Err(err.map_err(|_| unreachable!("outbound failure doesn't contain handler errors")))
        }
        _ => panic!("unexpected response"),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        error::{Infallible, SendError},
        mailbox::{
            bounded::{BoundedMailbox, BoundedMailboxReceiver},
            unbounded::UnboundedMailbox,
        },
        message::{Context, Message},
        request::{BlockingMessageSend, MessageSend, TryBlockingMessageSend, TryMessageSend},
        spawn, Actor,
    };

    #[tokio::test]
    async fn bounded_ask_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;
            type Error = Infallible;
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = spawn(MyActor);

        assert!(actor_ref.ask(Msg).await?); // Should be a regular MessageSend request
        assert!(actor_ref.ask(Msg).send().await?);
        assert!(actor_ref.ask(Msg).try_send().await?);
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await??
        );
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).try_blocking_send()
            })
            .await??
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = UnboundedMailbox<Self>;
            type Error = Infallible;
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = spawn(MyActor);

        assert!(actor_ref.ask(Msg).await?); // Should be a regular MessageSend request
        assert!(actor_ref.ask(Msg).send().await?);
        assert!(actor_ref.ask(Msg).try_send().await?);
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await??
        );
        assert!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).try_blocking_send()
            })
            .await??
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = spawn(MyActor);
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_stop().await;

        assert_eq!(
            actor_ref.ask(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_blocking_send(),
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = UnboundedMailbox<Self>;
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                true
            }
        }

        let actor_ref = spawn(MyActor);
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_stop().await;

        assert_eq!(
            actor_ref.ask(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.ask(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.ask(Msg).try_blocking_send(),
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_mailbox_full() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;
            type Error = Infallible;

            fn new_mailbox() -> (BoundedMailbox<Self>, BoundedMailboxReceiver<Self>) {
                BoundedMailbox::new(1)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(Duration::from_secs(10)).await;
                true
            }
        }

        let actor_ref = spawn(MyActor);
        assert_eq!(actor_ref.tell(Msg).try_send().await, Ok(()));
        assert_eq!(
            actor_ref.ask(Msg).try_send().await,
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        let actor_ref = spawn(MyActor);
        assert_eq!(actor_ref.tell(Msg).try_blocking_send(), Ok(()));
        assert_eq!(
            actor_ref.ask(Msg).try_blocking_send(),
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_mailbox_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;
            type Error = Infallible;

            fn new_mailbox() -> (BoundedMailbox<Self>, BoundedMailboxReceiver<Self>) {
                BoundedMailbox::new(1)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = spawn(MyActor);
        // Mailbox empty, this will succeed
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(10))
                .send()
                .await,
            Ok(())
        );
        // Mailbox still empty, this will add one message to it
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(10))
                .send()
                .await,
            Ok(())
        );
        // Mailbox has one item, this will fail
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .send()
                .await,
            Err(SendError::Timeout(Some(Sleep(Duration::from_millis(100)))))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_ask_requests_reply_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = spawn(MyActor);
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(120))
                .send()
                .await,
            Ok(true)
        );
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(90))
                .send()
                .await,
            Err(SendError::Timeout(None))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_ask_requests_reply_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = UnboundedMailbox<Self>;
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = bool;

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
                true
            }
        }

        let actor_ref = spawn(MyActor);
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(120))
                .send()
                .await,
            Ok(true)
        );
        assert_eq!(
            actor_ref
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(90))
                .send()
                .await,
            Err(SendError::Timeout(None))
        );
        actor_ref.kill();

        Ok(())
    }
}
