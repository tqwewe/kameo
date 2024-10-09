use core::panic;
use std::{marker::PhantomData, time::Duration};

#[cfg(feature = "remote")]
use crate::remote;

use crate::{
    actor, error,
    mailbox::{bounded::BoundedMailbox, unbounded::UnboundedMailbox, Signal},
    message::Message,
    Actor, Reply,
};

use super::{
    BlockingMessageSend, MaybeRequestTimeout, MessageSend, MessageSendSync, TryBlockingMessageSend,
    TryMessageSend, TryMessageSendSync, WithRequestTimeout, WithoutRequestTimeout,
};

/// A request to send a message to an actor without any reply.
///
/// This can be thought of as "fire and forget".
#[allow(missing_debug_implementations)]
pub struct TellRequest<L, Mb, M, T> {
    location: L,
    timeout: T,
    phantom: PhantomData<(Mb, M)>,
}

/// A request to a local actor.
#[allow(missing_debug_implementations)]
pub struct LocalTellRequest<'a, A, Mb>
where
    A: Actor<Mailbox = Mb>,
{
    mailbox: &'a Mb,
    signal: Signal<A>,
}

/// A request to a remote actor.
#[allow(missing_debug_implementations)]
#[cfg(feature = "remote")]
pub struct RemoteTellRequest<'a, A, M>
where
    A: Actor,
{
    actor_ref: &'a actor::RemoteActorRef<A>,
    msg: &'a M,
}

impl<'a, A, M>
    TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, WithoutRequestTimeout>
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new(actor_ref: &'a actor::ActorRef<A>, msg: M) -> Self
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest {
            location: LocalTellRequest {
                mailbox: actor_ref.mailbox(),
                signal: Signal::Message {
                    message: Box::new(msg),
                    actor_ref: actor_ref.clone(),
                    reply: None,
                    sent_within_actor: actor_ref.is_current(),
                },
            },
            timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

#[cfg(feature = "remote")]
impl<'a, A, M> TellRequest<RemoteTellRequest<'a, A, M>, A::Mailbox, M, WithoutRequestTimeout>
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new_remote(actor_ref: &'a actor::RemoteActorRef<A>, msg: &'a M) -> Self {
        TellRequest {
            location: RemoteTellRequest { actor_ref, msg },
            timeout: WithoutRequestTimeout,
            phantom: PhantomData,
        }
    }
}

impl<L, A, M, T> TellRequest<L, BoundedMailbox<A>, M, T>
where
    A: Actor<Mailbox = BoundedMailbox<A>>,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    #[inline]
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> TellRequest<L, BoundedMailbox<A>, M, WithRequestTimeout> {
        TellRequest {
            location: self.location,
            timeout: WithRequestTimeout(duration),
            phantom: PhantomData,
        }
    }
}

#[cfg(feature = "remote")]
impl<L, Mb, M, T> TellRequest<L, Mb, M, T> {
    #[inline]
    pub(crate) fn into_maybe_timeouts(
        self,
        mailbox_timeout: MaybeRequestTimeout,
    ) -> TellRequest<L, Mb, M, MaybeRequestTimeout> {
        TellRequest {
            location: self.location,
            timeout: mailbox_timeout,
            phantom: PhantomData,
        }
    }
}

/////////////////////////
// === MessageSend === //
/////////////////////////
macro_rules! impl_message_send {
    (local, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> MessageSend
            for TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: Send + 'static,
        {
            type Ok = ();
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            async fn send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
    (remote, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> MessageSend
            for TellRequest<RemoteTellRequest<'a, A, M>, $mailbox<A>, M, $timeout>
        where
            TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >: MessageSend,
            A: Actor<Mailbox = $mailbox<A>> + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
            M: serde::Serialize + Send + Sync,
            <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
        {
            type Ok = ();
            type Error = error::RemoteSendError<<A::Reply as Reply>::Error>;

            #[inline]
            async fn send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                remote_tell($req.location.actor_ref, &$req.location.msg, $($body)*, false).await
            }
        }
    };
}

impl_message_send!(local, BoundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.send(req.location.signal).await?;
    Ok(())
});
impl_message_send!(local, BoundedMailbox, WithRequestTimeout, |req| {
    req.location
        .mailbox
        .0
        .send_timeout(req.location.signal, req.timeout.0)
        .await?;
    Ok(())
});
#[cfg(feature = "remote")]
impl_message_send!(remote, BoundedMailbox, WithoutRequestTimeout, |req| None);
#[cfg(feature = "remote")]
impl_message_send!(remote, BoundedMailbox, WithRequestTimeout, |req| Some(
    req.timeout.0
));

impl_message_send!(local, UnboundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.send(req.location.signal)?;
    Ok(())
});
#[cfg(feature = "remote")]
impl_message_send!(remote, UnboundedMailbox, WithoutRequestTimeout, |req| None);

impl_message_send!(local, BoundedMailbox, MaybeRequestTimeout, |req| {
    match req.timeout {
        MaybeRequestTimeout::NoTimeout => {
            req.location.mailbox.0.send(req.location.signal).await?;
        }
        MaybeRequestTimeout::Timeout(timeout) => {
            req.location
                .mailbox
                .0
                .send_timeout(req.location.signal, timeout)
                .await?;
        }
    }
    Ok(())
});

impl_message_send!(local, UnboundedMailbox, MaybeRequestTimeout, |req| {
    match req.timeout {
        MaybeRequestTimeout::NoTimeout => {
            TellRequest {
                location: req.location,
                timeout: WithoutRequestTimeout,
                phantom: PhantomData,
            }
            .send()
            .await
        }
        MaybeRequestTimeout::Timeout(_) => {
            panic!("mailbox timeout is not available with unbounded mailboxes")
        }
    }
});

/////////////////////////////
// === MessageSendSync === //
/////////////////////////////
macro_rules! impl_message_send_sync {
    (local, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> MessageSendSync
            for TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: 'static,
        {
            type Ok = ();
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            fn send_sync(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
}

impl_message_send_sync!(local, UnboundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.send(req.location.signal)?;
    Ok(())
});

////////////////////////////
// === TryMessageSend === //
////////////////////////////
macro_rules! impl_try_message_send {
    (local, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> TryMessageSend
            for TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: Send + 'static,
        {
            type Ok = ();
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            async fn try_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
    (remote, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> TryMessageSend
            for TellRequest<RemoteTellRequest<'a, A, M>, $mailbox<A>, M, $timeout>
        where
            TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >: TryMessageSend,
            A: Actor<Mailbox = $mailbox<A>> + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
            M: serde::Serialize + Send + Sync,
            <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
        {
            type Ok = ();
            type Error = error::RemoteSendError<<A::Reply as Reply>::Error>;

            #[inline]
            async fn try_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                remote_tell($req.location.actor_ref, &$req.location.msg, $($body)*, true).await
            }
        }
    };
}

impl_try_message_send!(local, BoundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.try_send(req.location.signal)?;
    Ok(())
});
#[cfg(feature = "remote")]
impl_try_message_send!(remote, BoundedMailbox, WithoutRequestTimeout, |req| None);
impl_try_message_send!(local, UnboundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.send(req.location.signal)?;
    Ok(())
});
#[cfg(feature = "remote")]
impl_try_message_send!(remote, UnboundedMailbox, WithoutRequestTimeout, |req| None);

impl_try_message_send!(local, BoundedMailbox, MaybeRequestTimeout, |req| {
    match req.timeout {
        MaybeRequestTimeout::NoTimeout => {
            TellRequest {
                location: req.location,
                timeout: WithoutRequestTimeout,
                phantom: PhantomData,
            }
            .try_send()
            .await
        }
        MaybeRequestTimeout::Timeout(_) => {
            panic!("try_send is not available when a mailbox timeout is set")
        }
    }
});

impl_try_message_send!(local, UnboundedMailbox, MaybeRequestTimeout, |req| {
    match req.timeout {
        MaybeRequestTimeout::NoTimeout => {
            TellRequest {
                location: req.location,
                timeout: WithoutRequestTimeout,
                phantom: PhantomData,
            }
            .try_send()
            .await
        }
        MaybeRequestTimeout::Timeout(_) => {
            panic!("try_send is not available when a mailbox timeout is set")
        }
    }
});

////////////////////////////////
// === TryMessageSendSync === //
////////////////////////////////
macro_rules! impl_try_message_send_sync {
    (local, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> TryMessageSendSync
            for TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: 'static,
        {
            type Ok = ();
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            fn try_send_sync(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
}

impl_try_message_send_sync!(local, BoundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.try_send(req.location.signal)?;
    Ok(())
});

impl_try_message_send_sync!(local, UnboundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.send(req.location.signal)?;
    Ok(())
});

////////////////////////////////
// === BlockingMessageSend === //
////////////////////////////////
macro_rules! impl_blocking_message_send {
    (local, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> BlockingMessageSend
            for TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: 'static,
        {
            type Ok = ();
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            fn blocking_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
}

impl_blocking_message_send!(local, BoundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.blocking_send(req.location.signal)?;
    Ok(())
});

impl_blocking_message_send!(local, UnboundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.send(req.location.signal)?;
    Ok(())
});

////////////////////////////////////
// === TryBlockingMessageSend === //
////////////////////////////////////
macro_rules! impl_try_blocking_message_send {
    (local, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> TryBlockingMessageSend
            for TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >
        where
            A: Actor<Mailbox = $mailbox<A>> + Message<M>,
            M: 'static,
        {
            type Ok = ();
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            fn try_blocking_send(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
}

impl_try_blocking_message_send!(local, BoundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.try_send(req.location.signal)?;
    Ok(())
});

impl_try_blocking_message_send!(local, UnboundedMailbox, WithoutRequestTimeout, |req| {
    req.location.mailbox.0.send(req.location.signal)?;
    Ok(())
});

#[cfg(feature = "remote")]
async fn remote_tell<A, M>(
    actor_ref: &actor::RemoteActorRef<A>,
    msg: &M,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), error::RemoteSendError<<A::Reply as Reply>::Error>>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: serde::Serialize,
    <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
{
    use remote::*;
    use std::borrow::Cow;

    let actor_id = actor_ref.id();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor_ref.send_to_swarm(SwarmCommand::Req {
        peer_id: actor_id
            .peer_id_intern()
            .cloned()
            .unwrap_or_else(|| *ActorSwarm::get().unwrap().local_peer_id_intern()),
        req: SwarmReq::Tell {
            actor_id,
            actor_remote_id: Cow::Borrowed(<A as RemoteActor>::REMOTE_ID),
            message_remote_id: Cow::Borrowed(<A as RemoteMessage<M>>::REMOTE_ID),
            payload: rmp_serde::to_vec_named(msg)
                .map_err(|err| error::RemoteSendError::SerializeMessage(err.to_string()))?,
            mailbox_timeout,
            immediate,
        },
        reply: reply_tx,
    });

    match reply_rx.await.unwrap() {
        SwarmResp::Tell(res) => match res {
            Ok(()) => Ok(()),
            Err(err) => Err(err
                .map_err(|err| match rmp_serde::decode::from_slice(&err) {
                    Ok(err) => error::RemoteSendError::HandlerError(err),
                    Err(err) => error::RemoteSendError::DeserializeHandlerError(err.to_string()),
                })
                .flatten()),
        },
        SwarmResp::OutboundFailure(err) => {
            Err(err.map_err(|_| unreachable!("outbound failure doesn't contain handler errors")))
        }
        _ => panic!("unexpected response"),
    }
}
