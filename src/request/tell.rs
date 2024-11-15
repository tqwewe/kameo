use core::panic;
use std::{future::IntoFuture, marker::PhantomData, time::Duration};

use futures::{future::BoxFuture, FutureExt};

#[cfg(feature = "remote")]
use crate::remote;

use crate::{
    actor, error,
    mailbox::{bounded::BoundedMailbox, unbounded::UnboundedMailbox, Mailbox, Signal},
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

impl<'a, A, M, T> IntoFuture for TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, T>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    T: 'static,
    TellRequest<LocalTellRequest<'a, A, A::Mailbox>, A::Mailbox, M, T>:
        MessageSend<Ok = (), Error = error::SendError<M, <A::Reply as Reply>::Error>>,
{
    type Output = Result<(), error::SendError<M, <A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        MessageSend::send(self).boxed()
    }
}

#[cfg(feature = "remote")]
impl<'a, A, M, T> IntoFuture for TellRequest<RemoteTellRequest<'a, A, M>, A::Mailbox, M, T>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    T: 'static,
    TellRequest<RemoteTellRequest<'a, A, M>, A::Mailbox, M, T>:
        MessageSend<Ok = (), Error = error::RemoteSendError<<A::Reply as Reply>::Error>>,
{
    type Output = Result<(), error::RemoteSendError<<A::Reply as Reply>::Error>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        MessageSend::send(self).boxed()
    }
}

macro_rules! impl_message_trait {
    (local, $($async:ident)? => $trait:ident :: $method:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> $trait
            for TellRequest<
                LocalTellRequest<'a, A, A::Mailbox>,
                A::Mailbox,
                M,
                $timeout,
            >
        where
            A: Actor + Message<M>,
            M: Send + 'static,
        {
            type Ok = ();
            type Error = error::SendError<M, <A::Reply as Reply>::Error>;

            #[inline]
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
    (local, $($async:ident)? => $trait:ident :: $method:ident, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> $trait
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
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                $($body)*
            }
        }
    };
    (remote, $($async:ident)? => $trait:ident :: $method:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> $trait
            for TellRequest<RemoteTellRequest<'a, A, M>, A::Mailbox, M, $timeout>
        where
            TellRequest<
                LocalTellRequest<'a, A, A::Mailbox>,
                A::Mailbox,
                M,
                $timeout,
            >: $trait,
            A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
            M: serde::Serialize + Send + Sync + 'static,
            <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
        {
            type Ok = ();
            type Error = error::RemoteSendError<<A::Reply as Reply>::Error>;

            #[inline]
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                remote_tell($req.location.actor_ref, &$req.location.msg, $($body)*, false).await
            }
        }
    };
    (remote, $($async:ident)? => $trait:ident :: $method:ident, $mailbox:ident, $timeout:ident, |$req:ident| $($body:tt)*) => {
        impl<'a, A, M> $trait
            for TellRequest<RemoteTellRequest<'a, A, M>, $mailbox<A>, M, $timeout>
        where
            TellRequest<
                LocalTellRequest<'a, A, $mailbox<A>>,
                $mailbox<A>,
                M,
                $timeout,
            >: $trait,
            A: Actor<Mailbox = $mailbox<A>> + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
            M: serde::Serialize + Send + Sync + 'static,
            <A::Reply as Reply>::Error: for<'de> serde::Deserialize<'de>,
        {
            type Ok = ();
            type Error = error::RemoteSendError<<A::Reply as Reply>::Error>;

            #[inline]
            $($async)? fn $method(self) -> Result<Self::Ok, Self::Error> {
                let $req = self;
                remote_tell($req.location.actor_ref, &$req.location.msg, $($body)*, false).await
            }
        }
    };
}

/////////////////////////
// === MessageSend === //
/////////////////////////
impl_message_trait!(local, async => MessageSend::send, WithoutRequestTimeout, |req| {
    Ok(req.location
        .mailbox
        .send(req.location.signal)
        .await?)
});
impl_message_trait!(
    local,
    async => MessageSend::send,
    BoundedMailbox,
    WithRequestTimeout,
    |req| {
        req.location
            .mailbox
            .0
            .send_timeout(req.location.signal, req.timeout.0)
            .await?;
        Ok(())
    }
);
#[cfg(feature = "remote")]
impl_message_trait!(remote, async => MessageSend::send, WithoutRequestTimeout, |req| None);
#[cfg(feature = "remote")]
impl_message_trait!(
    remote,
    async => MessageSend::send,
    BoundedMailbox,
    WithRequestTimeout,
    |req| Some(req.timeout.0)
);

impl_message_trait!(
    local,
    async => MessageSend::send,
    BoundedMailbox,
    MaybeRequestTimeout,
    |req| {
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
    }
);

impl_message_trait!(
    local,
    async => MessageSend::send,
    UnboundedMailbox,
    MaybeRequestTimeout,
    |req| {
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
    }
);

/////////////////////////////
// === MessageSendSync === //
/////////////////////////////
impl_message_trait!(
    local,
    => MessageSendSync::send_sync,
    UnboundedMailbox,
    WithoutRequestTimeout,
    |req| {
        req.location.mailbox.0.send(req.location.signal)?;
        Ok(())
    }
);

////////////////////////////
// === TryMessageSend === //
////////////////////////////
impl_message_trait!(local, async => TryMessageSend::try_send, WithoutRequestTimeout, |req| {
    Ok(req.location
        .mailbox
        .try_send(req.location.signal)?)
});
#[cfg(feature = "remote")]
impl_message_trait!(remote, async => TryMessageSend::try_send, WithoutRequestTimeout, |req| None);

impl_message_trait!(local, async => TryMessageSend::try_send, BoundedMailbox, MaybeRequestTimeout, |req| {
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

impl_message_trait!(local, async => TryMessageSend::try_send, UnboundedMailbox, MaybeRequestTimeout, |req| {
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
impl_message_trait!(local, => TryMessageSendSync::try_send_sync, WithoutRequestTimeout, |req| {
    Ok(req.location.mailbox.try_send(req.location.signal)?)
});

////////////////////////////////
// === BlockingMessageSend === //
////////////////////////////////
impl_message_trait!(local, => BlockingMessageSend::blocking_send, WithoutRequestTimeout, |req| {
    Ok(req.location.mailbox.blocking_send(req.location.signal)?)
});

////////////////////////////////////
// === TryBlockingMessageSend === //
////////////////////////////////////
impl_message_trait!(local, => TryBlockingMessageSend::try_blocking_send, WithoutRequestTimeout, |req| {
    Ok(req.location.mailbox.try_send(req.location.signal)?)
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
    M: serde::Serialize + Send + 'static,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        error::SendError,
        mailbox::{
            bounded::{BoundedMailbox, BoundedMailboxReceiver},
            unbounded::UnboundedMailbox,
        },
        message::{Context, Message},
        request::{
            BlockingMessageSend, MessageSend, MessageSendSync, TryBlockingMessageSend,
            TryMessageSend, TryMessageSendSync,
        },
        spawn, Actor,
    };

    #[tokio::test]
    async fn bounded_tell_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: Context<'_, Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn(MyActor);

        actor_ref.tell(Msg).await?; // Should be a regular MessageSend request
        actor_ref.tell(Msg).send().await?;
        actor_ref.tell(Msg).try_send().await?;
        actor_ref.tell(Msg).try_send_sync()?;
        tokio::task::spawn_blocking({
            let actor_ref = actor_ref.clone();
            move || actor_ref.tell(Msg).blocking_send()
        })
        .await??;
        actor_ref.tell(Msg).try_blocking_send()?;

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_tell_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = UnboundedMailbox<Self>;
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: Context<'_, Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn(MyActor);

        actor_ref.tell(Msg).await?; // Should be a regular MessageSend request
        actor_ref.tell(Msg).send().await?;
        actor_ref.tell(Msg).send_sync()?;
        actor_ref.tell(Msg).try_send().await?;
        actor_ref.tell(Msg).try_send_sync()?;
        actor_ref.tell(Msg).blocking_send()?;
        actor_ref.tell(Msg).try_blocking_send()?;

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: Context<'_, Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn(MyActor);
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_stop().await;

        assert_eq!(
            actor_ref.tell(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send_sync(),
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.tell(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_blocking_send(),
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_tell_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = UnboundedMailbox<Self>;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: Context<'_, Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn(MyActor);
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_stop().await;

        assert_eq!(
            actor_ref.tell(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).send_sync(),
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send_sync(),
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            tokio::task::spawn_blocking({
                let actor_ref = actor_ref.clone();
                move || actor_ref.tell(Msg).blocking_send()
            })
            .await?,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_blocking_send(),
            Err(SendError::ActorNotRunning(Msg))
        );

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_mailbox_full() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;

            fn new_mailbox() -> (BoundedMailbox<Self>, BoundedMailboxReceiver<Self>) {
                BoundedMailbox::new(1)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: Context<'_, Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }

        let actor_ref = spawn(MyActor);
        assert_eq!(actor_ref.tell(Msg).try_send().await, Ok(()));
        assert_eq!(
            actor_ref.tell(Msg).try_send().await,
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        let actor_ref = spawn(MyActor);
        assert_eq!(actor_ref.tell(Msg).try_send_sync(), Ok(()));
        assert_eq!(
            actor_ref.tell(Msg).try_send_sync(),
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        let actor_ref = spawn(MyActor);
        assert_eq!(actor_ref.tell(Msg).try_blocking_send(), Ok(()));
        assert_eq!(
            actor_ref.tell(Msg).try_blocking_send(),
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_mailbox_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Mailbox = BoundedMailbox<Self>;

            fn new_mailbox() -> (BoundedMailbox<Self>, BoundedMailboxReceiver<Self>) {
                BoundedMailbox::new(1)
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: Context<'_, Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
            }
        }

        let actor_ref = spawn(MyActor);
        // Mailbox empty, will succeed
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(10))
                .send()
                .await,
            Ok(())
        );
        // Mailbox is empty, this will make there be one item in the mailbox
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(10))
                .send()
                .await,
            Ok(())
        );
        // Finally, this one will fail because there's one item in the mailbox already.
        assert_eq!(
            actor_ref
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .send()
                .await,
            Err(SendError::Timeout(Some(Sleep(Duration::from_millis(100)))))
        );
        actor_ref.kill();

        Ok(())
    }
}
