use std::{future::IntoFuture, time::Duration};

use futures::{future::BoxFuture, FutureExt};

#[cfg(feature = "remote")]
use crate::{actor, error, remote};

use crate::{
    actor::{ActorRef, Recipient},
    error::SendError,
    mailbox::{MailboxSender, Signal},
    message::Message,
    Actor,
};

use super::{WithRequestTimeout, WithoutRequestTimeout};

/// A request to send a message to an actor without any reply.
///
/// This can be thought of as "fire and forget".
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    actor_ref: &'a ActorRef<A>,
    msg: M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, A, M, Tm> TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    pub(crate) fn new(
        actor_ref: &'a ActorRef<A>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
    {
        TellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }

    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(self, duration: Duration) -> TellRequest<'a, A, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> TellRequest<'a, A, M, WithRequestTimeout> {
        TellRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(self) -> Result<(), SendError<M>>
    where
        Tm: Into<Option<Duration>>,
    {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: None,
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                #[cfg(all(debug_assertions, feature = "tracing"))]
                warn_deadlock(self.actor_ref, "An actor is sending a `tell` request to itself using a bounded mailbox, which may lead to a deadlock. To avoid this, use `.try_send()`.", self.called_at);
                match self.mailbox_timeout.into() {
                    Some(timeout) => Ok(tx.send_timeout(signal, timeout).await?),
                    None => Ok(tx.send(signal).await?),
                }
            }
            MailboxSender::Unbounded(tx) => Ok(tx.send(signal)?),
        }
    }
}

impl<A, M> TellRequest<'_, A, M, WithoutRequestTimeout>
where
    A: Actor + Message<M>,
    M: Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub fn try_send(self) -> Result<(), SendError<M>> {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: None,
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => Ok(tx.try_send(signal)?),
            MailboxSender::Unbounded(tx) => Ok(tx.send(signal)?),
        }
    }

    /// Sends the message in a blocking context.
    pub fn blocking_send(self) -> Result<(), SendError<M>> {
        let signal = Signal::Message {
            message: Box::new(self.msg),
            actor_ref: self.actor_ref.clone(),
            reply: None,
            sent_within_actor: self.actor_ref.is_current(),
        };

        match self.actor_ref.mailbox_sender() {
            MailboxSender::Bounded(tx) => {
                #[cfg(all(debug_assertions, feature = "tracing"))]
                warn_deadlock(self.actor_ref, "An actor is sending a blocking `tell` request to itself using a bounded mailbox, which may lead to a deadlock.", self.called_at);
                Ok(tx.blocking_send(signal)?)
            }
            MailboxSender::Unbounded(tx) => Ok(tx.send(signal)?),
        }
    }
}

impl<'a, A, M, Tm> IntoFuture for TellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M>,
    M: Send + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<(), SendError<M>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

/// A request to send a message to a typed actor without any reply.
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
{
    actor_ref: &'a Recipient<M>,
    msg: M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

impl<'a, M, Tm> RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
{
    pub(crate) fn new(
        actor_ref: &'a Recipient<M>,
        msg: M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self
    where
        Tm: Default,
    {
        RecipientTellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }

    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> RecipientTellRequest<'a, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> RecipientTellRequest<'a, M, WithRequestTimeout> {
        RecipientTellRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }

    /// Sends the message.
    pub async fn send(self) -> Result<(), SendError<M>>
    where
        Tm: Into<Option<Duration>>,
    {
        self.actor_ref
            .handler
            .tell(self.msg, self.mailbox_timeout.into())
            .await
    }
}

impl<M> RecipientTellRequest<'_, M, WithoutRequestTimeout>
where
    M: Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub fn try_send(self) -> Result<(), SendError<M>> {
        self.actor_ref.handler.try_tell(self.msg)
    }

    /// Sends the message in a blocking context.
    pub fn blocking_send(self) -> Result<(), SendError<M>> {
        self.actor_ref.handler.blocking_tell(self.msg)
    }
}

impl<'a, M, Tm> IntoFuture for RecipientTellRequest<'a, M, Tm>
where
    M: Send + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<(), SendError<M>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.actor_ref
            .handler
            .tell(self.msg, self.mailbox_timeout.into())
    }
}

/// A request to send a message to a remote actor without any reply.
///
/// This can be thought of as "fire and forget".
#[cfg(feature = "remote")]
#[allow(missing_debug_implementations)]
#[must_use = "request won't be sent without awaiting, or calling a send method"]
pub struct RemoteTellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: Send + 'static,
{
    actor_ref: &'a actor::RemoteActorRef<A>,
    msg: &'a M,
    mailbox_timeout: Tm,
    #[cfg(all(debug_assertions, feature = "tracing"))]
    called_at: &'static std::panic::Location<'static>,
}

#[cfg(feature = "remote")]
impl<'a, A, M, Tm> RemoteTellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: Send + 'static,
    Tm: Default,
{
    pub(crate) fn new(
        actor_ref: &'a actor::RemoteActorRef<A>,
        msg: &'a M,
        #[cfg(all(debug_assertions, feature = "tracing"))] called_at: &'static std::panic::Location<
            'static,
        >,
    ) -> Self {
        RemoteTellRequest {
            actor_ref,
            msg,
            mailbox_timeout: Tm::default(),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at,
        }
    }
}

#[cfg(feature = "remote")]
impl<'a, A, M, Tm> RemoteTellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: serde::Serialize + Send + 'static,
{
    /// Sets the timeout for waiting for the actors mailbox to have capacity.
    pub fn mailbox_timeout(
        self,
        duration: Duration,
    ) -> RemoteTellRequest<'a, A, M, WithRequestTimeout> {
        self.mailbox_timeout_opt(Some(duration))
    }

    pub(crate) fn mailbox_timeout_opt(
        self,
        duration: Option<Duration>,
    ) -> RemoteTellRequest<'a, A, M, WithRequestTimeout> {
        RemoteTellRequest {
            actor_ref: self.actor_ref,
            msg: self.msg,
            mailbox_timeout: WithRequestTimeout(duration),
            #[cfg(all(debug_assertions, feature = "tracing"))]
            called_at: self.called_at,
        }
    }
}

#[cfg(feature = "remote")]
impl<A, M, Tm> RemoteTellRequest<'_, A, M, Tm>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: serde::Serialize + Send + 'static,
    Tm: Into<Option<Duration>>,
{
    /// Sends the message.
    pub async fn send(self) -> Result<(), error::RemoteSendError> {
        remote_tell(self.actor_ref, self.msg, self.mailbox_timeout.into(), false).await
    }
}

#[cfg(feature = "remote")]
impl<A, M> RemoteTellRequest<'_, A, M, WithoutRequestTimeout>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: serde::Serialize + Send + 'static,
{
    /// Tries to send the message without waiting for mailbox capacity.
    pub async fn try_send(self) -> Result<(), error::RemoteSendError> {
        remote_tell(self.actor_ref, self.msg, self.mailbox_timeout.into(), true).await
    }
}

#[cfg(feature = "remote")]
impl<'a, A, M, Tm> IntoFuture for RemoteTellRequest<'a, A, M, Tm>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: serde::Serialize + Send + Sync + 'static,
    Tm: Into<Option<Duration>> + Send + 'static,
{
    type Output = Result<(), error::RemoteSendError>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.send().boxed()
    }
}

#[cfg(feature = "remote")]
async fn remote_tell<A, M>(
    actor_ref: &actor::RemoteActorRef<A>,
    msg: &M,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), error::RemoteSendError>
where
    A: Actor + Message<M> + remote::RemoteActor + remote::RemoteMessage<M>,
    M: serde::Serialize + Send + 'static,
{
    use remote::*;
    use std::borrow::Cow;

    let actor_id = actor_ref.id();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    actor_ref.send_to_swarm(SwarmCommand::Tell {
        peer_id: *actor_id
            .peer_id()
            .expect("actor swarm should be bootstrapped"),
        actor_id,
        actor_remote_id: Cow::Borrowed(<A as RemoteActor>::REMOTE_ID),
        message_remote_id: Cow::Borrowed(<A as RemoteMessage<M>>::REMOTE_ID),
        payload: rmp_serde::to_vec_named(msg)
            .map_err(|err| error::RemoteSendError::SerializeMessage(err.to_string()))?,
        mailbox_timeout,
        immediate,
        reply: reply_tx,
    });

    match reply_rx.await.unwrap() {
        SwarmResponse::Tell(res) => match res {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        },
        SwarmResponse::OutboundFailure(err) => {
            Err(err.map_err(|_| unreachable!("outbound failure doesn't contain handler errors")))
        }
        _ => panic!("unexpected response"),
    }
}

#[cfg(all(debug_assertions, feature = "tracing"))]
fn warn_deadlock<A: Actor>(
    actor_ref: &ActorRef<A>,
    msg: &'static str,
    called_at: &'static std::panic::Location<'static>,
) {
    use tracing::warn;

    use crate::mailbox::MailboxSender;

    match actor_ref.mailbox_sender() {
        MailboxSender::Bounded(_) => {
            if actor_ref.is_current() {
                warn!("At {called_at}, {msg}");
            }
        }
        MailboxSender::Unbounded(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        actor::spawn_with_mailbox,
        error::{Infallible, SendError},
        mailbox,
        message::{Context, Message},
        Actor,
    };

    #[tokio::test]
    async fn bounded_tell_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Error = Infallible;
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn_with_mailbox(MyActor, mailbox::bounded(100));

        actor_ref.tell(Msg).await?; // Should be a regular MessageSend request
        actor_ref.tell(Msg).send().await?;
        actor_ref.tell(Msg).try_send()?;
        tokio::task::spawn_blocking({
            let actor_ref = actor_ref.clone();
            move || actor_ref.tell(Msg).blocking_send()
        })
        .await??;

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_tell_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Error = Infallible;
        }

        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn_with_mailbox(MyActor, mailbox::unbounded());

        actor_ref.tell(Msg).await?; // Should be a regular MessageSend request
        actor_ref.tell(Msg).send().await?;
        actor_ref.tell(Msg).try_send()?;
        actor_ref.tell(Msg).blocking_send()?;

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn_with_mailbox(MyActor, mailbox::bounded(100));
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.tell(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send(),
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

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_tell_requests_actor_not_running() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
            }
        }

        let actor_ref = spawn_with_mailbox(MyActor, mailbox::unbounded());
        actor_ref.stop_gracefully().await?;
        actor_ref.wait_for_shutdown().await;

        assert_eq!(
            actor_ref.tell(Msg).send().await,
            Err(SendError::ActorNotRunning(Msg))
        );
        assert_eq!(
            actor_ref.tell(Msg).try_send(),
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

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_mailbox_full() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Msg;

        impl Message<Msg> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                _msg: Msg,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }

        let actor_ref = spawn_with_mailbox(MyActor, mailbox::bounded(1));
        assert_eq!(actor_ref.tell(Msg).try_send(), Ok(()));
        assert_eq!(
            actor_ref.tell(Msg).try_send(),
            Err(SendError::MailboxFull(Msg))
        );
        actor_ref.kill();

        Ok(())
    }

    #[tokio::test]
    async fn bounded_tell_requests_mailbox_timeout() -> Result<(), Box<dyn std::error::Error>> {
        struct MyActor;

        impl Actor for MyActor {
            type Error = Infallible;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        struct Sleep(Duration);

        impl Message<Sleep> for MyActor {
            type Reply = ();

            async fn handle(
                &mut self,
                Sleep(duration): Sleep,
                _ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                tokio::time::sleep(duration).await;
            }
        }

        let actor_ref = spawn_with_mailbox(MyActor, mailbox::bounded(1));
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
