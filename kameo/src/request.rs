//! Types for sending requests including messages and queries to actors.

use std::time::Duration;

use futures::Future;

use crate::{
    actor::{ActorRef, BoundedMailbox, UnboundedMailbox},
    error::SendError,
    message::Message,
    reply::ReplySender,
    Actor, Reply,
};

mod ask;
mod query;
mod tell;

pub use ask::{AskRequest, LocalAskRequest, RemoteAskRequest};
pub use query::QueryRequest;
pub use tell::{LocalTellRequest, RemoteTellRequest, TellRequest};

/// A trait for sending messages to an actor.
///
/// Typically `ActorRef::tell` and `ActorRef::ask` would be used directly, as they provide more functionality such as setting timeouts.
/// Methods on this trait are helpful shorthand methods, which can be used for ActorRef's with any mailbox type (bounded or unbounded).
#[doc(hidden)]
pub trait Request<A, M, Mb>
where
    A: Actor<Mailbox = Mb> + Message<M>,
{
    /// Sends a message to an actor without waiting for any reply.
    fn tell(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> impl Future<Output = Result<(), SendError<M, <A::Reply as Reply>::Error>>> + Send;

    /// Sends a message to an actor, waiting for a reply.
    fn ask(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> impl Future<
        Output = Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>,
    > + Send;

    /// Forwards a message to an actor, waiting for a reply to be sent back to `tx`.
    fn ask_forwarded(
        &self,
        msg: M,
        tx: ReplySender<<A::Reply as Reply>::Value>,
    ) -> impl Future<
        Output = Result<
            (),
            SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
        >,
    > + Send;
}

impl<A, M> Request<A, M, BoundedMailbox<A>> for ActorRef<A>
where
    A: Actor<Mailbox = BoundedMailbox<A>> + Message<M>,
    M: Send + 'static,
{
    async fn tell(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        let req = ActorRef::tell(self, msg);
        match (mailbox_timeout, immediate) {
            (None, true) => req.try_send(),
            (None, false) => req.send().await,
            (Some(_), true) => panic!("immediate is not available when a mailbox timeout is set"),
            (Some(t), false) => req.timeout(t).send().await,
        }
    }

    async fn ask(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        let req = ActorRef::ask(self, msg);
        match (mailbox_timeout, reply_timeout, immediate) {
            (None, None, true) => req.try_send().await,
            (None, None, false) => req.send().await,
            (None, Some(t), true) => req.reply_timeout(t).try_send().await,
            (None, Some(t), false) => req.reply_timeout(t).send().await,
            (Some(_), None, true) => {
                panic!("immediate is not available when a mailbox timeout is set")
            }
            (Some(t), None, false) => req.mailbox_timeout(t).send().await,
            (Some(_), Some(_), true) => {
                panic!("immediate is not available when a mailbox timeout is set")
            }
            (Some(mt), Some(rt), false) => req.mailbox_timeout(mt).reply_timeout(rt).send().await,
        }
    }

    async fn ask_forwarded(
        &self,
        msg: M,
        tx: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        ActorRef::ask(self, msg).forward(tx).await
    }
}

impl<A, M> Request<A, M, UnboundedMailbox<A>> for ActorRef<A>
where
    A: Actor<Mailbox = UnboundedMailbox<A>> + Message<M>,
    M: Send + 'static,
{
    async fn tell(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        let req = ActorRef::tell(self, msg);
        match (mailbox_timeout, immediate) {
            (None, false) => req.send(),
            (_, true) => panic!("immediate is not available with unbounded mailboxes"),
            (Some(_), _) => {
                panic!("mailbox timeout is not available with unbounded mailboxes")
            }
        }
    }

    async fn ask(
        &self,
        msg: M,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        let req = ActorRef::ask(self, msg);
        match (mailbox_timeout, reply_timeout, immediate) {
            (None, None, false) => req.send().await,
            (None, Some(t), false) => req.reply_timeout(t).send().await,
            (_, _, true) => panic!("immediate is not available with unbounded mailboxes"),
            (Some(_), _, _) => {
                panic!("mailbox timeout is not available with unbounded mailboxes")
            }
        }
    }

    async fn ask_forwarded(
        &self,
        msg: M,
        tx: ReplySender<<A::Reply as Reply>::Value>,
    ) -> Result<
        (),
        SendError<(M, ReplySender<<A::Reply as Reply>::Value>), <A::Reply as Reply>::Error>,
    > {
        ActorRef::ask(self, msg).forward(tx)
    }
}

/// A type for requests without any timeout set.
#[allow(missing_debug_implementations)]
pub struct WithoutRequestTimeout;

/// A type for timeouts in actor requests.
#[allow(missing_debug_implementations)]
pub struct WithRequestTimeout(Duration);
