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

pub use ask::AskRequest;
pub use query::QueryRequest;
pub use tell::TellRequest;

/// A trait for sending messages to an actor.
///
/// Typically `ActorRef::tell` and `ActorRef::ask` would be used directly, as they provide more functionality such as setting timeouts.
/// Methods on this trait are helpful shorthand methods, which can be used for ActorRef's with any mailbox type (bounded or unbounded).
pub trait Request<A, M, Mb>
where
    A: Actor<Mailbox = Mb> + Message<M>,
{
    /// Sends a message to an actor without waiting for any reply.
    fn tell(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<(), SendError<M, <A::Reply as Reply>::Error>>> + Send;

    /// Sends a message to an actor, waiting for a reply.
    fn ask(
        &self,
        msg: M,
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
    async fn tell(&self, msg: M) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        ActorRef::tell(self, msg).send().await
    }

    async fn ask(
        &self,
        msg: M,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        ActorRef::ask(self, msg).send().await
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
    async fn tell(&self, msg: M) -> Result<(), SendError<M, <A::Reply as Reply>::Error>> {
        ActorRef::tell(self, msg).send()
    }

    async fn ask(
        &self,
        msg: M,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>> {
        ActorRef::ask(self, msg).send().await
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
