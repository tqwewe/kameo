//! Types for sending requests including messages and queries to actors.
//!
//! # Supported Send Methods Overview
//!
//! Below is a table showing which features are supported for different requests.
//!
//! - **Ask (bounded)**: refers to sending a message using [`ask`] on an actor with a [`BoundedMailbox`].
//! - **Ask (unbounded)**: refers to sending a message using [`ask`] on an actor with an [`UnboundedMailbox`].
//! - **Tell (bounded)**: refers to sending a message using [`tell`] on an actor with a [`BoundedMailbox`].
//! - **Tell (unbounded)**: refers to sending a message using [`tell`] on an actor with an [`UnboundedMailbox`].
//!
//! [`tell`]: method@crate::actor::ActorRef::tell
//! [`ask`]: method@crate::actor::ActorRef::ask
//! [`BoundedMailbox`]: crate::mailbox::bounded::BoundedMailbox
//! [`UnboundedMailbox`]: crate::mailbox::unbounded::UnboundedMailbox
//!
//! **Legend**
//!
//! - Supported: ✅
//! - With mailbox timeout: 📬
//! - With reply timeout: ⏳
//! - Remote: 🌐
//! - Unsupported: ❌
//!
//! | Method                | Ask (bounded) | Ask (unbounded) | Tell (bounded) | Tell (unbounded) |
//! |-----------------------|---------------|-----------------|----------------|------------------|
//! | [`send`]              |    ✅ 📬 ⏳ 🌐    |      ✅ ⏳ 🌐      |      ✅ 📬 🌐     |        ✅ 🌐       |
//! | [`try_send`]          |     ✅ ⏳ 🌐     |      ✅ ⏳ 🌐      |       ✅ 🌐      |        ✅ 🌐       |
//! | [`blocking_send`]     |       ✅       |        ✅        |        ✅       |         ✅        |
//! | [`try_blocking_send`] |       ✅       |        ✅        |        ✅       |         ✅        |
//! | [`forward`]           |      ✅ 📬      |        ✅        |        ❌       |         ❌        |
//!
//! [`send`]: method@MessageSend::send
//! [`try_send`]: method@TryMessageSend::try_send
//! [`blocking_send`]: method@BlockingMessageSend::blocking_send
//! [`try_blocking_send`]: method@TryBlockingMessageSend::try_blocking_send
//! [`forward`]: method@ForwardMessageSend::forward

use std::time::Duration;

use futures::Future;

mod ask;
mod tell;

#[cfg(feature = "remote")]
pub use ask::RemoteAskRequest;

#[cfg(feature = "remote")]
pub use tell::RemoteTellRequest;

pub use ask::AskRequest;
pub use tell::TellRequest;

use crate::{actor::ActorRef, error::SendError, reply::ReplySender, Actor, Reply};

/// Trait representing the ability to send a message.
pub trait MessageSend {
    /// Success value.
    type Ok;
    /// Error value.
    type Error;

    /// Sends a message.
    fn send(self) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send;
}

/// Trait representing the ability to attempt to send a message without waiting for mailbox capacity.
pub trait TryMessageSend {
    /// Success value.
    type Ok;
    /// Error value.
    type Error;

    /// Attempts to sends a message without waiting for mailbox capacity.
    ///
    /// If the actors mailbox is full, [`SendError::MailboxFull`] error will be returned.
    ///
    /// [`SendError::MailboxFull`]: crate::error::SendError::MailboxFull
    fn try_send(self) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send;
}

/// Trait representing the ability to send a message in a blocking context, useful outside an async runtime.
pub trait BlockingMessageSend {
    /// Success value.
    type Ok;
    /// Error value.
    type Error;

    /// Sends a message, blocking the current thread.
    fn blocking_send(self) -> Result<Self::Ok, Self::Error>;
}

/// Trait representing the ability to send a message in a blocking context without waiting for mailbox capacity,
/// useful outside an async runtime.
pub trait TryBlockingMessageSend {
    /// Success value.
    type Ok;
    /// Error value.
    type Error;

    /// Attempts to sends a message in a blocking context without waiting for mailbox capacity.
    ///
    /// If the actors mailbox is full, [`SendError::MailboxFull`] error will be returned.
    ///
    /// [`SendError::MailboxFull`]: crate::error::SendError::MailboxFull
    fn try_blocking_send(self) -> Result<Self::Ok, Self::Error>;
}

/// Trait representing the ability to send a message with the reply being sent back to a channel.
pub trait ForwardMessageSend<R: Reply, M> {
    /// Sends a message with the reply being sent back to a channel.
    #[allow(clippy::type_complexity)]
    fn forward(
        self,
        tx: ReplySender<R::Value>,
    ) -> impl Future<Output = Result<(), SendError<(M, ReplySender<R::Value>), R::Error>>> + Send;
}

/// A type for requests without any timeout set.
#[derive(Clone, Copy, Debug, Default)]
pub struct WithoutRequestTimeout;

/// A type for timeouts in actor requests.
#[derive(Clone, Copy, Debug, Default)]
pub struct WithRequestTimeout(Option<Duration>);

/// A type which might contain a request timeout.
///
/// This type is used internally for remote messaging and will panic if used incorrectly with any MessageSend trait.
#[derive(Clone, Copy, Debug)]
pub enum MaybeRequestTimeout {
    /// No timeout set.
    NoTimeout,
    /// A timeout with a duration.
    Timeout(Duration),
}

impl From<Option<Duration>> for MaybeRequestTimeout {
    fn from(timeout: Option<Duration>) -> Self {
        match timeout {
            Some(timeout) => MaybeRequestTimeout::Timeout(timeout),
            None => MaybeRequestTimeout::NoTimeout,
        }
    }
}

impl From<WithoutRequestTimeout> for MaybeRequestTimeout {
    fn from(_: WithoutRequestTimeout) -> Self {
        MaybeRequestTimeout::NoTimeout
    }
}

impl From<WithRequestTimeout> for MaybeRequestTimeout {
    fn from(WithRequestTimeout(timeout): WithRequestTimeout) -> Self {
        match timeout {
            Some(timeout) => MaybeRequestTimeout::Timeout(timeout),
            None => MaybeRequestTimeout::NoTimeout,
        }
    }
}

impl From<WithoutRequestTimeout> for Option<Duration> {
    fn from(_: WithoutRequestTimeout) -> Self {
        None
    }
}

impl From<WithRequestTimeout> for Option<Duration> {
    fn from(WithRequestTimeout(duration): WithRequestTimeout) -> Self {
        duration
    }
}

impl From<MaybeRequestTimeout> for Option<Duration> {
    fn from(timeout: MaybeRequestTimeout) -> Self {
        match timeout {
            MaybeRequestTimeout::NoTimeout => None,
            MaybeRequestTimeout::Timeout(duration) => Some(duration),
        }
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

#[cfg(not(all(debug_assertions, feature = "tracing")))]
fn warn_deadlock<A: Actor>(
    _actor_ref: &ActorRef<A>,
    _msg: &'static str,
    _called_at: &'static std::panic::Location<'static>,
) {
}
