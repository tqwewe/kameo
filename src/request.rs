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
//! | [`send_sync`]         |       ❌       |        ❌        |        ❌       |         ✅        |
//! | [`try_send`]          |     ✅ ⏳ 🌐     |      ✅ ⏳ 🌐      |       ✅ 🌐      |        ✅ 🌐       |
//! | [`try_send_sync`]     |       ❌       |        ❌        |        ✅       |         ✅        |
//! | [`blocking_send`]     |       ✅       |        ✅        |        ✅       |         ✅        |
//! | [`try_blocking_send`] |       ✅       |        ✅        |        ✅       |         ✅        |
//! | [`forward`]           |      ✅ 📬      |        ✅        |        ❌       |         ❌        |
//! | [`forward_sync`]      |       ❌       |        ✅        |        ❌       |         ❌        |
//!
//! [`send`]: method@MessageSend::send
//! [`send_sync`]: method@MessageSendSync::send_sync
//! [`try_send`]: method@TryMessageSend::try_send
//! [`try_send_sync`]: method@TryMessageSendSync::try_send_sync
//! [`blocking_send`]: method@BlockingMessageSend::blocking_send
//! [`try_blocking_send`]: method@TryBlockingMessageSend::try_blocking_send
//! [`forward`]: method@ForwardMessageSend::forward
//! [`forward_sync`]: method@ForwardMessageSendSync::forward_sync

use std::time::Duration;

use futures::Future;

mod ask;
mod tell;

#[cfg(feature = "remote")]
pub use ask::RemoteAskRequest;

#[cfg(feature = "remote")]
pub use tell::RemoteTellRequest;

pub use ask::{AskRequest, LocalAskRequest};
pub use tell::{LocalTellRequest, TellRequest};

use crate::{error::SendError, reply::ReplySender, Reply};

/// Trait representing the ability to send a message.
pub trait MessageSend {
    /// Success value.
    type Ok;
    /// Error value.
    type Error;

    /// Sends a message.
    fn send(self) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send;
}

/// Trait representing the ability to send a message synchronously.
pub trait MessageSendSync {
    /// Success value.
    type Ok;
    /// Error value.
    type Error;

    /// Sends a message synchronously.
    fn send_sync(self) -> Result<Self::Ok, Self::Error>;
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

/// Trait representing the ability to attempt to send a message without waiting for mailbox capacity synchronously.
pub trait TryMessageSendSync {
    /// Success value.
    type Ok;
    /// Error value.
    type Error;

    /// Attemps to send a message synchronously without waiting for mailbox capacity.
    fn try_send_sync(self) -> Result<Self::Ok, Self::Error>;
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
    fn forward(
        self,
        tx: ReplySender<R::Value>,
    ) -> impl Future<Output = Result<(), SendError<(M, ReplySender<R::Value>), R::Error>>> + Send;
}

/// Trait representing the ability to send a message with the reply being sent back to a channel synchronously.
pub trait ForwardMessageSendSync<R: Reply, M> {
    /// Sends a message synchronously with the reply being sent back to a channel.
    fn forward_sync(
        self,
        tx: ReplySender<R::Value>,
    ) -> Result<(), SendError<(M, ReplySender<R::Value>), R::Error>>;
}

/// A type for requests without any timeout set.
#[derive(Clone, Copy, Debug)]
pub struct WithoutRequestTimeout;

/// A type for timeouts in actor requests.
#[derive(Clone, Copy, Debug)]
pub struct WithRequestTimeout(Duration);

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
        MaybeRequestTimeout::Timeout(timeout)
    }
}
