//! Types for sending requests including messages and queries to actors.

use std::time::Duration;

mod ask;
mod tell;

// TODO: These types need to be updated to use DistributedAskRequest/DistributedTellRequest
// #[cfg(feature = "remote")]
// pub use ask::RemoteAskRequest;
//
// #[cfg(feature = "remote")]
// pub use tell::RemoteTellRequest;

pub use ask::{AskRequest, BlockingPendingReply, PendingReply, ReplyRecipientAskRequest};
pub use tell::{RecipientRequest, RecipientTellRequest, TellRequest};

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
