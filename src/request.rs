//! Types for sending requests including messages and queries to actors.

use std::time::Duration;

mod ask;
mod tell;

pub use ask::{AskRequest, BlockingPendingReply, PendingReply, ReplyRecipientAskRequest};
pub use tell::{RecipientTellRequest, ReplyRecipientTellRequest, TellRequest};

/// A type for requests without any timeout set.
#[derive(Clone, Copy, Debug, Default)]
pub struct WithoutRequestTimeout;

/// A type for timeouts in actor requests.
#[derive(Clone, Copy, Debug, Default)]
pub struct WithRequestTimeout(Option<Duration>);

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
