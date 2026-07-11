#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(unused_must_use)]

pub mod actor;
#[cfg(feature = "console")]
pub mod console;
pub mod error;
pub(crate) mod links;
pub mod mailbox;
pub mod message;
pub mod registry;
pub mod reply;
pub mod request;
pub mod supervision;

pub use actor::Actor;
#[cfg(feature = "macros")]
pub use kameo_macros::{Actor, Reply, messages};
pub use reply::Reply;

#[cfg(all(feature = "otel", not(feature = "tracing")))]
compile_error!("the `otel` feature requires the `tracing` feature to be enabled");

/// Commonly used types and functions that can be imported with a single use statement.
///
/// ```
/// use kameo::prelude::*;
/// ```
///
/// This module includes the most essential actor components, messaging types,
/// and traits needed for typical actor system usage.
pub mod prelude {
    #[cfg(feature = "macros")]
    pub use kameo_macros::{Actor, Reply, messages};

    pub use crate::actor::{
        Actor, ActorId, ActorRef, PreparedActor, Recipient, ReplyRecipient, Spawn, WeakActorRef,
        WeakRecipient, WeakReplyRecipient,
    };
    pub use crate::error::{ActorStopReason, PanicError, PanicReason, SendError};
    pub use crate::mailbox::{self, MailboxReceiver, MailboxSender};
    pub use crate::message::{BoxMessage, Context, Message};
    pub use crate::reply::{DelegatedReply, ForwardedReply, Reply, ReplyError, ReplySender};
}
