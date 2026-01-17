#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(unused_must_use)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
// #![cfg_attr(feature = "nightly", feature(hash_extract_if))]

pub mod actor;
pub mod error;
pub mod mailbox;
pub mod message;
pub mod registry;
#[cfg(feature = "remote")]
pub mod remote;
#[cfg(feature = "remote")]
pub mod remote_v2;
pub mod reply;
pub mod request;

#[cfg(all(feature = "remote", feature = "macros"))]
pub use crate::remote::RemoteMessage;
pub use actor::{Actor, Spawn};
#[cfg(feature = "macros")]
pub use kameo_macros::{Actor, RemoteMessage, Reply, messages, remote_message_derive};
pub use reply::Reply;

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
    pub use kameo_macros::{Actor, RemoteMessage, Reply, messages, remote_message_derive};

    pub use crate::actor::{
        Actor, ActorId, ActorRef, ActorRegistration, PreparedActor, Recipient, ReplyRecipient,
        Spawn, WeakActorRef, WeakRecipient, WeakReplyRecipient,
    };
    #[cfg(feature = "remote")]
    pub use crate::error::RemoteSendError;
    pub use crate::error::{ActorStopReason, PanicError, SendError};
    pub use crate::mailbox::{self, MailboxReceiver, MailboxSender};
    pub use crate::message::{Context, Message};
    #[cfg(feature = "remote")]
    pub use crate::remote;
    #[cfg(feature = "remote")]
    pub use crate::remote::{DistributedActorRef, RemoteMessage};
    pub use crate::reply::{DelegatedReply, ForwardedReply, Reply, ReplyError, ReplySender};
}
