#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(unused_must_use)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod actor;
pub mod error;
pub mod mailbox;
pub mod message;
#[cfg(not(feature = "remote"))]
pub mod registry;
#[cfg(feature = "remote")]
pub mod remote;
pub mod reply;
pub mod request;

pub use actor::{spawn, Actor};
#[cfg(feature = "macros")]
pub use kameo_macros::{messages, remote_message, Actor, RemoteActor, Reply};
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
    pub use kameo_macros::{messages, remote_message, Actor, RemoteActor, Reply};

    #[cfg(feature = "remote")]
    pub use crate::actor::RemoteActorRef;
    pub use crate::actor::{
        spawn, spawn_in_thread, spawn_link, Actor, ActorID, ActorRef, PreparedActor, WeakActorRef,
    };
    #[cfg(feature = "remote")]
    pub use crate::error::RemoteSendError;
    pub use crate::error::{ActorStopReason, PanicError, SendError};
    pub use crate::mailbox::{self, MailboxReceiver, MailboxSender};
    pub use crate::message::{Context, Message};
    #[cfg(feature = "remote")]
    pub use crate::remote::{ActorSwarm, RemoteActor, RemoteMessage};
    pub use crate::reply::{DelegatedReply, ForwardedReply, Reply, ReplyError, ReplySender};
}
