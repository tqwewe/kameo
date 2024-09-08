#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(unused_must_use)]

pub mod actor;
pub mod error;
pub mod mailbox;
pub mod message;
pub mod remote;
pub mod reply;
pub mod request;

pub use actor::{spawn, Actor};
pub use kameo_macros::{messages, remote_message, Actor, RemoteActor, Reply};
pub use reply::Reply;
