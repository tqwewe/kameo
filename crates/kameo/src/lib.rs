//! # Kameo 🧚🏻
//!
//! **Fault-tolerant Async Actors Built on Tokio**
//!
//! - **Async**: Built on tokio, actors run asyncronously in their own isolated spawned tasks.
//! - **Supervision**: Link actors, creating dependencies through child/parent/sibbling relationships.
//! - **MPSC Unbounded Channels**: Uses mpsc channels for messaging between actors.
//! - **Concurrent Queries**: Support concurrent processing of queries when mutable state isn't necessary.
//! - **Panic Safe**: Catches panics internally, allowing actors to be restarted.
//!
//! ## Installing
//!
//! ```toml
//! [dependencies]
//! kameo = "*"
//! ```
//!
//! ## Defining an Actor without Macros
//!
//! ```
//! use kameo::Actor;
//! use kameo::message::{Context, Message};
//!
//! // Define the actor state
//! struct Counter {
//!   count: i64,
//! }
//!
//! impl Actor for Counter {}
//!
//! // Define messages
//! struct Inc(u32);
//!
//! impl Message<Inc> for Counter {
//!     type Reply = i64;
//!
//!     async fn handle(&mut self, msg: Counter, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
//!         self.count += msg.0 as i64;
//!         self.count
//!     }
//! }
//! ```
//!
//! ## Defining an Actor with Macros
//!
//! ```
//! use kameo::{Actor, messages};
//!
//! // Define the actor state
//! #[derive(Actor)]
//! struct Counter {
//!     count: i64,
//! }
//!
//! // Define messages
//! #[messages]
//! impl Counter {
//!     #[message]
//!     fn inc(&mut self, amount: u32) -> i64 {
//!         self.count += amount as i64;
//!         self.count
//!     }
//! }
//! ```
//!
//! <details>
//!   <summary>See generated macro code</summary>
//!
//! ```rust
//! // Derive Actor
//! impl kameo::actor::Actor for Counter {
//!     fn name(&self) -> Cow<'_, str> {
//!         Cow::Borrowed("Counter")
//!     }
//! }
//!
//! // Messages
//! struct Inc { amount: u32 }
//!
//! impl kameo::Message<Inc> for Counter {
//!     type Reply = i64;
//!
//!     async fn handle(&mut self, msg: Counter) -> Self::Reply {
//!         self.inc(msg.amount)
//!     }
//! }
//! ```
//! </details>
//!
//! ## Spawning an Actor & Messaging
//!
//! ```
//! let counter_ref = kameo::spawn(Counter { count: 0 });
//!
//! let count = counter_ref.send(Inc(42)).await?;
//! println!("Count is {count}");
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(unused_must_use)]

pub mod actor;
mod actor_kind;
pub mod error;
pub mod message;
pub mod reply;

pub use actor::{spawn, Actor};
pub use kameo_macros::{messages, Actor, Reply};
pub use reply::Reply;
