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
//! **Stable**
//!
//! ```toml
//! [dependencies]
//! kameo = "*"
//! ```
//!
//! **Nightly**
//!
//! ```toml
//! [dependencies]
//! kameo = { version = "*", features = ["nightly"] }
//! ```
//!
//! ## `nightly` feature flag
//!
//! The `nightly` feature flag allows for any type to be used in a [Message] or [Query]'s reply.
//! It also removes the need for `spawn_unsend` and other `_unsend` methods, since actor "sendness" can be inferred.
//! This is done though specialization, which requires nightly rust.
//!
//! Without the nightly feature flag, all replies must be a `Result<T, E>`, where `E: Debug + Send + Sync + 'static`.
//! This is to ensure that asyncronous messages that fail will cause the actor to panic,
//! since otherwise the error would be silently ignored.
//!
//! ## Defining an Actor without Macros
//!
//! ```
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
//!     type Reply = Result<i64, Infallible>;
//!
//!     async fn handle(&mut self, msg: Counter) -> Self::Reply {
//!         self.count += msg.0 as i64;
//!         Ok(self.count)
//!     }
//! }
//! ```
//!
//! Note, with the `nightly` feature flag enabled, this reply type can be `i64` directly without the result.
//!
//! ## Defining an Actor with Macros
//!
//! ```
//! // Define the actor state
//! #[derive(Actor)]
//! struct Counter {
//!     count: i64,
//! }
//!
//! // Define messages
//! #[actor]
//! impl Counter {
//!     #[message]
//!     fn inc(&mut self, amount: u32) -> Result<i64, Infallible> {
//!         self.count += amount as i64;
//!         Ok(self.count)
//!     }
//! }
//! ```
//!
//! <details>
//!   <summary>See generated macro code</summary>
//!
//! ```rust
//! // Derive Actor
//! impl kameo::Actor for Counter {
//!     fn name(&self) -> Cow<'_, str> {
//!         Cow::Borrowed("Counter")
//!     }
//! }
//!
//! // Messages
//! struct Inc { amount: u32 }
//!
//! impl kameo::Message<Inc> for Counter {
//!     type Reply = Result<i64, Infallible>;
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
//! use kameo::{Spawn, ActorRef};
//!
//! let counter_ref: ActorRef<Counter> = Counter { count: 0 }.spawn();
//!
//! let count = counter_ref.send(Inc(42)).await?;
//! println!("Count is {count}");
//! ```

#![cfg_attr(feature = "nightly", feature(specialization))]
#![cfg_attr(feature = "nightly", allow(incomplete_features))]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(unused_must_use)]

mod actor;
mod actor_kind;
mod actor_ref;
mod error;
mod message;
mod pool;
mod spawn;

pub use actor::Actor;
pub use actor_ref::ActorRef;
pub use error::{ActorStopReason, BoxError, PanicError, SendError};
pub use kameo_macros::{actor, Actor};
pub use message::{Message, Query, Reply};
pub use pool::ActorPool;
pub use spawn::Spawn;
