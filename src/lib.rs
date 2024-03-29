//! # Kameo
//!
//! **Simple tokio actors**
//!
//! - ✅ No Macros
//! - ✅ Async Support
//! - ✅ Links Between Actors (`start_link`/`start_child`)
//! - ✅ MPSC Unbounded Channel for Messaging
//! - ✅ Concurrent Queries
//! - ✅ Panic Safe
//!
//! ---
//!
//! # `nightly` feature flag
//!
//! The `nightly` feature flag allows for any type to be used in a [Message] or [Query]'s reply.
//! This is done though specialization, which requires nightly rust.
//!
//! Without the nightly feature flag, all replies must be a `Result<T, E>`, where `E: Debug + Send + Sync + 'static`.
//! This is to ensure that asyncronous messages force the actor to panic,
//! since otherwise the error would be silently ignored.
//!
//! # Defining an Actor
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
//! #[async_trait]
//! impl Message<Counter> for Inc {
//!     type Reply = Result<i64, Infallible>;
//!
//!     async fn handle(self, state: &mut Counter) -> Self::Reply {
//!         state.count += self.0 as i64;
//!         Ok(state.count)
//!     }
//! }
//! ```
//!
//! Note, with the `nightly` feature flag enabled, this reply type can be `i64` directly without the result.
//!
//! # Starting an Actor & Messaging
//!
//! ```
//! let counter_ref: ActorRef<Counter> = Counter { count: 0 }.start();
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
mod actor_ref;
mod error;
mod message;
mod stop_reason;

pub use actor::Actor;
pub use actor_ref::ActorRef;
pub use error::{PanicError, SendError};
pub use message::{Message, Query, Reply};
pub use stop_reason::ActorStopReason;
