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
//! > Unfortunately, **kameo requires unstable Rust**, as it depends on specialization for error handling in actors.
//!
//! ---
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
//!     type Reply = i64;
//!
//!     async fn handle(self, state: &mut Counter) -> Self::Reply {
//!         state.count += self.0 as i64;
//!         state.count
//!     }
//! }
//! ```
//!
//! # Starting an Actor & Messaging
//!
//! ```
//! let counter_ref: ActorRef<Counter> = Counter { count: 0 }.start();
//!
//! let count = counter_ref.send(Inc(42)).await?;
//! println!("Count is {count}");
//! ```

#![allow(incomplete_features)]
#![feature(specialization)]
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
pub use message::{Message, Query};
pub use stop_reason::ActorStopReason;
