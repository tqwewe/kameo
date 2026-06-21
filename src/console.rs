//! Live monitoring of a running actor system for the kameo console.
//!
//! Enabling the `console` feature instruments every actor with a lightweight per-instance
//! monitor (counters, status, mailbox depth) kept in a global registry. Call [`serve`] (or
//! build one with [`Console`]) to expose those snapshots over TCP to a console client.

// A ready-made demo actor system for showcasing the console (used by the `console` example and
// `kameo_console --demo`). Hidden from the docs as it isn't part of the public API; its docs live
// in the module's own `//!` comment so intra-doc links resolve in the module's scope.
#[doc(hidden)]
#[allow(missing_docs, missing_debug_implementations)]
pub mod demo;
pub(crate) mod registry;
mod server;
/// The console wire protocol (the serialization contract with console clients).
///
/// **Unstable:** hidden from the docs because the format may change in any release. The types
/// are public only so a console client crate can deserialize snapshots.
#[doc(hidden)]
#[allow(missing_docs)] // the protocol is intentionally undocumented; see the note above
pub mod wire;

pub use server::{Console, ConsoleHandle, serve};
