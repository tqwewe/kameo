//! Distributed actors for kameo via gossip clustering and TCP messaging.
//!
//! This crate connects kameo actors across nodes without any external infrastructure:
//!
//! - **Registry**: actors register under a name and become visible to every node in the
//!   cluster. Cluster membership and the registry are replicated with gossip
//!   ([chitchat]), so lookups are local and instant, and a dead node's registrations
//!   vanish automatically. Any number of actors may share a name (set semantics);
//!   [`RemoteNode::lookup_all`] returns them all.
//! - **Messaging**: [`RemoteActorRef::ask`] and [`RemoteActorRef::tell`] send messages
//!   over plain TCP, with one pooled connection per node pair. Tells are acknowledged
//!   on mailbox delivery, matching local tell semantics (with
//!   [`send_unacked`](RemoteTellRequest::send_unacked) as the fire-and-forget path),
//!   and refs resolving to the local node dispatch in-process without touching the
//!   network. Messages from one node to one target actor are delivered in send order
//!   (per-pair FIFO ordering, as in Akka).
//!
//! Nodes join the cluster through seed nodes: a starting node contacts any configured
//! seed and learns the rest of the membership through gossip.
//!
//! # Example
//!
//! ```no_run
//! use kameo::prelude::*;
//! use kameo_remote::{RemoteActor, RemoteMessage, RemoteMessages, RemoteNode, RemoteNodeConfig};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Actor)]
//! struct Counter {
//!     count: i64,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Inc {
//!     amount: i64,
//! }
//!
//! impl Message<Inc> for Counter {
//!     type Reply = i64;
//!
//!     async fn handle(&mut self, msg: Inc, _: &mut Context<Self, Self::Reply>) -> i64 {
//!         self.count += msg.amount;
//!         self.count
//!     }
//! }
//!
//! impl RemoteMessage for Inc {
//!     const REMOTE_ID: &'static str = "example::Inc";
//! }
//!
//! impl RemoteActor for Counter {
//!     const REMOTE_ID: &'static str = "example::Counter";
//!
//!     fn remote_messages(handlers: &mut RemoteMessages<Self>) {
//!         handlers.add::<Inc>();
//!     }
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let node = RemoteNode::bootstrap(RemoteNodeConfig::default()).await?;
//!
//! let counter = Counter::spawn(Counter { count: 0 });
//! node.register(&counter, "counter").await?;
//!
//! // On any node in the cluster:
//! if let Some(counter) = node.lookup::<Counter>("counter").await? {
//!     let count = counter.ask(&Inc { amount: 1 }).await?;
//!     println!("count: {count}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [chitchat]: https://docs.rs/chitchat

mod dispatch;
mod error;
mod id;
mod messaging;
mod node;
mod registry;
mod remote_actor;
mod remote_ref;

pub use chitchat::FailureDetectorConfig;

pub use error::{BootstrapError, RegistryError, RemoteSendError, ShutdownError};
pub use id::{NodeId, RemoteActorId};
pub use node::{RemoteNode, RemoteNodeConfig};
pub use remote_actor::{RemoteActor, RemoteMessage, RemoteMessages};
pub use remote_ref::{RemoteActorRef, RemoteAskRequest, RemoteTellRequest};
