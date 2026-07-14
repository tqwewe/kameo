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
//!   on mailbox delivery, refs resolving to the local node are dispatched in-process,
//!   and messages from one node to one target actor are delivered in send order.
//!
//! Nodes join the cluster through seed nodes: a starting node contacts any configured
//! seed and learns the rest of the membership through gossip.
//!
//! # Security
//!
//! By default a node is open: anyone who can reach its ports can join the cluster and
//! send messages. Two independent settings close this, and full security requires
//! both:
//!
//! - [`RemoteNodeConfig::cluster_key`] — a 32-byte pre-shared secret
//!   ([`ClusterKey`]). Gossip datagrams are encrypted and authenticated with a key
//!   derived from it, so nodes without the key can neither read nor poison the
//!   registry. Messaging connections mutually prove possession of it during the
//!   connection handshake (an HMAC challenge-response; the key never crosses the
//!   wire). Message payloads themselves are only encrypted when TLS is also enabled.
//! - `RemoteNodeConfig::tls` (cargo feature `tls`) — mutual TLS for the messaging
//!   layer via `TlsConfig`: connections are encrypted with TLS 1.3 and both peers
//!   must present a certificate signed by the cluster CA. Certificates are verified
//!   against the CA without hostname verification, since nodes are dialed by
//!   gossip-advertised IPs; possession of a CA-signed certificate is what makes a
//!   peer a cluster member.
//!
//! With only `cluster_key`, membership is authenticated and gossip is confidential,
//! but message payloads cross the network in plaintext. With only `tls`, messaging is
//! encrypted but gossip is open, so anyone reaching the gossip port can poison the
//! registry (a warning is logged at bootstrap). Key and certificate distribution are
//! up to the application; rotating the cluster key requires a rolling restart, during
//! which old-key and new-key nodes temporarily cannot see each other.
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
mod gossip;
mod id;
mod messaging;
mod node;
mod registry;
mod remote_actor;
mod remote_ref;
mod security;
#[cfg(feature = "tls")]
mod tls;

pub use chitchat::FailureDetectorConfig;

pub use error::{BootstrapError, RegistryError, RemoteSendError, ShutdownError};
pub use id::{NodeId, RemoteActorId};
pub use node::{RemoteNode, RemoteNodeConfig};
pub use remote_actor::{RemoteActor, RemoteMessage, RemoteMessages};
pub use remote_ref::{RemoteActorRef, RemoteAskRequest, RemoteTellRequest};
pub use security::ClusterKey;
#[cfg(feature = "tls")]
pub use tls::{TlsConfig, TlsError};
