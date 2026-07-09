//! A peer-to-peer chat room with no central server.
//!
//! Every node registers its own user actor under the shared name `chat`. Sending a
//! message looks up all providers and tells each one; presence comes from watching
//! the provider set change as users join and leave.
//!
//! Run in two or more terminals. With no arguments, instances discover each other on
//! localhost automatically: each one claims the first free gossip port in a well-known
//! range and seeds the rest of the range.
//!
//! ```sh
//! cargo run -p kameo_remote --example chat
//! cargo run -p kameo_remote --example chat -- alice
//! cargo run -p kameo_remote --example chat -- alice 127.0.0.1:7400 127.0.0.1:7401 [seeds...]
//! ```
//!
//! Arguments: `[username] [<gossip addr> <messaging addr> [seed gossip addr...]]`
//!
//! Exit with ctrl-d to leave gracefully (peers see the departure immediately); a
//! killed process is noticed by the failure detector after a few seconds.

use std::{collections::HashSet, env, error::Error, net::SocketAddr, pin::pin};

use futures::StreamExt;
use kameo::prelude::*;
use kameo_remote::{
    RemoteActor, RemoteActorId, RemoteMessage, RemoteMessages, RemoteNode, RemoteNodeConfig,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;

#[derive(Actor)]
struct ChatUser;

#[derive(Serialize, Deserialize)]
struct ChatMessage {
    from: String,
    body: String,
}

impl Message<ChatMessage> for ChatUser {
    type Reply = ();

    async fn handle(&mut self, msg: ChatMessage, _: &mut Context<Self, Self::Reply>) {
        println!("{}: {}", msg.from, msg.body);
    }
}

impl RemoteMessage for ChatMessage {
    const REMOTE_ID: &'static str = "chat::ChatMessage";
}

impl RemoteActor for ChatUser {
    const REMOTE_ID: &'static str = "chat::ChatUser";

    fn remote_messages(handlers: &mut RemoteMessages<Self>) {
        handlers.add::<ChatMessage>();
    }
}

/// The well-known localhost gossip ports used for zero-config discovery: an instance
/// claims the first free one and seeds the others, so up to 10 instances find each
/// other with no configuration.
const AUTO_PORTS: std::ops::Range<u16> = 7400..7410;

fn auto_network() -> Result<(SocketAddr, Vec<String>), Box<dyn Error>> {
    for port in AUTO_PORTS {
        // Best-effort availability check; bootstrap binds it for real right after.
        if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
            let seeds = AUTO_PORTS
                .filter(|seed_port| *seed_port != port)
                .map(|seed_port| format!("127.0.0.1:{seed_port}"))
                .collect();
            return Ok((SocketAddr::from(([127, 0, 0, 1], port)), seeds));
        }
    }
    Err(format!("no free gossip port in {AUTO_PORTS:?}; the chat room is full").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let username = args
        .next()
        .unwrap_or_else(|| format!("user-{}", &uuid::Uuid::new_v4().simple().to_string()[..4]));
    let (gossip_addr, messaging_addr, seed_nodes) = match args.next() {
        Some(gossip_addr) => {
            let messaging_addr = args.next().expect("missing messaging addr").parse()?;
            (gossip_addr.parse()?, messaging_addr, args.collect())
        }
        None => {
            let (gossip_addr, seeds) = auto_network()?;
            (gossip_addr, "127.0.0.1:0".parse()?, seeds)
        }
    };

    let node = RemoteNode::bootstrap(
        RemoteNodeConfig::default()
            .with_cluster_id("chat-example")
            .with_node_name(&username)
            .with_gossip_addr(gossip_addr)
            .with_messaging_addr(messaging_addr)
            .with_seed_nodes(seed_nodes),
    )
    .await?;

    let user = ChatUser::spawn(ChatUser);
    node.register(&user, "chat").await?;

    // Print join and leave notices from changes to the provider set.
    let presence = node.watch::<ChatUser>("chat").await?;
    let self_id = node.node_id().clone();
    tokio::spawn(async move {
        let mut presence = pin!(presence);
        let mut previous: HashSet<RemoteActorId> = HashSet::new();
        while let Some(providers) = presence.next().await {
            let current: HashSet<RemoteActorId> =
                providers.iter().map(|user| user.id().clone()).collect();
            for joined in current.difference(&previous) {
                if joined.node_id != self_id {
                    println!("* {} joined", joined.node_id);
                }
            }
            for left in previous.difference(&current) {
                if left.node_id != self_id {
                    println!("* {} left", left.node_id);
                }
            }
            previous = current;
        }
    });

    println!("joined the chat as {username}; type a message and press enter");

    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let body = line.trim();
        if body.is_empty() {
            continue;
        }
        for peer in node.lookup_all::<ChatUser>("chat").await? {
            if peer.node_id() == node.node_id() {
                continue;
            }
            let message = ChatMessage {
                from: username.clone(),
                body: body.to_string(),
            };
            if let Err(err) = peer.tell(&message).await {
                println!("* failed to reach {}: {err}", peer.node_id());
            }
        }
    }

    node.shutdown().await?;
    Ok(())
}
