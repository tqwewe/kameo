//! A peer-to-peer chat room with no central server.
//!
//! Every node registers its own user actor under the shared name `chat`. Sending a
//! message looks up all providers and tells each one; presence comes from watching
//! the provider set change as users join and leave.
//!
//! Run in two or more terminals:
//!
//! ```sh
//! cargo run -p kameo_remote --example chat -- alice 127.0.0.1:7400 127.0.0.1:7401
//! cargo run -p kameo_remote --example chat -- bob 127.0.0.1:7410 127.0.0.1:7411 127.0.0.1:7400
//! ```
//!
//! Arguments: `<username> <gossip addr> <messaging addr> [seed gossip addr...]`
//!
//! Exit with ctrl-d to leave gracefully (peers see the departure immediately); a
//! killed process is noticed by the failure detector after a few seconds.

use std::{collections::HashSet, env, error::Error, pin::pin};

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let username = args.next().expect("missing username");
    let gossip_addr = args.next().expect("missing gossip addr").parse()?;
    let messaging_addr = args.next().expect("missing messaging addr").parse()?;
    let seed_nodes: Vec<String> = args.collect();

    let node = RemoteNode::bootstrap(
        RemoteNodeConfig::default()
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
