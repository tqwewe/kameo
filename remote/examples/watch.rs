//! Watches the live providers of the `incrementor` name from the `cluster` example.
//!
//! Start one or more `cluster` example nodes, then:
//!
//! ```sh
//! cargo run -p kameo_remote --example watch -- 127.0.0.1:7420 127.0.0.1:7421 127.0.0.1:7400
//! ```
//!
//! Arguments: `<gossip addr> <messaging addr> [seed gossip addr...]`

use std::{env, error::Error};

use futures::StreamExt;
use kameo::prelude::*;
use kameo_remote::{RemoteActor, RemoteMessage, RemoteMessages, RemoteNode, RemoteNodeConfig};
use serde::{Deserialize, Serialize};

#[derive(Actor)]
struct Incrementor {
    count: i64,
}

#[derive(Serialize, Deserialize)]
struct Inc {
    amount: i64,
    from: String,
}

impl Message<Inc> for Incrementor {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _: &mut Context<Self, Self::Reply>) -> i64 {
        self.count += msg.amount;
        self.count
    }
}

impl RemoteMessage for Inc {
    const REMOTE_ID: &'static str = "cluster::Inc";
}

impl RemoteActor for Incrementor {
    const REMOTE_ID: &'static str = "cluster::Incrementor";

    fn remote_messages(handlers: &mut RemoteMessages<Self>) {
        handlers.add::<Inc>();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let gossip_addr = args.next().expect("missing gossip addr").parse()?;
    let messaging_addr = args.next().expect("missing messaging addr").parse()?;
    let seed_nodes: Vec<String> = args.collect();

    let node = RemoteNode::bootstrap(
        RemoteNodeConfig::default()
            .with_gossip_addr(gossip_addr)
            .with_messaging_addr(messaging_addr)
            .with_seed_nodes(seed_nodes),
    )
    .await?;
    println!("watching from {}", node.node_id());

    let mut providers = std::pin::pin!(node.watch::<Incrementor>("incrementor").await);
    while let Some(providers) = providers.next().await {
        println!(
            "providers ({}): {:?}",
            providers.len(),
            providers
                .iter()
                .map(|provider| provider.id().to_string())
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}
