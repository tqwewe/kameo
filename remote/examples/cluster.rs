//! A two-node cluster where each node increments the other's counter.
//!
//! Run in two terminals:
//!
//! ```sh
//! cargo run -p kameo_remote --example cluster -- 127.0.0.1:7400 127.0.0.1:7401
//! cargo run -p kameo_remote --example cluster -- 127.0.0.1:7410 127.0.0.1:7411 127.0.0.1:7400
//! ```
//!
//! Arguments: `<gossip addr> <messaging addr> [seed gossip addr...]`

use std::{env, error::Error, time::Duration};

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
        println!("+{} from {} (count: {})", msg.amount, msg.from, self.count);
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
    println!("started {} on {gossip_addr}", node.node_id());

    let incrementor = Incrementor::spawn(Incrementor { count: 0 });
    node.register(&incrementor, "incrementor").await?;

    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        for remote in node.lookup_all::<Incrementor>("incrementor").await? {
            if remote.node_id() == node.node_id() {
                continue;
            }
            match remote
                .ask(&Inc {
                    amount: 1,
                    from: node.node_id().to_string(),
                })
                .await
            {
                Ok(count) => println!("{} is now at {count}", remote.node_id()),
                Err(err) => println!("failed to increment {}: {err}", remote.node_id()),
            }
        }
    }
}
