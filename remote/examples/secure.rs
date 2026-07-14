//! A fully secured two-node cluster: mutual TLS on messaging plus a pre-shared
//! cluster key for gossip encryption and connection authentication.
//!
//! The CA and node certificates are generated in-memory with rcgen for the demo; a
//! real deployment would provision PEM files and load them with
//! [`TlsConfig::from_pem_files`], and distribute the cluster key through a secret
//! manager.
//!
//! ```sh
//! cargo run --example secure --features tls
//! ```

use kameo::prelude::*;
use kameo_remote::{
    ClusterKey, RemoteActor, RemoteMessage, RemoteMessages, RemoteNode, RemoteNodeConfig, TlsConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Actor)]
struct Counter {
    count: i64,
}

#[derive(Serialize, Deserialize)]
struct Inc {
    amount: i64,
}

impl Message<Inc> for Counter {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _: &mut Context<Self, Self::Reply>) -> i64 {
        self.count += msg.amount;
        self.count
    }
}

impl RemoteMessage for Inc {
    const REMOTE_ID: &'static str = "example::Inc";
}

impl RemoteActor for Counter {
    const REMOTE_ID: &'static str = "example::Counter";

    fn remote_messages(handlers: &mut RemoteMessages<Self>) {
        handlers.add::<Inc>();
    }
}

/// Generates a cluster CA and issues a node certificate signed by it.
fn issue_node_tls(
    ca_key: &rcgen::KeyPair,
    ca_cert: &rcgen::Certificate,
) -> Result<TlsConfig, Box<dyn std::error::Error>> {
    let key = rcgen::KeyPair::generate()?;
    let params = rcgen::CertificateParams::new(vec!["node".to_string()])?;
    let cert = params.signed_by(&key, ca_cert, ca_key)?;
    Ok(TlsConfig::from_pem(
        ca_cert.pem().as_bytes(),
        cert.pem().as_bytes(),
        key.serialize_pem().as_bytes(),
    )?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .init();

    // One CA for the cluster; every node gets its own CA-signed certificate.
    let ca_key = rcgen::KeyPair::generate()?;
    let mut ca_params = rcgen::CertificateParams::new(Vec::new())?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key)?;

    // One pre-shared key for the cluster, securing gossip and authenticating peers.
    let cluster_key = ClusterKey::generate();

    let node_a = RemoteNode::bootstrap(
        RemoteNodeConfig::default()
            .with_gossip_addr("127.0.0.1:7400".parse()?)
            .with_messaging_addr("127.0.0.1:7401".parse()?)
            .with_cluster_key(cluster_key.clone())
            .with_tls(issue_node_tls(&ca_key, &ca_cert)?),
    )
    .await?;

    let node_b = RemoteNode::bootstrap(
        RemoteNodeConfig::default()
            .with_gossip_addr("127.0.0.1:7402".parse()?)
            .with_messaging_addr("127.0.0.1:7403".parse()?)
            .with_seed_nodes(["127.0.0.1:7400"])
            .with_cluster_key(cluster_key)
            .with_tls(issue_node_tls(&ca_key, &ca_cert)?),
    )
    .await?;

    let counter = Counter::spawn(Counter { count: 0 });
    node_a.register(&counter, "counter").await?;

    // Wait for gossip to replicate the registration to node b.
    let remote = loop {
        if let Some(remote) = node_b.lookup::<Counter>("counter").await? {
            break remote;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };

    let count = remote.ask(&Inc { amount: 1 }).await?;
    println!("count over mutual tls: {count}");

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}
