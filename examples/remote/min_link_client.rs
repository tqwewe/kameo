//! Minimal remote link client (kameo).
//!
//! Run after server:
//!   cargo run --release --example min_link_client --features remote /tmp/kameo_tls/min_link_server.pub

use kameo::{distributed_actor, Actor};
use kameo::actor::{ActorRef, ActorId, WeakActorRef};
use kameo::error::ActorStopReason;
use kameo::remote::v2_bootstrap;
use kameo::remote::transport::BoxError;
use kameo_remote::{PeerId, SecretKey};
use kameo_remote::tls;
use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

const CLIENT_NAME: &str = "min_link_client";

struct LinkActor;

#[derive(kameo::RemoteMessage, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug)]
struct Noop;

impl kameo::message::Message<Noop> for LinkActor {
    type Reply = ();

    async fn handle(&mut self, _msg: Noop, _ctx: &mut kameo::message::Context<Self, Self::Reply>) {
    }
}

distributed_actor! {
    LinkActor {
        Noop,
    }
}

impl Actor for LinkActor {
    type Args = ();
    type Error = LinkError;

    async fn on_start(
        _args: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self)
    }

    async fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorId,
        reason: ActorStopReason,
    ) -> Result<std::ops::ControlFlow<ActorStopReason>, Self::Error> {
        tracing::info!(
            local_actor_id = ?actor_ref.id(),
            remote_actor_id = ?id,
            reason = ?reason,
            "🔌 [CLIENT] on_link_died"
        );
        Ok(std::ops::ControlFlow::Continue(()))
    }

    async fn on_link_established(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorId,
    ) -> Result<std::ops::ControlFlow<ActorStopReason>, Self::Error> {
        tracing::info!(
            local_actor_id = ?actor_ref.id(),
            remote_actor_id = ?id,
            "🔗 [CLIENT] on_link_established"
        );
        Ok(std::ops::ControlFlow::Continue(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), LinkError> {
    tls::ensure_crypto_provider();
    tracing_subscriber::fmt().init();

    println!("🔗 Minimal Remote Link Client");
    println!("=============================\n");

    let args: Vec<String> = env::args().collect();
    let server_pub_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "/tmp/kameo_tls/min_link_server.pub".to_string());
    let server_addr = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:29200".to_string());

    let server_node_id = load_node_id(&server_pub_path)?;
    println!("Server NodeId: {}", server_node_id.fmt_short());
    println!("Server key: {}\n", server_pub_path);

    let key_path = "/tmp/kameo_tls/min_link_client.key";
    let secret_key = load_or_generate_key(key_path)?;
    let client_addr = "127.0.0.1:0"
        .parse::<std::net::SocketAddr>()
        .map_err(|e| LinkError::ParseAddr(e.to_string()))?;
    let transport =
        v2_bootstrap::bootstrap_with_keypair(client_addr, secret_key.to_keypair()).await?;

    let client_ref: ActorRef<LinkActor> = <LinkActor as Actor>::spawn(());

    // Connect to server via gossip BEFORE registering (avoid duplicate connection attempts).
    if let Some(handle) = transport.handle() {
        let server_peer_id = PeerId::from_public_key(&server_node_id);
        let peer = handle.add_peer(&server_peer_id).await;
        let server_addr = server_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| LinkError::ParseAddr(e.to_string()))?;
        peer.connect(&server_addr).await?;
    }

    transport
        .register_distributed_actor_sync(
            CLIENT_NAME.to_string(),
            &client_ref,
            Duration::from_secs(2),
        )
        .await
        .map_err(|e| LinkError::Transport(BoxError::from(e)))?;

    println!("Press Ctrl+C to terminate (force stop)\n");

    // Force termination on Ctrl+C (simulate kill)
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("🛑 [CLIENT] Ctrl+C received, exiting immediately");
        std::process::exit(1);
    });

    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

fn load_node_id(path: &str) -> Result<kameo_remote::NodeId, LinkError> {
    let pub_key_hex = fs::read_to_string(path)?;
    let pub_key_bytes = hex::decode(pub_key_hex.trim())?;

    if pub_key_bytes.len() != 32 {
        return Err(LinkError::InvalidKeyLength(pub_key_bytes.len()));
    }

    kameo_remote::NodeId::from_bytes(&pub_key_bytes)
        .map_err(|e| LinkError::InvalidNodeId(e.to_string()))
}

fn load_or_generate_key(path: &str) -> Result<SecretKey, LinkError> {
    let key_path = Path::new(path);

    if key_path.exists() {
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;

        if key_bytes.len() != 32 {
            return Err(LinkError::InvalidKeyLength(key_bytes.len()));
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let secret_key = SecretKey::generate();
        fs::write(key_path, hex::encode(secret_key.to_bytes()))?;
        Ok(secret_key)
    }
}

#[derive(Debug, Error)]
enum LinkError {
    #[error("invalid key length: expected 32, got {0}")]
    InvalidKeyLength(usize),
    #[error("invalid node id: {0}")]
    InvalidNodeId(String),
    #[error("invalid bind address: {0}")]
    ParseAddr(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Hex(#[from] hex::FromHexError),
    #[error(transparent)]
    Gossip(#[from] kameo_remote::GossipError),
    #[error(transparent)]
    Transport(#[from] BoxError),
    #[error(transparent)]
    Registry(#[from] kameo::error::RegistryError),
}
