//! Minimal remote link server (kameo).
//!
//! Run:
//!   cargo run --release --example min_link_server --features remote
//!
//! Then in another terminal:
//!   cargo run --release --example min_link_client --features remote /tmp/kameo_tls/min_link_server.pub

use kameo::{distributed_actor, Actor};
use kameo::actor::{ActorRef, ActorId, WeakActorRef};
use kameo::error::ActorStopReason;
use kameo::remote::v2_bootstrap;
use kameo::remote::transport::BoxError;
use kameo_remote::SecretKey;
use kameo_remote::tls;
use std::fs;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

const SERVER_NAME: &str = "min_link_server";
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
            "🔌 [SERVER] on_link_died"
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
            "🔗 [SERVER] on_link_established"
        );
        Ok(std::ops::ControlFlow::Continue(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), LinkError> {
    tls::ensure_crypto_provider();
    tracing_subscriber::fmt().init();

    println!("🔗 Minimal Remote Link Server");
    println!("=============================\n");

    let key_path = "/tmp/kameo_tls/min_link_server.key";
    let secret_key = load_or_generate_key(key_path)?;
    let node_id = secret_key.public();
    let pub_path = key_path.replace(".key", ".pub");
    fs::write(&pub_path, hex::encode(node_id.as_bytes()))?;

    let server_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:29200".to_string())
        .parse::<std::net::SocketAddr>()
        .map_err(|e| LinkError::ParseAddr(e.to_string()))?;
    let transport =
        v2_bootstrap::bootstrap_with_keypair(server_addr, secret_key.to_keypair()).await?;

    let server_ref: ActorRef<LinkActor> = <LinkActor as Actor>::spawn(());
    transport
        .register_distributed_actor_sync(
            SERVER_NAME.to_string(),
            &server_ref,
            Duration::from_secs(2),
        )
        .await
        .map_err(|e| LinkError::Transport(BoxError::from(e)))?;

    println!("✅ Listening on: {}", server_addr);
    println!("✅ Server actor registered as '{}'", SERVER_NAME);
    println!("Public key: {}", pub_path);
    println!("Run the client in another terminal:");
    println!(
        "  cargo run --release --example min_link_client --features remote {} {}",
        pub_path, server_addr
    );
    println!("Immediate disconnect test:");
    println!("  Kill the client with Ctrl+C or SIGKILL; server should log on_link_died immediately.");
    println!("Idle/blackhole test (no heartbeats):");
    println!("  Block TCP traffic to simulate a dead peer; keepalive should detect within ~2s.");
    println!("Press Ctrl+C to terminate (force stop)\n");

    // Force termination on Ctrl+C (simulate kill)
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("🛑 [SERVER] Ctrl+C received, exiting immediately");
        std::process::exit(1);
    });

    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
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
