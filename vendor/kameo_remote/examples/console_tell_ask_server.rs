use anyhow::Result;
use kameo_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use kameo_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Console tell/ask server (TLS).
///
/// Usage:
///   cargo run --example console_tell_ask_server
///
/// Then in another terminal:
///   cargo run --example console_tell_ask_client /tmp/kameo_tls/console_tell_ask_server.pub
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt().init();

    println!("🔐 Console Tell/Ask Server (TLS)");
    println!("================================\n");

    // Load or generate server TLS keypair
    let key_path = "/tmp/kameo_tls/console_tell_ask_server.key";
    let secret_key = load_or_generate_key(key_path)?;
    let node_id = secret_key.public();

    let pub_path = key_path.replace(".key", ".pub");
    fs::write(&pub_path, hex::encode(node_id.as_bytes()))?;

    println!("Server NodeId: {}", node_id.fmt_short());
    println!("Public key: {}\n", pub_path);

    let server_addr = "127.0.0.1:29200".parse()?;
    let registry =
        GossipRegistryHandle::new_with_tls(server_addr, secret_key, Some(GossipConfig::default()))
            .await?;

    registry
        .registry
        .set_actor_message_handler(Arc::new(ConsoleActorHandler))
        .await;

    println!("✅ Listening on: {}", server_addr);
    println!("✅ Actor handler ready: console_echo\n");
    println!("Run the client in another terminal:");
    println!("  cargo run --example console_tell_ask_client {}", pub_path);
    println!("Press Ctrl+C to stop\n");

    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

struct ConsoleActorHandler;

impl ActorMessageHandler for ConsoleActorHandler {
    fn handle_actor_message(
        &self,
        actor_id: &str,
        type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let actor_id = actor_id.to_string();
        let payload = payload.to_vec();

        Box::pin(async move {
            let payload_str = String::from_utf8_lossy(&payload);
            if correlation_id.is_some() {
                let reply = format!("reply:{}:{}", actor_id, payload_str);
                Ok(Some(reply.into_bytes()))
            } else {
                Ok(None)
            }
        })
    }
}

fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);

    if key_path.exists() {
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;

        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid key length: expected 32, got {}",
                key_bytes.len()
            ));
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
