//! Minimal test server that demonstrates distributed actor registration/lookup issue

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::transport::RemoteTransport;

// Simple test actor
struct TestActor {
    name: String,
}

impl Actor for TestActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: "TestServer".to_string(),
        })
    }
}

// Simple message - must derive RemoteMessage for distributed use
#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Ping;

// Handler implementation
impl TestActor {
    async fn handle_ping(&mut self, _msg: &rkyv::Archived<Ping>) -> String {
        format!("Pong from {}", self.name)
    }
}

// Register with distributed actor macro - THIS IS KEY!
distributed_actor! {
    TestActor {
        Ping => handle_ping,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,kameo_remote=debug")
        .try_init();

    println!("Starting server on port 9320...");

    // Bootstrap transport on port 9320 WITH TLS
    let server_keypair = kameo_remote::KeyPair::new_for_testing("test_server_key");
    println!("🔐 Server using Ed25519 keypair for TLS encryption");

    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9320".parse()?,
        server_keypair,
    )
    .await?;

    println!("Server transport ready on {}", transport.local_addr());

    // Create and spawn the test actor using macro-generated spawn
    let actor_ref = <TestActor as Actor>::spawn(());

    println!("Actor spawned with ID: {}", actor_ref.id());

    // Add client as known peer for TLS authentication
    if let Some(handle) = transport.handle() {
        let client_peer_id = kameo_remote::KeyPair::new_for_testing("test_client_key").peer_id();
        let _client_peer = handle.add_peer(&client_peer_id).await;
        println!("✅ Server registered client as known peer for TLS authentication");
    }

    // Wait briefly then register
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Register actor with transport
    let actor_name = "test_distributed_actor";
    transport
        .register_actor(actor_name.to_string(), actor_ref.id())
        .await?;

    println!("Actor registered as '{}'", actor_name);
    println!("Waiting for client to lookup the actor...");

    // Keep server running
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    println!("Server shutting down");
    Ok(())
}
