//! Minimal test client that demonstrates distributed actor registration/lookup issue

use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import same message from server
#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Ping;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,kameo_remote=debug")
        .try_init();

    println!("Starting client on port 9321...");

    // Bootstrap transport on port 9321 WITH TLS
    let client_keypair = kameo_remote::KeyPair::new_for_testing("test_client_key");
    println!("🔐 Client using Ed25519 keypair for TLS encryption");

    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9321".parse()?,
        client_keypair,
    )
    .await?;

    println!("Client transport ready on {}", transport.local_addr());

    // Connect to server with TLS authentication
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("test_server_key");
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9320".parse()?).await?;
        println!("✅ Connected to server with TLS encryption and mutual authentication");
    }

    // Wait for server to register the actor
    println!("Waiting for server to register actor...");
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    // Attempt to lookup the distributed actor
    println!("Looking up distributed actor 'test_distributed_actor'...");

    let actor_name = "test_distributed_actor";
    match DistributedActorRef::lookup(actor_name).await {
        Ok(Some(actor_ref)) => {
            println!("SUCCESS: Found distributed actor!");
            println!("Actor ID: {}", actor_ref.id());

            // Try sending a message using tell (one-way)
            match actor_ref.tell(Ping).send().await {
                Ok(_) => {
                    println!("Successfully sent Ping message to actor");
                }
                Err(e) => {
                    println!("Failed to send message: {:?}", e);
                }
            }
        }
        Ok(None) => {
            println!(
                "FAILURE: Actor '{}' not found in distributed registry",
                actor_name
            );
            println!("This reproduces the issue - registration not propagated to client");
            std::process::exit(1);
        }
        Err(e) => {
            println!("ERROR: Failed to lookup actor: {:?}", e);
            std::process::exit(1);
        }
    }

    println!("Test completed successfully");
    Ok(())
}
