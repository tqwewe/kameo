#!//! Simple authentication test server - just listens for connections
//! 
//! Run with:
//! cargo run --example auth_test_server --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::remote::{transport::RemoteTransport, DistributedActor};
use kameo::remote::v2_bootstrap;
use std::time::Duration;

// Simple actor that just exists to be looked up
struct TestActor;

impl Actor for TestActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🎬 TestActor started!");
        Ok(Self)
    }
}

impl DistributedActor for TestActor {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=warn")
        .try_init();

    println!("\n🔒 === AUTHENTICATION TEST SERVER ===");

    // Create server keypair (seed 42)
    let server_keypair = kameo_remote::KeyPair::new_for_testing("42");
    let server_peer_id = server_keypair.peer_id();
    println!("🔑 Server PeerId: {}", server_peer_id);
    
    // Start transport with keypair
    let mut transport = v2_bootstrap::bootstrap_with_keypair("127.0.0.1:9310".parse()?, server_keypair).await?;
    println!("✅ Server listening on 127.0.0.1:9310");
    
    // Start the transport
    transport.start().await?;
    
    // Create and register test actor
    let test_actor_ref = <TestActor as Actor>::spawn(());
    transport.register_actor("test_actor".to_string(), test_actor_ref.id()).await?;
    println!("✅ TestActor registered");
    
    println!("\n📡 Server ready and waiting for connections...");
    println!("   Authentication will be enforced on incoming connections");
    println!("\n💤 Server will run for 30 seconds...\n");
    
    // Wait for test duration
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    println!("\n🛑 Server shutting down...");
    Ok(())
}
