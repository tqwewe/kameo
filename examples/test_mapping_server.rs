//! Test server using MAPPING syntax in distributed_actor! macro
//! This reproduces the issue with PreBacktestMessage

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo_remote;

mod test_mapping_messages;
use test_mapping_messages::*;

#[derive(Debug)]
struct TestActor {
    message_count: u32,
}

impl Actor for TestActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    
    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🚀 TestActor started");
        Ok(Self { message_count: 0 })
    }
}

// Archived handler for zero-copy
impl TestActor {
    async fn handle_test_data(&mut self, msg: &rkyv::Archived<TestDataMessage>) {
        println!("🎯 MAPPING HANDLER CALLED!");
        self.message_count += 1;
        println!("  Message ID: {}", msg.id);
        println!("  Data count: {}", msg.data.len());
    }
}

// MAPPING syntax - this is what fails
distributed_actor! {
    TestActor {
        TestDataMessage => handle_test_data,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🧪 TEST MAPPING SYNTAX SERVER");
    
    // Use fixed keypair for testing
    let server_keypair = kameo_remote::KeyPair::new_for_testing("test_mapping_server");
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9320".parse()?,
        server_keypair
    ).await?;
    println!("✅ Server on 127.0.0.1:9320 with TLS");
    
    let actor_ref = TestActor::spawn(());
    let actor_id = actor_ref.id();
    
    transport.register_actor("test_actor".to_string(), actor_id).await?;
    println!("✅ Actor registered with ID {:?}", actor_id);
    
    // Add client as trusted peer for TLS
    if let Some(handle) = transport.handle() {
        let client_peer_id = kameo_remote::PeerId::new("test_mapping_client");
        let _peer = handle.add_peer(&client_peer_id).await;
        println!("✅ Added client as trusted peer for TLS");
    }
    
    println!("📡 Ready. Run: cargo run --example test_mapping_client --features remote");
    tokio::signal::ctrl_c().await?;
    Ok(())
}