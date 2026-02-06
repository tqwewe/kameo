//! Test server using LIST syntax in distributed_actor! macro
//! This should work correctly

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
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

// Message trait implementation
impl Message<TestDataMessage> for TestActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: TestDataMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("📨 LIST HANDLER CALLED (Message trait)!");
        self.message_count += 1;
        println!("  Message ID: {}", msg.id);
        println!("  Data count: {}", msg.data.len());
    }
}

// LIST syntax - this should work
distributed_actor! {
    TestActor {
        TestDataMessage,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🧪 TEST LIST SYNTAX SERVER");

    // Use fixed keypair for testing
    let server_keypair = kameo_remote::KeyPair::new_for_testing("test_list_server");
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9330".parse()?,
        server_keypair,
    )
    .await?;
    println!("✅ Server on 127.0.0.1:9330 with TLS");

    let actor_ref = <TestActor as Actor>::spawn(());

    // Register with transport - automatically handles registration
    transport
        .register_distributed_actor("test_actor".to_string(), &actor_ref)
        .await?;
    let actor_id = actor_ref.id();

    println!("✅ Actor registered with ID {:?}", actor_id);

    // Add client as trusted peer for TLS
    if let Some(handle) = transport.handle() {
        let client_peer_id = kameo_remote::KeyPair::new_for_testing("test_list_client").peer_id();
        let _peer = handle.add_peer(&client_peer_id).await;
        println!("✅ Added client as trusted peer for TLS");
    }

    println!("📡 Ready. Run: cargo run --example test_list_client --features remote");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
