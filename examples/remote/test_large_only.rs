//! Minimal test for large message handling
//!
//! Server: cargo run --example test_large_only --features remote -- server
//! Client: cargo run --example test_large_only --features remote -- client

#![allow(dead_code, unused_variables)]

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::remote::transport::RemoteTransport;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct LargeTestMessage {
    pub size_mb: usize,
    pub data: Vec<u8>,
}

#[derive(Debug)]
struct TestActor {
    count: u32,
}

impl Actor for TestActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { count: 0 })
    }
}

impl TestActor {
    async fn handle_large(&mut self, msg: &rkyv::Archived<LargeTestMessage>) {
        self.count += 1;
        // Access archived fields - they're already the right type
        let size_mb = msg.size_mb;
        let data_len = msg.data.len();
        println!(
            "✅ RECEIVED: {}MB message (actual: {} bytes, count: {})",
            size_mb, data_len, self.count
        );
    }
}

distributed_actor! {
    TestActor {
        LargeTestMessage => handle_large,
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=warn")
        .try_init();

    println!("🚀 Starting Large Message Test Server...");

    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9400".parse()?).await?;
    println!("✅ Server listening on {}", transport.local_addr());

    let actor_ref = TestActor::spawn(());
    let actor_id = actor_ref.id();

    // Register with transport - automatically handles distributed ask/reply
    transport
        .register_distributed_actor("test_large".to_string(), &actor_ref)
        .await?;

    use kameo::remote::type_hash::HasTypeHash;
    println!(
        "🔍 Server LargeTestMessage type hash: {:08x}",
        <LargeTestMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!("✅ TestActor registered with ID {:?}", actor_id);

    // Add client as peer
    if let Some(handle) = transport.handle() {
        let _peer = handle
            .add_peer(&kameo_remote::PeerId::new("kameo_node_9401"))
            .await;
        println!("✅ Added client node as peer");
    }

    println!("\n📡 Waiting for messages... Press Ctrl+C to exit\n");

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_client() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=warn")
        .try_init();

    println!("🚀 Starting Large Message Test Client...");

    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9401".parse()?).await?;
    println!("✅ Client listening on {}", transport.local_addr());

    // Connect to server
    if let Some(handle) = transport.handle() {
        let peer = handle
            .add_peer(&kameo_remote::PeerId::new("kameo_node_9400"))
            .await;
        peer.connect(&"127.0.0.1:9400".parse()?).await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Lookup the actor
    let actor = kameo::remote::DistributedActorRef::lookup("test_large")
        .await?
        .ok_or("TestActor not found")?;

    use kameo::remote::type_hash::HasTypeHash;
    println!(
        "🔍 Client LargeTestMessage type hash: {:08x}",
        <LargeTestMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!("✅ Found TestActor on server");

    // Send different sized messages
    for (i, size_mb) in [(1, 1)].iter() {
        println!("\n📤 Test {}: Sending {}MB message...", i, size_mb);
        let data = vec![42u8; size_mb * 1024 * 1024];
        let msg = LargeTestMessage {
            size_mb: *size_mb,
            data,
        };

        let start = std::time::Instant::now();
        actor.tell(msg).send().await?;
        println!("   ✅ Sent in {:?}", start.elapsed());

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    println!("\n✅ All messages sent!");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} [server|client]", args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "server" => run_server().await,
        "client" => run_client().await,
        _ => {
            eprintln!("Invalid argument. Use 'server' or 'client'");
            std::process::exit(1);
        }
    }
}
