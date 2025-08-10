//! Test large message handling in kameo
//! 
//! This example tests if kameo can properly handle messages larger than the buffer size.
//! Run the server first, then the client:
//! cargo run --example test_large_messages --features remote -- server
//! cargo run --example test_large_messages --features remote -- client

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use kameo::distributed_actor;
use kameo::RemoteMessage;
use kameo::remote::transport::RemoteTransport;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use std::time::Instant;

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct LargeMessage {
    pub id: u32,
    pub size_mb: usize,
    pub data: Vec<u8>,
}

#[derive(Debug)]
struct TestActor {
    messages_received: u32,
}

impl Actor for TestActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    
    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { messages_received: 0 })
    }
}

impl TestActor {
    async fn handle_large_message(&mut self, msg: &rkyv::Archived<LargeMessage>) {
        self.messages_received += 1;
        let size_mb = msg.size_mb;
        let data_len = msg.data.len();
        println!("✅ Received {}MB message (id: {}, actual size: {} bytes)", 
                 size_mb, msg.id, data_len);
    }
}

distributed_actor! {
    TestActor {
        LargeMessage => handle_large_message,
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Starting Large Message Test Server...");
    
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9320".parse()?).await?;
    println!("✅ Server listening on {}", transport.local_addr());
    
    let actor_ref = TestActor::spawn(());
    let actor_id = actor_ref.id();
    
    transport.register_actor("test_actor".to_string(), actor_id).await?;
    
    let handler = kameo::remote::v2_bootstrap::get_distributed_handler();
    handler.registry().register(actor_id, actor_ref.clone());
    
    println!("✅ TestActor registered");
    println!("\n📡 Waiting for messages... Press Ctrl+C to exit\n");
    
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_client() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Starting Large Message Test Client...");
    
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9321".parse()?).await?;
    println!("✅ Client listening on {}", transport.local_addr());
    
    // Connect to server
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&kameo_remote::PeerId::new("kameo_node_9320")).await;
        peer.connect(&"127.0.0.1:9320".parse()?).await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Lookup the actor
    let actor = kameo::remote::DistributedActorRef::lookup("test_actor", transport.clone())
        .await?
        .ok_or("TestActor not found")?;
    
    println!("✅ Found TestActor on server");
    
    // Test different message sizes
    let test_sizes = vec![
        (1, 1),      // 1MB
        (2, 5),      // 5MB
        (3, 10),     // 10MB
        (4, 35),     // 35MB (like PreBacktest)
        (5, 50),     // 50MB
    ];
    
    for (id, size_mb) in test_sizes {
        println!("\n📤 Sending {}MB message...", size_mb);
        let data = vec![42u8; size_mb * 1024 * 1024];
        let msg = LargeMessage {
            id,
            size_mb,
            data,
        };
        
        let start = Instant::now();
        actor.tell(msg).send().await?;
        let elapsed = start.elapsed();
        
        println!("   ✅ Sent in {:?} ({:.2} MB/s)", 
                 elapsed, 
                 (size_mb as f64) / elapsed.as_secs_f64());
        
        // Give server time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    println!("\n✅ All messages sent successfully!");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,kameo=warn")
        .try_init();
    
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