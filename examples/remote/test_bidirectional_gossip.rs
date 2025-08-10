//! Test to diagnose bidirectional gossip synchronization issue
//! 
//! This test verifies that when a client connects to a server and registers
//! an actor with Immediate priority, the server can immediately lookup the client.

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};
use std::time::Instant;

// Simple test actor
struct TestActor {
    name: String,
    peer_ref: Option<DistributedActorRef>,
}

impl Actor for TestActor {
    type Args = String;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(name: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🎬 {} started", name);
        Ok(Self {
            name,
            peer_ref: None,
        })
    }
}

#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Ping {
    from: String,
    timestamp: u64,
}

#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Pong {
    from: String,
    timestamp: u64,
}

impl TestActor {
    async fn handle_ping(&mut self, msg: &rkyv::Archived<Ping>) {
        println!("📥 {} received Ping from {}", self.name, msg.from);
        
        // Try to send Pong back if we have peer reference
        if let Some(peer_ref) = &self.peer_ref {
            let pong = Pong {
                from: self.name.clone(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };
            
            if let Err(e) = peer_ref.tell(pong).send().await {
                println!("❌ {} failed to send Pong: {:?}", self.name, e);
            } else {
                println!("📤 {} sent Pong back", self.name);
            }
        } else {
            println!("⚠️ {} has no peer reference to send Pong", self.name);
        }
    }
    
    async fn handle_pong(&mut self, msg: &rkyv::Archived<Pong>) {
        println!("📥 {} received Pong from {}", self.name, msg.from);
    }
}

distributed_actor! {
    TestActor {
        Ping => handle_ping,
        Pong => handle_pong,
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🚀 === SERVER STARTING ===");
    
    // Bootstrap server on port 9500
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9500".parse()?).await?;
    println!("✅ Server listening on {}", transport.local_addr());
    
    // Create and register server actor
    let server_actor = TestActor::spawn("Server".to_string());
    let server_id = server_actor.id();
    
    transport.register_actor("server".to_string(), server_id).await?;
    println!("✅ Server actor registered with ID {:?}", server_id);
    
    // Add client as peer (for bidirectional connection)
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&kameo_remote::PeerId::new("kameo_node_9501")).await;
        let _ = peer.connect(&"127.0.0.1:9501".parse()?).await;
        println!("✅ Added client node as peer");
    }
    
    // Try to lookup client immediately and periodically
    let lookup_start = Instant::now();
    let mut found = false;
    
    // Also check the raw registry directly
    let registry_handle = transport.handle().unwrap();
    
    for attempt in 1..=20 {
        println!("🔍 [SERVER] Lookup attempt #{} at {:?}", attempt, lookup_start.elapsed());
        
        // First check raw registry
        let raw_lookup = registry_handle.lookup("client").await;
        println!("   Raw registry lookup: {:?}", raw_lookup.is_some());
        
        match DistributedActorRef::lookup("client").await {
            Ok(Some(client_ref)) => {
                println!("✅ [SERVER] Found client after {:?}!", lookup_start.elapsed());
                println!("   Client actor ID: {:?}", client_ref.id());
                found = true;
                
                // Send a Ping to client
                let ping = Ping {
                    from: "Server".to_string(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };
                
                if let Err(e) = client_ref.tell(ping).send().await {
                    println!("❌ Server failed to send Ping: {:?}", e);
                } else {
                    println!("📤 Server sent Ping to client");
                }
                
                break;
            }
            Ok(None) => {
                println!("⏳ [SERVER] Client not found yet");
            }
            Err(e) => {
                println!("❌ [SERVER] Lookup error: {:?}", e);
            }
        }
        
        // Wait 100ms before next attempt
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    
    if !found {
        println!("❌ [SERVER] Failed to find client after 2 seconds!");
    }
    
    // Keep server running
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    
    Ok(())
}

async fn run_client() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Wait a bit for server to start
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    
    println!("\n🚀 === CLIENT STARTING ===");
    
    // Bootstrap client on port 9501
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9501".parse()?).await?;
    println!("✅ Client listening on {}", transport.local_addr());
    
    // Connect to server
    println!("📡 Connecting to server at 127.0.0.1:9500...");
    if let Some(handle) = transport.handle() {
        let peer = handle.add_peer(&kameo_remote::PeerId::new("kameo_node_9500")).await;
        peer.connect(&"127.0.0.1:9500".parse()?).await?;
        println!("✅ Connected to server");
    }
    
    // Create and register client actor WITH IMMEDIATE PRIORITY
    let client_actor = TestActor::spawn("Client".to_string());
    let client_id = client_actor.id();
    
    // Register with IMMEDIATE priority (using our modified register_actor that uses Immediate)
    let register_start = Instant::now();
    transport.register_actor("client".to_string(), client_id).await?;
    println!("✅ Client actor registered with ID {:?} (took {:?})", client_id, register_start.elapsed());
    println!("   Registration should trigger IMMEDIATE gossip sync!");
    
    // Now immediately try to send a message to server to establish connection
    println!("\n🔍 Client looking up server...");
    match DistributedActorRef::lookup("server").await? {
        Some(server_ref) => {
            println!("✅ Client found server!");
            
            // Send a Ping to establish connection
            let ping = Ping {
                from: "Client".to_string(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };
            
            println!("📤 Client sending Ping to server...");
            if let Err(e) = server_ref.tell(ping).send().await {
                println!("❌ Client failed to send Ping: {:?}", e);
            } else {
                println!("✅ Client sent Ping - this should trigger gossip sync!");
            }
        }
        None => {
            println!("❌ Client couldn't find server!");
        }
    }
    
    // Keep client running
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable debug logging for kameo_remote
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug,kameo=info")
        .try_init();
    
    println!("🧪 === BIDIRECTIONAL GOSSIP SYNC TEST ===");
    println!("This test verifies that Immediate priority registration triggers instant gossip sync\n");
    
    // Start server and client in parallel
    let server_handle = tokio::spawn(run_server());
    let client_handle = tokio::spawn(run_client());
    
    // Wait for both to complete
    let (server_result, client_result) = tokio::join!(server_handle, client_handle);
    
    if let Err(e) = server_result {
        println!("❌ Server error: {:?}", e);
    }
    if let Err(e) = client_result {
        println!("❌ Client error: {:?}", e);
    }
    
    println!("\n📊 === TEST COMPLETE ===");
    println!("Expected: Server should find client immediately after client registers with Immediate priority");
    println!("If server takes multiple attempts, there's a gossip sync issue.");
    
    Ok(())
}