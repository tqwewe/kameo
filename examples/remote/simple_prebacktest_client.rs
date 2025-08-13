//! Simplified client to test PreBacktestMessage without FxHashMap

use kameo::remote::transport::RemoteTransport;
use kameo::remote::distributed_actor_ref::DistributedActorRef;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use std::borrow::Cow;
use std::time::Instant;

// Simplified message without HashMap
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct SimpleMessage {
    pub symbol_base: String,
    pub symbol_quote: String,
    pub candle_count: u32,
    pub test_value: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🚀 === SIMPLE MESSAGE TEST CLIENT ===");
    
    let client_keypair = kameo_remote::KeyPair::new_for_testing("simple_client_test_key");
    
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9343".parse()?,
        client_keypair,
    ).await?;
    
    println!("✅ Client listening on {}", transport.local_addr());
    
    // Connect to server
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("simple_server_test_key");
        let peer = handle.add_peer(&server_peer_id).await;
        peer.connect(&"127.0.0.1:9342".parse()?).await?;
        println!("✅ Connected to server");
    }
    
    // Wait for gossip
    println!("⏳ Waiting for gossip propagation...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    
    let simple_actor = match DistributedActorRef::lookup("simple_actor").await? {
        Some(ref_) => {
            println!("✅ Found SimpleActor");
            ref_
        }
        None => {
            return Err("Actor not found".into());
        }
    };
    
    // Send simple message
    println!("\n📤 Sending SimpleMessage...");
    println!("   Actor ID: {:?}", simple_actor.id());
    
    let msg = SimpleMessage {
        symbol_base: "BTC".to_string(),
        symbol_quote: "USDT".to_string(),
        candle_count: 100,
        test_value: 50000.0,
    };
    
    println!("📨 Calling tell()...");
    let tell_request = simple_actor.tell(msg);
    
    println!("📮 Calling send()...");
    let send_start = Instant::now();
    match tell_request.send().await {
        Ok(_) => println!("✅ Message sent successfully in {:?}", send_start.elapsed()),
        Err(e) => println!("❌ Send failed: {:?}", e),
    }
    
    // Wait to see server output
    println!("⏳ Waiting for server to process...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    
    println!("\n✅ Test complete!");
    Ok(())
}