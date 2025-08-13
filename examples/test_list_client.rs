//! Test client for list syntax server

use kameo::remote::DistributedActorRef;
use kameo_remote;

mod test_mapping_messages;
use test_mapping_messages::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🧪 TEST LIST SYNTAX CLIENT");
    
    // Use fixed keypair for testing
    let client_keypair = kameo_remote::KeyPair::new_for_testing("test_list_client");
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9331".parse()?,
        client_keypair
    ).await?;
    println!("✅ Client on 127.0.0.1:9331 with TLS");
    
    // Connect to server with TLS
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("test_list_server");
        let server_peer = handle.add_peer(&server_peer_id).await;
        server_peer.connect(&"127.0.0.1:9330".parse()?).await?;
        println!("✅ Connected to server with TLS");
    }
    
    // Wait for gossip
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    
    let actor_ref = match DistributedActorRef::lookup("test_actor").await {
        Ok(Some(actor)) => actor,
        Ok(None) => return Err("Actor not found".into()),
        Err(e) => return Err(e.into()),
    };
    println!("✅ Found actor");
    
    let mut data = std::collections::HashMap::new();
    data.insert("test".to_string(), vec![1, 2, 3]);
    
    let msg = TestDataMessage { 
        id: 42, 
        data,
        values: vec![1.0, 2.0] 
    };
    
    println!("📤 Sending message...");
    actor_ref.tell(msg).send().await?;
    println!("✅ Message sent");
    
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    println!("✅ Done");
    
    Ok(())
}