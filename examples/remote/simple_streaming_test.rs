//! Simple test to check streaming trigger
//!
//! Run:
//! Terminal 1: cargo run --example streaming_prebacktest_server --features remote
//! Terminal 2: cargo run --example simple_streaming_test --features remote

use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import shared message definitions
mod streaming_messages;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable detailed logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo=info,kameo_remote=info")
        .try_init();

    println!("\n🔍 === SIMPLE STREAMING TEST ===");

    // Bootstrap on port 9311
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9311".parse()?).await?;
    println!("✅ Client listening on {}", transport.local_addr());

    // Connect to server
    println!("\n📡 Connecting to server at 127.0.0.1:9310...");
    if let Some(handle) = transport.handle() {
        let peer = handle
            .add_peer(&kameo_remote::PeerId::new("kameo_node_9310"))
            .await;
        peer.connect(&"127.0.0.1:9310".parse()?).await?;
        println!("✅ Connected to server");
    }

    // Wait for connection
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Look up remote actor
    println!("\n🔍 Looking up remote BacktestActor...");
    let backtest_ref = match DistributedActorRef::lookup("backtest_executor", transport).await? {
        Some(ref_) => {
            println!("✅ Found BacktestActor");
            ref_
        }
        None => {
            return Err("Actor not found".into());
        }
    };

    // Send a 2MB message
    println!("\n🧪 Sending 2MB message...");
    let msg = PreBacktestMessage::create_mock(2);
    
    backtest_ref.tell(msg).send().await?;
    println!("✅ Message sent");
    
    // Wait to see results
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    
    println!("\n📊 Check logs for '🚨 LARGE MESSAGE DETECTED' to confirm streaming path");

    Ok(())
}