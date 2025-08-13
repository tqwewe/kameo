//! Test client for FuturesOHLCVCandle with distributed actor messaging

use kameo::remote::DistributedActorRef;
use kameo_remote;

// Import FuturesOHLCVCandle from trading-poc
use trading_exchange_core::types::FuturesOHLCVCandle;

// Test message containing FuturesOHLCVCandle
#[derive(kameo::RemoteMessage, Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CandleMessage {
    pub candle: FuturesOHLCVCandle,
    pub symbol: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🧪 TEST FUTURES OHLCV CLIENT");
    
    // Use fixed keypair for testing
    let client_keypair = kameo_remote::KeyPair::new_for_testing("test_futures_client");
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9351".parse()?,
        client_keypair
    ).await?;
    println!("✅ Client on 127.0.0.1:9351 with TLS");
    
    // Connect to server with TLS
    if let Some(handle) = transport.handle() {
        let server_peer_id = kameo_remote::PeerId::new("test_futures_server");
        let server_peer = handle.add_peer(&server_peer_id).await;
        server_peer.connect(&"127.0.0.1:9350".parse()?).await?;
        println!("✅ Connected to server with TLS");
    }
    
    // Wait for gossip to propagate
    println!("⏳ Waiting for gossip to propagate...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Lookup the remote actor
    let actor_ref = match DistributedActorRef::lookup("trading_actor").await {
        Ok(Some(actor)) => {
            println!("✅ Found trading_actor");
            actor
        },
        Ok(None) => return Err("Actor not found".into()),
        Err(e) => return Err(e.into()),
    };
    
    // Create a test candle
    let candle = FuturesOHLCVCandle {
        symbol: "BTCUSDT".to_string(),
        open_timestamp: 1700000000000,
        close_timestamp: 1700000060000,
        open: 35000.0,
        high: 35500.0,
        low: 34800.0,
        close: 35200.0,
        volume: 1234.5,
        taker_buy_base_asset_volume: 600.0,
        taker_buy_quote_asset_volume: 21000000.0,
        count: 1500,
    };
    
    let msg = CandleMessage {
        candle: candle.clone(),
        symbol: "BTCUSDT".to_string(),
    };
    
    println!("📤 Sending first candle message...");
    actor_ref.tell(msg.clone()).send().await?;
    println!("✅ First message sent");
    
    // Send a few more with different values
    for i in 1..=3 {
        let mut modified_candle = candle.clone();
        modified_candle.open_timestamp += i * 60000;
        modified_candle.close_timestamp += i * 60000;
        modified_candle.open = 35000.0 + (i as f64 * 100.0);
        modified_candle.close = 35200.0 + (i as f64 * 100.0);
        
        let msg = CandleMessage {
            candle: modified_candle,
            symbol: format!("BTCUSDT_{}", i),
        };
        
        println!("📤 Sending candle message #{}...", i + 1);
        actor_ref.tell(msg).send().await?;
    }
    
    println!("✅ All messages sent");
    
    // Wait a bit to see server output
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    println!("✅ Done");
    
    Ok(())
}