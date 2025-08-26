//! Executor (server) for bugfix test
//!
//! This simulates the real Executor that should send BacktestSummary messages
//! to TAManager but they currently fail to be received.
//!
//! Run this first:
//! cargo run --example tell_bugfix_server --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::{transport::RemoteTransport, DistributedActorRef};

// Import SHARED bugfix message definitions - SINGLE SOURCE OF TRUTH
mod bugfix_messages;
use bugfix_messages::*;


// ExecutionRouter actor - simulates real ExecutionRouter that should send BacktestSummary
struct ExecutorActor {
    messages_received: u32,
    prebacktest_received: bool,
    backtest_iteration_received: bool,
    backtest_summary_received: bool, // THIS IS THE CRITICAL FLAG - what we're testing
    executor_ref: Option<DistributedActorRef>, // CACHED EXECUTOR REF - like working example
}

impl Actor for ExecutorActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🚀 ExecutorActor started - simulating real Executor");
        println!("🎯 Ready to receive: PreBacktest → BacktestIteration, then send BacktestSummary");
        println!("🚨 Testing BacktestSummary sending - the one that fails in real system!");
        Ok(Self {
            messages_received: 0,
            prebacktest_received: false,
            backtest_iteration_received: false,
            backtest_summary_received: false, // THIS IS WHAT WE'RE TESTING
            executor_ref: None, // Will cache ta_manager ref like working example
        })
    }
}

// Handler for SetExecutorRef - sets up bidirectional communication like working example
impl Message<SetExecutorRef> for ExecutorActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SetExecutorRef,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("🔗 [EXECUTOR] Executor reference set for bidirectional messaging!");
        self.executor_ref = Some(msg.executor_ref);
    }
}

// Handler for PreBacktestMessage
impl Message<PreBacktestMessage> for ExecutorActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PreBacktestMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.messages_received += 1;
        self.prebacktest_received = true;
        
        println!(
            "🎯 [EXECUTOR] PreBacktest received #{}: strategy={}, symbol={}, timeframe={}",
            self.messages_received, msg.strategy, msg.symbol, msg.timeframe
        );
    }
}

// Handler for BacktestIterationMessage
impl Message<BacktestIterationMessage> for ExecutorActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: BacktestIterationMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.messages_received += 1;
        self.backtest_iteration_received = true;
        
        println!(
            "📊 [EXECUTOR] BacktestIteration received #{}: iteration={}, progress={}%, pnl={}",
            self.messages_received, msg.iteration, msg.progress_pct, msg.current_pnl
        );
        
        // NOW SEND BACKTESTSUMMARY BACK TO TA MANAGER (the critical test!)
        if let Some(ta_manager_ref) = &self.executor_ref {
            println!("🚨 [EXECUTOR] Preparing to send BacktestSummary to TAManager - THE CRITICAL TEST!");
            
            let backtest_summary = BacktestSummaryMessage {
                backtest_id: "test_backtest_from_executor".to_string(),
                total_pnl: 2500.50,
                total_trades: 89,
                win_rate: 73.2,
                summary_data: vec![42u8; 1_100_000], // Large payload - 1.1MB to force streaming mode
            };
            
            println!("🔬 [EXECUTOR] BacktestSummary created - size: {} bytes", backtest_summary.summary_data.len());
            println!("🔬 [EXECUTOR] TAManager actor_ref available");
            
            // Note: Cannot access private fields, but we can still try to send
            println!("🔬 [EXECUTOR] Message size: {} bytes (testing against streaming threshold)", backtest_summary.summary_data.len());
            
            println!("🔬 [EXECUTOR] About to call tell().send()...");
            match ta_manager_ref.tell(backtest_summary).send().await {
                Ok(_) => {
                    println!("✅ [EXECUTOR] tell().send() returned Ok - message queued for sending!");
                    println!("🔬 [EXECUTOR] Note: This doesn't guarantee delivery, just successful queuing");
                },
                Err(e) => {
                    println!("❌ [EXECUTOR] tell().send() failed: {:?}", e);
                    println!("🔬 [EXECUTOR] This indicates an immediate sending error");
                }
            }
        } else {
            println!("⚠️ [EXECUTOR] Cannot send BacktestSummary - no TAManager reference");
        }
    }
}

// Handler for BacktestSummaryMessage - THIS IS THE ONE THAT FAILS IN REAL SYSTEM
impl Message<BacktestSummaryMessage> for ExecutorActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: BacktestSummaryMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.messages_received += 1;
        self.backtest_summary_received = true;
        
        println!(
            "🚨🎯📡 *** [EXECUTOR] BacktestSummary RECEIVED #{} *** 🚨🎯📡",
            self.messages_received
        );
        println!(
            "🚨 SUCCESS: BacktestSummary handler IS working! ID={}, PnL={}, trades={}, win_rate={}%, data_size={} bytes",
            msg.backtest_id, msg.total_pnl, msg.total_trades, msg.win_rate, msg.summary_data.len()
        );
        println!("🔥 [EXECUTOR] BacktestSummary handler SUCCESS! This should work in real system too!");

        // Send acknowledgment back to executor (using cached ref from SetExecutorRef)
        if let Some(executor_ref) = &self.executor_ref {
            let response = TAManagerResponse {
                message_id: self.messages_received,
                response_data: format!(
                    "BacktestSummary processed successfully: {} PnL with {} trades",
                    msg.total_pnl, msg.total_trades
                ),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .to_string(),
            };

            if let Err(e) = executor_ref.tell(response).send().await {
                println!("❌ [EXECUTOR] Failed to send response to executor: {:?}", e);
            } else {
                println!("📤 [EXECUTOR] Sent BacktestSummary response to executor");
            }
        } else {
            println!("⚠️  [EXECUTOR] Cannot send response - no executor reference available");
        }
    }
}

// Handler for TestComplete - ends test and shows stats
impl Message<TestComplete> for ExecutorActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: TestComplete,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("\n🏁 [EXECUTOR] Test completed!");
        println!("   Messages received by TAManager: {}", self.messages_received);
        println!("   Messages sent by executor: {}", msg.total_messages_sent);
        println!("   Test duration: {}ms", msg.test_duration_ms);
        
        // Show which messages were received
        println!("\n📋 [EXECUTOR] Message Status:");
        println!("   ✅ PreBacktest received: {}", self.prebacktest_received);
        println!("   ✅ BacktestIteration received: {}", self.backtest_iteration_received);
        println!("   🚨 BacktestSummary received: {} 🚨", self.backtest_summary_received);
        
        if self.backtest_summary_received {
            println!("🎉 [EXECUTOR] SUCCESS: BacktestSummary message was received!");
            println!("🔬 [EXECUTOR] If this works but real system doesn't, the issue is elsewhere");
        } else {
            println!("❌ [EXECUTOR] FAILURE: BacktestSummary message was NOT received!");
            println!("🔬 [EXECUTOR] This reproduces the real system issue");
        }
        
        if msg.test_duration_ms > 0 {
            println!("   Messages/second: {:.2}", 
                     msg.total_messages_sent as f64 / (msg.test_duration_ms as f64 / 1000.0));
        }
        
        println!("\n✨ [EXECUTOR] BacktestSummary message flow test complete!");
    }
}

// CRITICAL FIX: Add archived handlers for zero-copy deserialization (like working example)
impl ExecutorActor {
    // Handle PreBacktestMessage with archived type - zero-copy deserialization
    async fn handle_prebacktest(&mut self, msg: &rkyv::Archived<PreBacktestMessage>) {
        self.messages_received += 1;
        self.prebacktest_received = true;
        
        println!(
            "🎯 [EXECUTOR] PreBacktest received #{}: strategy={}, symbol={}, timeframe={}",
            self.messages_received, &msg.strategy, &msg.symbol, &msg.timeframe
        );
    }

    // Handle BacktestIterationMessage with archived type - zero-copy deserialization  
    async fn handle_backtest_iteration(&mut self, msg: &rkyv::Archived<BacktestIterationMessage>) {
        self.messages_received += 1;
        self.backtest_iteration_received = true;
        
        println!(
            "📊 [EXECUTOR] BacktestIteration received #{}: iteration={}, progress={}%, pnl={}",
            self.messages_received, msg.iteration, msg.progress_pct, msg.current_pnl
        );
    }

    // Handle BacktestSummaryMessage with archived type - THE CRITICAL ONE
    async fn handle_backtest_summary(&mut self, msg: &rkyv::Archived<BacktestSummaryMessage>) {
        self.messages_received += 1;
        self.backtest_summary_received = true;
        
        println!(
            "🚨🎯📡 *** [EXECUTOR] BacktestSummary RECEIVED #{} *** 🚨🎯📡",
            self.messages_received
        );
        println!(
            "🚨 SUCCESS: BacktestSummary handler IS working! ID={}, PnL={}, trades={}, win_rate={}%, data_size={} bytes",
            &msg.backtest_id, msg.total_pnl, msg.total_trades, msg.win_rate, msg.summary_data.len()
        );
        println!("🔥 [EXECUTOR] BacktestSummary archived handler SUCCESS! Zero-copy deserialization working!");

        // Send acknowledgment back to executor (using cached ref from SetExecutorRef)
        if let Some(executor_ref) = &self.executor_ref {
            // SERIALIZATION TEST: Send BacktestIterationMessage back to test if serialization works
            println!("🧪 [EXECUTOR] Testing BacktestIterationMessage serialization back to ExecutionRouter...");
            let test_iteration_message = BacktestIterationMessage {
                iteration: 999,
                progress_pct: 100.0,
                current_pnl: 12345.67,
            };

            if let Err(e) = executor_ref.tell(test_iteration_message).send().await {
                println!("❌ [EXECUTOR] Failed to send BacktestIterationMessage test to executor: {:?}", e);
            } else {
                println!("✅ [EXECUTOR] Successfully sent BacktestIterationMessage test back to executor!");
            }

            // Also send the original TAManagerResponse
            let response = TAManagerResponse {
                message_id: self.messages_received,
                response_data: format!(
                    "BacktestSummary processed successfully: {} PnL with {} trades",
                    msg.total_pnl, msg.total_trades
                ),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .to_string(),
            };

            if let Err(e) = executor_ref.tell(response).send().await {
                println!("❌ [EXECUTOR] Failed to send TAManagerResponse to executor: {:?}", e);
            } else {
                println!("📤 [EXECUTOR] Sent TAManagerResponse to executor");
            }
        } else {
            println!("⚠️  [EXECUTOR] Cannot send response - no executor reference available");
        }
    }

    // Handle TestComplete with archived type - zero-copy deserialization
    async fn handle_test_complete(&mut self, msg: &rkyv::Archived<TestComplete>) {
        println!(
            "🏁 [EXECUTOR] Client completed! Sent {} messages in {}ms",
            msg.total_messages_sent, msg.test_duration_ms
        );

        // Show which messages were received
        println!("\n📋 [EXECUTOR] Message Status:");
        println!("   ✅ PreBacktest received: {}", self.prebacktest_received);
        println!("   ✅ BacktestIteration received: {}", self.backtest_iteration_received);
        println!("   🚨 BacktestSummary received: {} 🚨", self.backtest_summary_received);
        
        if self.backtest_summary_received {
            println!("🎉 [EXECUTOR] SUCCESS: BacktestSummary message was received!");
            println!("🔬 [EXECUTOR] If this works but real system doesn't, the issue is elsewhere");
        } else {
            println!("❌ [EXECUTOR] FAILURE: BacktestSummary message was NOT received!");
            println!("🔬 [EXECUTOR] This reproduces the real system issue");
        }

        println!("✅ [EXECUTOR] Archived handler test complete!");
    }
}


// Register ExecutorActor for distributed messaging
// REPRODUCTION SUCCESS: We've confirmed the issue is message size related
// BacktestSummaryMessage with 122,836 bytes causes connection to close
distributed_actor! {
    ExecutorActor {
        PreBacktestMessage,
        BacktestIterationMessage,
        BacktestSummaryMessage,
        TestComplete,
    }
}

// ISSUE REPRODUCED: Large BacktestSummary messages cause "peer closed connection without sending TLS close_notify"

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Ensure the rustls CryptoProvider is installed (required for TLS)
    kameo_remote::tls::ensure_crypto_provider();

    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=warn,kameo=warn")
        .try_init();

    println!("\n🚀 === BUGFIX TAMANAGER SERVER (WITH TLS) ===");

    // For production, you would load keypair from secure storage or generate and save it
    // Here we use a deterministic keypair for the example (using seed-based generation)
    let server_keypair = {
        // In production: load from file or use proper key management
        // For this example, we'll use a fixed seed for reproducibility
        kameo_remote::KeyPair::new_for_testing("tls_server_production_key")
    };
    println!("🔐 TAManager server using Ed25519 keypair for TLS encryption");
    println!("⚠️  Note: In production, use properly generated and stored keypairs");

    // Bootstrap on port 9310 with TLS enabled using keypair
    let transport = kameo::remote::v2_bootstrap::bootstrap_with_keypair(
        "127.0.0.1:9310".parse()?,
        server_keypair,
    )
    .await?;
    println!(
        "✅ TAManager server listening on {} with TLS encryption",
        transport.local_addr()
    );

    // Create and register ExecutorActor
    println!("\n🎬 Creating ExecutorActor to handle distributed messages...");
    let executor_actor_ref = ExecutorActor::spawn(());
    let executor_actor_id = executor_actor_ref.id();

    // Use sync registration to wait for peer confirmation (eliminates need for sleep delays)
    transport
        .register_distributed_actor_sync(
            "executor".to_string(),
            &executor_actor_ref,
            std::time::Duration::from_secs(2),
        )
        .await?;
    println!(
        "✅ ExecutorActor registered as 'executor' with ID {:?} and gossip confirmed",
        executor_actor_id
    );

    // Debug: Print the type hashes being used
    use kameo::remote::type_hash::HasTypeHash;
    println!("\n🔍 TAManager type hashes:");
    println!(
        "   PreBacktestMessage: {:08x}",
        <PreBacktestMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!(
        "   BacktestIterationMessage: {:08x}",
        <BacktestIterationMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );
    println!(
        "   BacktestSummaryMessage: {:08x} 🚨",
        <BacktestSummaryMessage as HasTypeHash>::TYPE_HASH.as_u32()
    );

    // Add the client as a trusted peer for TLS authentication
    if let Some(handle) = transport.handle() {
        // The client will use this same keypair name, so we add it as a trusted peer
        let client_peer_id = kameo_remote::PeerId::new("tls_client_production_key");
        let _peer = handle.add_peer(&client_peer_id).await;
        println!(
            "✅ Added ExecutionRouter client as trusted peer: {}",
            client_peer_id
        );
    }

    println!("\n📡 TAManager server ready to receive distributed messages!");
    println!("🎯 ExecutorActor can now receive PreBacktest, BacktestIteration, and BacktestSummary messages");
    println!("🔄 Server will send responses back to ExecutionRouter client");
    println!("🚨 Testing BacktestSummary message handling - the one that fails in real system!");

    println!("\n📡 TAManager server ready. Run ExecutionRouter client with:");
    println!("   cargo run --example tell_bugfix_client --features remote");
    println!("\n💤 TAManager server will run until you press Ctrl+C...\n");

    // Clone actor_ref for client lookup task (like working example)
    let lookup_actor_ref = executor_actor_ref.clone();

    // Continuous client connection task - attempt to connect to ExecutionRouter every 1 second
    let transport_cloned = transport.clone();
    tokio::spawn(async move {
        let mut retry_count = 0;
        loop {
            retry_count += 1;

            // Try to connect to ExecutionRouter client
            if let Some(handle) = transport_cloned.handle() {
                let client_peer_id = kameo_remote::PeerId::new("tls_client_production_key");
                let peer = handle.add_peer(&client_peer_id).await;
                match peer.connect(&"127.0.0.1:9311".parse().unwrap()).await {
                    Ok(_) => {
                        println!("✅ [EXECUTOR] Connected to TAManager client!");
                        // Connection established, now try to lookup ta_manager actor
                        match DistributedActorRef::lookup("ta_manager").await {
                            Ok(Some(ta_manager_ref)) => {
                                println!("✅ [EXECUTOR] Found TAManager! Setting up bidirectional messaging...");
                                // Send SetExecutorRef message to properly set up bidirectional communication
                                if let Err(e) = lookup_actor_ref
                                    .tell(SetExecutorRef { executor_ref: ta_manager_ref })
                                    .send()
                                    .await
                                {
                                    println!("❌ [EXECUTOR] Failed to set executor reference: {:?}", e);
                                } else {
                                    println!("🎉 [EXECUTOR] Bidirectional messaging ready!");
                                    break; // Stop looking once we find the executor
                                }
                            }
                            Ok(None) => {
                                println!("⏳ [EXECUTOR] ExecutionRouter connected but actor not registered yet, will retry...");
                            }
                            Err(e) => {
                                println!("❌ [EXECUTOR] Error looking up ExecutionRouter: {:?}", e);
                            }
                        }
                    }
                    Err(_) => {
                        // Only log periodically to avoid spam
                        if retry_count == 1 || retry_count % 30 == 0 {
                            println!("🔍 [EXECUTOR] Attempting to connect to ExecutionRouter for bidirectional messaging (attempt #{})...", retry_count);
                            println!("🔐 [EXECUTOR] Attempting to establish TLS connection to ExecutionRouter at 127.0.0.1:9311...");
                            println!("⚠️  [EXECUTOR] Could not connect to ExecutionRouter yet: Connection refused");
                            println!("⏳ [EXECUTOR] ExecutionRouter not found yet, will retry...");
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    // Keep server running
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\n🛑 TAManager server shutting down...");
        }
    }

    Ok(())
}