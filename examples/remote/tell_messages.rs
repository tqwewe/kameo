//! Shared message definitions for tell examples - defined once at compile time
//!
//! This demonstrates the "define once, use everywhere" pattern for distributed actor messages.
//! Both client and server import from this single source of truth.

#![allow(dead_code, unused_variables)]

use kameo::actor::ActorRef;
use kameo::{Actor, RemoteMessage};
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

/// Log message - defined ONCE at compile time with zero-cost RemoteMessage derive
/// This single definition works for both client (with zero-cost abstractions) and server (with distributed_actor!)
/// The RemoteMessage derive generates the same HasTypeHash implementation that distributed_actor! would create
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct LogMessage {
    pub level: String,
    pub content: String,
}

// Get count message - for asking the server how many messages it has processed
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct GetCountMessage;

/// Count response message
#[derive(kameo::Reply, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct CountResponse {
    pub count: u32,
}

/// String-specific event processing message - defined ONCE for both client and server
/// Using concrete String type to avoid complex generic constraints
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct ProcessEvent {
    pub event_type: String,
    pub data: String,
}

/// Concrete message with data for testing large messages
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct TellConcrete {
    pub id: u32,
    pub data: Vec<u8>,
}

/// Server-to-client response message - for bidirectional communication
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct ServerResponse {
    pub message_id: u32,
    pub response_data: String,
    pub timestamp: String,
}

/// Completion marker - signals end of client→server phase
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct TestComplete {
    pub total_messages_sent: u32,
    pub test_duration_ms: u64,
}

/// Server→Client benchmark message
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct ServerBenchmark {
    pub sequence: u32,
    pub payload: Vec<u8>,
    pub timestamp: u64,
}

/// String event processor actor - shared between client and server
/// This avoids duplicate struct definitions and ensures type consistency
#[derive(Debug)]
#[allow(dead_code)]
pub struct EventProcessor {
    pub events_processed: u32,
}

impl Actor for EventProcessor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        println!("🎬 EventProcessor (String) started!");
        Ok(Self {
            events_processed: 0,
        })
    }
}

// Simple main function to make this a valid example
// This demonstrates the shared message types that are used by both
// tell_concrete_server.rs and tell_concrete_client.rs
#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("📋 Shared Message Definitions for Tell Examples");
    println!("==============================================");
    println!();
    println!("This file defines the message types used by:");
    println!("  • tell_concrete_server.rs");
    println!("  • tell_concrete_client.rs");
    println!();
    println!("Message Types Defined:");
    println!("  • LogMessage - for logging with level and content");
    println!("  • GetCountMessage - for requesting message count");
    println!("  • ProcessEvent - for processing events");
    println!("  • TellConcrete - for testing large messages");
    println!("  • ServerResponse - for server-to-client responses (BIDIRECTIONAL!)");
    println!("  • TestComplete - completion marker for client→server phase");
    println!("  • ServerBenchmark - server→client benchmark messages");
    println!("  • EventProcessor - shared actor definition");
    println!();
    println!("All message types use #[derive(RemoteMessage)] for:");
    println!("  • Single source of truth across client/server");
    println!("  • Automatic type hash generation");
    println!("  • Zero-cost abstractions");
    println!("  • Sub-microsecond performance (~750ns)");
    println!();
    println!("Run the actual examples:");
    println!("  cargo run --example tell_concrete_server --features remote");
    println!("  cargo run --example tell_concrete_client --features remote");

    Ok(())
}
