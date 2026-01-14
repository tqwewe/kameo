//! Simplified concrete actor ask server (previously generic)
//!
//! Run this first:
//! cargo run --example ask_generic_server --features remote

#![allow(dead_code, unused_variables)]

use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// Concrete calculator actor for integers
#[derive(Debug)]
struct CalculatorActor {
    last_result: Option<i32>,
    operation_count: u32,
}

impl Actor for CalculatorActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            last_result: None,
            operation_count: 0,
        })
    }
}

// Add message for i32
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Add {
    a: i32,
    b: i32,
}

// Multiply message for i32
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Multiply {
    a: i32,
    b: i32,
}

// Get last result message
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct GetLastResult;

// Response with optional last result
#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
struct LastResult {
    value: Option<i32>,
    operation_count: u32,
}

impl kameo::reply::Reply for LastResult {
    type Ok = Self;
    type Error = kameo::error::Infallible;
    type Value = Self;

    fn to_result(self) -> Result<Self, kameo::error::Infallible> {
        Ok(self)
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl Message<Add> for CalculatorActor {
    type Reply = i32;

    async fn handle(&mut self, msg: Add, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.operation_count += 1;
        let result = msg.a + msg.b;
        println!(
            "🧮 Computing {} + {} = {} (operation #{})",
            msg.a, msg.b, result, self.operation_count
        );
        self.last_result = Some(result);
        result
    }
}

impl Message<Multiply> for CalculatorActor {
    type Reply = i32;

    async fn handle(
        &mut self,
        msg: Multiply,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.operation_count += 1;
        let result = msg.a * msg.b;
        println!(
            "🧮 Computing {} × {} = {} (operation #{})",
            msg.a, msg.b, result, self.operation_count
        );
        self.last_result = Some(result);
        result
    }
}

impl Message<GetLastResult> for CalculatorActor {
    type Reply = LastResult;

    async fn handle(
        &mut self,
        _msg: GetLastResult,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!(
            "📊 Getting last result (operation count: {})",
            self.operation_count
        );
        LastResult {
            value: self.last_result,
            operation_count: self.operation_count,
        }
    }
}

// Handler methods for distributed actor with zero-copy archived types
impl CalculatorActor {
    async fn handle_add(&mut self, msg: &rkyv::Archived<Add>) -> i32 {
        self.operation_count += 1;
        let result = msg.a + msg.b;
        println!(
            "🧮 Computing {} + {} = {} (operation #{})",
            msg.a, msg.b, result, self.operation_count
        );
        self.last_result = Some(result);
        result
    }

    async fn handle_multiply(&mut self, msg: &rkyv::Archived<Multiply>) -> i32 {
        self.operation_count += 1;
        let result = msg.a * msg.b;
        println!(
            "🧮 Computing {} × {} = {} (operation #{})",
            msg.a, msg.b, result, self.operation_count
        );
        self.last_result = Some(result);
        result
    }

    async fn handle_get_last(&mut self, _msg: &rkyv::Archived<GetLastResult>) -> LastResult {
        println!(
            "📊 Getting last result (operation count: {})",
            self.operation_count
        );
        LastResult {
            value: self.last_result,
            operation_count: self.operation_count,
        }
    }
}

// Register with distributed actor macro
distributed_actor! {
    CalculatorActor {
        Add => handle_add,
        Multiply => handle_multiply,
        GetLastResult => handle_get_last,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=info")
        .try_init();

    println!("\n🚀 === SIMPLIFIED CALCULATOR ASK SERVER ===");

    // Bootstrap on port 9340
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9340".parse()?).await?;
    println!("✅ Server listening on {}", transport.local_addr());

    // Create and register CalculatorActor
    let calc_ref = CalculatorActor::spawn(());
    let calc_id = calc_ref.id();

    // Register with transport - automatically handles distributed ask/reply
    transport
        .register_distributed_actor("calculator".to_string(), &calc_ref)
        .await?;

    println!("✅ CalculatorActor registered with ID {:?}", calc_id);

    // Add client as peer
    if let Some(handle) = transport.handle() {
        let _peer = handle
            .add_peer(&kameo_remote::PeerId::new("kameo_node_9341"))
            .await;
        println!("✅ Added client node as peer");
    }

    println!("\n📡 Server ready. Run client with:");
    println!("   cargo run --example ask_generic_client --features remote");
    println!("\n💤 Server will run until you press Ctrl+C...\n");

    // Keep server running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
