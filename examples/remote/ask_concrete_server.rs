//! Concrete actor ask server
//! 
//! Run this first:
//! cargo run --example ask_concrete_server --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo::distributed_actor;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// Concrete actor - calculator that can add and multiply
#[derive(Debug)]
struct CalculatorActor {
    operation_count: u32,
}

impl Actor for CalculatorActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    
    async fn on_start(
        _args: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self { operation_count: 0 })
    }
}

// Request message for addition
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Add {
    a: i32,
    b: i32,
}

// Response for addition
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RSerialize, RDeserialize)]
struct AddResult {
    result: i32,
    operation_count: u32,
}

impl kameo::reply::Reply for AddResult {
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

// Request message for multiplication
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Multiply {
    a: i32,
    b: i32,
}

// Response for multiplication
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RSerialize, RDeserialize)]
struct MultiplyResult {
    result: i32,
    operation_count: u32,
}

impl kameo::reply::Reply for MultiplyResult {
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
    type Reply = AddResult;
    
    async fn handle(
        &mut self,
        msg: Add,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.operation_count += 1;
        let result = msg.a + msg.b;
        println!("➕ Computing {} + {} = {} (operation #{})", msg.a, msg.b, result, self.operation_count);
        AddResult {
            result,
            operation_count: self.operation_count,
        }
    }
}

impl Message<Multiply> for CalculatorActor {
    type Reply = MultiplyResult;
    
    async fn handle(
        &mut self,
        msg: Multiply,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.operation_count += 1;
        let result = msg.a * msg.b;
        println!("✖️  Computing {} × {} = {} (operation #{})", msg.a, msg.b, result, self.operation_count);
        MultiplyResult {
            result,
            operation_count: self.operation_count,
        }
    }
}

// Handler methods for distributed actor - now with zero-copy archived types
impl CalculatorActor {
    async fn handle_add(&mut self, msg: &rkyv::Archived<Add>) -> AddResult {
        self.operation_count += 1;
        // Zero-copy access to fields
        let result = msg.a + msg.b;
        println!("➕ Computing {} + {} = {} (operation #{})", msg.a, msg.b, result, self.operation_count);
        AddResult {
            result,
            operation_count: self.operation_count,
        }
    }
    
    async fn handle_multiply(&mut self, msg: &rkyv::Archived<Multiply>) -> MultiplyResult {
        self.operation_count += 1;
        // Zero-copy access to fields
        let result = msg.a * msg.b;
        println!("✖️  Computing {} × {} = {} (operation #{})", msg.a, msg.b, result, self.operation_count);
        MultiplyResult {
            result,
            operation_count: self.operation_count,
        }
    }
}

// Tell message - just increments a counter
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Increment {
    amount: i32,
}

impl Message<Increment> for CalculatorActor {
    type Reply = ();
    
    async fn handle(
        &mut self,
        msg: Increment,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.operation_count += 1;
        println!("📈 Increment by {} (operation #{})", msg.amount, self.operation_count);
    }
}

// Handler for distributed actor
impl CalculatorActor {
    async fn handle_increment(&mut self, msg: &rkyv::Archived<Increment>) {
        self.operation_count += 1;
        println!("📈 Increment by {} (operation #{})", msg.amount, self.operation_count);
    }
}

// Register with distributed actor macro
distributed_actor! {
    CalculatorActor {
        Add => handle_add,
        Multiply => handle_multiply,
        Increment => handle_increment,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=info")
        .try_init();
    
    println!("\n🚀 === CONCRETE ACTOR ASK SERVER ===");
    
    // Bootstrap on port 9330
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9330".parse()?).await?;
    println!("✅ Server listening on {}", transport.local_addr());
    
    // Create and register CalculatorActor
    let actor_ref = CalculatorActor::spawn(());
    let actor_id = actor_ref.id();
    
    transport.register_actor("calculator".to_string(), actor_id).await?;
    
    let handler = kameo::remote::v2_bootstrap::get_distributed_handler();
    handler.registry().register(actor_id, actor_ref.clone());
    
    println!("✅ CalculatorActor registered with ID {:?}", actor_id);
    
    // Add client as peer
    if let Some(handle) = transport.handle() {
        let _peer = handle.add_peer(&kameo_remote::PeerId::new("kameo_node_9331")).await;
        println!("✅ Added client node as peer");
    }
    
    println!("\n📡 Server ready. Run client with:");
    println!("   cargo run --example ask_concrete_client --features remote");
    println!("\n💤 Server will run until you press Ctrl+C...\n");
    
    // Keep server running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}