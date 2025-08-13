//! Example showing proper usage of distributed_actor! macro
//! 
//! This demonstrates the three components needed for distributed actors:
//! 1. Actor implementation with regular spawn()
//! 2. Messages with #[derive(RemoteMessage)]
//! 3. distributed_actor! macro for registering handlers

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use kameo::distributed_actor;
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// Step 1: Define your actor normally
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

// Step 2: Define messages with RemoteMessage derive
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct Add {
    a: i32,
    b: i32,
}

#[derive(Debug, Clone, Archive, RSerialize, RDeserialize)]
struct AddResult {
    result: i32,
    operation_count: u32,
}

// Implement Reply trait for response types
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

// Step 3: Implement Message trait for ask/reply support
impl Message<Add> for CalculatorActor {
    type Reply = AddResult;
    
    async fn handle(
        &mut self,
        msg: Add,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.operation_count += 1;
        let result = msg.a + msg.b;
        println!("➕ Computing {} + {} = {} (operation #{})", 
                 msg.a, msg.b, result, self.operation_count);
        AddResult {
            result,
            operation_count: self.operation_count,
        }
    }
}

// Step 4: Use distributed_actor! macro to register handlers
// This generates the code to handle remote messages
distributed_actor! {
    CalculatorActor {
        Add,  // List all message types that should be remotely accessible
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\n🚀 === DISTRIBUTED ACTOR EXAMPLE ===");
    
    // Initialize transport for remote communication
    let transport = kameo::remote::v2_bootstrap::bootstrap_on(
        "127.0.0.1:9340".parse()?
    ).await?;
    println!("✅ Transport listening on {}", transport.local_addr());
    
    // Step 5: Spawn actor using regular Actor::spawn()
    let actor_ref = CalculatorActor::spawn(());
    
    // Step 6: Register with transport for remote discovery
    // This automatically registers handlers and makes the actor discoverable
    transport.register_distributed_actor("calculator".to_string(), &actor_ref).await?;
    
    println!("✅ CalculatorActor spawned and registered for distributed operations");
    println!("   - Actor ID: {:?}", actor_ref.id());
    println!("   - Registered name: 'calculator'");
    println!("   - Supports remote ask/reply for: Add");
    
    // The actor is now ready to receive remote messages!
    // Other nodes can look it up by name and send Add messages
    
    println!("\n📡 Actor ready for remote operations. Press Ctrl+C to exit.");
    
    // Keep running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}