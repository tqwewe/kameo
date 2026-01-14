//! Generic actor tell client
//!
//! Run after starting the server:
//! cargo run --example tell_generic_client --features remote

#![allow(dead_code, unused_variables)]

use kameo::remote::{distributed_actor_ref::DistributedActorRef, transport::RemoteTransport};
use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// We need to define the same actor and message types to get the type hashes
#[derive(Debug)]
struct StorageActor {
    _phantom: std::marker::PhantomData<()>,
}

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};

impl Actor for StorageActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            _phantom: std::marker::PhantomData,
        })
    }
}

// Same message types as server
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct StoreItem {
    item: String,
}

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct ClearStorage;

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct PrintItems;

impl Message<StoreItem> for StorageActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: StoreItem,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

impl Message<ClearStorage> for StorageActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: ClearStorage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

impl Message<PrintItems> for StorageActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: PrintItems,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        panic!("Client should not handle messages")
    }
}

// Handler methods for distributed actor (just for type hash generation)
impl StorageActor {
    async fn handle_store_item(&mut self, _msg: &rkyv::Archived<StoreItem>) {
        panic!("Client should not handle messages")
    }

    async fn handle_clear_storage(&mut self, _msg: &rkyv::Archived<ClearStorage>) {
        panic!("Client should not handle messages")
    }

    async fn handle_print_items(&mut self, _msg: &rkyv::Archived<PrintItems>) {
        panic!("Client should not handle messages")
    }
}

use kameo::distributed_actor;

// Register to generate type hashes
distributed_actor! {
    StorageActor {
        StoreItem => handle_store_item,
        ClearStorage => handle_clear_storage,
        PrintItems => handle_print_items,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Enable logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,kameo=info")
        .try_init();

    println!("\n🚀 === GENERIC STORAGE TELL CLIENT ===");

    // Bootstrap on port 9321
    let transport = kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9321".parse()?).await?;
    println!("✅ Client listening on {}", transport.local_addr());

    // Connect to server
    println!("\n📡 Connecting to server at 127.0.0.1:9320...");
    if let Some(handle) = transport.handle() {
        let peer = handle
            .add_peer(&kameo_remote::PeerId::new("kameo_node_9320"))
            .await;
        peer.connect(&"127.0.0.1:9320".parse()?).await?;
        println!("✅ Connected to server");
    }

    // Wait for connection to stabilize
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Look up remote actor
    println!("\n🔍 Looking up remote StorageActor...");
    let storage_ref = match DistributedActorRef::lookup("storage").await? {
        Some(ref_) => {
            println!("✅ Found StorageActor on server");
            ref_
        }
        None => {
            println!("❌ StorageActor not found on server");
            return Err("Storage actor not found".into());
        }
    };

    // Send tell messages
    println!("\n📤 Sending tell messages to remote actor...");

    // Test 1: Store some items
    println!("\n🧪 Test 1: Storing items");
    let items = vec!["apple", "banana", "cherry", "date"];
    for item in items {
        let start = std::time::Instant::now();
        storage_ref
            .tell(StoreItem {
                item: item.to_string(),
            })
            .send()
            .await?;
        let duration = start.elapsed();
        println!("   Stored '{}' in {:?}", item, duration);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Test 2: Print items
    println!("\n🧪 Test 2: Printing stored items");
    let start = std::time::Instant::now();
    storage_ref.tell(PrintItems).send().await?;
    let duration = start.elapsed();
    println!("   Print command sent in {:?}", duration);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 3: Store more items
    println!("\n🧪 Test 3: Storing more items");
    let more_items = vec!["elderberry", "fig", "grape"];
    for item in more_items {
        let start = std::time::Instant::now();
        storage_ref
            .tell(StoreItem {
                item: item.to_string(),
            })
            .send()
            .await?;
        let duration = start.elapsed();
        println!("   Stored '{}' in {:?}", item, duration);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Test 4: Print all items
    println!("\n🧪 Test 4: Printing all stored items");
    let start = std::time::Instant::now();
    storage_ref.tell(PrintItems).send().await?;
    let duration = start.elapsed();
    println!("   Print command sent in {:?}", duration);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 5: Clear storage
    println!("\n🧪 Test 5: Clearing storage");
    let start = std::time::Instant::now();
    storage_ref.tell(ClearStorage).send().await?;
    let duration = start.elapsed();
    println!("   Clear command sent in {:?}", duration);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 6: Print empty storage
    println!("\n🧪 Test 6: Printing cleared storage");
    let start = std::time::Instant::now();
    storage_ref.tell(PrintItems).send().await?;
    let duration = start.elapsed();
    println!("   Print command sent in {:?}", duration);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("\n🎉 All tell messages sent successfully! Check the server output for the results.");

    Ok(())
}
