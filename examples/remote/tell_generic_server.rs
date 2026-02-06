//! Generic actor tell server
//!
//! Run this first:
//! cargo run --example tell_generic_server --features remote

use kameo::RemoteMessage;
use kameo::actor::{Actor, ActorRef};
use kameo::distributed_actor;
use kameo::message::{Context, Message};
use kameo::remote::transport::RemoteTransport;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

// Storage actor that can store items
#[derive(Debug)]
struct StorageActor {
    items: Vec<String>,
    message_count: u32,
}

impl Actor for StorageActor {
    type Args = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn on_start(_args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            items: Vec::new(),
            message_count: 0,
        })
    }
}

// Store message
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct StoreItem {
    item: String,
}

// Clear storage message
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct ClearStorage;

// Print items message
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
struct PrintItems;

impl Message<StoreItem> for StorageActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StoreItem,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.message_count += 1;
        self.items.push(msg.item.clone());
        println!(
            "📦 Stored item '{}' (total items: {}, message #{})",
            msg.item,
            self.items.len(),
            self.message_count
        );
    }
}

impl Message<ClearStorage> for StorageActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: ClearStorage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.message_count += 1;
        let count = self.items.len();
        self.items.clear();
        println!(
            "🗑️  Cleared {} items from storage (message #{})",
            count, self.message_count
        );
    }
}

impl Message<PrintItems> for StorageActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: PrintItems,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.message_count += 1;
        println!(
            "📋 Storage contents ({} items, message #{}):",
            self.items.len(),
            self.message_count
        );
        for (i, item) in self.items.iter().enumerate() {
            println!("   {}. {}", i + 1, item);
        }
        if self.items.is_empty() {
            println!("   (empty)");
        }
    }
}

// Handler methods for distributed actor with zero-copy archived types
impl StorageActor {
    async fn handle_store_item(&mut self, msg: &rkyv::Archived<StoreItem>) {
        self.message_count += 1;
        let item = msg.item.to_string();
        self.items.push(item.clone());
        println!(
            "📦 Stored item '{}' (total items: {}, message #{})",
            item,
            self.items.len(),
            self.message_count
        );
    }

    async fn handle_clear_storage(&mut self, _msg: &rkyv::Archived<ClearStorage>) {
        self.message_count += 1;
        let count = self.items.len();
        self.items.clear();
        println!(
            "🗑️  Cleared {} items from storage (message #{})",
            count, self.message_count
        );
    }

    async fn handle_print_items(&mut self, _msg: &rkyv::Archived<PrintItems>) {
        self.message_count += 1;
        println!(
            "📋 Storage contents ({} items, message #{}):",
            self.items.len(),
            self.message_count
        );
        for (i, item) in self.items.iter().enumerate() {
            println!("   {}. {}", i + 1, item);
        }
        if self.items.is_empty() {
            println!("   (empty)");
        }
    }
}

// Register with distributed actor macro
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

    println!("\n🚀 === GENERIC STORAGE TELL SERVER ===");

    // Bootstrap on port 9320 with an explicit keypair
    let server_keypair = kameo::remote::v2_bootstrap::test_keypair(9320);
    let client_peer_id = kameo::remote::v2_bootstrap::test_keypair(9321).peer_id();
    let transport =
        kameo::remote::v2_bootstrap::bootstrap_on("127.0.0.1:9320".parse()?, server_keypair)
            .await?;
    println!("✅ Server listening on {}", transport.local_addr());

    // Create and register StorageActor
    let storage_ref = <StorageActor as Actor>::spawn(());
    let storage_id = storage_ref.id();

    transport
        .register_distributed_actor("storage".to_string(), &storage_ref)
        .await?;

    println!("✅ StorageActor registered with ID {:?}", storage_id);

    // Add client as peer
    if let Some(handle) = transport.handle() {
        let _peer = handle.add_peer(&client_peer_id).await;
        println!("✅ Added client node as peer");
    }

    println!("\n📡 Server ready. Run client with:");
    println!("   cargo run --example tell_generic_client --features remote");
    println!("\n💤 Server will run until you press Ctrl+C...\n");

    // Keep server running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
