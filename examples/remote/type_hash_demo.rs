//! Demo of the new type hash system for generic actors
//! 
//! This example shows how type hashes enable generic actors without linkme registration
//! Run with: cargo run --example type_hash_demo --features remote

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use kameo::remote::type_hash::{HasTypeHash, TypeHash};

#[cfg(feature = "remote")]
use kameo_type_hash_macros::TypeHash;

// Generic cache actor that can store any type
#[derive(Actor, TypeHash)]
struct Cache<K, V> {
    items: std::collections::HashMap<K, V>,
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> Cache<K, V> {
    fn new() -> Self {
        Self {
            items: std::collections::HashMap::new(),
        }
    }
}

// Messages for the cache
#[derive(Debug, Clone, Serialize, Deserialize, TypeHash)]
struct Get<K>(K);

#[derive(Debug, Clone, Serialize, Deserialize, TypeHash)]
struct Set<K, V> {
    key: K,
    value: V,
}

#[derive(Debug, Clone, Serialize, Deserialize, TypeHash)]
struct Remove<K>(K);

impl<K, V> Message<Cache<K, V>> for Get<K>
where
    K: Clone + Eq + std::hash::Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    type Reply = Option<V>;
    
    async fn handle(self, _ctx: Context<'_, Cache<K, V>>, actor: &mut Cache<K, V>) -> Self::Reply {
        actor.items.get(&self.0).cloned()
    }
}

impl<K, V> Message<Cache<K, V>> for Set<K, V>
where
    K: Clone + Eq + std::hash::Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    type Reply = Option<V>;
    
    async fn handle(self, _ctx: Context<'_, Cache<K, V>>, actor: &mut Cache<K, V>) -> Self::Reply {
        actor.items.insert(self.key, self.value)
    }
}

impl<K, V> Message<Cache<K, V>> for Remove<K>
where
    K: Clone + Eq + std::hash::Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    type Reply = Option<V>;
    
    async fn handle(self, _ctx: Context<'_, Cache<K, V>>, actor: &mut Cache<K, V>) -> Self::Reply {
        actor.items.remove(&self.0)
    }
}

// Specialized string cache with its own type hash
#[derive(Actor, TypeHash)]
struct StringCache {
    cache: Cache<String, String>,
}

impl StringCache {
    fn new() -> Self {
        Self {
            cache: Cache::new(),
        }
    }
}

#[cfg(not(feature = "remote"))]
fn main() {
    eprintln!("This example requires the 'remote' feature");
    eprintln!("Run with: cargo run --example type_hash_demo --features remote");
}

#[cfg(feature = "remote")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Type Hash Demo");
    println!("==============");
    
    // Show compile-time computed type hashes
    println!("\nType Hashes (computed at compile time):");
    println!("  Cache<String, i32>: 0x{:08x}", Cache::<String, i32>::TYPE_HASH.as_u32());
    println!("  Cache<i32, String>: 0x{:08x}", Cache::<i32, String>::TYPE_HASH.as_u32());
    println!("  StringCache: 0x{:08x}", StringCache::TYPE_HASH.as_u32());
    println!("  Get<String>: 0x{:08x}", Get::<String>::TYPE_HASH.as_u32());
    println!("  Set<String, i32>: 0x{:08x}", Set::<String, i32>::TYPE_HASH.as_u32());
    
    // Create different cache instances
    let string_int_cache = ActorRef::spawn(Cache::<String, i32>::new()).await;
    let int_string_cache = ActorRef::spawn(Cache::<i32, String>::new()).await;
    
    // Use the caches
    println!("\nUsing Cache<String, i32>:");
    string_int_cache.ask(Set {
        key: "age".to_string(),
        value: 42,
    }).await?;
    
    string_int_cache.ask(Set {
        key: "year".to_string(),
        value: 2024,
    }).await?;
    
    let age = string_int_cache.ask(Get("age".to_string())).await?;
    println!("  age = {:?}", age);
    
    println!("\nUsing Cache<i32, String>:");
    int_string_cache.ask(Set {
        key: 1,
        value: "first".to_string(),
    }).await?;
    
    int_string_cache.ask(Set {
        key: 2,
        value: "second".to_string(),
    }).await?;
    
    let first = int_string_cache.ask(Get(1)).await?;
    println!("  cache[1] = {:?}", first);
    
    // Show that different instantiations have different type hashes
    println!("\nType Safety:");
    println!("  Cache<String, i32> and Cache<i32, String> have different type hashes");
    println!("  This ensures type safety across the network");
    
    // Demonstrate const evaluation
    const CACHE_HASH: TypeHash = TypeHash::from_bytes(b"Cache<String,i32>");
    println!("\nConst evaluation works:");
    println!("  const CACHE_HASH = 0x{:08x}", CACHE_HASH.as_u32());
    
    println!("\nDemo completed successfully!");
    Ok(())
}