//! This example proves that type hashes are computed at compile time
//! by examining the generated assembly code.

#![allow(dead_code, unused_variables)]

#[cfg(not(feature = "remote"))]
compile_error!("This example requires the 'remote' feature");

#[cfg(feature = "remote")]
use kameo::remote::type_hash::{TypeHash, HasTypeHash};

// Define some types with type hashes
struct MyActor;
struct MyMessage;

// Implement HasTypeHash with compile-time hash
impl HasTypeHash for MyActor {
    const TYPE_HASH: TypeHash = TypeHash::from_bytes(b"MyActor");
}

impl HasTypeHash for MyMessage {
    const TYPE_HASH: TypeHash = TypeHash::from_bytes(b"MyMessage");
}

// Function that uses type hashes
#[inline(never)]  // Prevent inlining to see the function in assembly
pub fn check_type_hash(hash: TypeHash) -> &'static str {
    // These comparisons should use immediate values in assembly
    if hash.0 == MyActor::TYPE_HASH.0 {
        "MyActor"
    } else if hash.0 == MyMessage::TYPE_HASH.0 {
        "MyMessage"
    } else {
        "Unknown"
    }
}

// Function that computes hash at runtime for comparison
#[inline(never)]
pub fn runtime_hash_computation(name: &str) -> TypeHash {
    TypeHash::from_bytes(name.as_bytes())
}

fn main() {
    // Print the compile-time computed hashes
    println!("MyActor hash (compile-time): 0x{:016x}", MyActor::TYPE_HASH.0);
    println!("MyMessage hash (compile-time): 0x{:016x}", MyMessage::TYPE_HASH.0);
    
    // Use the check function
    let actor_hash = MyActor::TYPE_HASH;
    let result = check_type_hash(actor_hash);
    println!("Check result: {}", result);
    
    // Compare with runtime computation
    let runtime_actor_hash = runtime_hash_computation("MyActor");
    println!("Runtime computed hash: 0x{:016x}", runtime_actor_hash.0);
    println!("Hashes match: {}", runtime_actor_hash.0 == MyActor::TYPE_HASH.0);
    
    // Force use of the values to prevent optimization
    std::hint::black_box(check_type_hash(MyMessage::TYPE_HASH));
}