kameo//! This example PROVES type hashes are compile-time by using them in const contexts

#[cfg(feature = "remote")]
use kameo::remote::type_hash::{TypeHash, HasTypeHash};

#[cfg(not(feature = "remote"))]
compile_error!("This example requires the 'remote' feature");

struct MyActor;
impl HasTypeHash for MyActor {
    const TYPE_HASH: TypeHash = TypeHash::from_bytes(b"MyActor");
}

// PROOF 1: Use the hash in another const context
// This would FAIL TO COMPILE if TYPE_HASH wasn't computed at compile time!
const MY_ACTOR_HASH_VALUE: u64 = MyActor::TYPE_HASH.0;

// PROOF 2: Use in array size (must be compile-time constant)
// This would FAIL TO COMPILE if not compile-time!
const HASH_AS_SIZE: usize = (MyActor::TYPE_HASH.0 & 0xFF) as usize;
type ArraySizedByHash = [u8; HASH_AS_SIZE];

// PROOF 3: Use in match patterns (must be compile-time constant)
const ACTOR_PATTERN: u64 = MyActor::TYPE_HASH.0;

fn match_on_hash(hash: u64) -> &'static str {
    match hash {
        ACTOR_PATTERN => "It's MyActor!",  // This only works with compile-time constants!
        _ => "Unknown"
    }
}

// PROOF 4: Use in const fn
const fn get_compile_time_hash() -> u64 {
    MyActor::TYPE_HASH.0  // This only works if TYPE_HASH is compile-time!
}

const COMPILE_TIME_RESULT: u64 = get_compile_time_hash();

fn main() {
    println!("=== PROOF: Type Hashes are Compile-Time Constants ===\n");
    
    println!("1. Used in const context: MY_ACTOR_HASH_VALUE = 0x{:016x}", MY_ACTOR_HASH_VALUE);
    println!("   If this compiled, the hash MUST be compile-time!\n");
    
    println!("2. Used as array size: HASH_AS_SIZE = {}", HASH_AS_SIZE);
    println!("   Arrays require compile-time sizes!\n");
    
    println!("3. Used in match pattern: {}", match_on_hash(MyActor::TYPE_HASH.0));
    println!("   Match patterns must be compile-time constants!\n");
    
    println!("4. Used in const fn: COMPILE_TIME_RESULT = 0x{:016x}", COMPILE_TIME_RESULT);
    println!("   const fn can only use compile-time values!\n");
    
    println!("=== CONCLUSION ===");
    println!("This program compiles successfully, which PROVES that");
    println!("TYPE_HASH is computed at compile time, not runtime!");
    
    // Verify the hash is deterministic
    let runtime_hash = TypeHash::from_bytes(b"MyActor");
    println!("\nVerification: runtime computation gives same result: {}", 
             runtime_hash.0 == MY_ACTOR_HASH_VALUE);
}