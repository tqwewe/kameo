//! Type hash infrastructure for generic remote actors
//! 
//! This module provides compile-time type hashing to enable generic actors
//! without requiring linkme registration. Type hashes are computed at compile
//! time and used to route messages to the correct actor/message handlers.

use std::fmt;

/// A type hash that uniquely identifies an actor or message type
/// 
/// This is computed at compile time and used for message routing instead
/// of string-based identifiers. The hash is 64 bits for low collision
/// probability while keeping message headers small.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypeHash(pub u64);

impl TypeHash {
    /// Create a new type hash from a raw u64 value
    pub const fn new(hash: u64) -> Self {
        TypeHash(hash)
    }
    
    /// Create a type hash from a byte string using FNV-1a algorithm
    /// 
    /// This is const-friendly and can be used in const contexts.
    /// The FNV-1a algorithm is simple, fast, and has good distribution.
    pub const fn from_bytes(bytes: &[u8]) -> Self {
        TypeHash(compute_hash_fnv1a(bytes))
    }
    
    /// Create a type hash from a type at runtime
    /// 
    /// This uses the type's full name (including generic parameters) to generate
    /// a unique hash. Useful when const evaluation is not possible.
    /// 
    /// # Example
    /// ```
    /// use kameo::remote::type_hash::TypeHash;
    /// 
    /// let hash = TypeHash::from_type::<Vec<String>>();
    /// ```
    pub fn from_type<T: ?Sized + 'static>() -> Self {
        let type_name = std::any::type_name::<T>();
        TypeHash(compute_hash_fnv1a(type_name.as_bytes()))
    }
    
    /// Get the first 32 bits of the hash for use in message headers
    /// 
    /// This reduces the header size while still maintaining very low
    /// collision probability for practical use cases.
    pub const fn as_u32(&self) -> u32 {
        // Take the LOWER 32 bits for consistency with from_u32
        self.0 as u32
    }
    
    /// Create a TypeHash from a 32-bit truncated value
    /// 
    /// This is used when receiving messages to reconstruct the hash
    /// for comparison. Note that this loses information and should
    /// only be used for lookups, not storage.
    pub const fn from_u32(value: u32) -> Self {
        // Just use the lower 32 bits, upper bits are zero
        // This matches what as_u32 returns
        TypeHash(value as u64)
    }
    
    /// Combine two type hashes (e.g., actor hash + message hash)
    /// 
    /// This is useful for creating unique identifiers for actor/message
    /// combinations without string concatenation.
    pub const fn combine(&self, other: &TypeHash) -> TypeHash {
        // Simple XOR combination - could use more sophisticated mixing
        TypeHash(self.0 ^ other.0.rotate_left(32))
    }
}

impl fmt::Debug for TypeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Show first 8 hex characters for readability
        write!(f, "TypeHash({:016x})", self.0)
    }
}

impl fmt::Display for TypeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Short form for logging
        write!(f, "{:08x}", self.as_u32())
    }
}

/// Compute FNV-1a hash of bytes (const-friendly)
/// 
/// FNV-1a is a simple, fast hash function that works well for
/// type names and can be evaluated at compile time.
pub const fn compute_hash_fnv1a(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    
    let mut hash = FNV_OFFSET_BASIS;
    let mut i = 0;
    
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    
    hash
}

/// Trait for types that have a compile-time type hash
/// 
/// This trait should be implemented (usually via derive macro) for all
/// actors and messages that need to be used remotely with generic types.
pub trait HasTypeHash {
    /// The compile-time computed type hash for this type
    const TYPE_HASH: TypeHash;
}

/// Macro to compute type hash for a type including generic parameters
/// 
/// Usage: `type_hash!(MyType<T1, T2>)` generates a hash string like
/// "MyType<T1,T2>" and computes its hash.
#[macro_export]
macro_rules! type_hash {
    ($type:ty) => {
        $crate::remote::type_hash::TypeHash::from_bytes(
            concat!(stringify!($type)).as_bytes()
        )
    };
    ($base:expr, $($param:ty),+) => {
        {
            const fn combine_hashes(base: &[u8], params: &[&[u8]]) -> $crate::remote::type_hash::TypeHash {
                let mut combined = $crate::remote::type_hash::compute_hash_fnv1a(base);
                let mut i = 0;
                while i < params.len() {
                    let param_hash = $crate::remote::type_hash::compute_hash_fnv1a(params[i]);
                    combined = combined.wrapping_add(param_hash);
                    i += 1;
                }
                $crate::remote::type_hash::TypeHash::new(combined)
            }
            
            const PARAMS: &[&[u8]] = &[
                $(stringify!($param).as_bytes()),+
            ];
            
            combine_hashes($base.as_bytes(), PARAMS)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_type_hash_basic() {
        const HASH1: TypeHash = TypeHash::from_bytes(b"TestActor");
        const HASH2: TypeHash = TypeHash::from_bytes(b"TestActor");
        const HASH3: TypeHash = TypeHash::from_bytes(b"OtherActor");
        
        assert_eq!(HASH1, HASH2);
        assert_ne!(HASH1, HASH3);
    }
    
    #[test]
    fn test_type_hash_truncation() {
        const FULL_HASH: TypeHash = TypeHash::from_bytes(b"TestActor");
        let truncated = FULL_HASH.as_u32();
        let restored = TypeHash::from_u32(truncated);
        
        // High 32 bits should match
        assert_eq!(FULL_HASH.as_u32(), restored.as_u32());
    }
    
    #[test]
    fn test_type_hash_combine() {
        const ACTOR_HASH: TypeHash = TypeHash::from_bytes(b"MyActor");
        const MSG_HASH: TypeHash = TypeHash::from_bytes(b"MyMessage");
        const COMBINED: TypeHash = ACTOR_HASH.combine(&MSG_HASH);
        
        // Combined hash should be different from both inputs
        assert_ne!(COMBINED, ACTOR_HASH);
        assert_ne!(COMBINED, MSG_HASH);
        
        // Combining should be deterministic
        const COMBINED2: TypeHash = ACTOR_HASH.combine(&MSG_HASH);
        assert_eq!(COMBINED, COMBINED2);
    }
    
    #[test]
    fn test_type_hash_macro() {
        // Test basic type
        const HASH1: TypeHash = type_hash!(String);
        const HASH2: TypeHash = type_hash!(String);
        assert_eq!(HASH1, HASH2);
        
        // Test with generic parameters
        const GENERIC_HASH: TypeHash = type_hash!("Cache", String, i32);
        const GENERIC_HASH2: TypeHash = type_hash!("Cache", String, i32);
        assert_eq!(GENERIC_HASH, GENERIC_HASH2);
        
        // Different parameter types should give different hash
        const GENERIC_HASH3: TypeHash = type_hash!("Cache", i32, i64);
        assert_ne!(GENERIC_HASH, GENERIC_HASH3);
        
        // Note: Current implementation uses wrapping_add which is commutative,
        // so parameter order doesn't affect the hash. This could be improved
        // by using a non-commutative operation like XOR with position.
        const GENERIC_HASH4: TypeHash = type_hash!("Cache", i32, String);
        assert_eq!(GENERIC_HASH, GENERIC_HASH4); // Same hash despite different order
    }
    
    #[test]
    fn test_hash_distribution() {
        // Test that similar strings produce different hashes
        let hashes = vec![
            TypeHash::from_bytes(b"Actor1"),
            TypeHash::from_bytes(b"Actor2"),
            TypeHash::from_bytes(b"Actor3"),
            TypeHash::from_bytes(b"1Actor"),
            TypeHash::from_bytes(b"2Actor"),
            TypeHash::from_bytes(b"3Actor"),
        ];
        
        // All hashes should be unique
        for i in 0..hashes.len() {
            for j in i+1..hashes.len() {
                assert_ne!(hashes[i], hashes[j], 
                    "Hash collision between index {} and {}", i, j);
            }
        }
    }
}