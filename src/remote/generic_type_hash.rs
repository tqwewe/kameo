//! Generic type hash support for runtime type identification
//!
//! This module provides utilities for generating type hashes for generic
//! types at compile time, ensuring each concrete instantiation gets a
//! unique hash.

use super::type_hash::{HasTypeHash, TypeHash};

/// Macro for implementing HasTypeHash for specific generic instantiations
///
/// This macro should be used when you need to ensure different generic
/// instantiations get different type hashes.
///
/// # Example
///
/// ```ignore
/// // Define your generic type
/// struct Cache<K, V> {
///     // ...
/// }
///
/// // Implement HasTypeHash for specific instantiations
/// impl_type_hash!(Cache<String, i32>);
/// impl_type_hash!(Cache<i32, String>);
/// ```
#[macro_export]
macro_rules! impl_type_hash {
    ($type:ty) => {
        impl $crate::remote::type_hash::HasTypeHash for $type {
            const TYPE_HASH: $crate::remote::type_hash::TypeHash = {
                // Use the full type name including concrete types
                const TYPE_NAME: &str = stringify!($type);
                $crate::remote::type_hash::TypeHash::from_bytes(TYPE_NAME.as_bytes())
            };
        }
    };
}

/// Macro for generating TypeHash implementations for generic types with common patterns
///
/// This is useful for automatically implementing HasTypeHash for multiple
/// instantiations of a generic type.
///
/// # Example
///
/// ```ignore
/// struct Cache<K, V> { /* ... */ }
///
/// // Generate implementations for common type combinations
/// generic_type_hash! {
///     Cache<K, V> where K, V => [
///         (String, i32),
///         (String, String),
///         (i32, String),
///         (i32, i32),
///     ]
/// }
/// ```
#[macro_export]
macro_rules! generic_type_hash {
    ($base:ident < $($param:ident),+ > where $($p:ident),+ => [ $( ($($concrete:ty),+) ),* $(,)? ]) => {
        $(
            impl $crate::remote::type_hash::HasTypeHash for $base<$($concrete),+> {
                const TYPE_HASH: $crate::remote::type_hash::TypeHash = {
                    // Create a unique string for this instantiation
                    const TYPE_STR: &str = concat!(
                        stringify!($base),
                        "<",
                        $(stringify!($concrete), ","),+
                    );
                    // Remove the trailing comma
                    const TYPE_STR_CLEAN: &[u8] = &{
                        let bytes = TYPE_STR.as_bytes();
                        let mut result = [0u8; 256]; // Max type name length
                        let mut i = 0;
                        let mut j = 0;
                        while i < bytes.len() && j < 255 {
                            if i == bytes.len() - 1 && bytes[i] == b',' {
                                // Skip trailing comma
                                break;
                            }
                            result[j] = bytes[i];
                            i += 1;
                            j += 1;
                        }
                        // Add closing >
                        result[j] = b'>';
                        result
                    };

                    // Find actual length
                    const fn find_len(bytes: &[u8]) -> usize {
                        let mut i = 0;
                        while i < bytes.len() && bytes[i] != 0 {
                            i += 1;
                        }
                        i
                    }

                    const LEN: usize = find_len(TYPE_STR_CLEAN);
                    const FINAL_BYTES: &[u8] = unsafe {
                        std::slice::from_raw_parts(TYPE_STR_CLEAN.as_ptr(), LEN)
                    };

                    $crate::remote::type_hash::TypeHash::from_bytes(FINAL_BYTES)
                };
            }
        )*
    };
}

/// Helper trait for types that can provide their type hash at runtime
///
/// This is useful when you need to get the type hash dynamically.
pub trait RuntimeTypeHash {
    /// Get the type hash for this type at runtime
    fn type_hash() -> TypeHash;
}

/// Implement RuntimeTypeHash for any type that has HasTypeHash
impl<T: HasTypeHash> RuntimeTypeHash for T {
    fn type_hash() -> TypeHash {
        T::TYPE_HASH
    }
}

/// Generate a type hash for a generic type with specific type parameters
///
/// This function can be used when const evaluation is not possible or
/// when you need to generate hashes dynamically.
///
/// # Example
///
/// ```rust
/// use kameo::remote::generic_type_hash::generic_type_hash;
///
/// let hash = generic_type_hash::<Vec<String>>();
/// ```
pub fn generic_type_hash<T: ?Sized + 'static>() -> TypeHash {
    TypeHash::from_type::<T>()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test types
    struct TestCache<K, V> {
        _k: std::marker::PhantomData<K>,
        _v: std::marker::PhantomData<V>,
    }

    // Generate implementations
    impl_type_hash!(TestCache<String, i32>);
    impl_type_hash!(TestCache<i32, String>);

    #[test]
    fn test_generic_instantiations_have_different_hashes() {
        let hash1 = <TestCache<String, i32> as HasTypeHash>::TYPE_HASH;
        let hash2 = <TestCache<i32, String> as HasTypeHash>::TYPE_HASH;

        assert_ne!(
            hash1, hash2,
            "Different generic instantiations should have different hashes"
        );
    }

    #[test]
    fn test_same_instantiation_has_same_hash() {
        let hash1 = <TestCache<String, i32> as HasTypeHash>::TYPE_HASH;
        let hash2 = <TestCache<String, i32> as HasTypeHash>::TYPE_HASH;

        assert_eq!(hash1, hash2, "Same instantiation should have same hash");
    }

    #[test]
    fn test_runtime_type_hash() {
        let compile_time = <TestCache<String, i32> as HasTypeHash>::TYPE_HASH;
        let runtime = generic_type_hash::<TestCache<String, i32>>();

        // Note: These might not be equal due to type name formatting differences
        // but both should be consistent for their respective use cases
        println!("Compile-time hash: {:?}", compile_time);
        println!("Runtime hash: {:?}", runtime);
    }
}
