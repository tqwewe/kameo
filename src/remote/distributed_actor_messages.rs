//! Macros for declaring distributed actor message types
//!
//! This module provides ergonomic macros for declaring which concrete
//! instantiations of generic message types should be supported for
//! distributed actors.

/// Declare message types for a distributed actor
///
/// This macro generates `HasTypeHash` implementations for all the
/// specified message type instantiations. Use this before implementing
/// the `distributed_actor!` macro.
///
/// # Example
///
/// ```ignore
/// // First, declare your generic types
/// struct Cache<K, V> { ... }
/// struct Get<K> { key: K }
/// struct Set<K, V> { key: K, value: V }
///
/// // Then declare which instantiations you'll use
/// declare_message_types! {
///     // For Cache<String, i32>
///     Cache<String, i32> => {
///         Get<String>,
///         Set<String, i32>,
///     },
///     
///     // For Cache<String, String>
///     Cache<String, String> => {
///         Get<String>,
///         Set<String, String>,
///     },
///     
///     // For Cache<i32, String>
///     Cache<i32, String> => {
///         Get<i32>,
///         Set<i32, String>,
///     }
/// }
///
/// // Now use distributed_actor! macro
/// distributed_actor! {
///     impl<K, V> DistributedActor for Cache<K, V>
///     where /* bounds */
///     {
///         Get<K> => handle_get,
///         Set<K, V> => handle_set,
///     }
/// }
/// ```
#[macro_export]
macro_rules! declare_message_types {
    (
        $(
            $actor:ty => {
                $($msg:ty),* $(,)?
            }
        ),* $(,)?
    ) => {
        // Generate HasTypeHash for the actor types
        $(
            $crate::impl_type_hash!($actor);
        )*

        // Generate HasTypeHash for all message types (deduped by compiler)
        $(
            $(
                $crate::impl_type_hash!($msg);
            )*
        )*
    };
}

/// Alternative syntax: declare message types inline with the actor
///
/// This provides a more compact way to declare message types right
/// where you define your distributed actor implementation.
///
/// # Example
///
/// ```ignore
/// distributed_actor_with_types! {
///     actor: Cache<K, V>
///     messages: {
///         Cache<String, i32> => [Get<String>, Set<String, i32>],
///         Cache<String, String> => [Get<String>, Set<String, String>],
///         Cache<i32, String> => [Get<i32>, Set<i32, String>],
///     }
///     
///     impl<K, V> DistributedActor
///     where
///         K: Eq + Hash + Clone + Send + 'static + Serialize + for<'de> Deserialize<'de>,
///         V: Clone + Send + 'static + Serialize + for<'de> Deserialize<'de>,
///     {
///         Get<K> => handle_get,
///         Set<K, V> => handle_set,
///     }
/// }
/// ```
#[macro_export]
macro_rules! distributed_actor_with_types {
    (
        actor: $actor_base:ident < $($gen:ident),* >
        messages: {
            $($actor_concrete:ty => [$($msg:ty),*]),* $(,)?
        }

        impl<$($g:ident),*> DistributedActor
        $(where $($where_clause:tt)*)?
        {
            $($msg_ty:ty => $handler:ident),* $(,)?
        }
    ) => {
        // First, generate all the type hash implementations
        $crate::declare_message_types! {
            $(
                $actor_concrete => {
                    $($msg),*
                }
            ),*
        }

        // Then use the regular distributed_actor! macro
        $crate::distributed_actor! {
            impl<$($g: $crate::remote::distributed_actor::DistributedBound),*> DistributedActor for $actor_base<$($g),*>
            $(where $($where_clause)*)?
            {
                $($msg_ty => $handler),*
            }
        }
    };
}

/// Helper trait that combines common bounds for distributed actors
///
/// This is used internally by the macros to reduce boilerplate
pub trait DistributedBound: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> DistributedBound for T {}
