//! Local actor registry for registering and looking up actors by arbitrary names.

use std::{
    any::Any,
    borrow::{Borrow, Cow},
    collections::{hash_map::Keys, HashMap},
    hash::Hash,
    sync::{Arc, Mutex},
};

use once_cell::sync::Lazy;

use crate::{actor::ActorRef, error::RegistryError, Actor};

/// Global actor registry for local actors.
pub static ACTOR_REGISTRY: Lazy<Arc<Mutex<ActorRegistry>>> =
    Lazy::new(|| Arc::new(Mutex::new(ActorRegistry::new())));

type AnyActorRef = Box<dyn Any + Send>;

/// A local actor registry storing actor refs by name.
#[derive(Debug)]
pub struct ActorRegistry {
    actor_refs: HashMap<Cow<'static, str>, AnyActorRef>,
}

impl ActorRegistry {
    /// Creates a new empty actor registry.
    pub fn new() -> Self {
        ActorRegistry {
            actor_refs: HashMap::new(),
        }
    }

    /// Creates a new empty actor registry with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        ActorRegistry {
            actor_refs: HashMap::with_capacity(capacity),
        }
    }

    /// Returns the number of actor refs that can be held without reallocating.
    pub fn capacity(&self) -> usize {
        self.actor_refs.capacity()
    }

    /// An iterator visiting all registered actor refs in arbitrary order.
    pub fn names(&self) -> Keys<'_, Cow<'static, str>, AnyActorRef> {
        self.actor_refs.keys()
    }

    /// The number of registered actor refs.
    pub fn len(&self) -> usize {
        self.actor_refs.len()
    }

    /// Returns `true` if the registry contains no actor refs.
    pub fn is_empty(&self) -> bool {
        self.actor_refs.is_empty()
    }

    /// Clears the registry, removing all actor refs. Keeps the allocated memory for reuse.
    pub fn clear(&mut self) {
        self.actor_refs.clear()
    }

    /// Gets an actor ref previously registered for a given actor type.
    ///
    /// If the actor type does not match the one it was registered with,
    /// a [`RegistryError::BadActorType`] error will be returned.
    pub fn get<A, Q>(&mut self, name: &Q) -> Result<Option<ActorRef<A>>, RegistryError>
    where
        A: Actor,
        Q: Hash + Eq + ?Sized,
        Cow<'static, str>: Borrow<Q>,
    {
        self.actor_refs
            .get(name)
            .map(|actor_ref| {
                actor_ref
                    .downcast_ref()
                    .cloned()
                    .ok_or(RegistryError::BadActorType)
            })
            .transpose()
    }

    /// Returns `true` if an actor has been registered under a given name.
    pub fn contains_name<Q>(&self, name: &Q) -> bool
    where
        Q: Hash + Eq + ?Sized,
        Cow<'static, str>: Borrow<Q>,
    {
        self.actor_refs.contains_key(name)
    }

    /// Inserts a new actor ref under a given name, which can be used later to be looked up.
    pub fn insert<A: Actor>(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        actor_ref: ActorRef<A>,
    ) -> bool {
        let name = name.into();
        if self.actor_refs.contains_key(&name) {
            return false;
        }

        self.actor_refs.insert(name, Box::new(actor_ref));
        true
    }

    /// Removes a previously registered actor ref under a given name.
    pub fn remove<Q>(&mut self, name: &Q) -> bool
    where
        Q: Hash + Eq + ?Sized,
        Cow<'static, str>: Borrow<Q>,
    {
        self.actor_refs.remove(name).is_some()
    }
}

impl Default for ActorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
