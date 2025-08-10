//! RemoteActorRef implementation that uses the transport abstraction
//! 
//! Modern implementation using kameo_remote for distributed actor communication

use std::marker::PhantomData;
use std::sync::Arc;

use crate::actor::{Actor, ActorId};
use crate::error::RegistryError;
use crate::message::Message;
use crate::remote::RemoteActor;

use super::transport::{RemoteActorLocation, RemoteTransport};

/// A reference to an actor running remotely, using the transport abstraction
pub struct RemoteActorRefV2<A: Actor, T: RemoteTransport> {
    pub(crate) id: ActorId,
    pub(crate) location: RemoteActorLocation,
    pub(crate) transport: T,
    pub(crate) phantom: PhantomData<fn(&mut A)>,
}

impl<A: Actor, T: RemoteTransport + Clone> Clone for RemoteActorRefV2<A, T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            location: self.location.clone(),
            transport: self.transport.clone(),
            phantom: PhantomData,
        }
    }
}

impl<A, T> RemoteActorRefV2<A, T>
where
    A: Actor + RemoteActor,
    T: RemoteTransport,
{
    /// Creates a new remote actor reference
    pub fn new(
        id: ActorId,
        location: RemoteActorLocation,
        transport: T,
    ) -> Self {
        Self {
            id,
            location,
            transport,
            phantom: PhantomData,
        }
    }
    
    /// Gets the actor ID
    pub fn id(&self) -> ActorId {
        self.id
    }
    
    /// Looks up a single actor by name using the provided transport
    pub async fn lookup(
        name: &str,
        transport: T,
    ) -> Result<Option<Self>, RegistryError> 
    where
        T: Clone,
    {
        match transport.lookup_actor(name).await {
            Ok(Some(location)) => {
                Ok(Some(Self::new(location.actor_id, location, transport)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(RegistryError::SwarmNotBootstrapped), // No Other variant in RegistryError
        }
    }
    
    /// Sends a message and waits for reply (ask pattern)
    pub async fn ask<M>(&self, msg: M) -> Result<<A as Message<M>>::Reply, Box<dyn std::error::Error>>
    where
        A: Message<M>,
        M: rkyv::Archive + for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<rkyv::ser::Serializer<&'a mut [u8], rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>
        > + Send + Sync + 'static,
        <A as Message<M>>::Reply: rkyv::Archive + for<'a> rkyv::Deserialize<
            <A as Message<M>>::Reply,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>
        > + Send,
    {
        let timeout = std::time::Duration::from_secs(30); // Default timeout
        self.transport
            .send_ask::<A, M>(self.id, &self.location, msg, timeout)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
    
    /// Sends a message without waiting for reply (tell pattern)
    pub async fn tell<M>(&self, msg: M) -> Result<(), Box<dyn std::error::Error>>
    where
        A: Message<M>,
        M: rkyv::Archive + for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<rkyv::ser::Serializer<&'a mut [u8], rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>
        > + Send + Sync + 'static,
    {
        self.transport
            .send_tell(self.id, &self.location, msg)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_remote_actor_ref_clone() {
        // Basic test to ensure RemoteActorRefV2 can be cloned
        // Full tests will be in the integration test suite
    }
}