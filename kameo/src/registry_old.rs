use libp2p::kad::store::{MemoryStore, RecordStore};
use libp2p::kad::Mode;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{kad, mdns, noise, tcp, yamux, Multiaddr, Swarm};
use std::borrow::Cow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::{SocketAddr, SocketAddrV6};
use std::time::Duration;
use tarpc::client::RpcError;
use tarpc::context;
use tonic::transport::{Channel, Uri};

use crate::actor::remote::{ActorServiceClient, RemoteActor};
use crate::actor::{ActorID, ActorRef, BoundedMailbox, RemoteActorRef};
use crate::message::{Context, Message};
use crate::Actor;

pub struct ActorRegistry {
    swarm: Swarm<Behaviour>,
    nodes: HashMap<SocketAddrV6, ActorServiceClient<Channel>>,
    addr: SocketAddrV6,
}

impl ActorRegistry {
    // Function to initialize a new registry actor
    pub async fn new(addr: SocketAddrV6) -> Result<Self, noise::Error> {
        let mut swarm = libp2p::SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|key| {
                let mut kademlia = kad::Behaviour::new(
                    key.public().to_peer_id(),
                    MemoryStore::new(key.public().to_peer_id()),
                );
                kademlia.set_mode(Some(Mode::Server));
                Ok(Behaviour {
                    kademlia,
                    mdns: mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        key.public().to_peer_id(),
                    )?,
                })
            })
            .unwrap()
            // .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        swarm
            .listen_on(
                format!("/ip6/{}/tcp/{}", addr.ip(), addr.port())
                    .parse()
                    .unwrap(),
            )
            .unwrap();

        Ok(ActorRegistry {
            swarm,
            nodes: HashMap::new(),
            addr,
        })
    }

    // Function to register a new actor by name
    pub async fn register_actor<A: RemoteActor>(
        &mut self,
        name: impl Into<String>,
        actor_id: ActorID,
    ) -> Result<(), RpcError> {
        let key = name.into().into_bytes();
        let value = ActorRegistration {
            actor_id,
            remote_id: A::REMOTE_ID,
        }
        .into_bytes();

        self.swarm
            .behaviour_mut()
            .kademlia
            .put_record(kad::Record::new(key, value), kad::Quorum::One)
            .unwrap();

        Ok(())
    }

    pub async fn lookup_actor_id(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<ActorID>, RpcError> {
        let key = name.into().into_bytes();

        let ctx = context::current();
        let Some(bytes) = self.client.get_rpc(ctx, key).await? else {
            return Ok(None);
        };
        let ActorRegistration { actor_id, .. } = ActorRegistration::from_bytes(&bytes);

        Ok(Some(actor_id))
    }

    // Function to look up an actor by name
    pub async fn lookup_actor<A: Actor + RemoteActor>(
        &mut self,
        name: impl Into<String>,
    ) -> Result<Option<RemoteActorRef<A>>, RpcError> {
        let key = name.into().into_bytes();

        let ctx = context::current();
        let Some(bytes) = self.client.get_rpc(ctx, key).await? else {
            return Ok(None);
        };
        let ActorRegistration {
            actor_id,
            remote_id,
        } = ActorRegistration::from_bytes(&bytes);
        if remote_id != A::REMOTE_ID {
            panic!("invalid actor type")
        }

        let node_addr = match actor_id.addr() {
            SocketAddr::V4(addr_v4) => {
                SocketAddrV6::new(addr_v4.ip().to_ipv6_mapped(), addr_v4.port(), 0, 0)
            }
            SocketAddr::V6(addr) => addr,
        };
        if node_addr == self.addr {
            // It's a local actor
            unimplemented!()
        }

        match self.nodes.get(&node_addr) {
            Some(client) => Ok(Some(RemoteActorRef::new(actor_id, client.clone()))),
            None => {
                let client = ActorServiceClient::connect(
                    Uri::builder()
                        .scheme("http")
                        .authority(node_addr.to_string())
                        .build()
                        .unwrap(),
                )
                .await
                .unwrap();
                self.nodes.insert(node_addr, client.clone());
                Ok(Some(RemoteActorRef::new(actor_id, client)))
            }
        }
    }
}

// Server setup function
pub async fn start_registry_server(addr: &str, node_id: u64) -> Result<()> {
    let node = Node {
        addr: addr.to_string(),
        id: node_id,
    };
    let mut server = NodeServer::new(node, Config::default());
    let manager = server.start(None).await?;
    manager.wait().await?;

    Ok(())
}

impl Actor for ActorRegistry {
    type Mailbox = BoundedMailbox<Self>;
}

pub struct RegisterActor<A: RemoteActor> {
    name: String,
    actor_id: ActorID,
    phantom: PhantomData<A>,
}

impl<A: RemoteActor + Send> Message<RegisterActor<A>> for ActorRegistry {
    type Reply = Result<(), RpcError>;

    async fn handle(
        &mut self,
        RegisterActor { name, actor_id, .. }: RegisterActor<A>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.register_actor::<A>(name, actor_id).await
    }
}

struct LookupActorID {
    name: String,
}

impl Message<LookupActorID> for ActorRegistry {
    type Reply = Result<Option<ActorID>, RpcError>;

    async fn handle(
        &mut self,
        LookupActorID { name }: LookupActorID,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.lookup_actor_id(name).await
    }
}

struct LookupActor<A: Actor + RemoteActor> {
    name: String,
    phantom: PhantomData<A>,
}

impl<A: Actor + RemoteActor + Send + 'static> Message<LookupActor<A>> for ActorRegistry {
    type Reply = Result<Option<RemoteActorRef<A>>, RpcError>;

    async fn handle(
        &mut self,
        LookupActor { name, .. }: LookupActor<A>,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.lookup_actor::<A>(name).await
    }
}

struct ActorRegistration<'a> {
    actor_id: ActorID,
    remote_id: &'a str,
}

impl<'a> ActorRegistration<'a> {
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = self.actor_id.to_bytes().to_vec();
        bytes.extend_from_slice(self.remote_id.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        let actor_id = ActorID::from_bytes(bytes).unwrap();
        let remote_id = std::str::from_utf8(&bytes[28..]).unwrap();
        ActorRegistration {
            actor_id,
            remote_id,
        }
    }
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    kademlia: kad::Behaviour<ForwarderStore>,
    mdns: mdns::tokio::Behaviour,
}

struct ForwarderStore {
    actor_ref: ActorRef<ActorRegistry>,
}

impl RecordStore for ForwarderStore {
    type RecordsIter<'a> = RecordsIter<'a>;

    type ProvidedIter<'a> = ProviderIter<'a>;

    fn get(&self, k: &kad::RecordKey) -> Option<Cow<'_, kad::Record>> {
        let actor_id = self
            .actor_ref
            .ask(LookupActor {
                name: std::str::from_utf8(k.as_ref()).unwrap().to_string(),
                phantom: PhantomData,
            })
            .blocking_send()
            .unwrap()?;
        Some(Cow::Owned(kad::Record::new(
            k.to_vec(),
            actor_id.to_bytes().to_vec(),
        )))
    }

    fn put(&mut self, r: kad::Record) -> kad::store::Result<()> {
        self.actor_ref
            .tell(RegisterActor {
                name: std::str::from_utf8(r.key.as_ref()).unwrap().to_string(),
                actor_id: ActorID::from_bytes(&r.value).unwrap(),
                phantom: PhantomData::<UnknownActor>,
            })
            .blocking_send()
            .unwrap();

        todo!()
    }

    fn remove(&mut self, k: &kad::RecordKey) {
        todo!()
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        unimplemented!()
    }

    fn add_provider(&mut self, record: kad::ProviderRecord) -> kad::store::Result<()> {
        unimplemented!()
    }

    fn providers(&self, key: &kad::RecordKey) -> Vec<kad::ProviderRecord> {
        unimplemented!()
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        unimplemented!()
    }

    fn remove_provider(&mut self, k: &kad::RecordKey, p: &libp2p::PeerId) {
        unimplemented!()
    }
}

struct UnknownActor;

impl RemoteActor for UnknownActor {
    const REMOTE_ID: &'static str = "__UNKNOWN_ACTOR__";
}

struct RecordsIter<'a> {
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for RecordsIter<'a> {
    type Item = Cow<'a, kad::Record>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

struct ProviderIter<'a> {
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for ProviderIter<'a> {
    type Item = Cow<'a, kad::ProviderRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
