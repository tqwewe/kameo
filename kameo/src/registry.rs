use std::{borrow::Cow, collections::HashMap, time::Duration};

use libp2p::{
    kad::{
        self,
        store::{MemoryStore, RecordStore},
    },
    mdns, noise,
    request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::trace;

use crate::{
    actor::{
        remote::{RemoteActor, REMOTE_REGISTRY},
        ActorID, ActorRef, RemoteActorRef,
    },
    error::{RemoteSendError, RemoteSpawnError},
    Actor,
};

static ACTOR_REGISTRY: OnceCell<ActorRegistry> = OnceCell::new();

pub struct ActorRegistry {
    swarm_tx: mpsc::Sender<SwarmMessage>,
    local_peer_id: PeerId,
}

impl ActorRegistry {
    pub fn bootstrap() -> &'static Self {
        ACTOR_REGISTRY.get_or_init(|| {
            let mut swarm = SwarmBuilder::with_new_identity()
                .with_tokio()
                .with_tcp(
                    tcp::Config::default(),
                    noise::Config::new,
                    yamux::Config::default,
                )
                .unwrap()
                .with_behaviour(|key| {
                    let mut kademlia = kad::Behaviour::new(
                        key.public().to_peer_id(),
                        MemoryStore::new(key.public().to_peer_id()),
                    );
                    kademlia.set_mode(Some(kad::Mode::Server));
                    Ok(Behaviour {
                        kademlia,
                        mdns: mdns::tokio::Behaviour::new(
                            mdns::Config::default(),
                            key.public().to_peer_id(),
                        )?,
                        request_response: request_response::cbor::Behaviour::new(
                            [(StreamProtocol::new("/kameo/1"), ProtocolSupport::Full)],
                            request_response::Config::default(),
                        ),
                    })
                })
                .unwrap()
                .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
                .build();
            swarm
                .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
                .unwrap();
            let local_peer_id = swarm.local_peer_id().clone();

            let (swarm_tx, swarm_rx) = mpsc::channel(24);
            tokio::spawn({
                let swarm_tx = swarm_tx.clone();
                async move { swarm_actor(swarm, swarm_tx, swarm_rx).await }
            });

            ActorRegistry {
                swarm_tx,
                local_peer_id,
            }
        })
    }

    pub fn get() -> Option<&'static Self> {
        ACTOR_REGISTRY.get()
    }

    pub async fn lookup_local<A: Actor + RemoteActor + 'static>(
        &self,
        name: String,
    ) -> Option<ActorRef<A>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.swarm_tx
            .send(SwarmMessage::LookupLocal {
                key: name.into_bytes().into(),
                reply: reply_tx,
            })
            .await
            .unwrap();

        let ActorRegistration {
            actor_id,
            remote_id,
        } = reply_rx.await.unwrap()?;
        if A::REMOTE_ID != remote_id {
            panic!("not the right actor type");
        }
        let actor_ref: ActorRef<A> = REMOTE_REGISTRY
            .lock()
            .await
            .get(&actor_id)?
            .downcast_ref()
            .cloned()
            .unwrap();

        Some(actor_ref)
    }

    pub async fn lookup<A: Actor + RemoteActor>(&self, name: String) -> Option<RemoteActorRef<A>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.swarm_tx
            .send(SwarmMessage::Lookup {
                key: name.into_bytes().into(),
                reply: reply_tx,
            })
            .await
            .unwrap();

        match reply_rx.await.unwrap() {
            Ok(kad::PeerRecord { peer, record }) => {
                let ActorRegistration {
                    actor_id,
                    remote_id,
                } = ActorRegistration::from_bytes(&record.value);
                if A::REMOTE_ID != remote_id {
                    panic!("not the right actor type");
                }

                match peer {
                    Some(_) => {
                        // Remote actor ref
                        Some(RemoteActorRef::new(actor_id, self.swarm_tx.clone()))
                    }
                    None => {
                        // Should be present in local registered actors hashmap
                        // todo!()
                        Some(RemoteActorRef::new(actor_id, self.swarm_tx.clone()))
                    }
                }
            }
            Err(err) => {
                println!("{err:?}");
                None
            }
        }
    }

    pub async fn register<A: Actor + RemoteActor + 'static>(
        &self,
        actor_ref: ActorRef<A>,
        name: String,
    ) {
        let actor_registration = ActorRegistration {
            actor_id: actor_ref.id().with_hydrate_peer_id(),
            remote_id: Cow::Borrowed(A::REMOTE_ID),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.swarm_tx
            .send(SwarmMessage::Register {
                record: kad::Record::new(
                    name.into_bytes(),
                    actor_registration.into_bytes(self.local_peer_id),
                ),
                reply: reply_tx,
            })
            .await
            .unwrap();

        let res = reply_rx.await.unwrap();
        if res.is_ok() {
            REMOTE_REGISTRY
                .lock()
                .await
                .insert(actor_ref.id().with_hydrate_peer_id(), Box::new(actor_ref));
        }
        let _ = dbg!(res);
    }

    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }
}

async fn swarm_actor(
    mut swarm: Swarm<Behaviour>,
    tx: mpsc::Sender<SwarmMessage>,
    mut rx: mpsc::Receiver<SwarmMessage>,
) {
    let mut get_queries: HashMap<
        kad::QueryId,
        oneshot::Sender<Result<kad::PeerRecord, kad::GetRecordError>>,
    > = HashMap::new();
    let mut put_queries: HashMap<kad::QueryId, oneshot::Sender<kad::PutRecordResult>> =
        HashMap::new();
    let mut requests: HashMap<OutboundRequestId, oneshot::Sender<Resp>> = HashMap::new();

    loop {
        tokio::select! {
            Some(event) = swarm.next() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        trace!("listening on {address:?}");
                    },
                    SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, multiaddr) in list {
                            swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed { id, result, ..})) => {
                        match result {
                            kad::QueryResult::GetRecord(Ok(
                                kad::GetRecordOk::FoundRecord(record @ kad::PeerRecord {
                                    record: kad::Record { .. },
                                    ..
                                })
                            )) => {
                                if let Some(tx) = get_queries.remove(&id) {
                                    let _ = tx.send(Ok(record));
                                }
                            }
                            kad::QueryResult::GetRecord(Ok(_)) => {}
                            kad::QueryResult::GetRecord(Err(err)) => {
                                if let Some(tx) = get_queries.remove(&id) {
                                    let _ = tx.send(Err(err));
                                }
                            }
                            kad::QueryResult::PutRecord(res @ Ok(kad::PutRecordOk { .. })) => {
                                if let Some(tx) = put_queries.remove(&id) {
                                    let _ = tx.send(res);
                                }
                            }
                            kad::QueryResult::PutRecord(res @ Err(_)) => {
                                if let Some(tx) = put_queries.remove(&id) {
                                    let _ = tx.send(res);
                                }
                            }
                            _ => {}
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(request_response::Event::Message {
                        peer: _,
                        message: request_response::Message::Request {
                            request_id: _, request, channel
                        }
                    })) => {
                        match request {
                            Req::Spawn { actor_name, payload } => {
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    let result = crate::actor::remote::spawn(actor_name, payload).await;
                                    let _ = tx.send(SwarmMessage::SendSpawnResponse { result, channel }).await;
                                });
                            }
                            Req::Ask { actor_id, actor_name, message_name, payload, mailbox_timeout, reply_timeout, immediate } => {
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    let result = crate::actor::remote::ask(
                                        actor_id,
                                        actor_name,
                                        message_name,
                                        payload,
                                        mailbox_timeout,
                                        reply_timeout,
                                        immediate,
                                    ).await;
                                    let _ = tx.send(SwarmMessage::SendAskResponse { result, channel }).await;
                                });
                            }
                            Req::Tell { actor_id, actor_name, message_name, payload, mailbox_timeout, immediate } => {
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    let result = crate::actor::remote::tell(
                                        actor_id,
                                        actor_name,
                                        message_name,
                                        payload,
                                        mailbox_timeout,
                                        immediate,
                                    ).await;
                                    let _ = tx.send(SwarmMessage::SendTellResponse { result, channel }).await;
                                });
                            }
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(request_response::Event::Message {
                        peer: _,
                        message: request_response::Message::Response {
                            request_id, response
                        }
                    })) => {
                        if let Some(tx) = requests.remove(&request_id) {
                            let _ = tx.send(response);
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(event)) => {
                        dbg!(event);
                    }
                    _ => {
                    }
                }
            }
            Some(event) = rx.recv() => {
                match event {
                    SwarmMessage::Lookup { key, reply } => {
                        let query_id = swarm.behaviour_mut().kademlia.get_record(key);
                        get_queries.insert(query_id, reply);
                    },
                    SwarmMessage::LookupLocal { key, reply } => {
                        let registration = swarm.behaviour_mut().kademlia.store_mut().get(&key).map(|record| {
                            ActorRegistration::from_bytes(&record.value).into_owned()
                        });
                        let _ = reply.send(registration);
                    }
                    SwarmMessage::Register { record, reply } => {
                        let query_id = swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One).unwrap();
                        put_queries.insert(query_id, reply);
                    },
                    SwarmMessage::Req { peer_id, req, reply } => {
                        let req_id = swarm.behaviour_mut().request_response.send_request(&peer_id, req);
                        requests.insert(req_id, reply);
                    }
                    SwarmMessage::SendSpawnResponse { result, channel } => {
                        let _ = swarm.behaviour_mut().request_response.send_response(channel, Resp::Spawn(result));
                    }
                    SwarmMessage::SendAskResponse { result, channel } => {
                        let _ = swarm.behaviour_mut().request_response.send_response(channel, Resp::Ask(result));
                    }
                    SwarmMessage::SendTellResponse { result, channel } => {
                        let _ = swarm.behaviour_mut().request_response.send_response(channel, Resp::Tell(result));
                    }
                }
            }
            else => break,
        }
    }
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    kademlia: kad::Behaviour<MemoryStore>,
    request_response: request_response::cbor::Behaviour<Req, Resp>,
    mdns: mdns::tokio::Behaviour,
}

pub(crate) enum SwarmMessage {
    Lookup {
        key: kad::RecordKey,
        reply: oneshot::Sender<Result<kad::PeerRecord, kad::GetRecordError>>,
    },
    LookupLocal {
        key: kad::RecordKey,
        reply: oneshot::Sender<Option<ActorRegistration<'static>>>,
    },
    Register {
        record: kad::Record,
        reply: oneshot::Sender<kad::PutRecordResult>,
    },
    Req {
        peer_id: PeerId,
        req: Req,
        reply: oneshot::Sender<Resp>,
    },
    SendSpawnResponse {
        result: Result<ActorID, RemoteSpawnError>,
        channel: ResponseChannel<Resp>,
    },
    SendAskResponse {
        result: Result<Vec<u8>, RemoteSendError<Vec<u8>>>,
        channel: ResponseChannel<Resp>,
    },
    SendTellResponse {
        result: Result<(), RemoteSendError<Vec<u8>>>,
        channel: ResponseChannel<Resp>,
    },
}

struct ActorRegistration<'a> {
    actor_id: ActorID,
    remote_id: Cow<'a, str>,
}

impl<'a> ActorRegistration<'a> {
    fn into_bytes(self, local_peer_id: PeerId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + 8 + 42 + self.remote_id.len());
        let actor_id_bytes = self.actor_id.to_bytes(local_peer_id);
        let peer_id_len = (actor_id_bytes.len() - 8) as u8;
        bytes.extend_from_slice(&peer_id_len.to_le_bytes());
        bytes.extend_from_slice(&actor_id_bytes);
        bytes.extend_from_slice(self.remote_id.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        let peer_id_bytes_len = u8::from_le_bytes(bytes[..1].try_into().unwrap()) as usize;
        let actor_id = ActorID::from_bytes(&bytes[1..1 + 8 + peer_id_bytes_len]).unwrap();
        let remote_id = std::str::from_utf8(&bytes[1 + 8 + peer_id_bytes_len..]).unwrap();
        ActorRegistration {
            actor_id,
            remote_id: Cow::Borrowed(remote_id),
        }
    }

    fn into_owned(self) -> ActorRegistration<'static> {
        ActorRegistration {
            actor_id: self.actor_id,
            remote_id: Cow::Owned(self.remote_id.into_owned()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Req {
    Spawn {
        actor_name: String,
        payload: Vec<u8>,
    },
    Ask {
        actor_id: ActorID,
        actor_name: String,
        message_name: String,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    },
    Tell {
        actor_id: ActorID,
        actor_name: String,
        message_name: String,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Resp {
    Spawn(Result<ActorID, RemoteSpawnError>),
    Ask(Result<Vec<u8>, RemoteSendError<Vec<u8>>>),
    Tell(Result<(), RemoteSendError<Vec<u8>>>),
}
