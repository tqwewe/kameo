use std::{borrow::Cow, collections::HashMap, time::Duration};

use libp2p::{
    kad::{
        self,
        store::{MemoryStore, RecordStore},
    },
    mdns, noise,
    request_response::{
        self, OutboundFailure, OutboundRequestId, ProtocolSupport, ResponseChannel,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::trace;

use crate::{
    actor::{ActorID, ActorRef, RemoteActorRef},
    error::{BootstrapError, RegistrationError, RemoteSendError, RemoteSpawnError},
    remote, Actor,
};

use super::{RemoteActor, REMOTE_REGISTRY};

static ACTOR_SWARM: OnceCell<ActorSwarm> = OnceCell::new();

/// `ActorSwarm` is the core component for remote actors within kameo.
///
/// It is responsible for managing the distributed swarm of nodes and coordinating
/// the registration and messaging of remote actors.
///
/// This struct handles the following key functionalities:
///
/// - **Swarm Management**: It initializes and manages a libp2p swarm that allows
///   nodes to discover each other and communicate in a peer-to-peer network.
///
/// - **Actor Registration**: Actors can be registered under a unique name, which can
///   then be looked up and interacted with from the same or different nodes in the network.
///
/// - **Message Routing**: The swarm also handles the routing and delivery of messages
///   to registered remote actors, ensuring reliable communication across nodes.
///
/// ## Example
///
/// ```rust
/// // Initialize the actor swarm
/// let actor_swarm = ActorSwarm::bootstrap();
/// ```
///
/// The `ActorSwarm` is essential for enabling distributed actor communication in a
/// decentralized network, leveraging libp2p's capabilities to provide robust and scalable
/// remote actor interactions.
#[derive(Clone, Debug)]
pub struct ActorSwarm {
    swarm_tx: mpsc::Sender<SwarmCommand>,
    local_peer_id: PeerId,
}

impl ActorSwarm {
    /// Bootstraps the remote actor system to start listening and accepting requests from other nodes.
    pub fn bootstrap() -> Result<&'static Self, BootstrapError> {
        if let Some(swarm) = ACTOR_SWARM.get() {
            return Ok(swarm);
        }

        let mut swarm = SwarmBuilder::with_new_identity()
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
            .map_err(|err| BootstrapError::BehaviourError(Box::new(err)))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())?;

        Ok(ACTOR_SWARM.get_or_init(move || {
            let local_peer_id = swarm.local_peer_id().clone();

            let (cmd_tx, cmd_rx) = mpsc::channel(24);
            tokio::spawn({
                let swarm_tx = cmd_tx.clone();
                async move { SwarmActor::new(swarm, swarm_tx, cmd_rx).run().await }
            });

            ActorSwarm {
                swarm_tx: cmd_tx,
                local_peer_id,
            }
        }))
    }

    /// Gets a reference to the actor swarm.
    pub fn get() -> Option<&'static Self> {
        ACTOR_SWARM.get()
    }

    /// Returns the local peer ID.
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Looks up an actor running locally.
    pub(crate) async fn lookup_local<A: Actor + RemoteActor + 'static>(
        &self,
        name: String,
    ) -> Result<Option<ActorRef<A>>, RegistrationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.swarm_tx
            .send(SwarmCommand::LookupLocal {
                key: name.into_bytes().into(),
                reply: reply_tx,
            })
            .await
            .expect("the swarm should never stop running");

        let Some(ActorRegistration {
            actor_id,
            remote_id,
        }) = reply_rx.await.unwrap()
        else {
            return Ok(None);
        };
        if A::REMOTE_ID != remote_id {
            return Err(RegistrationError::BadActorType);
        }

        let registry = REMOTE_REGISTRY.lock().await;
        let Some(actor_ref_any) = registry.get(&actor_id) else {
            return Ok(None);
        };
        let actor_ref = actor_ref_any
            .downcast_ref::<ActorRef<A>>()
            .ok_or(RegistrationError::BadActorType)?
            .clone();

        Ok(Some(actor_ref))
    }

    /// Looks up an actor in the swarm.
    pub(crate) async fn lookup<A: Actor + RemoteActor>(
        &self,
        name: String,
    ) -> Result<Option<RemoteActorRef<A>>, RegistrationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.swarm_tx
            .send(SwarmCommand::Lookup {
                key: name.into_bytes().into(),
                reply: reply_tx,
            })
            .await
            .expect("the swarm should never stop running");

        match reply_rx.await.unwrap() {
            Ok(kad::PeerRecord { record, .. }) => {
                let ActorRegistration {
                    actor_id,
                    remote_id,
                } = ActorRegistration::from_bytes(&record.value);
                if A::REMOTE_ID != remote_id {
                    return Err(RegistrationError::BadActorType);
                }

                Ok(Some(RemoteActorRef::new(actor_id, self.swarm_tx.clone())))
            }
            Err(kad::GetRecordError::NotFound { .. }) => Ok(None),
            Err(kad::GetRecordError::QuorumFailed { quorum, .. }) => {
                Err(RegistrationError::QuorumFailed { quorum })
            }
            Err(kad::GetRecordError::Timeout { .. }) => Err(RegistrationError::Timeout),
        }
    }

    /// Registers an actor within the swarm.
    pub(crate) async fn register<A: Actor + RemoteActor + 'static>(
        &self,
        actor_ref: ActorRef<A>,
        name: String,
    ) -> Result<(), RegistrationError> {
        let actor_registration = ActorRegistration {
            actor_id: actor_ref.id().with_hydrate_peer_id(),
            remote_id: Cow::Borrowed(A::REMOTE_ID),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.swarm_tx
            .send(SwarmCommand::Register {
                record: kad::Record::new(
                    name.into_bytes(),
                    actor_registration.into_bytes(self.local_peer_id),
                ),
                reply: reply_tx,
            })
            .await
            .expect("the swarm should never stop running");

        match reply_rx.await.unwrap() {
            Ok(kad::PutRecordOk { .. }) => {
                REMOTE_REGISTRY
                    .lock()
                    .await
                    .insert(actor_ref.id().with_hydrate_peer_id(), Box::new(actor_ref));
                Ok(())
            }
            Err(kad::PutRecordError::QuorumFailed { quorum, .. }) => {
                Err(RegistrationError::QuorumFailed { quorum })
            }
            Err(kad::PutRecordError::Timeout { .. }) => Err(RegistrationError::Timeout),
        }
    }
}

struct SwarmActor {
    swarm: Swarm<Behaviour>,
    cmd_tx: mpsc::Sender<SwarmCommand>,
    cmd_rx: mpsc::Receiver<SwarmCommand>,
    get_queries:
        HashMap<kad::QueryId, oneshot::Sender<Result<kad::PeerRecord, kad::GetRecordError>>>,
    put_queries: HashMap<kad::QueryId, oneshot::Sender<kad::PutRecordResult>>,
    requests: HashMap<OutboundRequestId, oneshot::Sender<SwarmResp>>,
}

impl SwarmActor {
    fn new(
        swarm: Swarm<Behaviour>,
        tx: mpsc::Sender<SwarmCommand>,
        rx: mpsc::Receiver<SwarmCommand>,
    ) -> Self {
        SwarmActor {
            swarm,
            cmd_tx: tx,
            cmd_rx: rx,
            get_queries: HashMap::new(),
            put_queries: HashMap::new(),
            requests: HashMap::new(),
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => self.handle_command(cmd),
                Some(event) = self.swarm.next() => self.handle_event(event),
                else => unreachable!("actor swarm should never stop since its stored globally and will never return None when progressing"),
            }
        }
    }

    fn handle_command(&mut self, cmd: SwarmCommand) {
        match cmd {
            SwarmCommand::Lookup { key, reply } => {
                let query_id = self.swarm.behaviour_mut().kademlia.get_record(key);
                self.get_queries.insert(query_id, reply);
            }
            SwarmCommand::LookupLocal { key, reply } => {
                let registration = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .get(&key)
                    .map(|record| ActorRegistration::from_bytes(&record.value).into_owned());
                let _ = reply.send(registration);
            }
            SwarmCommand::Register { record, reply } => {
                if self.swarm.network_info().num_peers() == 0 {
                    let key = record.key.clone();
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .store_mut()
                        .put(record)
                        .unwrap();
                    let _ = reply.send(Ok(kad::PutRecordOk { key }));
                } else {
                    let query_id = self
                        .swarm
                        .behaviour_mut()
                        .kademlia
                        .put_record(record, kad::Quorum::One)
                        .unwrap();
                    self.put_queries.insert(query_id, reply);
                }
            }
            SwarmCommand::Req {
                peer_id,
                req,
                reply,
            } => {
                if self.swarm.network_info().num_peers() == 0 {
                    match req {
                        SwarmReq::Spawn {
                            actor_name,
                            payload,
                        } => {
                            tokio::spawn(async move {
                                let result = remote::spawn(actor_name, payload).await;
                                let _ = reply.send(SwarmResp::Spawn(result));
                            });
                        }
                        SwarmReq::Ask {
                            actor_id,
                            actor_name,
                            message_name,
                            payload,
                            mailbox_timeout,
                            reply_timeout,
                            immediate,
                        } => {
                            tokio::spawn(async move {
                                let result = remote::ask(
                                    actor_id,
                                    actor_name,
                                    message_name,
                                    payload,
                                    mailbox_timeout,
                                    reply_timeout,
                                    immediate,
                                )
                                .await;
                                let _ = reply.send(SwarmResp::Ask(result));
                            });
                        }
                        SwarmReq::Tell {
                            actor_id,
                            actor_name,
                            message_name,
                            payload,
                            mailbox_timeout,
                            immediate,
                        } => {
                            tokio::spawn(async move {
                                let result = remote::tell(
                                    actor_id,
                                    actor_name,
                                    message_name,
                                    payload,
                                    mailbox_timeout,
                                    immediate,
                                )
                                .await;
                                let _ = reply.send(SwarmResp::Tell(result));
                            });
                        }
                    }
                } else {
                    let req_id = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&peer_id, req);
                    self.requests.insert(req_id, reply);
                }
            }
            SwarmCommand::SendSpawnResponse { result, channel } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, SwarmResp::Spawn(result));
            }
            SwarmCommand::SendAskResponse { result, channel } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, SwarmResp::Ask(result));
            }
            SwarmCommand::SendTellResponse { result, channel } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, SwarmResp::Tell(result));
            }
        }
    }

    fn handle_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                trace!("listening on {address:?}");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer_id, multiaddr) in list {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, multiaddr);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed { id, result, .. },
            )) => match result {
                kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                    record @ kad::PeerRecord {
                        record: kad::Record { .. },
                        ..
                    },
                ))) => {
                    if let Some(tx) = self.get_queries.remove(&id) {
                        let _ = tx.send(Ok(record));
                    }
                }
                kad::QueryResult::GetRecord(Ok(_)) => {}
                kad::QueryResult::GetRecord(Err(err)) => {
                    if let Some(tx) = self.get_queries.remove(&id) {
                        let _ = tx.send(Err(err));
                    }
                }
                kad::QueryResult::PutRecord(res @ Ok(kad::PutRecordOk { .. })) => {
                    if let Some(tx) = self.put_queries.remove(&id) {
                        let _ = tx.send(res);
                    }
                }
                kad::QueryResult::PutRecord(res @ Err(_)) => {
                    if let Some(tx) = self.put_queries.remove(&id) {
                        let _ = tx.send(res);
                    }
                }
                _ => {}
            },
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::Message {
                    peer: _,
                    message:
                        request_response::Message::Request {
                            request_id: _,
                            request,
                            channel,
                        },
                },
            )) => match request {
                SwarmReq::Spawn {
                    actor_name,
                    payload,
                } => {
                    let tx = self.cmd_tx.clone();
                    tokio::spawn(async move {
                        let result = remote::spawn(actor_name, payload).await;
                        let _ = tx
                            .send(SwarmCommand::SendSpawnResponse { result, channel })
                            .await;
                    });
                }
                SwarmReq::Ask {
                    actor_id,
                    actor_name,
                    message_name,
                    payload,
                    mailbox_timeout,
                    reply_timeout,
                    immediate,
                } => {
                    let tx = self.cmd_tx.clone();
                    tokio::spawn(async move {
                        let result = remote::ask(
                            actor_id,
                            actor_name,
                            message_name,
                            payload,
                            mailbox_timeout,
                            reply_timeout,
                            immediate,
                        )
                        .await;
                        let _ = tx
                            .send(SwarmCommand::SendAskResponse { result, channel })
                            .await;
                    });
                }
                SwarmReq::Tell {
                    actor_id,
                    actor_name,
                    message_name,
                    payload,
                    mailbox_timeout,
                    immediate,
                } => {
                    let tx = self.cmd_tx.clone();
                    tokio::spawn(async move {
                        let result = remote::tell(
                            actor_id,
                            actor_name,
                            message_name,
                            payload,
                            mailbox_timeout,
                            immediate,
                        )
                        .await;
                        let _ = tx
                            .send(SwarmCommand::SendTellResponse { result, channel })
                            .await;
                    });
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::Message {
                    peer: _,
                    message:
                        request_response::Message::Response {
                            request_id,
                            response,
                        },
                },
            )) => {
                if let Some(tx) = self.requests.remove(&request_id) {
                    let _ = tx.send(response);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                if let Some(tx) = self.requests.remove(&request_id) {
                    let err = match error {
                        OutboundFailure::DialFailure => RemoteSendError::DialFailure,
                        OutboundFailure::Timeout => RemoteSendError::NetworkTimeout,
                        OutboundFailure::ConnectionClosed => RemoteSendError::ConnectionClosed,
                        OutboundFailure::UnsupportedProtocols => {
                            unreachable!("the protocol is hard coded")
                        }
                        OutboundFailure::Io(err) => RemoteSendError::Io(Some(err)),
                    };
                    let _ = tx.send(SwarmResp::OutboundFailure(err));
                }
            }
            _ => {}
        }
    }
}

pub(crate) enum SwarmCommand {
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
        req: SwarmReq,
        reply: oneshot::Sender<SwarmResp>,
    },
    SendSpawnResponse {
        result: Result<ActorID, RemoteSpawnError>,
        channel: ResponseChannel<SwarmResp>,
    },
    SendAskResponse {
        result: Result<Vec<u8>, RemoteSendError<Vec<u8>>>,
        channel: ResponseChannel<SwarmResp>,
    },
    SendTellResponse {
        result: Result<(), RemoteSendError<Vec<u8>>>,
        channel: ResponseChannel<SwarmResp>,
    },
}

pub(crate) struct ActorRegistration<'a> {
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
pub(crate) enum SwarmReq {
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
pub(crate) enum SwarmResp {
    Spawn(Result<ActorID, RemoteSpawnError>),
    Ask(Result<Vec<u8>, RemoteSendError<Vec<u8>>>),
    Tell(Result<(), RemoteSendError<Vec<u8>>>),
    OutboundFailure(RemoteSendError<()>),
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    kademlia: kad::Behaviour<MemoryStore>,
    request_response: request_response::cbor::Behaviour<SwarmReq, SwarmResp>,
    mdns: mdns::tokio::Behaviour,
}
