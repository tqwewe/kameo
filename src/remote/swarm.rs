use core::task;
use std::{borrow::Cow, collections::HashMap, io, pin, time::Duration};

use futures::{ready, Future, FutureExt};
use internment::Intern;
use libp2p::{
    core::transport::ListenerId,
    identity::Keypair,
    kad::{
        self,
        store::{MemoryStore, RecordStore},
    },
    mdns,
    request_response::{
        self, OutboundFailure, OutboundRequestId, ProtocolSupport, ResponseChannel,
    },
    swarm::{dial_opts::DialOpts, DialError, NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder, TransportError,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::trace;

use crate::{
    actor::{ActorID, ActorRef, RemoteActorRef},
    error::{BootstrapError, RegistrationError, RemoteSendError},
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
    swarm_tx: SwarmSender,
    local_peer_id: Intern<PeerId>,
}

impl ActorSwarm {
    /// Bootstraps the remote actor system to start listening and accepting requests from other nodes.
    pub fn bootstrap() -> Result<&'static Self, BootstrapError> {
        Self::bootstrap_with_identity(Keypair::generate_ed25519())
    }

    /// Bootstraps the remote actor system with a keypair to start listening and accepting requests from other nodes.
    pub fn bootstrap_with_identity(keypair: Keypair) -> Result<&'static Self, BootstrapError> {
        if let Some(swarm) = ACTOR_SWARM.get() {
            return Ok(swarm);
        }

        let swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic()
            .with_behaviour(|key| {
                let kademlia = kad::Behaviour::new(
                    key.public().to_peer_id(),
                    MemoryStore::new(key.public().to_peer_id()),
                );
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

        Ok(ACTOR_SWARM.get_or_init(move || {
            let local_peer_id = Intern::new(swarm.local_peer_id().clone());

            let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
            let swarm_tx = SwarmSender(cmd_tx);
            tokio::spawn({
                let swarm_tx = swarm_tx.clone();
                async move { SwarmActor::new(swarm, swarm_tx, cmd_rx).run().await }
            });

            ActorSwarm {
                swarm_tx,
                local_peer_id,
            }
        }))
    }

    /// Starts listening on the given address, allowing other nodes to lookup registered actors.
    ///
    /// Awaiting this function does not block, and will cause the swarm to start listening in the background.
    ///
    /// For information on `Multiaddr`, see <https://docs.libp2p.io/concepts/fundamentals/addressing/>.
    ///
    /// # Example
    ///
    /// ```
    /// ActorSwarm::bootstrap()?
    ///     .listen_on("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?)
    ///     .await?;
    /// ```
    pub fn listen_on(
        &self,
        addr: Multiaddr,
    ) -> SwarmFuture<Result<ListenerId, TransportError<io::Error>>> {
        self.swarm_tx
            .send_with_reply(|reply| SwarmCommand::ListenOn { addr, reply })
    }

    /// Gets a reference to the actor swarm.
    pub fn get() -> Option<&'static Self> {
        ACTOR_SWARM.get()
    }

    /// Returns the local peer ID.
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub(crate) fn local_peer_id_intern(&self) -> &Intern<PeerId> {
        &self.local_peer_id
    }

    /// Dial a known or unknown peer.
    ///
    /// See also [`DialOpts`].
    pub fn dial(&self, opts: impl Into<DialOpts>) -> SwarmFuture<Result<(), DialError>> {
        self.swarm_tx.send_with_reply(|reply| SwarmCommand::Dial {
            opts: opts.into(),
            reply,
        })
    }

    /// Add a new external address of a remote peer.
    pub fn add_peer_address(&self, peer_id: PeerId, addr: Multiaddr) {
        self.swarm_tx
            .send(SwarmCommand::AddPeerAddress { peer_id, addr })
    }

    /// Add a new external address of a remote peer.
    pub fn disconnect_peer_id(&self, peer_id: PeerId) {
        self.swarm_tx
            .send(SwarmCommand::DisconnectPeerId { peer_id })
    }

    /// Looks up an actor running locally.
    pub(crate) fn lookup_local<A: Actor + RemoteActor + 'static>(
        &self,
        name: String,
    ) -> impl Future<Output = Result<Option<ActorRef<A>>, RegistrationError>> {
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::LookupLocal {
                key: name.into_bytes().into(),
                reply,
            });

        async move {
            let Some(ActorRegistration {
                actor_id,
                remote_id,
            }) = reply_rx.await
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
    }

    /// Looks up an actor in the swarm.
    pub(crate) fn lookup<A: Actor + RemoteActor>(
        &self,
        name: String,
    ) -> impl Future<Output = Result<Option<RemoteActorRef<A>>, RegistrationError>> {
        let reply_rx = self.swarm_tx.send_with_reply(|reply| SwarmCommand::Lookup {
            key: name.into_bytes().into(),
            reply,
        });

        let swarm_tx = self.swarm_tx.clone();
        async move {
            match reply_rx.await {
                Ok(kad::PeerRecord { record, .. }) => {
                    let ActorRegistration {
                        actor_id,
                        remote_id,
                    } = ActorRegistration::from_bytes(&record.value);
                    if A::REMOTE_ID != remote_id {
                        return Err(RegistrationError::BadActorType);
                    }

                    Ok(Some(RemoteActorRef::new(actor_id, swarm_tx.clone())))
                }
                Err(kad::GetRecordError::NotFound { .. }) => Ok(None),
                Err(kad::GetRecordError::QuorumFailed { quorum, .. }) => {
                    Err(RegistrationError::QuorumFailed { quorum })
                }
                Err(kad::GetRecordError::Timeout { .. }) => Err(RegistrationError::Timeout),
            }
        }
    }

    /// Registers an actor within the swarm.
    pub(crate) fn register<A: Actor + RemoteActor + 'static>(
        &self,
        actor_ref: ActorRef<A>,
        name: String,
    ) -> impl Future<Output = Result<(), RegistrationError>> {
        let actor_registration = ActorRegistration {
            actor_id: actor_ref.id().with_hydrate_peer_id(),
            remote_id: Cow::Borrowed(A::REMOTE_ID),
        };
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::Register {
                record: kad::Record::new(name.into_bytes(), actor_registration.into_bytes()),
                reply,
            });

        async move {
            match reply_rx.await {
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
}

struct SwarmActor {
    swarm: Swarm<Behaviour>,
    cmd_tx: SwarmSender,
    cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
    get_queries:
        HashMap<kad::QueryId, oneshot::Sender<Result<kad::PeerRecord, kad::GetRecordError>>>,
    put_queries: HashMap<kad::QueryId, oneshot::Sender<kad::PutRecordResult>>,
    requests: HashMap<OutboundRequestId, oneshot::Sender<SwarmResp>>,
}

impl SwarmActor {
    fn new(
        swarm: Swarm<Behaviour>,
        tx: SwarmSender,
        rx: mpsc::UnboundedReceiver<SwarmCommand>,
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
            SwarmCommand::ListenOn { addr, reply } => {
                let res = self.swarm.listen_on(addr);
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .set_mode(Some(kad::Mode::Server));
                let _ = reply.send(res);
            }
            SwarmCommand::Dial { opts, reply } => {
                let res = self.swarm.dial(opts);
                let _ = reply.send(res);
            }
            SwarmCommand::AddPeerAddress { peer_id, addr } => {
                self.swarm.add_peer_address(peer_id, addr);
            }
            SwarmCommand::DisconnectPeerId { peer_id } => {
                let _ = self.swarm.disconnect_peer_id(peer_id);
            }
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
                        SwarmReq::Ask {
                            actor_id,
                            actor_remote_id,
                            message_remote_id,
                            payload,
                            mailbox_timeout,
                            reply_timeout,
                            immediate,
                        } => {
                            tokio::spawn(async move {
                                let result = remote::ask(
                                    actor_id,
                                    actor_remote_id,
                                    message_remote_id,
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
                            actor_remote_id,
                            message_remote_id,
                            payload,
                            mailbox_timeout,
                            immediate,
                        } => {
                            tokio::spawn(async move {
                                let result = remote::tell(
                                    actor_id,
                                    actor_remote_id,
                                    message_remote_id,
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
                SwarmReq::Ask {
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    reply_timeout,
                    immediate,
                } => {
                    let tx = self.cmd_tx.clone();
                    tokio::spawn(async move {
                        let result = remote::ask(
                            actor_id,
                            actor_remote_id,
                            message_remote_id,
                            payload,
                            mailbox_timeout,
                            reply_timeout,
                            immediate,
                        )
                        .await;
                        let _ = tx.send(SwarmCommand::SendAskResponse { result, channel });
                    });
                }
                SwarmReq::Tell {
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    immediate,
                } => {
                    let tx = self.cmd_tx.clone();
                    tokio::spawn(async move {
                        let result = remote::tell(
                            actor_id,
                            actor_remote_id,
                            message_remote_id,
                            payload,
                            mailbox_timeout,
                            immediate,
                        )
                        .await;
                        let _ = tx.send(SwarmCommand::SendTellResponse { result, channel });
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

#[derive(Clone, Debug)]
pub(crate) struct SwarmSender(mpsc::UnboundedSender<SwarmCommand>);

impl SwarmSender {
    pub(crate) fn send(&self, cmd: SwarmCommand) {
        self.0
            .send(cmd)
            .expect("the swarm should never stop running");
    }

    pub(crate) fn send_with_reply<T>(
        &self,
        cmd_fn: impl FnOnce(oneshot::Sender<T>) -> SwarmCommand,
    ) -> SwarmFuture<T> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = cmd_fn(reply_tx);
        self.send(cmd);

        SwarmFuture(reply_rx)
    }
}

pub(crate) enum SwarmCommand {
    ListenOn {
        addr: Multiaddr,
        reply: oneshot::Sender<Result<ListenerId, TransportError<io::Error>>>,
    },
    Dial {
        opts: DialOpts,
        reply: oneshot::Sender<Result<(), DialError>>,
    },
    AddPeerAddress {
        peer_id: PeerId,
        addr: Multiaddr,
    },
    DisconnectPeerId {
        peer_id: PeerId,
    },
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
        peer_id: Intern<PeerId>,
        req: SwarmReq,
        reply: oneshot::Sender<SwarmResp>,
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
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + 8 + 42 + self.remote_id.len());
        let actor_id_bytes = self.actor_id.to_bytes();
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
    Ask {
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    },
    Tell {
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum SwarmResp {
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

/// A future containing the response from the actor swarm.
///
/// This future does not need to be awaited if the response is not needed, and can simply be dropped.
#[derive(Debug)]
pub struct SwarmFuture<T>(oneshot::Receiver<T>);

impl<T> Future for SwarmFuture<T> {
    type Output = T;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(
            ready!(self.0.poll_unpin(cx))
                .expect("the oneshot sender should never be dropped before being sent to"),
        )
    }
}
