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

use crate::{
    actor::{ActorID, ActorRef, RemoteActorRef},
    error::{BootstrapError, RegistrationError, RemoteSendError},
    remote, Actor,
};

use super::{RemoteActor, REMOTE_REGISTRY};

static ACTOR_SWARM: OnceCell<ActorSwarm> = OnceCell::new();

/// `ActorSwarm` is the core component for remote actors within Kameo.
///
/// It is responsible for managing a swarm of distributed nodes using libp2p,
/// enabling peer discovery, actor registration, and remote message routing.
///
/// ## Key Features
///
/// - **Swarm Management**: Initializes and manages the libp2p swarm, allowing nodes to discover
///   and communicate in a peer-to-peer network.
/// - **Actor Registration**: Actors can be registered under a unique name, making them discoverable
///   and accessible across the network.
/// - **Message Routing**: Handles reliable message delivery to remote actors using Kademlia DHT.
///
/// # Example
///
/// ```
/// use kameo::remote::ActorSwarm;
///
/// # tokio_test::block_on(async {
/// // Initialize the actor swarm
/// let actor_swarm = ActorSwarm::bootstrap()?;
///
/// // Set up the swarm to listen on a specific address
/// actor_swarm.listen_on("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// ```
///
/// The `ActorSwarm` is the essential component for enabling distributed actor communication
/// and message passing across decentralized nodes.
#[derive(Clone, Debug)]
pub struct ActorSwarm {
    swarm_tx: SwarmSender,
    local_peer_id: Intern<PeerId>,
}

impl ActorSwarm {
    /// Bootstraps the remote actor system, initializing the swarm and preparing it to listen
    /// and accept requests from other nodes in the network.
    ///
    /// This method starts the distributed actor system, enabling remote actors to communicate
    /// across different nodes using libp2p. It must be called before any other remote actor operations.
    ///
    /// ## Returns
    /// A reference to the initialized `ActorSwarm` if successful, or an error if the bootstrap fails.
    pub fn bootstrap() -> Result<&'static Self, BootstrapError> {
        Self::bootstrap_with_identity(Keypair::generate_ed25519())
    }

    /// Bootstraps the remote actor system with a specified keypair, initializing the swarm
    /// and preparing it to listen and accept requests from other nodes in the network.
    ///
    /// The provided `Keypair` will be used to identify this node in the network, ensuring
    /// secure communication with peers.
    ///
    /// ## Parameters
    /// - `keypair`: The cryptographic keypair used to establish the identity of the node.
    ///
    /// ## Returns
    /// A reference to the initialized `ActorSwarm` if successful, or an error if the bootstrap fails.
    pub fn bootstrap_with_identity(keypair: Keypair) -> Result<&'static Self, BootstrapError> {
        let behaviour = ActorSwarmBehaviour::new(&keypair)
            .map_err(|err| BootstrapError::BehaviourError(Box::new(err)))?;
        ActorSwarm::bootstrap_with_behaviour(keypair, behaviour)
    }

    /// Bootstraps the remote actor system with a behaviour struct.
    ///
    /// This method allows more fine grained control over the mdns, kademlia, and request response configs.
    ///
    /// ## Parameters
    /// - `keypair`: The cryptographic keypair used to establish the identity of the node.
    /// - `behaviour`: The behaviour instance.
    ///
    /// ## Returns
    /// A reference to the initialized `ActorSwarm` if successful, or an error if the bootstrap fails.
    pub fn bootstrap_with_behaviour(
        keypair: Keypair,
        behaviour: ActorSwarmBehaviour,
    ) -> Result<&'static Self, BootstrapError> {
        if let Some(swarm) = ACTOR_SWARM.get() {
            return Err(BootstrapError::AlreadyBootstrapped(swarm, None));
        }

        ActorSwarm::bootstrap_with_swarm(
            SwarmBuilder::with_existing_identity(keypair)
                .with_tokio()
                .with_quic()
                .with_behaviour(|_| Ok(behaviour))
                .map_err(|err| BootstrapError::BehaviourError(Box::new(err)))?
                .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
                .build(),
        )
    }

    /// Bootstraps the remote actor system with a libp2p swarm.
    ///
    /// This method allows more fine grained control over the swarm, including swarm behaviour configs.
    ///
    /// ## Parameters
    /// - `swarm`: The libp2p swarm.
    ///
    /// ## Returns
    /// A reference to the initialized `ActorSwarm` if successful, or an error if the bootstrap fails.
    pub fn bootstrap_with_swarm(
        mut swarm: Swarm<ActorSwarmBehaviour>,
    ) -> Result<&'static Self, BootstrapError> {
        let local_peer_id = Intern::new(*swarm.local_peer_id());
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let swarm_tx = SwarmSender(cmd_tx);

        match ACTOR_SWARM.try_insert(ActorSwarm {
            swarm_tx: swarm_tx.clone(),
            local_peer_id,
        }) {
            Ok(actor_swarm) => {
                tokio::spawn({
                    async move {
                        ActorSwarmHandler::new(swarm_tx, cmd_rx)
                            .run(&mut swarm)
                            .await
                    }
                });

                Ok(actor_swarm)
            }
            Err((actor_swarm, _)) => Err(BootstrapError::AlreadyBootstrapped(
                actor_swarm,
                Some(swarm),
            )),
        }
    }

    /// Bootstraps a blank swarm for completely manual processing a libp2p swarm.
    ///
    /// This is for advanced cases and provides full control, returning an `ActorSwarmBehaviour` instance which
    /// should be used to process the swarm manually.
    pub fn bootstrap_manual(local_peer_id: PeerId) -> Option<(&'static Self, ActorSwarmHandler)> {
        let local_peer_id = Intern::new(local_peer_id);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let swarm_tx = SwarmSender(cmd_tx);

        ACTOR_SWARM
            .try_insert(ActorSwarm {
                swarm_tx: swarm_tx.clone(),
                local_peer_id,
            })
            .map(|swarm| (swarm, ActorSwarmHandler::new(swarm_tx, cmd_rx)))
            .ok()
    }

    /// Starts listening on the specified multiaddress, allowing other nodes to connect
    /// and perform actor lookups and message passing.
    ///
    /// This function initiates background listening on the provided address. It does not block
    /// the current task, and messages from other nodes will be handled in the background.
    ///
    /// ## Parameters
    /// - `addr`: The multiaddress to start listening on, which specifies the protocol and address
    ///   (e.g., `/ip4/0.0.0.0/udp/8020/quic-v1`).
    ///
    /// For more information on multiaddresses, see [libp2p addressing](https://docs.libp2p.io/concepts/fundamentals/addressing/).
    ///
    /// ## Example
    ///
    /// ```
    /// use kameo::remote::ActorSwarm;
    ///
    /// # tokio_test::block_on(async {
    /// ActorSwarm::bootstrap()?
    ///     .listen_on("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?)
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    ///
    /// ## Returns
    /// A `SwarmFuture` containing either the listener ID if successful or a transport error if the listen operation fails.
    pub fn listen_on(
        &self,
        addr: Multiaddr,
    ) -> SwarmFuture<Result<ListenerId, TransportError<io::Error>>> {
        self.swarm_tx
            .send_with_reply(|reply| SwarmCommand::ListenOn { addr, reply })
    }

    /// Retrieves a reference to the current `ActorSwarm` if it has been bootstrapped.
    ///
    /// This function is useful for getting access to the swarm after initialization without
    /// needing to store the reference manually.
    ///
    /// ## Returns
    /// An optional reference to the `ActorSwarm`, or `None` if it has not been bootstrapped.
    pub fn get() -> Option<&'static Self> {
        ACTOR_SWARM.get()
    }

    /// Returns the local peer ID, which uniquely identifies this node in the libp2p network.
    ///
    /// ## Returns
    /// A reference to the local `PeerId`.
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub(crate) fn local_peer_id_intern(&self) -> &Intern<PeerId> {
        &self.local_peer_id
    }

    /// Dials a peer using the provided dialing options.
    ///
    /// This method can be used to connect to a known or unknown peer, specified by the options
    /// given in `DialOpts`. If successful, the peer will be added to the swarm and able to communicate
    /// with local actors.
    ///
    /// ## Parameters
    /// - `opts`: Dialing options specifying the peer to connect to and any additional parameters.
    ///
    /// ## Returns
    /// A `SwarmFuture` that resolves to either `Ok` if the dialing succeeds or a `DialError` if it fails.
    pub fn dial(&self, opts: impl Into<DialOpts>) -> SwarmFuture<Result<(), DialError>> {
        self.swarm_tx.send_with_reply(|reply| SwarmCommand::Dial {
            opts: opts.into(),
            reply,
        })
    }

    /// Adds an external address for a remote peer, allowing the swarm to discover and connect to that peer.
    ///
    /// This method can be used to manually add a known address for a peer to facilitate
    /// discovery and messaging.
    ///
    /// ## Parameters
    /// - `peer_id`: The `PeerId` of the remote peer.
    /// - `addr`: The `Multiaddr` of the remote peer.
    pub fn add_peer_address(&self, peer_id: PeerId, addr: Multiaddr) {
        self.swarm_tx
            .send(SwarmCommand::AddPeerAddress { peer_id, addr })
    }

    /// Disconnects a peer from the swarm, terminating the connection with the given `PeerId`.
    ///
    /// This method can be used to forcibly disconnect from a peer.
    ///
    /// ## Parameters
    /// - `peer_id`: The `PeerId` of the remote peer to disconnect from.
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

/// A concrete implementation of the `SwarmBehaviour` trait.
///
/// `ActorSwarmHandler` manages swarm-related operations, including handling
/// commands, tracking ongoing Kademlia queries, and managing outbound requests.
/// It facilitates communication with peers, processes incoming and outgoing messages,
/// and interacts with the Kademlia distributed hash table (DHT) for record management.
///
/// This struct serves as the backbone for swarm interactions, ensuring efficient
/// and organized handling of network behavior within the swarm.
#[derive(Debug)]
pub struct ActorSwarmHandler {
    cmd_tx: SwarmSender,
    cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
    get_queries:
        HashMap<kad::QueryId, oneshot::Sender<Result<kad::PeerRecord, kad::GetRecordError>>>,
    put_queries: HashMap<kad::QueryId, oneshot::Sender<kad::PutRecordResult>>,
    requests: HashMap<OutboundRequestId, oneshot::Sender<SwarmResponse>>,
}

impl ActorSwarmHandler {
    fn new(tx: SwarmSender, rx: mpsc::UnboundedReceiver<SwarmCommand>) -> Self {
        ActorSwarmHandler {
            cmd_tx: tx,
            cmd_rx: rx,
            get_queries: HashMap::new(),
            put_queries: HashMap::new(),
            requests: HashMap::new(),
        }
    }

    async fn run(&mut self, swarm: &mut Swarm<ActorSwarmBehaviour>) {
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => self.handle_command(swarm, cmd),
                Some(event) = swarm.next() => {
                    match event {
                        SwarmEvent::Behaviour(ActorSwarmBehaviourEvent::Kademlia(event)) => {
                            self.handle_event(swarm, ActorSwarmBehaviourEvent::Kademlia(event));
                        }
                        SwarmEvent::Behaviour(ActorSwarmBehaviourEvent::RequestResponse(event)) => {
                            self.handle_event(swarm, ActorSwarmBehaviourEvent::RequestResponse(event));
                        }
                        SwarmEvent::Behaviour(ActorSwarmBehaviourEvent::Mdns(event)) => {
                            self.handle_event(swarm, ActorSwarmBehaviourEvent::Mdns(event));
                        }
                        _ => {},
                    }
                }
                else => unreachable!("actor swarm should never stop since its stored globally and will never return None when progressing"),
            }
        }
    }

    /// Waits for the next swarm command to be received.
    ///
    /// If the channel has been dropped, None is returned.
    pub async fn next_command(&mut self) -> Option<SwarmCommand> {
        self.cmd_rx.recv().await
    }

    /// Handles a swarm command.
    pub fn handle_command<B: SwarmBehaviour>(&mut self, swarm: &mut Swarm<B>, cmd: SwarmCommand) {
        match cmd {
            SwarmCommand::ListenOn { addr, reply } => {
                let res = swarm.listen_on(addr);
                swarm
                    .behaviour_mut()
                    .kademlia_set_mode(Some(kad::Mode::Server));
                let _ = reply.send(res);
            }
            SwarmCommand::Dial { opts, reply } => {
                let res = swarm.dial(opts);
                let _ = reply.send(res);
            }
            SwarmCommand::AddPeerAddress { peer_id, addr } => {
                swarm.add_peer_address(peer_id, addr);
            }
            SwarmCommand::DisconnectPeerId { peer_id } => {
                let _ = swarm.disconnect_peer_id(peer_id);
            }
            SwarmCommand::Lookup { key, reply } => {
                let query_id = swarm.behaviour_mut().kademlia_get_record(key);
                self.get_queries.insert(query_id, reply);
            }
            SwarmCommand::LookupLocal { key, reply } => {
                let registration = swarm
                    .behaviour_mut()
                    .kademlia_get_record_local(&key)
                    .map(|record| ActorRegistration::from_bytes(&record.value).into_owned());
                let _ = reply.send(registration);
            }
            SwarmCommand::Register { record, reply } => {
                if swarm.network_info().num_peers() == 0 {
                    let key = record.key.clone();
                    swarm
                        .behaviour_mut()
                        .kademlia_put_record_local(record)
                        .unwrap();
                    let _ = reply.send(Ok(kad::PutRecordOk { key }));
                } else {
                    let query_id = swarm
                        .behaviour_mut()
                        .kademlia_put_record(record, kad::Quorum::One)
                        .unwrap();
                    self.put_queries.insert(query_id, reply);
                }
            }
            SwarmCommand::Ask {
                peer_id,
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
                reply,
            } => {
                if swarm.network_info().num_peers() == 0 {
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
                        let _ = reply.send(SwarmResponse::Ask(result));
                    });
                } else {
                    let req_id = swarm.behaviour_mut().ask(
                        &peer_id,
                        actor_id,
                        actor_remote_id,
                        message_remote_id,
                        payload,
                        mailbox_timeout,
                        reply_timeout,
                        immediate,
                    );
                    self.requests.insert(req_id, reply);
                }
            }
            SwarmCommand::Tell {
                peer_id,
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
                reply,
            } => {
                if swarm.network_info().num_peers() == 0 {
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
                        let _ = reply.send(SwarmResponse::Tell(result));
                    });
                } else {
                    let req_id = swarm.behaviour_mut().tell(
                        &peer_id,
                        actor_id,
                        actor_remote_id,
                        message_remote_id,
                        payload,
                        mailbox_timeout,
                        immediate,
                    );
                    self.requests.insert(req_id, reply);
                }
            }
            SwarmCommand::SendAskResponse { result, channel } => {
                let _ = swarm.behaviour_mut().send_ask_response(channel, result);
            }
            SwarmCommand::SendTellResponse { result, channel } => {
                let _ = swarm.behaviour_mut().send_tell_response(channel, result);
            }
        }
    }

    /// Handles a swarm event.
    ///
    /// Mdns, Kademlia, and RequestResponse events should be handled always.
    pub fn handle_event<B: SwarmBehaviour>(
        &mut self,
        swarm: &mut Swarm<B>,
        event: ActorSwarmBehaviourEvent,
    ) {
        match event {
            ActorSwarmBehaviourEvent::Mdns(mdns::Event::Discovered(list)) => {
                for (peer_id, multiaddr) in list {
                    swarm
                        .behaviour_mut()
                        .kademlia_add_address(&peer_id, multiaddr);
                }
            }
            ActorSwarmBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                id,
                result,
                ..
            }) => match result {
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
            ActorSwarmBehaviourEvent::RequestResponse(request_response::Event::Message {
                peer: _,
                message:
                    request_response::Message::Request {
                        request_id: _,
                        request,
                        channel,
                    },
            }) => match request {
                SwarmRequest::Ask {
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
                        tx.send(SwarmCommand::SendAskResponse { result, channel });
                    });
                }
                SwarmRequest::Tell {
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
                        tx.send(SwarmCommand::SendTellResponse { result, channel });
                    });
                }
            },
            ActorSwarmBehaviourEvent::RequestResponse(request_response::Event::Message {
                peer: _,
                message:
                    request_response::Message::Response {
                        request_id,
                        response,
                    },
            }) => {
                if let Some(tx) = self.requests.remove(&request_id) {
                    let _ = tx.send(response);
                }
            }
            ActorSwarmBehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                },
            ) => {
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
                    let _ = tx.send(SwarmResponse::OutboundFailure(err));
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

/// A swarm command.
#[derive(Debug)]
pub enum SwarmCommand {
    /// Listen on a given multiaddr.
    ListenOn {
        /// Address to listen on.
        addr: Multiaddr,
        /// Reply sender.
        reply: oneshot::Sender<Result<ListenerId, TransportError<io::Error>>>,
    },
    /// Dial a peer.
    Dial {
        /// Dial options.
        opts: DialOpts,
        /// Reply sender.
        reply: oneshot::Sender<Result<(), DialError>>,
    },
    /// Add a known peer id and address.
    AddPeerAddress {
        /// Peer ID.
        peer_id: PeerId,
        /// Peer address.
        addr: Multiaddr,
    },
    /// Disconnect a peer by id.
    DisconnectPeerId {
        /// Peer ID.
        peer_id: PeerId,
    },
    /// Lookup an actor by name in the kademlia network.
    Lookup {
        /// Kademlia record key.
        key: kad::RecordKey,
        /// Reply sender.
        reply: oneshot::Sender<Result<kad::PeerRecord, kad::GetRecordError>>,
    },
    /// Lookup an actor by name on the local node only.
    LookupLocal {
        /// Kademlia record key.
        key: kad::RecordKey,
        /// Reply sender.
        reply: oneshot::Sender<Option<ActorRegistration<'static>>>,
    },
    /// Register an actor in the kademlia network.
    Register {
        /// Kademlia record.
        record: kad::Record,
        /// Reply sender.
        reply: oneshot::Sender<kad::PutRecordResult>,
    },
    /// An actor ask request.
    Ask {
        /// Peer ID.
        peer_id: Intern<PeerId>,
        /// Actor ID.
        actor_id: ActorID,
        /// Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Message remote ID.
        message_remote_id: Cow<'static, str>,
        /// Payload.
        payload: Vec<u8>,
        /// Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        /// Reply timeout.
        reply_timeout: Option<Duration>,
        /// Fail if mailbox is full.
        immediate: bool,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
    /// An actor tell request.
    Tell {
        /// Peer ID.
        peer_id: Intern<PeerId>,
        /// Actor ID.
        actor_id: ActorID,
        /// Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Message remote ID.
        message_remote_id: Cow<'static, str>,
        /// Payload.
        payload: Vec<u8>,
        /// Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        /// Fail if mailbox is full.
        immediate: bool,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
    /// Send an ask response.
    SendAskResponse {
        /// Ask result.
        result: Result<Vec<u8>, RemoteSendError<Vec<u8>>>,
        /// Response channel.
        channel: ResponseChannel<SwarmResponse>,
    },
    /// Send a tell response.
    SendTellResponse {
        /// Tell result.
        result: Result<(), RemoteSendError<Vec<u8>>>,
        /// Response channel.
        channel: ResponseChannel<SwarmResponse>,
    },
}

/// An actor registration record.
#[derive(Debug)]
pub struct ActorRegistration<'a> {
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

/// Represents different types of requests that can be made within the swarm.
#[derive(Debug, Serialize, Deserialize)]
pub enum SwarmRequest {
    /// Represents a request to ask a peer for some data or action.
    ///
    /// This variant includes information about the actor, the message, payload, and timeout settings.
    Ask {
        /// Identifier of the actor initiating the request.
        actor_id: ActorID,

        /// Remote identifier of the actor as a static string.
        actor_remote_id: Cow<'static, str>,

        /// Remote identifier of the message as a static string.
        message_remote_id: Cow<'static, str>,

        /// The payload data to be sent with the request.
        payload: Vec<u8>,

        /// Optional timeout duration for the mailbox to receive the request.
        mailbox_timeout: Option<Duration>,

        /// Optional timeout duration to wait for a reply to the request.
        reply_timeout: Option<Duration>,

        /// Indicates whether the request should be sent immediately.
        immediate: bool,
    },

    /// Represents a request to tell a peer some information without expecting a response.
    ///
    /// This variant includes information about the actor, the message, payload, and timeout settings.
    Tell {
        /// Identifier of the actor initiating the message.
        actor_id: ActorID,

        /// Remote identifier of the actor as a static string.
        actor_remote_id: Cow<'static, str>,

        /// Remote identifier of the message as a static string.
        message_remote_id: Cow<'static, str>,

        /// The payload data to be sent with the message.
        payload: Vec<u8>,

        /// Optional timeout duration for the mailbox to receive the message.
        mailbox_timeout: Option<Duration>,

        /// Indicates whether the message should be sent immediately.
        immediate: bool,
    },
}

/// Represents different types of responses that can be sent within the swarm.
#[derive(Debug, Serialize, Deserialize)]
pub enum SwarmResponse {
    /// Represents the response to an `Ask` request.
    ///
    /// Contains either the successful payload data or an error indicating why the send failed.
    Ask(Result<Vec<u8>, RemoteSendError<Vec<u8>>>),

    /// Represents the response to a `Tell` request.
    ///
    /// Contains either a successful acknowledgment or an error indicating why the send failed.
    Tell(Result<(), RemoteSendError<Vec<u8>>>),

    /// Represents a failure that occurred while attempting to send an outbound request.
    ///
    /// Contains the error that caused the outbound request to fail.
    OutboundFailure(RemoteSendError<()>),
}

/// Defines the behavior of a swarm, extending the capabilities of `NetworkBehaviour`.
pub trait SwarmBehaviour: NetworkBehaviour {
    /// Initiates a request to a specified peer with the given actor and message identifiers.
    ///
    /// This method sends a payload to a peer and optionally waits for a reply within specified timeouts.
    #[allow(clippy::too_many_arguments)]
    fn ask(
        &mut self,
        peer: &PeerId,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> OutboundRequestId;

    /// Sends a message to a specified peer without expecting a response.
    ///
    /// This method is used for fire-and-forget communication with a peer.
    #[allow(clippy::too_many_arguments)]
    fn tell(
        &mut self,
        peer: &PeerId,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> OutboundRequestId;

    /// Sends a response to a previously received `ask` request.
    ///
    /// This method handles the result of processing an `ask` request and sends back the appropriate response.
    fn send_ask_response(
        &mut self,
        channel: ResponseChannel<SwarmResponse>,
        result: Result<Vec<u8>, RemoteSendError<Vec<u8>>>,
    ) -> Result<(), SwarmResponse>;

    /// Sends a response to a previously received `tell` request.
    ///
    /// This method handles the result of processing a `tell` request and sends back an acknowledgment.
    fn send_tell_response(
        &mut self,
        channel: ResponseChannel<SwarmResponse>,
        result: Result<(), RemoteSendError<Vec<u8>>>,
    ) -> Result<(), SwarmResponse>;

    /// Adds a network address for a peer to the Kademlia routing table.
    ///
    /// This method updates the routing information for a peer by adding a new address.
    fn kademlia_add_address(&mut self, peer: &PeerId, address: Multiaddr) -> kad::RoutingUpdate;

    /// Sets the operational mode for the Kademlia protocol.
    ///
    /// This method configures the Kademlia behavior, allowing it to switch between different modes of operation.
    fn kademlia_set_mode(&mut self, mode: Option<kad::Mode>);

    /// Retrieves a record from the Kademlia network using the specified key.
    ///
    /// This method initiates a query to fetch a record associated with the given key from the network.
    fn kademlia_get_record(&mut self, key: kad::RecordKey) -> kad::QueryId;

    /// Retrieves a local record from the Kademlia store using the specified key.
    ///
    /// This method accesses the local storage to obtain a record without querying the network.
    fn kademlia_get_record_local(&mut self, key: &kad::RecordKey) -> Option<Cow<'_, kad::Record>>;

    /// Stores a record in the Kademlia network with the specified quorum requirement.
    ///
    /// This method adds a new record to the network, ensuring that it meets the required quorum for replication.
    fn kademlia_put_record(
        &mut self,
        record: kad::Record,
        quorum: kad::Quorum,
    ) -> Result<kad::QueryId, kad::store::Error>;

    /// Stores a record locally in the Kademlia store without replicating it across the network.
    ///
    /// This method adds a new record to the local storage, bypassing network-wide replication.
    fn kademlia_put_record_local(&mut self, record: kad::Record) -> Result<(), kad::store::Error>;
}

#[allow(missing_docs)]
mod behaviour {
    use super::*;

    /// Network behaviour for the actor swarm.
    ///
    /// Uses kademlia for actor registration, request response for messaging, and mdns for discovery.
    #[allow(missing_debug_implementations)]
    #[derive(NetworkBehaviour)]
    pub struct ActorSwarmBehaviour {
        /// Kademlia network for actor registration.
        pub kademlia: kad::Behaviour<MemoryStore>,
        /// Request response for actor messaging.
        pub request_response: request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>,
        /// Mdns for discovery.
        pub mdns: mdns::tokio::Behaviour,
    }
}

pub use behaviour::*;

impl ActorSwarmBehaviour {
    /// Creates a new default actor behaviour with a keypair.
    pub fn new(keypair: &Keypair) -> io::Result<Self> {
        Ok(ActorSwarmBehaviour {
            kademlia: kad::Behaviour::new(
                keypair.public().to_peer_id(),
                MemoryStore::new(keypair.public().to_peer_id()),
            ),
            mdns: mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                keypair.public().to_peer_id(),
            )?,
            request_response: request_response::cbor::Behaviour::new(
                [(StreamProtocol::new("/kameo/1"), ProtocolSupport::Full)],
                request_response::Config::default(),
            ),
        })
    }
}

impl SwarmBehaviour for ActorSwarmBehaviour {
    fn ask(
        &mut self,
        peer: &PeerId,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> OutboundRequestId {
        self.request_response.send_request(
            peer,
            SwarmRequest::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            },
        )
    }

    fn tell(
        &mut self,
        peer: &PeerId,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> OutboundRequestId {
        self.request_response.send_request(
            peer,
            SwarmRequest::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            },
        )
    }

    fn send_ask_response(
        &mut self,
        channel: ResponseChannel<SwarmResponse>,
        result: Result<Vec<u8>, RemoteSendError<Vec<u8>>>,
    ) -> Result<(), SwarmResponse> {
        self.request_response
            .send_response(channel, SwarmResponse::Ask(result))
    }

    fn send_tell_response(
        &mut self,
        channel: ResponseChannel<SwarmResponse>,
        result: Result<(), RemoteSendError<Vec<u8>>>,
    ) -> Result<(), SwarmResponse> {
        self.request_response
            .send_response(channel, SwarmResponse::Tell(result))
    }

    fn kademlia_add_address(&mut self, peer: &PeerId, address: Multiaddr) -> kad::RoutingUpdate {
        self.kademlia.add_address(peer, address)
    }

    fn kademlia_set_mode(&mut self, mode: Option<kad::Mode>) {
        self.kademlia.set_mode(mode)
    }

    fn kademlia_get_record(&mut self, key: kad::RecordKey) -> kad::QueryId {
        self.kademlia.get_record(key)
    }

    fn kademlia_get_record_local(&mut self, key: &kad::RecordKey) -> Option<Cow<'_, kad::Record>> {
        self.kademlia.store_mut().get(key)
    }

    fn kademlia_put_record(
        &mut self,
        record: kad::Record,
        quorum: kad::Quorum,
    ) -> Result<kad::QueryId, kad::store::Error> {
        self.kademlia.put_record(record, quorum)
    }

    fn kademlia_put_record_local(&mut self, record: kad::Record) -> Result<(), kad::store::Error> {
        self.kademlia.store_mut().put(record)
    }
}

/// `SwarmFuture` represents a future that contains the response from a remote actor.
///
/// This future is returned when sending a message to a remote actor via the actor swarm.
/// If the response is not needed, the future can simply be dropped without awaiting it.
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
