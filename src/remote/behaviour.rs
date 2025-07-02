use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    str, task,
    time::Duration,
};

use either::Either;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use libp2p::{
    core::{transport::PortUse, Endpoint},
    kad::{self, store::RecordStore},
    request_response,
    swarm::{
        ConnectionClosed, ConnectionDenied, ConnectionHandler, ConnectionHandlerSelect,
        ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent, THandlerOutEvent,
        ToSwarm,
    },
    Multiaddr, PeerId, StreamProtocol,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::{
    actor::ActorId,
    error::{ActorStopReason, RegistryError, RemoteSendError},
    remote,
};

use super::{ActorRegistration, ActorSwarm, RemoteRegistryActorRef, SwarmCommand, REMOTE_REGISTRY};

type RegisterResult = Result<(), RegistryError>;
pub(super) type LookupResult = Result<ActorRegistration<'static>, RegistryError>;
type LookupLocalResult = Result<Option<ActorRegistration<'static>>, RegistryError>;

type AskResult = Result<Vec<u8>, RemoteSendError<Vec<u8>>>;
type TellResult = Result<(), RemoteSendError>;
type LinkResult = Result<(), RemoteSendError>;
type UnlinkResult = Result<(), RemoteSendError>;
type SignalLinkDiedResult = Result<(), RemoteSendError>;

pub(super) type RegisterReply = oneshot::Sender<RegisterResult>;
pub(super) type LookupReply = mpsc::UnboundedSender<LookupResult>;
pub(super) type LookupLocalReply = oneshot::Sender<LookupLocalResult>;
pub(super) type UnregisterReply = oneshot::Sender<()>;

/// Identifier for a request within the swarm behavior.
///
/// Requests can be either local (handled within the same peer) or outbound
/// (sent to remote peers via libp2p's request-response protocol).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RequestId {
    /// A local request handled within the same peer.
    Local(u64),
    /// An outbound request sent to a remote peer.
    Outbound(request_response::OutboundRequestId),
}

impl RequestId {
    fn unwrap_outbound(self) -> request_response::OutboundRequestId {
        match self {
            RequestId::Local(_) => panic!("called unwrap_outbound on a local request id"),
            RequestId::Outbound(id) => id,
        }
    }
}

impl PartialEq<request_response::OutboundRequestId> for RequestId {
    fn eq(&self, other: &request_response::OutboundRequestId) -> bool {
        match self {
            RequestId::Local(_) => false,
            RequestId::Outbound(id) => id.eq(other),
        }
    }
}

impl PartialEq<RequestId> for request_response::OutboundRequestId {
    fn eq(&self, other: &RequestId) -> bool {
        match other {
            RequestId::Local(_) => false,
            RequestId::Outbound(other) => self.eq(other),
        }
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Local(id) => write!(f, "{id}"),
            RequestId::Outbound(id) => id.fmt(f),
        }
    }
}

enum ReplyChannel {
    Event(RequestId),
    Local(oneshot::Sender<SwarmResponse>),
    Remote(request_response::ResponseChannel<SwarmResponse>),
}

/// The main network behavior for Kameo's distributed actor system.
///
/// `Behaviour` implements libp2p's `NetworkBehaviour` trait and combines two core protocols:
/// - **Kademlia DHT**: For distributed actor discovery and registration
/// - **Request-Response**: For direct actor-to-actor messaging across the network
///
/// This behavior handles all network-level operations for remote actors, including:
/// - Registering actors in the distributed registry so they can be discovered by other peers
/// - Looking up actors by name across the network
/// - Sending ask/tell messages between actors on different peers
/// - Managing actor links and supervision across network boundaries
/// - Handling connection events and peer failures
///
/// The behavior operates at a low level and emits events for all operations. Higher-level
/// Kameo APIs like `ActorRef::register()` and `RemoteActorRef::lookup()` use this behavior
/// internally but provide a more ergonomic interface without event emission.
///
/// # Architecture
///
/// The behavior maintains several internal components:
/// - A Kademlia DHT for distributed storage of actor registrations
/// - A request-response protocol handler for direct peer communication  
/// - Command channels for receiving requests from other parts of Kameo
/// - Query tracking for ongoing lookups and registrations
/// - A task pool for handling concurrent operations
///
/// # Event-Driven Operation
///
/// All operations are asynchronous and progress is reported through events:
/// - `LookupProgressed` events report actors found during discovery
/// - `RegisteredActor` events confirm successful registrations
/// - `AskResult`/`TellResult` events report message delivery outcomes
/// - Error events for timeouts, failures, and network issues
///
/// # Example
///
/// ```rust,no_run
/// use libp2p::{PeerId, request_response};
/// use kameo::remote::Behaviour;
///
/// let peer_id = PeerId::random();
/// let config = request_response::Config::default();
/// let behaviour = Behaviour::new(peer_id, config);
/// ```
#[allow(missing_debug_implementations)]
pub struct Behaviour {
    /// Kademlia network for actor registration.
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    /// Request response for actor messaging.
    request_response: request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>,

    // Internal fields
    cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
    local_peer_id: PeerId,
    pending_events: VecDeque<Event>,
    registration_queries: HashMap<kad::QueryId, RegistrationQuery>,
    lookup_queries: HashMap<kad::QueryId, LookupQuery>,

    next_id: u64,
    requests: HashMap<RequestId, Option<oneshot::Sender<SwarmResponse>>>,
    join_set: JoinSet<(ReplyChannel, SwarmResponse)>,
}

struct RegistrationQuery {
    name: String,
    registration: Option<ActorRegistration<'static>>,
    put_query_id: Option<kad::QueryId>,
    provider_result: Option<kad::AddProviderResult>,
    reply: Option<RegisterReply>,
}

struct LookupQuery {
    name: String,
    providers_finished: bool,
    metadata_queries: HashSet<kad::QueryId>,
    queried_providers: HashSet<PeerId>,
    reported_providers: HashSet<PeerId>,
    reply: Option<LookupReply>,
}

impl LookupQuery {
    fn get_metadata_record(
        &mut self,
        kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
        provider: &PeerId,
    ) -> bool {
        // Skip if we've already queried this provider
        if self.queried_providers.contains(provider) {
            return false;
        }

        self.queried_providers.insert(*provider);
        let key = format!("{}:meta:{provider}", self.name);
        let query_id = kademlia.get_record(key.into_bytes().into());
        self.metadata_queries.insert(query_id);
        true
    }

    fn has_metadata_query(&self, query_id: &kad::QueryId) -> bool {
        self.metadata_queries.contains(query_id)
    }

    fn metadata_query_finished(&mut self, query_id: &kad::QueryId) {
        self.metadata_queries.remove(query_id);
    }

    fn is_finished(&self) -> bool {
        self.providers_finished && self.metadata_queries.is_empty()
    }
}

impl Behaviour {
    /// Creates a new swarm behavior for Kameo actor communication.
    ///
    /// This initializes the libp2p network behavior with Kademlia DHT for actor
    /// discovery and request-response protocol for actor messaging.
    ///
    /// # Arguments
    ///
    /// * `id` - The peer ID for this node
    /// * `request_response_config` - Configuration for the request-response protocol
    pub fn new(id: PeerId, request_response_config: request_response::Config) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        ActorSwarm::set(cmd_tx, id).unwrap();

        let mut kademlia = kad::Behaviour::new(id, kad::store::MemoryStore::new(id));
        kademlia.set_mode(Some(kad::Mode::Server));
        let request_response = request_response::cbor::Behaviour::new(
            [(
                StreamProtocol::new("/kameo/1"),
                request_response::ProtocolSupport::Full,
            )],
            request_response_config,
        );

        Behaviour {
            kademlia,
            request_response,

            cmd_rx,
            local_peer_id: id,
            pending_events: VecDeque::new(),
            registration_queries: HashMap::new(),
            lookup_queries: HashMap::new(),
            next_id: 0,
            requests: HashMap::new(),
            join_set: JoinSet::new(),
        }
    }

    /// Registers an actor in the distributed registry.
    ///
    /// This is a low-level method that directly interacts with the Kademlia DHT
    /// and generates events. Use `ActorRef::register` for higher-level registration
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to register the actor under
    /// * `registration` - The actor registration information
    ///
    /// # Returns
    ///
    /// The query ID for tracking the registration progress, or an error if
    /// registration fails immediately.
    pub fn register(
        &mut self,
        name: String,
        registration: ActorRegistration<'static>,
    ) -> Result<kad::QueryId, kad::store::Error> {
        self.register_with_reply(name, registration, None)
            .map_err(|(_, err)| err)
    }

    fn register_with_reply(
        &mut self,
        name: String,
        registration: ActorRegistration<'static>,
        reply: Option<RegisterReply>,
    ) -> Result<kad::QueryId, (Option<RegisterReply>, kad::store::Error)> {
        let key = kad::RecordKey::new(&name);
        let provider_query_id = match self.kademlia.start_providing(key) {
            Ok(id) => id,
            Err(err) => {
                return Err((reply, err));
            }
        };

        self.registration_queries.insert(
            provider_query_id,
            RegistrationQuery {
                name,
                registration: Some(registration),
                put_query_id: None,
                provider_result: None,
                reply,
            },
        );

        Ok(provider_query_id)
    }

    /// Cancels an ongoing actor registration.
    ///
    /// This is a low-level method. Returns `true` if the registration was found
    /// and cancelled, `false` if no registration with the given query ID exists.
    pub fn cancel_registration(&mut self, query_id: &kad::QueryId) -> bool {
        self.registration_queries.remove(query_id).is_some()
    }

    /// Unregisters an actor from the distributed registry.
    ///
    /// This is a low-level method that removes the actor from both the provider
    /// records and metadata storage in the Kademlia DHT.
    ///
    /// # Arguments
    ///
    /// * `name` - The name the actor was registered under
    pub fn unregister(&mut self, name: &str) {
        self.kademlia
            .stop_providing(&kad::RecordKey::new(&name.as_bytes()));
        let key = format!("{}:meta:{}", name, self.local_peer_id);
        self.kademlia
            .remove_record(&kad::RecordKey::from(key.into_bytes()));
    }

    /// Looks up actors by name in the distributed registry.
    ///
    /// This is a low-level method that queries the Kademlia DHT and generates
    /// events as actors are discovered. Use `RemoteActorRef::lookup` for
    /// higher-level lookups that don't emit events.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to search for
    ///
    /// # Returns
    ///
    /// The query ID for tracking the lookup progress.
    pub fn lookup(&mut self, name: String) -> kad::QueryId {
        self.lookup_with_reply(name, None)
    }

    fn lookup_with_reply(&mut self, name: String, reply: Option<LookupReply>) -> kad::QueryId {
        let query_id = self.kademlia.get_providers(kad::RecordKey::new(&name));
        self.lookup_queries.insert(
            query_id,
            LookupQuery {
                name,
                providers_finished: false,
                metadata_queries: HashSet::new(),
                queried_providers: HashSet::new(),
                reported_providers: HashSet::new(),
                reply,
            },
        );

        query_id
    }

    /// Looks up an actor in the local registry only.
    ///
    /// This is a low-level method that checks if this peer is providing
    /// the requested actor name without querying remote peers.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to search for locally
    ///
    /// # Returns
    ///
    /// The actor registration if found locally, `None` if not found,
    /// or an error if the lookup fails.
    pub fn lookup_local(
        &mut self,
        name: &str,
    ) -> Result<Option<ActorRegistration<'static>>, RegistryError> {
        // Check if we're providing this key locally
        let key = kad::RecordKey::new(&name);
        let store_mut = self.kademlia.store_mut();
        let is_providing = store_mut.provided().any(|k| k.key == key);

        if is_providing {
            // Get metadata for local provider
            let metadata_key = format!("{name}:meta:{}", self.local_peer_id);
            let registration = store_mut
                .get(&kad::RecordKey::new(&metadata_key))
                .map(|record| {
                    ActorRegistration::from_bytes(&record.value)
                        .map(ActorRegistration::into_owned)
                        .map_err(RegistryError::from)
                })
                .transpose();
            registration
        } else {
            Ok(None)
        }
    }

    /// Cancels an ongoing actor lookup.
    ///
    /// This is a low-level method. Returns `true` if the lookup was found
    /// and cancelled, `false` if no lookup with the given query ID exists.
    pub fn cancel_lookup(&mut self, query_id: &kad::QueryId) -> bool {
        self.lookup_queries.remove(query_id).is_some()
    }

    /// Sends an ask request to a remote actor.
    ///
    /// This is a low-level method that sends a request expecting a reply and
    /// generates events. Use `RemoteActorRef::ask` for higher-level messaging
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The target actor's ID
    /// * `actor_remote_id` - The target actor's remote type ID
    /// * `message_remote_id` - The message's remote type ID
    /// * `payload` - The serialized message payload
    /// * `mailbox_timeout` - Optional timeout for mailbox delivery
    /// * `reply_timeout` - Optional timeout for receiving a reply
    /// * `immediate` - Whether to fail if the mailbox is full
    ///
    /// # Returns
    ///
    /// The request ID for tracking the ask progress.
    #[allow(clippy::too_many_arguments)]
    pub fn ask(
        &mut self,
        // Actor ID.
        actor_id: ActorId,
        // Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        // Message remote ID.
        message_remote_id: Cow<'static, str>,
        // Payload.
        payload: Vec<u8>,
        // Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        // Reply timeout.
        reply_timeout: Option<Duration>,
        // Fail if mailbox is full.
        immediate: bool,
    ) -> RequestId {
        self.ask_with_reply(
            actor_id,
            actor_remote_id,
            message_remote_id,
            payload,
            mailbox_timeout,
            reply_timeout,
            immediate,
            None,
        )
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn ask_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            peer_id,
            reply,
            (
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            ),
            |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            )| {
                remote::ask(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    reply_timeout,
                    immediate,
                )
                .map(SwarmResponse::Ask)
            },
            move |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            )| SwarmRequest::Ask {
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

    /// Sends a tell message to a remote actor.
    ///
    /// This is a low-level method that sends a one-way message and generates
    /// events. Use `RemoteActorRef::tell` for higher-level messaging that
    /// doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The target actor's ID
    /// * `actor_remote_id` - The target actor's remote type ID
    /// * `message_remote_id` - The message's remote type ID
    /// * `payload` - The serialized message payload
    /// * `mailbox_timeout` - Optional timeout for mailbox delivery
    /// * `immediate` - Whether to fail if the mailbox is full
    ///
    /// # Returns
    ///
    /// The request ID for tracking the tell progress.
    pub fn tell(
        &mut self,
        // Actor ID.
        actor_id: ActorId,
        // Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        // Message remote ID.
        message_remote_id: Cow<'static, str>,
        // Payload.
        payload: Vec<u8>,
        // Mailbox timeout.
        mailbox_timeout: Option<Duration>,
        // Fail if mailbox is full.
        immediate: bool,
    ) -> RequestId {
        self.tell_with_reply(
            actor_id,
            actor_remote_id,
            message_remote_id,
            payload,
            mailbox_timeout,
            immediate,
            None,
        )
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn tell_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            peer_id,
            reply,
            (
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            ),
            |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            )| {
                remote::tell(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    immediate,
                )
                .map(SwarmResponse::Tell)
            },
            move |(
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            )| SwarmRequest::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            },
        )
    }

    /// Creates a link between two actors across the network.
    ///
    /// This is a low-level method that establishes supervision relationships
    /// and generates events. Use `ActorRef::link` for higher-level linking
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The first actor's ID
    /// * `actor_remote_id` - The first actor's remote type ID  
    /// * `sibbling_id` - The second actor's ID to link with
    /// * `sibbling_remote_id` - The second actor's remote type ID
    ///
    /// # Returns
    ///
    /// The request ID for tracking the link progress.
    pub fn link(
        &mut self,
        // Actor A ID.
        actor_id: ActorId,
        // Actor A remote ID.
        actor_remote_id: Cow<'static, str>,
        // Actor B ID.
        sibbling_id: ActorId,
        // Actor B remote ID.
        sibbling_remote_id: Cow<'static, str>,
    ) -> RequestId {
        self.link_with_reply(
            actor_id,
            actor_remote_id,
            sibbling_id,
            sibbling_remote_id,
            None,
        )
        .unwrap()
    }

    fn link_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        sibbling_id: ActorId,
        sibbling_remote_id: Cow<'static, str>,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            peer_id,
            reply,
            (actor_id, actor_remote_id, sibbling_id, sibbling_remote_id),
            |(actor_id, actor_remote_id, sibbling_id, sibbling_remote_id)| {
                remote::link(actor_id, actor_remote_id, sibbling_id, sibbling_remote_id)
                    .map(SwarmResponse::Link)
            },
            move |(actor_id, actor_remote_id, sibbling_id, sibbling_remote_id)| {
                SwarmRequest::Link {
                    actor_id,
                    actor_remote_id,
                    sibbling_id,
                    sibbling_remote_id,
                }
            },
        )
    }

    /// Removes a link between two actors across the network.
    ///
    /// This is a low-level method that removes supervision relationships
    /// and generates events. Use `ActorRef::unlink` for higher-level unlinking
    /// that doesn't emit events.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The first actor's ID
    /// * `actor_remote_id` - The first actor's remote type ID
    /// * `sibbling_id` - The second actor's ID to unlink from
    ///
    /// # Returns
    ///
    /// The request ID for tracking the unlink progress.
    pub fn unlink(
        &mut self,
        // Actor ID.
        actor_id: ActorId,
        // Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        // Sibbling ID.
        sibbling_id: ActorId,
    ) -> RequestId {
        self.unlink_with_reply(actor_id, actor_remote_id, sibbling_id, None)
            .unwrap()
    }

    fn unlink_with_reply(
        &mut self,
        actor_id: ActorId,
        actor_remote_id: Cow<'static, str>,
        sibbling_id: ActorId,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = actor_id.peer_id().expect("swarm should be bootstrapped");
        self.request_with_reply(
            peer_id,
            reply,
            (actor_id, actor_remote_id, sibbling_id),
            |(actor_id, actor_remote_id, sibbling_id)| {
                remote::unlink(actor_id, actor_remote_id, sibbling_id).map(SwarmResponse::Unlink)
            },
            move |(actor_id, actor_remote_id, sibbling_id)| SwarmRequest::Unlink {
                actor_id,
                actor_remote_id,
                sibbling_id,
            },
        )
    }

    /// Signals that a linked actor has died to another actor.
    ///
    /// This is a low-level method that notifies actors of link failures
    /// and generates events. This is typically called automatically by
    /// the actor system when links are broken.
    ///
    /// # Arguments
    ///
    /// * `dead_actor_id` - The ID of the actor that died
    /// * `notified_actor_id` - The ID of the actor to notify
    /// * `notified_actor_remote_id` - The remote type ID of the actor to notify
    /// * `stop_reason` - The reason the actor stopped
    ///
    /// # Returns
    ///
    /// The request ID for tracking the signal progress.
    pub fn signal_link_died(
        &mut self,
        // The actor which died.
        dead_actor_id: ActorId,
        // The actor to notify.
        notified_actor_id: ActorId,
        // Actor remote iD
        notified_actor_remote_id: Cow<'static, str>,
        // The reason the actor died.
        stop_reason: ActorStopReason,
    ) -> RequestId {
        self.signal_link_died_with_reply(
            dead_actor_id,
            notified_actor_id,
            notified_actor_remote_id,
            stop_reason,
            None,
        )
        .unwrap()
    }

    fn signal_link_died_with_reply(
        &mut self,
        dead_actor_id: ActorId,
        notified_actor_id: ActorId,
        notified_actor_remote_id: Cow<'static, str>,
        stop_reason: ActorStopReason,
        reply: Option<oneshot::Sender<SwarmResponse>>,
    ) -> Option<RequestId> {
        let peer_id = notified_actor_id
            .peer_id()
            .expect("swarm should be bootstrapped");
        self.request_with_reply(
            peer_id,
            reply,
            (
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
            ),
            |(dead_actor_id, notified_actor_id, notified_actor_remote_id, stop_reason)| {
                remote::signal_link_died(
                    dead_actor_id,
                    notified_actor_id,
                    notified_actor_remote_id,
                    stop_reason,
                )
                .map(SwarmResponse::SignalLinkDied)
            },
            move |(dead_actor_id, notified_actor_id, notified_actor_remote_id, stop_reason)| {
                SwarmRequest::SignalLinkDied {
                    dead_actor_id,
                    notified_actor_id,
                    notified_actor_remote_id,
                    stop_reason,
                }
            },
        )
    }

    fn new_local_request_id(&mut self) -> RequestId {
        let id = RequestId::Local(self.next_id);
        self.next_id += 1;
        id
    }

    fn request_with_reply<L, LF, R, T>(
        &mut self,
        peer_id: &PeerId,
        reply: Option<oneshot::Sender<SwarmResponse>>,
        shared_data: T,
        local: L,
        remote: R,
    ) -> Option<RequestId>
    where
        L: FnOnce(T) -> LF,
        LF: Future<Output = SwarmResponse> + Send + 'static,
        R: FnOnce(T) -> SwarmRequest,
    {
        if peer_id == &self.local_peer_id {
            let (request_id, channel) = match reply {
                Some(tx) => (None, ReplyChannel::Local(tx)),
                None => {
                    let request_id = self.new_local_request_id();
                    (Some(request_id), ReplyChannel::Event(request_id))
                }
            };

            self.join_set
                .spawn(local(shared_data).map(|resp| (channel, resp)));

            request_id
        } else {
            let request_id = RequestId::Outbound(
                self.request_response
                    .send_request(peer_id, remote(shared_data)),
            );
            self.requests.insert(request_id, reply);

            Some(request_id)
        }
    }

    fn handle_command(&mut self, cmd: SwarmCommand) -> bool {
        match cmd {
            SwarmCommand::Lookup { name, reply } => {
                self.lookup_with_reply(name, Some(reply));
                true // We triggered a get_providers on kad, we need to call the waker
            }
            SwarmCommand::LookupLocal { name, reply } => {
                let _ = reply.send(self.lookup_local(&name));
                false // Local lookups don't need to be polled again
            }
            SwarmCommand::Register {
                name,
                registration,
                reply,
            } => match self.register_with_reply(name, registration, Some(reply)) {
                Ok(_) => true, // We started a new lookup
                Err((Some(reply), err)) => {
                    let _ = reply.send(Err(err.into()));
                    false
                }
                Err((None, _)) => unreachable!("we should have the reply type here"),
            },
            SwarmCommand::Unregister { name, reply } => {
                self.unregister(&name);
                let _ = reply.send(());
                false // We only delete the actor from our local registry
            }
            SwarmCommand::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
                reply,
            } => {
                self.ask_with_reply(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    reply_timeout,
                    immediate,
                    Some(reply),
                );
                true
            }
            SwarmCommand::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
                reply,
            } => {
                self.tell_with_reply(
                    actor_id,
                    actor_remote_id,
                    message_remote_id,
                    payload,
                    mailbox_timeout,
                    immediate,
                    Some(reply),
                );
                true
            }
            SwarmCommand::Link {
                actor_id,
                actor_remote_id,
                sibbling_id,
                sibbling_remote_id,
                reply,
            } => {
                self.link_with_reply(
                    actor_id,
                    actor_remote_id,
                    sibbling_id,
                    sibbling_remote_id,
                    Some(reply),
                );
                true
            }
            SwarmCommand::Unlink {
                actor_id,
                sibbling_id,
                sibbling_remote_id,
                reply,
            } => {
                self.unlink_with_reply(sibbling_id, sibbling_remote_id, actor_id, Some(reply));
                true
            }
            SwarmCommand::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
                reply,
            } => {
                self.signal_link_died_with_reply(
                    dead_actor_id,
                    notified_actor_id,
                    notified_actor_remote_id,
                    stop_reason,
                    Some(reply),
                );
                true
            }
        }
    }

    fn handle_kademlia_event(&mut self, ev: &kad::Event) -> bool {
        match ev {
            kad::Event::InboundRequest { .. } => false,
            kad::Event::OutboundQueryProgressed {
                id,
                result,
                stats: _,
                step,
            } => match result {
                kad::QueryResult::Bootstrap(_) => false,
                kad::QueryResult::GetClosestPeers(_) => false,
                // Getting the providers has progressed
                kad::QueryResult::GetProviders(res) => match res {
                    Ok(kad::GetProvidersOk::FoundProviders { providers, .. }) => {
                        let mut wake = false;
                        if let Some(lookup_query) = self.lookup_queries.get_mut(id) {
                            lookup_query.providers_finished = step.last;

                            for provider in providers {
                                wake |=
                                    lookup_query.get_metadata_record(&mut self.kademlia, provider);
                            }

                            let last = step.last && lookup_query.is_finished();
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: *id,
                                    });
                                }
                                self.lookup_queries.remove(id);
                            }
                        }

                        wake
                    }
                    Ok(kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. }) => {
                        if let Some(lookup_query) = self.lookup_queries.get_mut(id) {
                            lookup_query.providers_finished = step.last;
                            let last = step.last && lookup_query.is_finished();
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: *id,
                                    });
                                }
                                self.lookup_queries.remove(id);
                            }
                        }

                        false
                    }
                    Err(kad::GetProvidersError::Timeout { .. }) => {
                        if let Some(lookup_query) = self.lookup_queries.get_mut(id) {
                            lookup_query.providers_finished = step.last;
                            let last = step.last && lookup_query.is_finished();
                            match &lookup_query.reply {
                                Some(tx) => {
                                    let _ = tx.send(Err(RegistryError::Timeout));
                                }
                                None => {
                                    self.pending_events.push_back(Event::LookupTimeout {
                                        provider_query_id: *id,
                                    });
                                }
                            }
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: *id,
                                    });
                                }
                                self.lookup_queries.remove(id);
                            }
                        }

                        false
                    }
                },
                // Registering an actor has progressed
                kad::QueryResult::StartProviding(res) => {
                    if let Some(registration_query) = self.registration_queries.get_mut(id) {
                        match res {
                            Ok(kad::AddProviderOk { .. }) => {
                                let Some(registration) = registration_query.registration.take()
                                else {
                                    panic!("the registration should exist here");
                                    // self.registration_queries.remove(id);
                                    // return;
                                };
                                registration_query.provider_result = Some(res.clone());

                                // Store the metadata record
                                let key = format!(
                                    "{}:meta:{}",
                                    registration_query.name, self.local_peer_id
                                );
                                let registration_bytes = registration.into_bytes();
                                let record = kad::Record::new(key.into_bytes(), registration_bytes);

                                match self.kademlia.put_record(record, kad::Quorum::One) {
                                    Ok(put_query_id) => {
                                        registration_query.put_query_id = Some(put_query_id);
                                    }
                                    Err(err) => {
                                        // Put record failed immediately
                                        match self
                                            .registration_queries
                                            .remove(id)
                                            .and_then(|q| q.reply)
                                        {
                                            Some(tx) => {
                                                let _ = tx.send(Err(err.into()));
                                            }
                                            None => {
                                                self.pending_events.push_back(
                                                    Event::RegistrationFailed {
                                                        provider_query_id: *id,
                                                        error: err.into(),
                                                    },
                                                );
                                            }
                                        }
                                    }
                                }

                                true
                            }
                            Err(kad::AddProviderError::Timeout { .. }) => {
                                match self.registration_queries.remove(id).and_then(|q| q.reply) {
                                    Some(tx) => {
                                        let _ = tx.send(Err(RegistryError::Timeout));
                                    }
                                    None => {
                                        self.pending_events.push_back(Event::RegistrationFailed {
                                            provider_query_id: *id,
                                            error: RegistryError::Timeout,
                                        });
                                    }
                                }

                                false
                            }
                        }
                    } else {
                        false
                    }
                }
                kad::QueryResult::RepublishProvider(_) => false,
                // Getting a metadata record has progressed
                kad::QueryResult::GetRecord(res) => {
                    let Some((provider_query_id, lookup_query)) = self
                        .lookup_queries
                        .iter_mut()
                        .find(|(_, lookup_query)| lookup_query.has_metadata_query(id))
                    else {
                        return false;
                    };
                    let provider_query_id = *provider_query_id;

                    match res {
                        Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                            record: kad::Record { value, .. },
                            ..
                        })) => {
                            let result = ActorRegistration::from_bytes(value)
                                .map(|registration| registration.into_owned())
                                .map_err(RegistryError::from);

                            // Check if we've already reported this provider to avoid duplicates
                            let should_emit = if let Ok(ref registration) = result {
                                if let Some(peer_id) = registration.actor_id.peer_id() {
                                    if lookup_query.reported_providers.contains(peer_id) {
                                        false // Already reported this provider
                                    } else {
                                        lookup_query.reported_providers.insert(*peer_id);
                                        true
                                    }
                                } else {
                                    true // No peer_id, emit anyway
                                }
                            } else {
                                true // Error case, emit anyway
                            };

                            if should_emit {
                                match &lookup_query.reply {
                                    Some(tx) => {
                                        let _ = tx.send(result);
                                    }
                                    None => {
                                        self.pending_events.push_back(Event::LookupProgressed {
                                            provider_query_id,
                                            get_query_id: *id,
                                            result,
                                        });
                                    }
                                }
                            }
                        }
                        // These cases don't provide useful information to the user
                        Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. })
                        | Err(kad::GetRecordError::NotFound { .. }) => {
                            // No progress event needed
                        }
                        // Error cases are still useful to report
                        Err(kad::GetRecordError::QuorumFailed { quorum, .. }) => {
                            match &lookup_query.reply {
                                Some(tx) => {
                                    let _ = tx
                                        .send(Err(RegistryError::QuorumFailed { quorum: *quorum }));
                                }
                                None => {
                                    self.pending_events.push_back(Event::LookupProgressed {
                                        provider_query_id,
                                        get_query_id: *id,
                                        result: Err(RegistryError::QuorumFailed {
                                            quorum: *quorum,
                                        }),
                                    });
                                }
                            }
                        }
                        Err(kad::GetRecordError::Timeout { .. }) => match &lookup_query.reply {
                            Some(tx) => {
                                let _ = tx.send(Err(RegistryError::Timeout));
                            }
                            None => {
                                self.pending_events.push_back(Event::LookupProgressed {
                                    provider_query_id,
                                    get_query_id: *id,
                                    result: Err(RegistryError::Timeout),
                                });
                            }
                        },
                    }

                    if step.last {
                        lookup_query.metadata_query_finished(id);
                        let last = lookup_query.is_finished();

                        if last {
                            if lookup_query.reply.is_none() {
                                self.pending_events
                                    .push_back(Event::LookupCompleted { provider_query_id });
                            }
                            self.lookup_queries.remove(&provider_query_id);
                        }
                    }

                    false
                }
                // Putting a metadata record has progressed
                kad::QueryResult::PutRecord(res) => {
                    let Some(provider_query_id) =
                        self.registration_queries
                            .iter()
                            .find_map(|(query_id, reg)| {
                                if reg.put_query_id == Some(*id) {
                                    Some(*query_id)
                                } else {
                                    None
                                }
                            })
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("received PutRecord event for unknown registration query");
                        return false;
                    };

                    match res {
                        Ok(ok) => {
                            let mut registration_query = self
                                .registration_queries
                                .remove(&provider_query_id)
                                .unwrap();
                            match registration_query.reply.take() {
                                Some(tx) => {
                                    let _ = tx.send(Ok(()));
                                }
                                None => {
                                    self.pending_events.push_back(Event::RegisteredActor {
                                        provider_result: registration_query
                                            .provider_result
                                            .unwrap(),
                                        provider_query_id,
                                        metadata_result: Ok(ok.clone()),
                                        metadata_query_id: *id,
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            match self
                                .registration_queries
                                .remove(&provider_query_id)
                                .and_then(|q| q.reply)
                            {
                                Some(tx) => {
                                    let _ = tx.send(Err(err.clone().into()));
                                }
                                None => {
                                    self.pending_events.push_back(Event::RegistrationFailed {
                                        provider_query_id,
                                        error: err.clone().into(),
                                    });
                                }
                            }
                        }
                    }

                    false
                }
                kad::QueryResult::RepublishRecord(_) => false,
            },
            kad::Event::RoutingUpdated { .. } => false,
            kad::Event::UnroutablePeer { .. } => false,
            kad::Event::RoutablePeer { .. } => false,
            kad::Event::PendingRoutablePeer { .. } => false,
            kad::Event::ModeChanged { .. } => false,
        }
    }

    fn handle_request_response_event(
        &mut self,
        ev: request_response::Event<SwarmRequest, SwarmResponse>,
    ) -> (bool, Option<Event>) {
        match ev {
            // Incoming message
            request_response::Event::Message {
                peer,
                connection_id,
                message,
            } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    self.handle_incoming_request(request, channel);
                    (true, None)
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let ev =
                        self.handle_incoming_response(peer, connection_id, request_id, response);
                    (false, ev)
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                connection_id,
                request_id,
                error,
            } => match self.requests.remove(&RequestId::Outbound(request_id)) {
                Some(Some(tx)) => {
                    let _ = tx.send(SwarmResponse::OutboundFailure(error.into()));
                    (false, None)
                }
                Some(None) | None => (
                    false,
                    Some(Event::OutboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error: error.into(),
                    }),
                ),
            },
            request_response::Event::InboundFailure {
                peer,
                connection_id,
                request_id,
                error,
            } => (
                false,
                Some(Event::InboundFailure {
                    peer,
                    connection_id,
                    request_id,
                    error,
                }),
            ),
            request_response::Event::ResponseSent {
                peer,
                connection_id,
                request_id,
            } => (
                false,
                Some(Event::ResponseSent {
                    peer,
                    connection_id,
                    request_id,
                }),
            ),
        }
    }

    fn handle_incoming_request(
        &mut self,
        req: SwarmRequest,
        channel: request_response::ResponseChannel<SwarmResponse>,
    ) {
        let channel = ReplyChannel::Remote(channel);
        match req {
            SwarmRequest::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            } => {
                self.join_set.spawn(
                    remote::ask(
                        actor_id,
                        actor_remote_id,
                        message_remote_id,
                        payload,
                        mailbox_timeout,
                        reply_timeout,
                        immediate,
                    )
                    .map(|res| (channel, SwarmResponse::Ask(res))),
                );
            }
            SwarmRequest::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            } => {
                self.join_set.spawn(
                    remote::tell(
                        actor_id,
                        actor_remote_id,
                        message_remote_id,
                        payload,
                        mailbox_timeout,
                        immediate,
                    )
                    .map(|res| (channel, SwarmResponse::Tell(res))),
                );
            }
            SwarmRequest::Link {
                actor_id,
                actor_remote_id,
                sibbling_id,
                sibbling_remote_id,
            } => {
                self.join_set.spawn(
                    remote::link(actor_id, actor_remote_id, sibbling_id, sibbling_remote_id)
                        .map(|res| (channel, SwarmResponse::Link(res))),
                );
            }
            SwarmRequest::Unlink {
                actor_id,
                actor_remote_id,
                sibbling_id,
            } => {
                self.join_set.spawn(
                    remote::unlink(actor_id, actor_remote_id, sibbling_id)
                        .map(|res| (channel, SwarmResponse::Unlink(res))),
                );
            }
            SwarmRequest::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
            } => {
                self.join_set.spawn(
                    remote::signal_link_died(
                        dead_actor_id,
                        notified_actor_id,
                        notified_actor_remote_id,
                        stop_reason,
                    )
                    .map(|res| (channel, SwarmResponse::SignalLinkDied(res))),
                );
            }
        }
    }

    fn handle_incoming_response(
        &mut self,
        peer: PeerId,
        connection_id: ConnectionId,
        req_id: request_response::OutboundRequestId,
        res: SwarmResponse,
    ) -> Option<Event> {
        match self.requests.remove(&RequestId::Outbound(req_id)) {
            Some(Some(tx)) => {
                // Reply to channel
                let _ = tx.send(res);
                None
            }
            Some(None) => {
                // Emit event
                Some(Event::from_swarm_resp(
                    res,
                    peer,
                    Some(connection_id),
                    RequestId::Outbound(req_id),
                ))
            }
            None => {
                // Unrecognized request id
                #[cfg(feature = "tracing")]
                tracing::warn!(%peer, %connection_id, %req_id, ?res, "unrecognised request id for response");
                None
            }
        }
    }
}

/// Events emitted by the Kameo swarm behavior.
///
/// These events represent the results of network operations, actor lookups,
/// registrations, and message passing between remote actors.
#[derive(Debug)]
pub enum Event
where
    kad::Behaviour<kad::store::MemoryStore>: NetworkBehaviour,
    request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>: NetworkBehaviour,
{
    /// A Kademlia DHT event.
    Kademlia(kad::Event),

    /// Progress in looking up actors by name.
    LookupProgressed {
        /// The original provider query ID.
        provider_query_id: kad::QueryId,
        /// The metadata query ID.
        get_query_id: kad::QueryId,
        /// The actor registration found, or an error.
        result: Result<ActorRegistration<'static>, RegistryError>,
    },

    /// A lookup operation timed out while searching for providers.
    /// More metadata might still arrive from providers found before timeout.
    LookupTimeout {
        /// The provider query ID that timed out.
        provider_query_id: kad::QueryId,
    },

    /// A lookup operation completed. This is always the final event for a lookup.
    LookupCompleted {
        /// The completed provider query ID.
        provider_query_id: kad::QueryId,
    },

    /// An actor registration failed.
    RegistrationFailed {
        /// The provider query ID that failed.
        provider_query_id: kad::QueryId,
        /// The error that caused the failure.
        error: RegistryError,
    },

    /// An actor was successfully registered.
    RegisteredActor {
        /// The result of the provider registration.
        provider_result: kad::AddProviderResult,
        /// The provider query ID.
        provider_query_id: kad::QueryId,
        /// The result of storing metadata.
        metadata_result: kad::PutRecordResult,
        /// The metadata query ID.
        metadata_query_id: kad::QueryId,
    },

    /// Result of an ask request to a remote actor.
    AskResult {
        /// The peer that handled the request.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the ask operation.
        result: AskResult,
    },

    /// Result of a tell message to a remote actor.
    TellResult {
        /// The peer that handled the message.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the tell operation.
        result: TellResult,
    },

    /// Result of a link operation between actors.
    LinkResult {
        /// The peer that handled the link.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the link operation.
        result: LinkResult,
    },

    /// Result of an unlink operation between actors.
    UnlinkResult {
        /// The peer that handled the unlink.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the unlink operation.
        result: UnlinkResult,
    },

    /// Result of signaling that a linked actor died.
    SignalLinkDiedResult {
        /// The peer that handled the signal.
        peer: PeerId,
        /// The connection used, if any.
        connection_id: Option<ConnectionId>,
        /// The request ID.
        request_id: RequestId,
        /// The result of the signal operation.
        result: SignalLinkDiedResult,
    },

    /// An outbound request failed.
    OutboundFailure {
        /// The peer to whom the request was sent.
        peer: PeerId,
        /// The connection used.
        connection_id: ConnectionId,
        /// The (local) ID of the failed request.
        request_id: request_response::OutboundRequestId,
        /// The error that occurred.
        error: RemoteSendError,
    },

    /// An inbound request failed.
    InboundFailure {
        /// The peer from whom the request was received.
        peer: PeerId,
        /// The connection used.
        connection_id: ConnectionId,
        /// The ID of the failed inbound request.
        request_id: request_response::InboundRequestId,
        /// The error that occurred.
        error: request_response::InboundFailure,
    },

    /// A response to an inbound request has been sent.
    ///
    /// When this event is received, the response has been flushed on
    /// the underlying transport connection.
    ResponseSent {
        /// The peer to whom the response was sent.
        peer: PeerId,
        /// The connection used.
        connection_id: ConnectionId,
        /// The ID of the inbound request whose response was sent.
        request_id: request_response::InboundRequestId,
    },
}

impl Event {
    fn from_swarm_resp(
        resp: SwarmResponse,
        peer: PeerId,
        connection_id: Option<ConnectionId>,
        request_id: RequestId,
    ) -> Self {
        match resp {
            SwarmResponse::Ask(result) => Event::AskResult {
                peer,
                connection_id,
                request_id,
                result,
            },
            SwarmResponse::Tell(result) => Event::TellResult {
                peer,
                connection_id,
                request_id,
                result,
            },
            SwarmResponse::Link(result) => Event::LinkResult {
                peer,
                connection_id,
                request_id,
                result,
            },
            SwarmResponse::Unlink(result) => Event::UnlinkResult {
                peer,
                connection_id,
                request_id,
                result,
            },
            SwarmResponse::SignalLinkDied(result) => Event::SignalLinkDiedResult {
                peer,
                connection_id,
                request_id,
                result,
            },
            SwarmResponse::OutboundFailure(error) => Event::OutboundFailure {
                peer,
                connection_id: connection_id.unwrap(),
                request_id: request_id.unwrap_outbound(),
                error,
            },
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
        actor_id: ActorId,
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
        actor_id: ActorId,
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
    /// A request to link two actors together.
    Link {
        /// Actor ID.
        actor_id: ActorId,
        /// Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Sibbling ID.
        sibbling_id: ActorId,
        /// Sibbling remote ID.
        sibbling_remote_id: Cow<'static, str>,
    },
    /// A request to unlink two actors.
    Unlink {
        /// Actor ID.
        actor_id: ActorId,
        /// Actor remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Sibbling ID.
        sibbling_id: ActorId,
    },
    /// A signal notifying a linked actor has died.
    SignalLinkDied {
        /// The actor which died.
        dead_actor_id: ActorId,
        /// The actor to notify.
        notified_actor_id: ActorId,
        /// The actor to notify.
        notified_actor_remote_id: Cow<'static, str>,
        /// The reason the actor died.
        stop_reason: ActorStopReason,
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
    Tell(Result<(), RemoteSendError>),

    /// Represents the response to a link request.
    Link(Result<(), RemoteSendError>),

    /// Represents the response to a link request.
    Unlink(Result<(), RemoteSendError>),

    /// Represents the response to a link died signal.
    SignalLinkDied(Result<(), RemoteSendError>),

    /// Represents a failure that occurred while attempting to send an outbound request.
    ///
    /// Contains the error that caused the outbound request to fail.
    OutboundFailure(RemoteSendError),
}

impl NetworkBehaviour for Behaviour
where
    kad::Behaviour<kad::store::MemoryStore>: NetworkBehaviour,
    request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>: NetworkBehaviour,
{
    type ConnectionHandler = ConnectionHandlerSelect<
        THandler<kad::Behaviour<kad::store::MemoryStore>>,
        THandler<request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>>,
    >;
    type ToSwarm = Event;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        NetworkBehaviour::handle_pending_inbound_connection(
            &mut self.kademlia,
            connection_id,
            local_addr,
            remote_addr,
        )?;

        NetworkBehaviour::handle_pending_inbound_connection(
            &mut self.request_response,
            connection_id,
            local_addr,
            remote_addr,
        )?;

        Ok(())
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(ConnectionHandler::select(
            self.kademlia.handle_established_inbound_connection(
                connection_id,
                peer,
                local_addr,
                remote_addr,
            )?,
            self.request_response
                .handle_established_inbound_connection(
                    connection_id,
                    peer,
                    local_addr,
                    remote_addr,
                )?,
        ))
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        let mut combined_addresses = Vec::new();

        combined_addresses.extend(NetworkBehaviour::handle_pending_outbound_connection(
            &mut self.kademlia,
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )?);

        combined_addresses.extend(NetworkBehaviour::handle_pending_outbound_connection(
            &mut self.request_response,
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )?);

        Ok(combined_addresses)
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(ConnectionHandler::select(
            self.kademlia.handle_established_outbound_connection(
                connection_id,
                peer,
                addr,
                role_override,
                port_use,
            )?,
            self.request_response
                .handle_established_outbound_connection(
                    connection_id,
                    peer,
                    addr,
                    role_override,
                    port_use,
                )?,
        ))
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {
            Either::Left(ev) => NetworkBehaviour::on_connection_handler_event(
                &mut self.kademlia,
                peer_id,
                connection_id,
                ev,
            ),
            Either::Right(ev) => NetworkBehaviour::on_connection_handler_event(
                &mut self.request_response,
                peer_id,
                connection_id,
                ev,
            ),
        }
    }

    fn poll(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // 1. Process buffered events first (highest priority)
        if let Some(ev) = self.pending_events.pop_front() {
            return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
        }

        // 2. Handle completed async work (task results)
        match self.join_set.poll_join_next(cx) {
            task::Poll::Ready(Some(Ok((ch, res)))) => match ch {
                ReplyChannel::Event(request_id) => {
                    return task::Poll::Ready(ToSwarm::GenerateEvent(Event::from_swarm_resp(
                        res,
                        self.local_peer_id,
                        None,
                        request_id,
                    )));
                }
                ReplyChannel::Local(tx) => {
                    let _ = tx.send(res);
                }
                ReplyChannel::Remote(ch) => {
                    let _ = self.request_response.send_response(ch, res);
                    cx.waker().wake_by_ref();
                }
            },
            task::Poll::Ready(Some(Err(err))) => {
                panic!("ask request futures should never fail: {err}");
            }
            task::Poll::Ready(None) => {}
            task::Poll::Pending => {}
        }

        // 3. Process external commands (user-initiated actions)
        match self.cmd_rx.poll_recv(cx) {
            task::Poll::Ready(Some(cmd)) => {
                if self.handle_command(cmd) {
                    cx.waker().wake_by_ref();
                }
            }
            task::Poll::Ready(None) => {}
            task::Poll::Pending => {}
        }

        // 4. Handle direct request/response (peer-to-peer communication)
        match NetworkBehaviour::poll(&mut self.request_response, cx) {
            task::Poll::Ready(ToSwarm::GenerateEvent(ev)) => {
                let (wake, ev) = self.handle_request_response_event(ev);
                if wake {
                    cx.waker().wake_by_ref();
                }
                if let Some(ev) = ev {
                    return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
                }
            }
            task::Poll::Ready(other_ev) => {
                return task::Poll::Ready(
                    other_ev
                        .map_out(|_| unreachable!("we handled GenerateEvent above"))
                        .map_in(Either::Right),
                );
            }
            task::Poll::Pending => {}
        }

        // 5. Handle Kademlia last (background DHT maintenance)
        match NetworkBehaviour::poll(&mut self.kademlia, cx) {
            task::Poll::Ready(ev) => {
                if let ToSwarm::GenerateEvent(ev) = &ev {
                    if self.handle_kademlia_event(ev) || !self.pending_events.is_empty() {
                        cx.waker().wake_by_ref();
                    }
                }

                return task::Poll::Ready(ev.map_out(Event::Kademlia).map_in(Either::Left));
            }
            task::Poll::Pending => {}
        }

        task::Poll::Pending
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        if let FromSwarm::ConnectionClosed(ConnectionClosed { peer_id, .. }) = &event {
            let peer_id = *peer_id;
            tokio::spawn(async move {
                let mut futures = FuturesUnordered::new();
                for RemoteRegistryActorRef {
                    signal_mailbox,
                    links,
                    ..
                } in REMOTE_REGISTRY.lock().await.values()
                {
                    for linked_actor_id in (*links.lock().await).keys() {
                        if linked_actor_id.peer_id() == Some(&peer_id) {
                            let signal_mailbox = signal_mailbox.clone();
                            let linked_actor_id = *linked_actor_id;
                            futures.push(async move {
                                signal_mailbox
                                    .signal_link_died(
                                        linked_actor_id,
                                        ActorStopReason::PeerDisconnected,
                                    )
                                    .await
                            });
                        }
                    }
                }

                while (futures.next().await).is_some() {}
            });
        }
        self.kademlia.on_swarm_event(event);
        self.request_response.on_swarm_event(event);
    }
}
