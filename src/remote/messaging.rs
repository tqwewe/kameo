//! Remote message passing infrastructure for actors across the network.
//!
//! This module provides the core messaging capabilities that enable actors running on different
//! nodes to communicate with each other seamlessly. It handles the serialization, routing, and
//! delivery of messages between remote actors while maintaining the same ergonomics as local
//! actor communication.
//!
//! # Key Responsibilities
//!
//! - **Message Serialization**: Automatically serializes and deserializes actor messages for
//!   network transmission using efficient binary protocols
//! - **Request-Response Communication**: Implements reliable ask/tell patterns for remote actors
//!   with configurable timeouts and delivery guarantees  
//! - **Connection Management**: Manages libp2p connections and handles connection failures,
//!   retries, and peer disconnections gracefully
//! - **Actor Lifecycle Integration**: Supports remote actor linking, unlinking, and death
//!   notifications across network boundaries
//! - **Backpressure Handling**: Provides mailbox timeout controls to prevent overwhelming
//!   remote actors with too many concurrent messages
//!
//! # Architecture
//!
//! The messaging system is built on top of libp2p's request-response protocol, providing
//! reliable delivery semantics while maintaining the actor model's message-passing paradigm.
//! Messages are automatically routed to the appropriate peer based on the target actor's
//! peer ID, with transparent handling of network-level concerns.
//!
//! The module integrates closely with Kameo's local actor system, allowing remote actors
//! to be used interchangeably with local actors through the same `ActorRef` interface.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    sync::LazyLock,
    task,
    time::Duration,
};

use futures::FutureExt;
use libp2p::{
    request_response,
    swarm::{
        ConnectionDenied, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    PeerId, StreamProtocol,
};
// Removed serde - using rkyv for zero-copy serialization
use tokio::{sync::oneshot, task::JoinSet};

use crate::{
    actor::ActorId,
    error::{ActorStopReason, Infallible, RemoteSendError},
};

use super::_internal::{
    RemoteActorFns, RemoteMessageFns, RemoteMessageRegistrationID, REMOTE_ACTORS, REMOTE_MESSAGES,
};

const PROTO_NAME: StreamProtocol = StreamProtocol::new("/kameo/messaging/1.0.0");

static REMOTE_ACTORS_MAP: LazyLock<HashMap<&'static str, RemoteActorFns>> = LazyLock::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_ACTORS {
        if !existing_ids.insert(id) {
            panic!("duplicate remote actor detected for actor '{id}'");
        }
    }
    REMOTE_ACTORS.iter().copied().collect()
});

static REMOTE_MESSAGES_MAP: LazyLock<
    HashMap<RemoteMessageRegistrationID<'static>, RemoteMessageFns>,
> = LazyLock::new(|| {
    let mut existing_ids = HashSet::new();
    for (id, _) in REMOTE_MESSAGES {
        if !existing_ids.insert(id) {
            panic!(
                "duplicate remote message detected for actor '{}' and message '{}'",
                id.actor_remote_id, id.message_remote_id
            );
        }
    }
    REMOTE_MESSAGES.iter().copied().collect()
});

type AskResult = Result<Vec<u8>, RemoteSendError<Vec<u8>>>;
type TellResult = Result<(), RemoteSendError>;
type LinkResult = Result<(), RemoteSendError>;
type UnlinkResult = Result<(), RemoteSendError>;
type SignalLinkDiedResult = Result<(), RemoteSendError>;

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

/// The events produced by the `Messaging` behaviour.
///
/// See [`NetworkBehaviour::poll`].
#[derive(Debug)]
pub enum Event {
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

/// The configuration for a `messaging::Behaviour` protocol.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    request_timeout: Duration,
    max_concurrent_streams: usize,
    request_size_maximum: u64,
    response_size_maximum: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            max_concurrent_streams: 100,
            request_size_maximum: 1024 * 1024,
            response_size_maximum: 10 * 1024 * 1024,
        }
    }
}

impl Config {
    /// Sets the timeout for inbound and outbound requests.
    pub fn with_request_timeout(mut self, v: Duration) -> Self {
        self.request_timeout = v;
        self
    }

    /// Sets the upper bound for the number of concurrent inbound + outbound streams.
    pub fn with_max_concurrent_streams(mut self, num_streams: usize) -> Self {
        self.max_concurrent_streams = num_streams;
        self
    }

    /// Sets the limit for request size in bytes.
    pub fn with_request_size_maximum(mut self, bytes: u64) -> Self {
        self.request_size_maximum = bytes;
        self
    }

    /// Sets the limit for response size in bytes.
    pub fn with_response_size_maximum(mut self, bytes: u64) -> Self {
        self.response_size_maximum = bytes;
        self
    }
}

impl From<Config> for request_response::Config {
    fn from(config: Config) -> Self {
        request_response::Config::default()
            .with_request_timeout(config.request_timeout)
            .with_max_concurrent_streams(config.max_concurrent_streams)
    }
}

impl<Req, Resp> From<Config> for request_response::cbor::codec::Codec<Req, Resp> {
    fn from(config: Config) -> Self {
        request_response::cbor::codec::Codec::default()
            .set_request_size_maximum(config.request_size_maximum)
            .set_response_size_maximum(config.response_size_maximum)
    }
}

/// `Behaviour` is a `NetworkBehaviour` that implements the kameo messaging behaviour
/// on top of the request response protocol.
#[allow(missing_debug_implementations)]
pub struct Behaviour {
    request_response: request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>,
    local_peer_id: PeerId,
    next_id: u64,
    requests: HashMap<RequestId, (PeerId, Option<oneshot::Sender<SwarmResponse>>)>,
    join_set: JoinSet<(ReplyChannel, SwarmResponse)>,
}

impl Behaviour {
    /// Creates a new messaging behaviour.
    pub fn new(local_peer_id: PeerId, config: Config) -> Self {
        let request_response = request_response::cbor::Behaviour::with_codec(
            config.into(),
            [(PROTO_NAME, request_response::ProtocolSupport::Full)],
            config.into(),
        );

        Behaviour {
            request_response,
            local_peer_id,
            next_id: 0,
            requests: HashMap::new(),
            join_set: JoinSet::new(),
        }
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn ask_with_reply(
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
                ask(
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tell_with_reply(
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
                tell(
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

    pub(super) fn link_with_reply(
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
                link(actor_id, actor_remote_id, sibbling_id, sibbling_remote_id)
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

    pub(super) fn unlink_with_reply(
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
                unlink(actor_id, actor_remote_id, sibbling_id).map(SwarmResponse::Unlink)
            },
            move |(actor_id, actor_remote_id, sibbling_id)| SwarmRequest::Unlink {
                actor_id,
                actor_remote_id,
                sibbling_id,
            },
        )
    }

    pub(super) fn signal_link_died_with_reply(
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
                signal_link_died(
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
            self.requests.insert(request_id, (*peer_id, reply));

            Some(request_id)
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
                Some((_, Some(tx))) => {
                    let _ = tx.send(SwarmResponse::OutboundFailure(error.into()));
                    (false, None)
                }
                Some((_, None)) | None => (
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
                    ask(
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
                    tell(
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
                    link(actor_id, actor_remote_id, sibbling_id, sibbling_remote_id)
                        .map(|res| (channel, SwarmResponse::Link(res))),
                );
            }
            SwarmRequest::Unlink {
                actor_id,
                actor_remote_id,
                sibbling_id,
            } => {
                self.join_set.spawn(
                    unlink(actor_id, actor_remote_id, sibbling_id)
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
                    signal_link_died(
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
            Some((_, Some(tx))) => {
                // Reply to channel
                let _ = tx.send(res);
                None
            }
            Some((_, None)) => {
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

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler =
        THandler<request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>>;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: libp2p::PeerId,
        local_addr: &libp2p::Multiaddr,
        remote_addr: &libp2p::Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.request_response.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: libp2p::PeerId,
        addr: &libp2p::Multiaddr,
        role_override: libp2p::core::Endpoint,
        port_use: libp2p::core::transport::PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.request_response
            .handle_established_outbound_connection(
                connection_id,
                peer,
                addr,
                role_override,
                port_use,
            )
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        if let FromSwarm::DialFailure(DialFailure {
            peer_id: Some(peer_id),
            ..
        }) = event
        {
            // Remove requests for this peer id
            #[cfg_attr(feature = "remote", allow(clippy::incompatible_msrv))]
            let dead_requests = self
                .requests
                .extract_if(|_, (req_peer_id, _)| req_peer_id == &peer_id);
            for (_, (_, reply)) in dead_requests {
                if let Some(tx) = reply {
                    let _ = tx.send(SwarmResponse::OutboundFailure(RemoteSendError::DialFailure));
                }
            }
        }

        self.request_response.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.request_response
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn poll(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        loop {
            // First, check for completed futures from join_set
            match self.join_set.poll_join_next(cx) {
                task::Poll::Ready(Some(Ok((ch, res)))) => {
                    match ch {
                        ReplyChannel::Event(request_id) => {
                            // We have an event to return immediately
                            return task::Poll::Ready(ToSwarm::GenerateEvent(
                                Event::from_swarm_resp(res, self.local_peer_id, None, request_id),
                            ));
                        }
                        ReplyChannel::Local(tx) => {
                            let _ = tx.send(res);
                            // Continue loop - no event to return but might have more work
                            continue;
                        }
                        ReplyChannel::Remote(ch) => {
                            let _ = self.request_response.send_response(ch, res);
                            // Continue loop - sending response might trigger more request_response events
                            continue;
                        }
                    }
                }
                task::Poll::Ready(Some(Err(err))) => {
                    panic!("ask request futures should never fail: {err}");
                }
                task::Poll::Ready(None) => {
                    // No completed futures, continue to poll request_response
                }
                task::Poll::Pending => {
                    // No completed futures ready, continue to poll request_response
                }
            }

            // Poll request_response for events
            match self.request_response.poll(cx) {
                task::Poll::Ready(ToSwarm::GenerateEvent(ev)) => {
                    let (wake, ev) = self.handle_request_response_event(ev);

                    if let Some(ev) = ev {
                        // We have an event to return
                        if wake {
                            cx.waker().wake_by_ref();
                        }
                        return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
                    }

                    // No event to return, but if wake was requested, continue loop
                    // to check for more request_response events or join_set completions
                    if wake {
                        continue;
                    }

                    // No event and no wake needed, continue loop to check for more
                    // request_response events (in case it has more queued)
                    continue;
                }
                task::Poll::Ready(other_ev) => {
                    // Non-GenerateEvent from request_response
                    return task::Poll::Ready(
                        other_ev.map_out(|_| unreachable!("we handled GenerateEvent above")),
                    );
                }
                task::Poll::Pending => {
                    // request_response has no more work ready
                    // Check one more time if join_set has completions
                    // (in case something completed while we were processing request_response events)
                    match self.join_set.poll_join_next(cx) {
                        task::Poll::Ready(Some(Ok((ch, res)))) => match ch {
                            ReplyChannel::Event(request_id) => {
                                return task::Poll::Ready(ToSwarm::GenerateEvent(
                                    Event::from_swarm_resp(
                                        res,
                                        self.local_peer_id,
                                        None,
                                        request_id,
                                    ),
                                ));
                            }
                            ReplyChannel::Local(tx) => {
                                let _ = tx.send(res);
                                continue; // Might have triggered more work
                            }
                            ReplyChannel::Remote(ch) => {
                                let _ = self.request_response.send_response(ch, res);
                                continue; // Might have triggered more request_response events
                            }
                        },
                        task::Poll::Ready(Some(Err(err))) => {
                            panic!("ask request futures should never fail: {err}");
                        }
                        task::Poll::Ready(None) | task::Poll::Pending => {
                            // Nothing more ready from either source
                            return task::Poll::Pending;
                        }
                    }
                }
            }
        }
    }
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

async fn ask(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    message_remote_id: Cow<'static, str>,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    reply_timeout: Option<Duration>,
    immediate: bool,
) -> Result<Vec<u8>, RemoteSendError<Vec<u8>>> {
    let Some(fns) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    if immediate {
        (fns.try_ask)(actor_id, payload, reply_timeout).await
    } else {
        (fns.ask)(actor_id, payload, mailbox_timeout, reply_timeout).await
    }
}

async fn tell(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    message_remote_id: Cow<'static, str>,
    payload: Vec<u8>,
    mailbox_timeout: Option<Duration>,
    immediate: bool,
) -> Result<(), RemoteSendError> {
    let Some(fns) = REMOTE_MESSAGES_MAP.get(&RemoteMessageRegistrationID {
        actor_remote_id: &actor_remote_id,
        message_remote_id: &message_remote_id,
    }) else {
        return Err(RemoteSendError::UnknownMessage {
            actor_remote_id,
            message_remote_id,
        });
    };
    if immediate {
        (fns.try_tell)(actor_id, payload).await
    } else {
        (fns.tell)(actor_id, payload, mailbox_timeout).await
    }
}

async fn link(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    sibbling_id: ActorId,
    sibbling_remote_id: Cow<'static, str>,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor { actor_remote_id });
    };

    (fns.link)(actor_id, sibbling_id, sibbling_remote_id).await
}

async fn unlink(
    actor_id: ActorId,
    actor_remote_id: Cow<'static, str>,
    sibbling_id: ActorId,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor { actor_remote_id });
    };

    (fns.unlink)(actor_id, sibbling_id).await
}

async fn signal_link_died(
    dead_actor_id: ActorId,
    notified_actor_id: ActorId,
    notified_actor_remote_id: Cow<'static, str>,
    stop_reason: ActorStopReason,
) -> Result<(), RemoteSendError<Infallible>> {
    let Some(fns) = REMOTE_ACTORS_MAP.get(&*notified_actor_remote_id) else {
        return Err(RemoteSendError::UnknownActor {
            actor_remote_id: notified_actor_remote_id,
        });
    };

    (fns.signal_link_died)(dead_actor_id, notified_actor_id, stop_reason).await
}
