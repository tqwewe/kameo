use core::task;
use std::{
    borrow::Cow,
    marker::PhantomData,
    pin, str,
    sync::{Arc, OnceLock},
    task::Poll,
    time::Duration,
};

use futures::{Future, FutureExt, Stream, StreamExt, ready};
use libp2p::PeerId;
use tokio::sync::{mpsc, oneshot};

use crate::{
    Actor,
    actor::{ActorId, ActorRef, RemoteActorRef},
    error::{ActorStopReason, Infallible, RegistryError, RemoteSendError},
};

use super::{
    DowncastRegsiteredActorRefError, REMOTE_REGISTRY, RemoteActor, RemoteRegistryActorRef,
    messaging::SwarmResponse,
    registry::{
        ActorRegistration, LookupLocalReply, LookupReply, LookupResult, RegisterReply,
        UnregisterReply,
    },
};

static ACTOR_SWARM: OnceLock<ActorSwarm> = OnceLock::new();

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
/// The `ActorSwarm` is the essential component for enabling distributed actor communication
/// and message passing across decentralized nodes.
#[derive(Clone, Debug)]
pub(crate) struct ActorSwarm {
    swarm_tx: SwarmSender,
    local_peer_id: PeerId,
}

impl ActorSwarm {
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

    pub(crate) fn set(
        swarm_tx: mpsc::UnboundedSender<SwarmCommand>,
        local_peer_id: PeerId,
    ) -> Result<(), Self> {
        ACTOR_SWARM.set(ActorSwarm {
            swarm_tx: SwarmSender(swarm_tx),
            local_peer_id,
        })
    }

    /// Returns the local peer ID, which uniquely identifies this node in the libp2p network.
    ///
    /// ## Returns
    /// A reference to the local `PeerId`.
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Looks up an actor running locally.
    pub(crate) fn lookup_local<A: Actor + RemoteActor + 'static>(
        &self,
        name: Arc<str>,
    ) -> impl Future<Output = Result<Option<ActorRef<A>>, RegistryError>> {
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::LookupLocal { name, reply });

        async move {
            let Some(ActorRegistration {
                actor_id,
                remote_id,
            }) = reply_rx.await?
            else {
                return Ok(None);
            };
            if A::REMOTE_ID != remote_id {
                return Err(RegistryError::BadActorType);
            }

            let registry = REMOTE_REGISTRY.lock().await;
            let Some(actor_ref_any) = registry.get(&actor_id) else {
                return Ok(None);
            };
            match actor_ref_any.downcast() {
                Ok(actor_ref) => Ok(Some(actor_ref)),
                Err(DowncastRegsiteredActorRefError::BadActorType) => {
                    Err(RegistryError::BadActorType)
                }
                Err(DowncastRegsiteredActorRefError::ActorNotRunning) => Ok(None),
            }
        }
    }

    /// Looks up an actor in the swarm.
    pub(crate) async fn lookup<A: Actor + RemoteActor>(
        &self,
        name: Arc<str>,
    ) -> Result<Option<RemoteActorRef<A>>, RegistryError> {
        #[cfg(all(debug_assertions, feature = "tracing"))]
        let name_clone = name.clone();
        let mut stream = self.lookup_all(name);

        let first = stream.next().await.transpose()?;

        #[cfg(all(debug_assertions, feature = "tracing"))]
        if first.is_some() {
            tokio::spawn(async move {
                // Check if there's a second actor
                if let Ok(Some(_)) = stream.next().await.transpose() {
                    tracing::warn!(
                        "Multiple actors found for '{name_clone}'. Consider using lookup_all() for deterministic behavior when multiple actors may exist."
                    );
                }
            });
        }

        Ok(first)
    }

    /// Looks up all actors with a given name in the swarm.
    pub(crate) fn lookup_all<A: Actor + RemoteActor>(&self, name: Arc<str>) -> LookupStream<A> {
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        let cmd = SwarmCommand::Lookup {
            name,
            reply: reply_tx,
        };
        self.swarm_tx.send(cmd);

        let swarm_tx = self.swarm_tx.clone();
        LookupStream::new(swarm_tx, reply_rx)
    }

    /// Registers an actor within the swarm.
    pub(crate) fn register<A: Actor + RemoteActor + 'static>(
        &self,
        actor_ref: ActorRef<A>,
        name: Arc<str>,
    ) -> impl Future<Output = Result<(), RegistryError>> {
        let registration = ActorRegistration::new(actor_ref.id(), Cow::Borrowed(A::REMOTE_ID));

        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::Register {
                name: name.clone(),
                registration,
                reply,
            });

        async move {
            let res = reply_rx.await;
            match res {
                Ok(()) | Err(RegistryError::QuorumFailed { .. }) => {
                    REMOTE_REGISTRY.lock().await.insert(
                        actor_ref.id(),
                        RemoteRegistryActorRef::new(actor_ref, Some(name)),
                    );

                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
    }

    /// Unregisters an actor within the swarm.
    ///
    /// The future returned by unregister does not have to be awaited.
    /// Awaiting it is only necessary to handle the result.
    pub fn unregister(&self, name: Arc<str>) -> impl Future<Output = ()> {
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::Unregister { name, reply });

        async move {
            reply_rx.await;
        }
    }

    pub(crate) fn link<A: Actor + RemoteActor, B: Actor + RemoteActor>(
        &self,
        actor_id: ActorId,
        sibling_id: ActorId,
    ) -> impl Future<Output = Result<(), RemoteSendError<Infallible>>> {
        let reply_rx = self.swarm_tx.send_with_reply(|reply| SwarmCommand::Link {
            actor_id,
            actor_remote_id: Cow::Borrowed(A::REMOTE_ID),
            sibling_id,
            sibling_remote_id: Cow::Borrowed(B::REMOTE_ID),
            reply,
        });

        async move {
            match reply_rx.await {
                SwarmResponse::Link(result) => result,
                SwarmResponse::OutboundFailure(err) => Err(err),
                _ => panic!("got an unexpected swarm response"),
            }
        }
    }

    pub(crate) fn unlink<B: Actor + RemoteActor>(
        &self,
        actor_id: ActorId,
        sibling_id: ActorId,
    ) -> impl Future<Output = Result<(), RemoteSendError<Infallible>>> {
        let reply_rx = self.swarm_tx.send_with_reply(|reply| SwarmCommand::Unlink {
            actor_id,
            sibling_id,
            sibling_remote_id: Cow::Borrowed(B::REMOTE_ID),
            reply,
        });

        async move {
            match reply_rx.await {
                SwarmResponse::Unlink(result) => result,
                SwarmResponse::OutboundFailure(err) => Err(err),
                _ => panic!("got an unexpected swarm response"),
            }
        }
    }

    pub(crate) fn signal_link_died(
        &self,
        dead_actor_id: ActorId,
        notified_actor_id: ActorId,
        notified_actor_remote_id: Cow<'static, str>,
        stop_reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), RemoteSendError<Infallible>>> {
        let reply_rx = self
            .swarm_tx
            .send_with_reply(|reply| SwarmCommand::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
                reply,
            });

        async move {
            match reply_rx.await {
                SwarmResponse::SignalLinkDied(result) => result,
                SwarmResponse::OutboundFailure(err) => Err(err),
                _ => panic!("got an unexpected swarm response"),
            }
        }
    }

    pub(crate) fn sender(&self) -> &SwarmSender {
        &self.swarm_tx
    }
}

/// A stream of remote actor references discovered during distributed lookup.
///
/// This stream yields [`RemoteActorRef<A>`] instances as they are discovered across
/// the network. The stream completes when all known actors matching the lookup
/// name have been found.
///
/// # Errors
///
/// Individual stream items may be errors if specific actors cannot be reached
/// or validated during lookup.
///
/// # Example
///
/// ```rust,no_run
/// # use kameo::{Actor, RemoteActor, actor::RemoteActorRef};
/// # use futures::TryStreamExt;
/// #
/// # #[derive(Actor, RemoteActor)]
/// # struct MyActor;
/// #
/// # tokio_test::block_on(async {
/// let mut stream = RemoteActorRef::<MyActor>::lookup_all("my-service");
/// while let Some(actor_ref) = stream.try_next().await? {
///     // Handle each discovered actor
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// ```
///
/// [`RemoteActorRef<A>`]: crate::actor::RemoteActorRef
#[derive(Debug)]
pub struct LookupStream<A> {
    inner: LookupStreamInner,
    _phantom: PhantomData<fn() -> A>,
}

impl<A> LookupStream<A> {
    fn new(swarm_tx: SwarmSender, reply_rx: mpsc::UnboundedReceiver<LookupResult>) -> Self {
        LookupStream {
            inner: LookupStreamInner::Stream { swarm_tx, reply_rx },
            _phantom: PhantomData,
        }
    }

    pub(crate) fn new_err() -> Self {
        LookupStream {
            inner: LookupStreamInner::SwarmNotBootstrapped { done: false },
            _phantom: PhantomData,
        }
    }
}

#[derive(Debug)]
enum LookupStreamInner {
    SwarmNotBootstrapped {
        done: bool,
    },
    Stream {
        swarm_tx: SwarmSender,
        reply_rx: mpsc::UnboundedReceiver<Result<ActorRegistration<'static>, RegistryError>>,
    },
}

impl<A: Actor + RemoteActor> Stream for LookupStream<A> {
    type Item = Result<RemoteActorRef<A>, RegistryError>;

    fn poll_next(
        self: pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.inner {
            LookupStreamInner::SwarmNotBootstrapped { done } => {
                if *done {
                    Poll::Ready(None)
                } else {
                    *done = true;
                    Poll::Ready(Some(Err(RegistryError::SwarmNotBootstrapped)))
                }
            }
            LookupStreamInner::Stream { swarm_tx, reply_rx } => {
                match ready!(reply_rx.poll_recv(cx)) {
                    Some(Ok(registration)) => {
                        if A::REMOTE_ID != registration.remote_id {
                            Poll::Ready(Some(Err(RegistryError::BadActorType)))
                        } else {
                            Poll::Ready(Some(Ok(RemoteActorRef::new(
                                registration.actor_id,
                                swarm_tx.clone(),
                            ))))
                        }
                    }
                    Some(Err(err)) => Poll::Ready(Some(Err(err))),
                    None => Poll::Ready(None),
                }
            }
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

    fn send_with_reply<T>(
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
pub(crate) enum SwarmCommand {
    /// Lookup providers for an actor by name in the kademlia network.
    Lookup {
        /// Registered name.
        name: Arc<str>,
        /// Reply sender.
        reply: LookupReply,
    },
    /// Lookup an actor by name on the local node only.
    LookupLocal {
        /// Actor name.
        name: Arc<str>,
        /// Reply sender.
        reply: LookupLocalReply,
    },
    /// Register an actor under a name.
    Register {
        /// Actor name.
        name: Arc<str>,
        /// Registration information.
        registration: ActorRegistration<'static>,
        /// Reply sender.
        reply: RegisterReply,
    },
    /// Stop providing a key.
    Unregister {
        /// Actor name.
        name: Arc<str>,
        /// Reply sender.
        reply: UnregisterReply,
    },
    /// An actor ask request.
    Ask {
        /// Actor ID.
        actor_id: ActorId,
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
        /// Actor ID.
        actor_id: ActorId,
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
        reply: Option<oneshot::Sender<SwarmResponse>>,
    },
    /// An actor link request.
    Link {
        /// Actor A ID.
        actor_id: ActorId,
        /// Actor A remote ID.
        actor_remote_id: Cow<'static, str>,
        /// Actor B ID.
        sibling_id: ActorId,
        /// Actor B remote ID.
        sibling_remote_id: Cow<'static, str>,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
    /// An actor unlink request.
    Unlink {
        /// Actor A ID.
        actor_id: ActorId,
        /// Actor B ID.
        sibling_id: ActorId,
        /// Actor B remote ID.
        sibling_remote_id: Cow<'static, str>,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
    /// Notifies a linked actor has died.
    SignalLinkDied {
        /// The actor which died.
        dead_actor_id: ActorId,
        /// The actor to notify.
        notified_actor_id: ActorId,
        /// Actor remote iD
        notified_actor_remote_id: Cow<'static, str>,
        /// The reason the actor died.
        stop_reason: ActorStopReason,
        /// Reply sender.
        reply: oneshot::Sender<SwarmResponse>,
    },
}

/// `SwarmFuture` represents a future that contains the response from a remote actor.
///
/// This future is returned when sending a message to a remote actor via the actor swarm.
/// If the response is not needed, the future can simply be dropped without awaiting it.
#[derive(Debug)]
struct SwarmFuture<T>(oneshot::Receiver<T>);

impl<T> Future for SwarmFuture<T> {
    type Output = T;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(
            ready!(self.0.poll_unpin(cx))
                .expect("the oneshot sender should never be dropped before being sent to"),
        )
    }
}
