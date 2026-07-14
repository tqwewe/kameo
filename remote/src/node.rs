//! The remote node: gossip membership, the actor registry, and the messaging server.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chitchat::{
    Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat,
    transport::UdpTransport,
};
use futures::Stream;
use kameo::actor::ActorRef;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{
    dispatch::DispatchTable,
    error::{BootstrapError, RegistryError, ShutdownError},
    gossip::EncryptedUdpTransport,
    id::NodeId,
    messaging::transport::{ConnectionPool, ServerContext, run_server},
    registry::{self, RegistrationValue},
    remote_actor::{RemoteActor, RemoteMessages},
    remote_ref::RemoteActorRef,
    security::{ClusterKey, ConnSecurity},
};

/// Configuration for a [`RemoteNode`].
pub struct RemoteNodeConfig {
    /// Cluster name; nodes only gossip within the same cluster id. Default: `"kameo"`.
    pub cluster_id: String,
    /// Stable node name, unique across the cluster. Default: generated `node-<uuid>`.
    pub node_name: Option<String>,
    /// UDP gossip bind address. Default: `0.0.0.0:7400`. Port 0 binds an ephemeral port.
    pub gossip_listen_addr: SocketAddr,
    /// Address other nodes gossip to. Default: the resolved gossip listen address.
    pub gossip_advertise_addr: Option<SocketAddr>,
    /// TCP messaging bind address. Default: `0.0.0.0:7401`. Port 0 binds an ephemeral port.
    pub messaging_listen_addr: SocketAddr,
    /// Address other nodes connect to for messaging. Default: the messaging listen
    /// address, with an unspecified IP replaced by the gossip advertise IP.
    pub messaging_advertise_addr: Option<SocketAddr>,
    /// Gossip addresses of seed nodes, as `"host:port"`. Hostnames are re-resolved
    /// periodically, supporting DNS-based discovery. Default: empty.
    pub seed_nodes: Vec<String>,
    /// Gossip round interval. Default: 500ms.
    pub gossip_interval: Duration,
    /// Failure detector tuning. Default: [`FailureDetectorConfig::default`].
    pub failure_detector_config: FailureDetectorConfig,
    /// Grace period before deleted registry keys are garbage collected. Default: 1h.
    pub marked_for_deletion_grace_period: Duration,
    /// Timeout for establishing a ready outbound connection: TCP connect, TLS when
    /// enabled, and the connection handshake. Default: 10s.
    pub connect_timeout: Duration,
    /// Default ask reply timeout when not set per-request. Default: 30s.
    pub default_reply_timeout: Duration,
    /// Maximum TCP frame length. Default: 16 MiB.
    pub max_frame_len: usize,
    /// Maximum concurrently processed inbound requests per connection. When the limit
    /// is reached, further frames are not read, propagating backpressure over TCP.
    /// Default: 512.
    pub max_concurrent_requests: usize,
    /// Pre-shared cluster key: encrypts and authenticates gossip, and authenticates
    /// messaging connections. Nodes with a different key (or none) cannot join or
    /// message this node. Default: `None` (open cluster, trusted networks only).
    pub cluster_key: Option<ClusterKey>,
    /// Server-side timeout for a new connection to complete the handshake (including
    /// TLS when enabled). Default: 10s.
    pub handshake_timeout: Duration,
    /// Mutual TLS for the messaging layer: encrypts connections and authenticates
    /// both peers against the cluster CA. Default: `None` (plaintext).
    #[cfg(feature = "tls")]
    pub tls: Option<crate::tls::TlsConfig>,
}

impl Default for RemoteNodeConfig {
    fn default() -> Self {
        RemoteNodeConfig {
            cluster_id: "kameo".to_string(),
            node_name: None,
            gossip_listen_addr: ([0, 0, 0, 0], 7400).into(),
            gossip_advertise_addr: None,
            messaging_listen_addr: ([0, 0, 0, 0], 7401).into(),
            messaging_advertise_addr: None,
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_millis(500),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(3600),
            connect_timeout: Duration::from_secs(10),
            default_reply_timeout: Duration::from_secs(30),
            max_frame_len: 16 * 1024 * 1024,
            max_concurrent_requests: 512,
            cluster_key: None,
            handshake_timeout: Duration::from_secs(10),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

impl RemoteNodeConfig {
    /// Sets the cluster id.
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = cluster_id.into();
        self
    }

    /// Sets a stable node name.
    pub fn with_node_name(mut self, node_name: impl Into<String>) -> Self {
        self.node_name = Some(node_name.into());
        self
    }

    /// Sets the UDP gossip bind address.
    pub fn with_gossip_addr(mut self, addr: SocketAddr) -> Self {
        self.gossip_listen_addr = addr;
        self
    }

    /// Sets the TCP messaging bind address.
    pub fn with_messaging_addr(mut self, addr: SocketAddr) -> Self {
        self.messaging_listen_addr = addr;
        self
    }

    /// Sets the seed node gossip addresses.
    pub fn with_seed_nodes(
        mut self,
        seed_nodes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.seed_nodes = seed_nodes.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the pre-shared cluster key.
    pub fn with_cluster_key(mut self, cluster_key: impl Into<ClusterKey>) -> Self {
        self.cluster_key = Some(cluster_key.into());
        self
    }

    /// Sets the mutual TLS configuration for the messaging layer.
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, tls: crate::tls::TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

/// A node in a kameo cluster.
///
/// Bootstrapping a node joins the gossip cluster (via seed nodes) and starts the TCP
/// messaging server. The node is cheap to clone; all clones share the same state.
///
/// Call [`shutdown`](RemoteNode::shutdown) for a clean exit. Dropping the last clone
/// aborts the background tasks without gossiping a clean departure.
#[derive(Clone)]
pub struct RemoteNode {
    inner: Arc<RemoteNodeInner>,
}

pub(crate) struct RemoteNodeInner {
    node_id: NodeId,
    generation_id: u64,
    gossip_advertise_addr: SocketAddr,
    messaging_advertise_addr: SocketAddr,
    gossip_interval: Duration,
    chitchat: Arc<tokio::sync::Mutex<Chitchat>>,
    // Option because ChitchatHandle::shutdown consumes the handle.
    chitchat_handle: tokio::sync::Mutex<Option<ChitchatHandle>>,
    dispatch: Arc<DispatchTable>,
    pool: ConnectionPool,
    server_task: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
    shutdown: AtomicBool,
}

impl RemoteNode {
    /// Starts a node: binds the gossip and messaging listeners and joins the cluster.
    pub async fn bootstrap(config: RemoteNodeConfig) -> Result<Self, BootstrapError> {
        #[cfg(feature = "tls")]
        let tls = config.tls.clone();
        let RemoteNodeConfig {
            cluster_id,
            node_name,
            gossip_listen_addr,
            gossip_advertise_addr,
            messaging_listen_addr,
            messaging_advertise_addr,
            seed_nodes,
            gossip_interval,
            failure_detector_config,
            marked_for_deletion_grace_period,
            connect_timeout,
            default_reply_timeout,
            max_frame_len,
            max_concurrent_requests,
            cluster_key,
            handshake_timeout,
            ..
        } = config;

        #[cfg(feature = "tls")]
        if tls.is_some() && cluster_key.is_none() {
            tracing::warn!(
                "tls is enabled but no cluster_key is set; gossip is unauthenticated and \
                 the registry can be poisoned by anyone who can reach the gossip port"
            );
        }
        let (gossip_key, handshake_key) = match &cluster_key {
            Some(key) => {
                let (gossip_key, handshake_key) = crate::security::derive_keys(key);
                (Some(gossip_key), Some(handshake_key))
            }
            None => (None, None),
        };
        let security = Arc::new(ConnSecurity {
            cluster_id: cluster_id.clone(),
            handshake_key,
            #[cfg(feature = "tls")]
            tls,
        });

        let node_name =
            node_name.unwrap_or_else(|| format!("node-{}", uuid::Uuid::new_v4().simple()));
        // Milliseconds so fast restarts still get a strictly higher generation.
        let generation_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is before the unix epoch")
            .as_millis() as u64;

        // Chitchat needs the advertise address before it binds, so an ephemeral gossip
        // port is allocated by binding and dropping a socket first. This is best-effort
        // (another process could grab the port); prefer concrete ports in production.
        let gossip_listen_addr = if gossip_listen_addr.port() == 0 {
            let socket = std::net::UdpSocket::bind(gossip_listen_addr)?;
            socket.local_addr()?
        } else {
            gossip_listen_addr
        };
        let gossip_advertise_addr = gossip_advertise_addr.unwrap_or(gossip_listen_addr);

        let listener = TcpListener::bind(messaging_listen_addr).await?;
        let local_addr = listener.local_addr()?;
        let messaging_advertise_addr = messaging_advertise_addr.unwrap_or_else(|| {
            if local_addr.ip().is_unspecified() {
                SocketAddr::new(gossip_advertise_addr.ip(), local_addr.port())
            } else {
                local_addr
            }
        });

        // An unspecified advertise address is only usable by an isolated single node:
        // peers would gossip back or connect to 0.0.0.0 and never reach this node.
        if gossip_advertise_addr.ip().is_unspecified() {
            tracing::warn!(
                "gossip advertise address {gossip_advertise_addr} is unspecified; other nodes \
                 cannot reach this node, set gossip_advertise_addr or a concrete listen address"
            );
        } else if messaging_advertise_addr.ip().is_unspecified() {
            tracing::warn!(
                "messaging advertise address {messaging_advertise_addr} is unspecified; other \
                 nodes cannot message this node, set messaging_advertise_addr"
            );
        }

        let chitchat_config = ChitchatConfig {
            chitchat_id: ChitchatId::new(node_name.clone(), generation_id, gossip_advertise_addr),
            cluster_id,
            gossip_interval,
            listen_addr: gossip_listen_addr,
            seed_nodes,
            failure_detector_config,
            marked_for_deletion_grace_period,
            catchup_callback: None,
            extra_liveness_predicate: None,
        };
        let gossip_transport: Box<dyn chitchat::transport::Transport> = match gossip_key {
            Some(gossip_key) => Box::new(EncryptedUdpTransport::new(
                gossip_key,
                security.cluster_id.clone(),
            )),
            None => Box::new(UdpTransport),
        };
        let handle = spawn_chitchat(
            chitchat_config,
            vec![(
                "kameo:messaging_addr".to_string(),
                messaging_advertise_addr.to_string(),
            )],
            &*gossip_transport,
        )
        .await
        .map_err(BootstrapError::Chitchat)?;

        let chitchat = handle.chitchat();
        let dispatch = Arc::new(DispatchTable::new(generation_id));
        let pool = ConnectionPool::new(
            connect_timeout,
            default_reply_timeout,
            max_frame_len,
            security.clone(),
        );
        let cancel = CancellationToken::new();
        let server_task = tokio::spawn(run_server(
            listener,
            Arc::new(ServerContext {
                dispatch: dispatch.clone(),
                security,
                max_frame_len,
                max_concurrent_requests,
                handshake_timeout,
            }),
            cancel.child_token(),
        ));

        Ok(RemoteNode {
            inner: Arc::new(RemoteNodeInner {
                node_id: NodeId::from(node_name),
                generation_id,
                gossip_advertise_addr,
                messaging_advertise_addr,
                gossip_interval,
                chitchat,
                chitchat_handle: tokio::sync::Mutex::new(Some(handle)),
                dispatch,
                pool,
                server_task,
                cancel,
                shutdown: AtomicBool::new(false),
            }),
        })
    }

    /// Registers an actor under a name, making it visible to all nodes in the cluster.
    ///
    /// Multiple actors (on this or other nodes) may register the same name; lookups
    /// return the whole set. The registration is removed when it is explicitly
    /// deregistered, when the actor stops, or when this node shuts down or dies.
    ///
    /// The registry holds a strong reference to the actor, so a registered actor is
    /// kept alive even if all other [`ActorRef`]s are dropped, until one of the above
    /// removes the registration.
    pub async fn register<A: RemoteActor>(
        &self,
        actor_ref: &ActorRef<A>,
        name: impl Into<String>,
    ) -> Result<(), RegistryError> {
        self.check_running()?;
        let name = name.into();
        registry::validate_name(&name)?;
        let sequence_id = actor_ref.id().sequence_id();

        let mut messages = RemoteMessages::new(actor_ref.clone());
        A::remote_messages(&mut messages);
        self.inner.dispatch.insert(
            sequence_id,
            A::REMOTE_ID,
            name.clone(),
            messages.into_handlers(),
        );

        let value = RegistrationValue {
            actor_remote_id: A::REMOTE_ID.to_string(),
            messaging_addr: self.inner.messaging_advertise_addr,
        };
        let value = serde_json::to_string(&value).expect("registration value serializes");
        {
            let mut chitchat = self.inner.chitchat.lock().await;
            // Re-checked under the lock: shutdown deletes registry keys under this
            // lock after setting the flag, so a racing registration cannot slip in
            // after the deletion sweep and gossip out from a shut-down node.
            if self.inner.shutdown.load(Ordering::Relaxed) {
                drop(chitchat);
                self.inner.dispatch.remove_name(sequence_id, &name);
                return Err(RegistryError::NodeShutdown);
            }
            chitchat
                .self_node_state()
                .set(registry::actor_key(&name, sequence_id), value);
        }

        // Auto-deregister when the actor stops.
        let weak_actor = actor_ref.downgrade();
        let weak_inner = Arc::downgrade(&self.inner);
        let cancel = self.inner.cancel.child_token();
        tokio::spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = weak_actor.wait_for_shutdown() => {
                    if let Some(inner) = weak_inner.upgrade() {
                        inner.remove_registration(sequence_id, &name).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Removes a registration. Idempotent; deregistering an unknown name is a no-op.
    pub async fn deregister<A: RemoteActor>(
        &self,
        actor_ref: &ActorRef<A>,
        name: &str,
    ) -> Result<(), RegistryError> {
        self.check_running()?;
        registry::validate_name(name)?;
        self.inner
            .remove_registration(actor_ref.id().sequence_id(), name)
            .await;
        Ok(())
    }

    /// Looks up an arbitrary live actor registered under `name`.
    pub async fn lookup<A: RemoteActor>(
        &self,
        name: &str,
    ) -> Result<Option<RemoteActorRef<A>>, RegistryError> {
        let refs = self.lookup_all(name).await?;
        Ok(refs.into_iter().next())
    }

    /// Looks up all live actors registered under `name`, across all nodes.
    ///
    /// The result includes actors registered on this node; messages to them are
    /// dispatched in-process without touching the network (though still serialized).
    ///
    /// Registrations under `name` whose actor type is not `A` are skipped. If the name
    /// exists but only under other actor types, this fails with
    /// [`RegistryError::BadActorType`]; a name with no registrations at all yields an
    /// empty result.
    pub async fn lookup_all<A: RemoteActor>(
        &self,
        name: &str,
    ) -> Result<Vec<RemoteActorRef<A>>, RegistryError> {
        self.check_running()?;
        registry::collect_providers(
            &self.inner.chitchat,
            &self.inner.pool,
            &self.inner.dispatch,
            name,
        )
        .await
    }

    /// Watches the live provider set of `name`, yielding it on every change.
    ///
    /// The current set is yielded immediately. Changes include registrations,
    /// deregistrations, and nodes joining or dying. The stream ends when this node
    /// shuts down.
    pub async fn watch<A: RemoteActor>(
        &self,
        name: impl Into<String>,
    ) -> Result<impl Stream<Item = Vec<RemoteActorRef<A>>> + Send + 'static, RegistryError> {
        self.check_running()?;
        Ok(registry::watch_providers(
            &self.inner.chitchat,
            self.inner.pool.clone(),
            self.inner.dispatch.clone(),
            self.inner.cancel.child_token(),
            name.into(),
        )
        .await)
    }

    /// Returns this node's id.
    pub fn node_id(&self) -> &NodeId {
        &self.inner.node_id
    }

    /// Returns this node incarnation's generation id, which increases on restart.
    pub fn generation_id(&self) -> u64 {
        self.inner.generation_id
    }

    /// Returns this node's gossip advertise address.
    pub fn gossip_addr(&self) -> SocketAddr {
        self.inner.gossip_advertise_addr
    }

    /// Returns this node's TCP messaging advertise address.
    pub fn messaging_addr(&self) -> SocketAddr {
        self.inner.messaging_advertise_addr
    }

    fn check_running(&self) -> Result<(), RegistryError> {
        if self.inner.shutdown.load(Ordering::Relaxed) {
            // After shutdown, gossip is frozen: registrations would never propagate
            // and lookups would reflect stale liveness, so all registry ops fail.
            return Err(RegistryError::NodeShutdown);
        }
        Ok(())
    }

    /// Returns the ids of the nodes currently considered alive, including this node.
    ///
    /// Reflects the last known gossip state; after [`shutdown`](RemoteNode::shutdown)
    /// this is frozen.
    pub async fn live_nodes(&self) -> Vec<NodeId> {
        let chitchat = self.inner.chitchat.lock().await;
        chitchat
            .live_nodes()
            .map(|chitchat_id| NodeId::from(chitchat_id.node_id.clone()))
            .collect()
    }

    /// Shuts the node down: deregisters all registrations, stops the messaging server,
    /// and leaves the gossip cluster.
    ///
    /// The registrations are deleted and given a couple of gossip rounds to propagate,
    /// so peers observe a clean departure quickly instead of waiting for the failure
    /// detector. The registry's strong references to registered actors are released,
    /// so actors kept alive only by their registration stop. Idempotent; a second call
    /// is a no-op.
    pub async fn shutdown(&self) -> Result<(), ShutdownError> {
        self.inner.shutdown.store(true, Ordering::Relaxed);
        let handle = self.inner.chitchat_handle.lock().await.take();
        let Some(handle) = handle else {
            self.inner.cancel.cancel();
            return Ok(());
        };

        let had_registrations = {
            let mut chitchat = self.inner.chitchat.lock().await;
            let state = chitchat.self_node_state();
            let keys: Vec<String> = state
                .iter_prefix(registry::ACTOR_KEY_PREFIX)
                .map(|(key, _)| key.to_string())
                .collect();
            for key in &keys {
                state.delete(key);
            }
            !keys.is_empty()
        };
        if had_registrations {
            // Give gossip a couple of rounds to propagate the deletions.
            tokio::time::sleep(2 * self.inner.gossip_interval).await;
        }

        self.inner.cancel.cancel();
        let result = handle.shutdown().await.map_err(ShutdownError::Chitchat);
        // Release the registry's strong ActorRefs; local fast-path refs now resolve
        // to nothing, consistent with how remote peers observe the departure.
        self.inner.dispatch.clear();
        // Tear down outbound connections; remote refs obtained through this node
        // fail with NodeShutdown from here on.
        self.inner.pool.shutdown();
        result
    }
}

impl RemoteNodeInner {
    pub(crate) async fn remove_registration(&self, sequence_id: u64, name: &str) {
        let key = registry::actor_key(name, sequence_id);
        {
            let mut chitchat = self.chitchat.lock().await;
            let state = chitchat.self_node_state();
            // Guarded so repeated deregistration stays silent and does not bump
            // tombstone versions.
            if state.get(&key).is_some() {
                state.delete(&key);
            }
        }
        self.dispatch.remove_name(sequence_id, name);
    }
}

impl Drop for RemoteNodeInner {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.server_task.abort();
        if let Ok(mut guard) = self.chitchat_handle.try_lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
        // Local fast-path refs may outlive the node's Arc; without this they would
        // keep dispatching (and keep registered actors alive) after the node is gone.
        self.dispatch.clear();
        // Remote refs hold pool clones; without this their connections and tasks
        // would outlive the node.
        self.pool.shutdown();
    }
}
