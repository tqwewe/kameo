use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{interval, Instant},
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    connection_pool::handle_incoming_message,
    registry::{GossipRegistry, GossipResult, GossipTask, RegistryMessage, RegistryStats},
    GossipConfig, GossipError, RegistrationPriority, RemoteActorLocation, Result,
};

/// Per-connection streaming state for managing partial streams
#[derive(Debug)]
struct StreamingState {
    active_streams: HashMap<u64, InProgressStream>,
    max_concurrent_streams: usize,
}

/// A stream that is currently being assembled
#[derive(Debug)]
struct InProgressStream {
    stream_id: u64,
    total_size: u64,
    type_hash: u32,
    actor_id: u64,
    chunks: BTreeMap<u32, Bytes>, // chunk_index -> chunk_data
    received_size: usize,
}

impl StreamingState {
    fn new() -> Self {
        Self {
            active_streams: HashMap::new(),
            max_concurrent_streams: 16, // Reasonable limit
        }
    }

    fn start_stream(&mut self, header: crate::StreamHeader) -> Result<()> {
        if self.active_streams.len() >= self.max_concurrent_streams {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "Too many concurrent streams",
            )));
        }

        let stream = InProgressStream {
            stream_id: header.stream_id,
            total_size: header.total_size,
            type_hash: header.type_hash,
            actor_id: header.actor_id,
            chunks: BTreeMap::new(),
            received_size: 0,
        };

        self.active_streams.insert(header.stream_id, stream);
        Ok(())
    }

    fn add_chunk(
        &mut self,
        header: crate::StreamHeader,
        chunk_data: Bytes,
    ) -> Result<Option<Vec<u8>>> {
        let stream = self
            .active_streams
            .get_mut(&header.stream_id)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Received chunk for unknown stream_id={}", header.stream_id),
                ))
            })?;

        // Store chunk
        stream.received_size += chunk_data.len();
        stream.chunks.insert(header.chunk_index, chunk_data);

        // Check if we have all chunks (when total matches expected size)
        if stream.received_size >= stream.total_size as usize {
            self.assemble_complete_message(header.stream_id)
        } else {
            Ok(None)
        }
    }

    fn finalize_stream(&mut self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        // StreamEnd received - assemble the message
        self.assemble_complete_message(stream_id)
    }

    fn assemble_complete_message(&mut self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        let stream = self.active_streams.remove(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Cannot finalize unknown stream_id={}", stream_id),
            ))
        })?;

        // Assemble chunks in order
        let mut complete_data = BytesMut::with_capacity(stream.total_size as usize);
        for (_chunk_index, chunk_data) in stream.chunks {
            complete_data.put_slice(&chunk_data);
        }

        info!(
            "✅ STREAMING: Assembled complete message for stream_id={} ({} bytes for actor={}, type_hash=0x{:x})",
            stream.stream_id,
            complete_data.len(),
            stream.actor_id,
            stream.type_hash
        );

        Ok(Some(complete_data.to_vec()))
    }
}

/// Main API for the gossip registry with vector clocks and separated locks
pub struct GossipRegistryHandle {
    pub registry: Arc<GossipRegistry>,
    _server_handle: tokio::task::JoinHandle<()>,
    _timer_handle: tokio::task::JoinHandle<()>,
    _monitor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl GossipRegistryHandle {
    /// Create and start a new gossip registry with TLS encryption
    ///
    /// This creates a secure gossip registry that uses TLS 1.3 for all connections.
    /// The secret_key is used to generate the node's identity certificate.
    pub async fn new_with_tls(
        bind_addr: SocketAddr,
        secret_key: crate::SecretKey,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        let mut config = config.unwrap_or_default();

        // Ensure config keypair matches the TLS identity (or set it)
        let derived_keypair = secret_key.to_keypair();
        match config.key_pair.as_ref() {
            Some(existing) => {
                if existing.peer_id() != derived_keypair.peer_id() {
                    return Err(GossipError::InvalidKeyPair(
                        "GossipConfig.key_pair does not match TLS secret key".to_string(),
                    ));
                }
            }
            None => {
                config.key_pair = Some(derived_keypair);
            }
        }

        // Create the TCP listener first to get the actual bound address
        let listener = TcpListener::bind(bind_addr).await?;
        let actual_bind_addr = listener.local_addr()?;

        // Create registry with TLS enabled
        let mut registry = GossipRegistry::new(actual_bind_addr, config.clone());
        registry.enable_tls(secret_key)?;

        let registry = Arc::new(registry);

        // Set the registry reference in the connection pool
        {
            let mut pool = registry.connection_pool.lock().await;
            pool.set_registry(registry.clone());
        }

        // Start the server with the existing listener
        let server_registry = registry.clone();
        let server_handle = tokio::spawn(async move {
            if let Err(err) = start_gossip_server_with_listener(server_registry, listener).await {
                error!(error = %err, "TLS server error");
            }
        });

        // Start the gossip timer
        let timer_registry = registry.clone();
        let timer_handle = tokio::spawn(async move {
            start_gossip_timer(timer_registry).await;
        });

        // Connection monitoring is now done in the gossip timer
        let monitor_handle = None;

        info!(bind_addr = %actual_bind_addr, "TLS-enabled gossip registry started");

        Ok(Self {
            registry,
            _server_handle: server_handle,
            _timer_handle: timer_handle,
            _monitor_handle: monitor_handle,
        })
    }

    /// Create and start a new gossip registry using a keypair (TLS-only helper)
    pub async fn new_with_keypair(
        bind_addr: SocketAddr,
        keypair: crate::KeyPair,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        let mut config = config.unwrap_or_default();
        match config.key_pair.as_ref() {
            Some(existing) => {
                if existing.peer_id() != keypair.peer_id() {
                    return Err(GossipError::InvalidKeyPair(
                        "GossipConfig.key_pair does not match provided keypair".to_string(),
                    ));
                }
            }
            None => {
                config.key_pair = Some(keypair.clone());
            }
        }
        let secret_key = keypair.to_secret_key();
        Self::new_with_tls(bind_addr, secret_key, Some(config)).await
    }

    /// Create and start a new gossip registry (TLS-only)
    #[instrument(skip(config))]
    pub async fn new(
        bind_addr: SocketAddr,
        secret_key: crate::SecretKey,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        Self::new_with_tls(bind_addr, secret_key, config).await
    }

    /// Register a local actor
    pub async fn register(&self, name: String, address: SocketAddr) -> Result<()> {
        let location = RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        self.registry.register_actor(name, location).await
    }

    /// Register a local actor with metadata
    pub async fn register_with_metadata(
        &self,
        name: String,
        address: SocketAddr,
        metadata: Vec<u8>,
    ) -> Result<()> {
        let location = RemoteActorLocation::new_with_metadata(
            address,
            self.registry.peer_id.clone(),
            metadata,
        );
        self.registry.register_actor(name, location).await
    }

    /// Register a local actor with high priority (faster propagation)
    pub async fn register_urgent(
        &self,
        name: String,
        address: SocketAddr,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let mut location =
            RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        location.priority = priority;
        self.registry
            .register_actor_with_priority(name, location, priority)
            .await
    }

    /// Register a local actor with specified priority
    pub async fn register_with_priority(
        &self,
        name: String,
        address: SocketAddr,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let mut location =
            RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        location.priority = priority;
        self.registry
            .register_actor_with_priority(name, location, priority)
            .await
    }

    /// Unregister a local actor
    pub async fn unregister(&self, name: &str) -> Result<Option<RemoteActorLocation>> {
        self.registry.unregister_actor(name).await
    }

    /// Lookup an actor (now much faster - read-only lock)
    pub async fn lookup(&self, name: &str) -> Option<RemoteActorLocation> {
        self.registry.lookup_actor(name).await
    }

    /// Get registry statistics including vector clock metrics
    pub async fn stats(&self) -> RegistryStats {
        self.registry.get_stats().await
    }

    /// Add a peer to the gossip network
    pub async fn add_peer(&self, peer_id: &crate::PeerId) -> crate::Peer {
        // Pre-configure the peer as allowed (address will be set when connect() is called)
        {
            let pool = self.registry.connection_pool.lock().await;
            // Use a placeholder address - will be updated when connect() is called
            pool.peer_id_to_addr
                .insert(peer_id.clone(), "0.0.0.0:0".parse().unwrap());
        }

        crate::Peer {
            peer_id: peer_id.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Get a connection handle for direct communication (reuses existing pool connections)
    pub async fn get_connection(
        &self,
        addr: SocketAddr,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        self.registry.get_connection(addr).await
    }

    /// Get a connection handle by peer ID (ensures TLS NodeId is known)
    pub async fn get_connection_to_peer(
        &self,
        peer_id: &crate::PeerId,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        self.registry
            .connection_pool
            .lock()
            .await
            .get_connection_to_peer(peer_id)
            .await
    }

    /// Bootstrap peer connections non-blocking (Phase 4)
    ///
    /// Dials seed peers asynchronously - doesn't block startup on gossip propagation.
    /// Failed connections are logged but don't prevent the node from starting.
    /// This is the recommended way to bootstrap a node with seed peers.
    pub async fn bootstrap_non_blocking(&self, seeds: Vec<SocketAddr>) {
        let seed_count = seeds.len();

        for seed in seeds {
            let registry = self.registry.clone();
            tokio::spawn(async move {
                match registry.get_connection(seed).await {
                    Ok(_conn) => {
                        debug!(seed = %seed, "bootstrap connection established");
                        // Mark peer as connected
                        registry.mark_peer_connected(seed).await;
                    }
                    Err(e) => {
                        warn!(seed = %seed, error = %e, "bootstrap peer unavailable");
                        // Note: Don't penalize at startup - peer might be starting up too
                    }
                }
            });
        }

        debug!(
            seed_count = seed_count,
            "initiated non-blocking bootstrap for seed peers"
        );
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        self.registry.shutdown().await;

        // Cancel background tasks (they will terminate when they detect shutdown)
        self._server_handle.abort();
        self._timer_handle.abort();
        if let Some(monitor_handle) = &self._monitor_handle {
            monitor_handle.abort();
        }

        // No artificial delays - connections will close immediately
    }
}

/// Start the gossip registry server with an existing listener
#[instrument(skip(registry, listener))]
async fn start_gossip_server_with_listener(
    registry: Arc<GossipRegistry>,
    listener: TcpListener,
) -> Result<()> {
    let bind_addr = registry.bind_addr;
    info!(bind_addr = %bind_addr, "gossip server started");

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!(peer_addr = %peer_addr, "📥 ACCEPTED incoming connection");
                // Set TCP_NODELAY for low-latency communication
                let _ = stream.set_nodelay(true);

                let registry_clone = registry.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer_addr, registry_clone).await;
                });
            }
            Err(err) => {
                error!(error = %err, "failed to accept connection");
            }
        }
    }
}

/// Start the gossip timer with vector clock support
#[instrument(skip(registry))]
async fn start_gossip_timer(registry: Arc<GossipRegistry>) {
    debug!("start_gossip_timer function called");

    let gossip_interval = registry.config.gossip_interval;
    let cleanup_interval = registry.config.cleanup_interval;
    let vector_clock_gc_interval = registry.config.vector_clock_gc_frequency;
    let peer_gossip_interval = registry.config.peer_gossip_interval;

    let max_jitter = std::cmp::min(gossip_interval, Duration::from_millis(1000));
    let jitter_ms = if max_jitter.is_zero() {
        0
    } else {
        let max_ms = max_jitter.as_millis().max(1) as u64;
        rand::random::<u64>() % max_ms
    };
    let jitter = Duration::from_millis(jitter_ms);
    let mut next_gossip_tick = Instant::now() + gossip_interval + jitter;
    let mut cleanup_timer = interval(cleanup_interval);
    let mut vector_clock_gc_timer = interval(vector_clock_gc_interval);

    // Peer gossip timer - only if peer discovery is enabled
    let mut peer_gossip_timer = peer_gossip_interval.map(|i| interval(i));

    debug!(
        gossip_interval_ms = gossip_interval.as_millis(),
        cleanup_interval_secs = cleanup_interval.as_secs(),
        vector_clock_gc_interval_secs = vector_clock_gc_interval.as_secs(),
        peer_gossip_interval_secs = peer_gossip_interval.map(|i| i.as_secs()),
        "gossip timer started with non-blocking I/O"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next_gossip_tick) => {
                let jitter_ms = if max_jitter.is_zero() {
                    0
                } else {
                    let max_ms = max_jitter.as_millis().max(1) as u64;
                    rand::random::<u64>() % max_ms
                };
                let jitter = Duration::from_millis(jitter_ms);
                next_gossip_tick += gossip_interval + jitter;

                // Step 1: Prepare gossip tasks while holding the lock briefly
                let tasks = {
                    if registry.is_shutdown().await {
                        break;
                    }
                    match registry.prepare_gossip_round().await {
                        Ok(tasks) => tasks,
                        Err(err) => {
                            error!(error = %err, "failed to prepare gossip round");
                            continue;
                        }
                    }
                };

                if tasks.is_empty() {
                    continue;
                }

                // Step 2: Execute all gossip tasks WITHOUT holding the registry lock
                // Use zero-copy optimized sending for each individual gossip message
                let results = {
                    let mut futures = Vec::new();

                    for task in tasks {
                        let registry_clone = registry.clone();
                        let peer_addr = task.peer_addr;
                        let sent_sequence = task.current_sequence;
                        let future = tokio::spawn(async move {
                            // Send the message using zero-copy persistent connections
                            let outcome = send_gossip_message_zero_copy(task, registry_clone).await;
                            GossipResult {
                                peer_addr,
                                sent_sequence,
                                outcome: outcome.map(|_| None),
                            }
                        });
                        futures.push(future);
                    }

                    // Wait for all gossip operations to complete concurrently
                    let mut results = Vec::new();
                    for future in futures {
                        match future.await {
                            Ok(result) => results.push(result),
                            Err(err) => {
                                error!(error = %err, "gossip task panicked");
                            }
                        }
                    }
                    results
                };

                // Step 3: Apply results while holding the lock briefly
                {
                    if !registry.is_shutdown().await {
                        registry.apply_gossip_results(results).await;
                    }
                }
            }
            _ = cleanup_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                registry.cleanup_stale_actors().await;
                // Also check for consensus timeouts
                registry.check_peer_consensus().await;
                // Clean up peers that have been dead for too long
                registry.cleanup_dead_peers().await;
                // Clean up stale peers from peer discovery (Phase 4)
                registry.prune_stale_peers().await;
            }
            _ = vector_clock_gc_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                // Run vector clock garbage collection
                registry.run_vector_clock_gc().await;
            }
            // Peer gossip timer - for peer list gossip (Phase 4)
            _ = async {
                if let Some(ref mut timer) = peer_gossip_timer {
                    timer.tick().await
                } else {
                    // If peer gossip is disabled, wait forever (never fires)
                    std::future::pending::<tokio::time::Instant>().await
                }
            } => {
                if registry.is_shutdown().await {
                    break;
                }
                // Only gossip peer list if peer discovery is enabled
                if registry.config.enable_peer_discovery {
                    let tasks = registry.gossip_peer_list().await;
                    if tasks.is_empty() {
                        continue;
                    }

                    let mut futures = Vec::new();
                    for task in tasks {
                        let registry_clone = registry.clone();
                        let future = tokio::spawn(async move {
                            if let Err(err) =
                                send_gossip_message_zero_copy(task, registry_clone).await
                            {
                                warn!(error = %err, "peer list gossip send failed");
                            }
                        });
                        futures.push(future);
                    }

                    for future in futures {
                        if let Err(err) = future.await {
                            error!(error = %err, "peer list gossip task panicked");
                        }
                    }
                }
            }
        }
    }

    debug!("gossip timer stopped");
}

/// Handle incoming TCP connections - immediately set up bidirectional communication
#[instrument(skip(stream, registry), fields(peer = %peer_addr))]
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    debug!("🔌 HANDLE_CONNECTION: Starting to handle new incoming connection");

    // Check if TLS is enabled
    if let Some(tls_config) = &registry.tls_config {
        // TLS is enabled - perform TLS handshake
        info!(
            "🔐 TLS ENABLED: Performing TLS handshake with peer {}",
            peer_addr
        );

        let acceptor = tls_config.acceptor();
        match acceptor.accept(stream).await {
            Ok(tls_stream) => {
                info!("✅ TLS handshake successful with peer {}", peer_addr);
                handle_tls_connection(tls_stream, peer_addr, registry).await;
            }
            Err(e) => {
                error!(error = %e, peer = %peer_addr, "❌ TLS handshake failed - rejecting connection");
                // Connection failed, don't continue
                // This is correct - we should NOT fall back to plain TCP if TLS is enabled
            }
        }
    } else {
        // No TLS - panic for now to ensure we're using TLS
        panic!("⚠️ TLS DISABLED: Server attempted to accept plain TCP connection from {}. TLS is required!", peer_addr);
    }
}

/// Handle an incoming TLS connection
async fn handle_tls_connection(
    mut tls_stream: tokio_rustls::server::TlsStream<TcpStream>,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    let negotiated_alpn = tls_stream
        .get_ref()
        .1
        .alpn_protocol()
        .map(|proto| proto.to_vec());

    let peer_node_id = tls_stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .and_then(|cert| crate::tls::extract_node_id_from_cert(cert).ok());

    let capabilities = match crate::handshake::perform_hello_handshake(
        &mut tls_stream,
        negotiated_alpn.as_deref(),
        registry.config.enable_peer_discovery,
    )
    .await
    {
        Ok(caps) => {
            eprintln!(
                "inbound hello capabilities from {} can_send={}",
                peer_addr,
                caps.can_send_peer_list()
            );
            caps
        }
        Err(err) => {
            warn!(
                peer = %peer_addr,
                error = %err,
                "Hello handshake failed, closing inbound TLS connection"
            );
            return;
        }
    };

    registry.set_peer_capabilities(peer_addr, capabilities.clone());
    if let Some(node_id) = registry.lookup_node_id(&peer_addr).await {
        registry
            .associate_peer_capabilities_with_node(peer_addr, node_id)
            .await;
    }

    // Ensure inbound-only peers participate in gossip fanout
    registry
        .ensure_peer_entry(peer_addr, peer_node_id.clone())
        .await;
    registry.mark_peer_connected(peer_addr).await;

    // Split the TLS stream
    let (reader, writer) = tokio::io::split(tls_stream);

    // Get registry reference for the handler
    let registry_weak = Some(Arc::downgrade(&registry));

    // Start the incoming persistent connection handler
    tokio::spawn(async move {
        debug!(peer = %peer_addr, "HANDLE.RS: Starting incoming TLS connection handler");
        match handle_incoming_connection_tls(
            reader,
            writer,
            peer_addr,
            registry.clone(),
            registry_weak,
            peer_node_id,
        )
        .await
        {
            ConnectionCloseOutcome::Normal {
                node_id: Some(failed_peer_id_hex),
            } => match crate::PeerId::from_hex(&failed_peer_id_hex) {
                Ok(peer_id) => {
                    debug!(peer_id = %peer_id, "HANDLE.RS: Triggering peer failure handling for node");
                    if let Err(e) = registry
                        .handle_peer_connection_failure_by_peer_id(&peer_id)
                        .await
                    {
                        warn!(error = %e, peer_id = %peer_id, "HANDLE.RS: Failed to handle peer connection failure");
                    }
                }
                Err(e) => {
                    warn!(error = %e, peer_id = %failed_peer_id_hex, "HANDLE.RS: Invalid peer id in connection close outcome");
                }
            },
            ConnectionCloseOutcome::Normal { node_id: None } => {
                warn!(peer = %peer_addr, "HANDLE.RS: Cannot handle peer failure - sender node ID unknown");
            }
            ConnectionCloseOutcome::DroppedByTieBreaker => {
                debug!(peer = %peer_addr, "HANDLE.RS: Dropped duplicate connection via tie-breaker");
            }
        }
        debug!(peer = %peer_addr, "HANDLE.RS: Incoming TLS connection handler exited");
    });
}

enum ConnectionCloseOutcome {
    Normal { node_id: Option<String> },
    DroppedByTieBreaker,
}

/// Handle an incoming TLS connection - processes all messages over encrypted stream
async fn handle_incoming_connection_tls<R, W>(
    mut reader: R,
    writer: W,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
    _registry_weak: Option<std::sync::Weak<GossipRegistry>>,
    peer_node_id: Option<crate::NodeId>,
) -> ConnectionCloseOutcome
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let max_message_size = registry.config.max_message_size;

    // Initialize streaming state for this connection
    let mut streaming_state = StreamingState::new();

    // First, read the initial message to identify the sender
    let msg_result = read_message_from_tls_reader(&mut reader, max_message_size).await;
    let known_node_id = match peer_node_id {
        Some(node_id) => Some(node_id),
        None => registry.lookup_node_id(&peer_addr).await,
    };

    let (sender_node_id, initial_correlation_id) = match &msg_result {
        Ok(MessageReadResult::Gossip(msg, correlation_id)) => {
            let node_id = match msg {
                RegistryMessage::DeltaGossip { delta } => delta.sender_peer_id.to_hex(),
                RegistryMessage::FullSync { sender_peer_id, .. } => sender_peer_id.to_hex(),
                RegistryMessage::FullSyncRequest { sender_peer_id, .. } => sender_peer_id.to_hex(),
                RegistryMessage::FullSyncResponse { sender_peer_id, .. } => sender_peer_id.to_hex(),
                RegistryMessage::DeltaGossipResponse { delta } => delta.sender_peer_id.to_hex(),
                RegistryMessage::PeerHealthQuery { sender, .. } => sender.to_hex(),
                RegistryMessage::PeerHealthReport { reporter, .. } => reporter.to_hex(),
                RegistryMessage::ImmediateAck { .. } => {
                    warn!("Received ImmediateAck as first message - cannot identify sender");
                    return ConnectionCloseOutcome::Normal { node_id: None };
                }
                RegistryMessage::ActorMessage { .. } => {
                    // For ActorMessage, we can't determine sender from the message
                    // But if it has a correlation_id, it's an Ask and we should handle it
                    if correlation_id.is_some() {
                        debug!("Received ActorMessage with Ask envelope as first message");
                        // We'll use a placeholder sender ID for now
                        "ask_sender".to_string()
                    } else {
                        warn!("Received ActorMessage as first message - cannot identify sender");
                        return ConnectionCloseOutcome::Normal { node_id: None };
                    }
                }
                RegistryMessage::PeerListGossip { sender_addr, .. } => sender_addr.clone(),
            };
            (node_id, *correlation_id)
        }
        Ok(MessageReadResult::AskRaw { correlation_id, .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), Some(*correlation_id))
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Ask request arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Response { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Response arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Actor { actor_id, .. }) => {
            // For actor messages received as the first message, we can't determine the sender
            // Use a placeholder identifier
            (format!("actor_sender_{}", actor_id), None)
        }
        Ok(MessageReadResult::Streaming { stream_header, .. }) => {
            // For streaming messages received as the first message, use the actor ID
            (format!("stream_sender_{}", stream_header.actor_id), None)
        }
        Ok(MessageReadResult::Raw(_)) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Raw message arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Err(e) => {
            warn!(error = %e, "Failed to read initial message from TLS stream");
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    };

    debug!(peer_addr = %peer_addr, node_id = %sender_node_id, "Identified incoming TLS connection from node");

    // Update the gossip state with the NodeId for this peer
    // This is critical for bidirectional TLS connections
    let peer_id = match crate::PeerId::from_hex(&sender_node_id) {
        Ok(peer_id) => peer_id,
        Err(err) => {
            warn!(
                peer_addr = %peer_addr,
                error = %err,
                "Invalid peer id in first message; dropping connection"
            );
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    };
    let node_id_opt = Some(peer_id.to_node_id());

    // Prefer the configured listening address for this peer (if known) instead of the
    // inbound socket's ephemeral address to avoid gossiping to a non-listening port.
    let configured_addr = {
        let pool = registry.connection_pool.lock().await;
        pool.peer_id_to_addr
            .get(&peer_id)
            .map(|entry| *entry.value())
    };
    let peer_state_addr = configured_addr
        .filter(|addr| addr.port() != 0)
        .unwrap_or(peer_addr);

    if let Some(node_id) = node_id_opt {
        registry
            .add_peer_with_node_id(peer_state_addr, Some(node_id))
            .await;
        // Associate capabilities captured during the Hello handshake (stored under peer_addr).
        registry
            .associate_peer_capabilities_with_node(peer_addr, node_id)
            .await;
        if peer_state_addr != peer_addr {
            registry
                .associate_peer_capabilities_with_node(peer_state_addr, node_id)
                .await;
        }
        if peer_state_addr != peer_addr {
            let mut gossip_state = registry.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&peer_state_addr) {
                peer_info.peer_address = Some(peer_addr);
            }
        }
        debug!(
            peer_addr = %peer_addr,
            peer_state_addr = %peer_state_addr,
            "Updated gossip state with NodeId for incoming TLS connection"
        );
    }

    // Register the TLS writer with the connection pool before handling the first message so responses work
    {
        use std::pin::Pin;
        use tokio::io::AsyncWrite;

        let boxed_writer: Pin<Box<dyn AsyncWrite + Send>> = Box::pin(writer);
        let buffer_config = crate::connection_pool::BufferConfig::default()
            .with_ask_inflight_limit(registry.config.ask_inflight_limit);
        let stream_handle = Arc::new(crate::connection_pool::LockFreeStreamHandle::new(
            boxed_writer,
            peer_addr,
            crate::connection_pool::ChannelId::Global,
            buffer_config,
        ));

        let mut connection = crate::connection_pool::LockFreeConnection::new(
            peer_state_addr,
            crate::connection_pool::ConnectionDirection::Inbound,
        );
        connection.stream_handle = Some(stream_handle);
        connection.set_state(crate::connection_pool::ConnectionState::Connected);
        connection.update_last_used();

        let connection_arc = Arc::new(connection);

        let keep_connection = {
            let pool = registry.connection_pool.lock().await;
            let has_existing = pool.has_connection_by_peer_id(&peer_id);

            if has_existing {
                if registry.should_keep_connection(&peer_id, false) {
                    debug!(
                        peer_id = %peer_id,
                        "tie-breaker: favoring inbound connection, dropping existing outbound"
                    );
                    if let Some(existing) = pool.disconnect_connection_by_peer_id(&peer_id) {
                        if let Some(handle) = existing.stream_handle.as_ref() {
                            handle.shutdown();
                        }
                    }
                    pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        peer_state_addr,
                        connection_arc.clone(),
                    );
                    true
                } else {
                    debug!(
                        peer_id = %peer_id,
                        "tie-breaker: rejecting inbound duplicate connection"
                    );
                    registry.clear_peer_capabilities(&peer_addr);
                    false
                }
            } else {
                pool.add_connection_by_peer_id(peer_id.clone(), peer_state_addr, connection_arc.clone());
                true
            }
        };

        if !keep_connection {
            if let Some(handle) = connection_arc.stream_handle.as_ref() {
                handle.shutdown();
            }
            return ConnectionCloseOutcome::DroppedByTieBreaker;
        }

        debug!(
            node_id = %sender_node_id,
            peer_addr = %peer_addr,
            "Added incoming TLS connection to pool for bidirectional communication"
        );
    }

    // Process the initial message with correlation ID if present
    match msg_result {
        Ok(MessageReadResult::Gossip(msg, _correlation_id)) => {
            // For ActorMessage with correlation_id, ensure it's set
            let msg_to_handle = if let RegistryMessage::ActorMessage {
                actor_id,
                type_hash,
                payload,
                correlation_id: _,
            } = msg
            {
                // Create a new ActorMessage with the correlation_id from the Ask envelope
                RegistryMessage::ActorMessage {
                    actor_id,
                    type_hash,
                    payload,
                    correlation_id: initial_correlation_id,
                }
            } else {
                msg
            };

            if let Err(e) =
                handle_incoming_message(registry.clone(), peer_addr, msg_to_handle).await
            {
                warn!(error = %e, "Failed to process initial TLS message");
            }
        }
        Ok(MessageReadResult::AskRaw {
            correlation_id,
            payload,
        }) => {
            handle_raw_ask_request(&registry, peer_addr, correlation_id, &payload).await;
        }
        Ok(MessageReadResult::Response {
            correlation_id,
            payload,
        }) => {
            handle_response_message(&registry, peer_addr, correlation_id, payload).await;
        }
        Ok(MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            payload,
        }) => {
            // Handle initial actor message directly
            if let Some(handler) = &*registry.actor_message_handler.lock().await {
                let actor_id_str = actor_id.to_string();
                let correlation = if msg_type == crate::MessageType::ActorAsk as u8 {
                    Some(correlation_id)
                } else {
                    None
                };
                let _ = handler
                    .handle_actor_message(&actor_id_str, type_hash, &payload, correlation)
                    .await;
            }
        }
        Ok(MessageReadResult::Streaming {
            msg_type,
            stream_header,
            chunk_data,
        }) => {
            // Handle initial streaming message
            match msg_type {
                msg_type if msg_type == crate::MessageType::StreamStart as u8 => {
                    if let Err(e) = streaming_state.start_stream(stream_header) {
                        warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamData as u8 => {
                    // Data chunk - this should not be the first message typically, but handle it
                    if let Err(e) = streaming_state.start_stream(stream_header) {
                        debug!(error = %e, "Auto-starting stream for data chunk: stream_id={}", stream_header.stream_id);
                    }
                    if let Ok(Some(complete_data)) =
                        streaming_state.add_chunk(stream_header, chunk_data)
                    {
                        // Complete message assembled - route to actor
                        if let Some(handler) = &*registry.actor_message_handler.lock().await {
                            let actor_id_str = stream_header.actor_id.to_string();
                            let _ = handler
                                .handle_actor_message(
                                    &actor_id_str,
                                    stream_header.type_hash,
                                    &complete_data,
                                    None,
                                )
                                .await;
                        }
                    }
                }
                _ => {
                    warn!(
                        "Unexpected streaming message type as initial message: 0x{:02x}",
                        msg_type
                    );
                }
            }
        }
        Ok(MessageReadResult::Raw(payload)) => {
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                if std::env::var("KAMEO_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
                    crate::test_helpers::record_raw_payload(payload.clone());
                }
            }
            debug!(peer_addr = %peer_addr, "Ignoring raw message payload");
        }
        Err(e) => {
            warn!(error = %e, "Failed to read initial message - connection will be closed");
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    }

    // Continue reading messages from the TLS stream
    // Note: writer has been moved to the connection pool, so we only have the reader
    loop {
        match read_message_from_tls_reader(&mut reader, max_message_size).await {
            Ok(MessageReadResult::Gossip(msg, correlation_id)) => {
                // For ActorMessage with correlation_id from Ask envelope, ensure it's set
                let msg_to_handle = if let RegistryMessage::ActorMessage {
                    actor_id,
                    type_hash,
                    payload,
                    correlation_id: _,
                } = msg
                {
                    // Create a new ActorMessage with the correlation_id from the Ask envelope
                    RegistryMessage::ActorMessage {
                        actor_id,
                        type_hash,
                        payload,
                        correlation_id,
                    }
                } else {
                    msg
                };

                if let Err(e) =
                    handle_incoming_message(registry.clone(), peer_addr, msg_to_handle).await
                {
                    warn!(error = %e, "Failed to process TLS message");
                }
            }
            Ok(MessageReadResult::AskRaw {
                correlation_id,
                payload,
            }) => {
                handle_raw_ask_request(&registry, peer_addr, correlation_id, &payload).await;
            }
            Ok(MessageReadResult::Response {
                correlation_id,
                payload,
            }) => {
                handle_response_message(&registry, peer_addr, correlation_id, payload).await;
            }
            Ok(MessageReadResult::Actor {
                msg_type,
                correlation_id,
                actor_id,
                type_hash,
                payload,
            }) => {
                // Handle actor message directly
                // Call the actor message handler if available
                if let Some(handler) = &*registry.actor_message_handler.lock().await {
                    let actor_id_str = actor_id.to_string();
                    let correlation = if msg_type == crate::MessageType::ActorAsk as u8 {
                        Some(correlation_id)
                    } else {
                        None
                    };
                    let _ = handler
                        .handle_actor_message(&actor_id_str, type_hash, &payload, correlation)
                        .await;
                }
            }
            Ok(MessageReadResult::Streaming {
                msg_type,
                stream_header,
                chunk_data,
            }) => {
                // Handle streaming messages
                match msg_type {
                    msg_type if msg_type == crate::MessageType::StreamStart as u8 => {
                        if let Err(e) = streaming_state.start_stream(stream_header) {
                            warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                        }
                    }
                    msg_type if msg_type == crate::MessageType::StreamData as u8 => {
                        if let Ok(Some(complete_data)) =
                            streaming_state.add_chunk(stream_header, chunk_data)
                        {
                            // Complete message assembled - route to actor
                            if let Some(handler) = &*registry.actor_message_handler.lock().await {
                                let actor_id_str = stream_header.actor_id.to_string();
                                let _ = handler
                                    .handle_actor_message(
                                        &actor_id_str,
                                        stream_header.type_hash,
                                        &complete_data,
                                        None,
                                    )
                                    .await;
                            }
                        }
                    }
                    msg_type if msg_type == crate::MessageType::StreamEnd as u8 => {
                        if let Ok(Some(complete_data)) =
                            streaming_state.finalize_stream(stream_header.stream_id)
                        {
                            // Complete message assembled - route to actor
                            if let Some(handler) = &*registry.actor_message_handler.lock().await {
                                let actor_id_str = stream_header.actor_id.to_string();
                                let _ = handler
                                    .handle_actor_message(
                                        &actor_id_str,
                                        stream_header.type_hash,
                                        &complete_data,
                                        None,
                                    )
                                    .await;
                            }
                        }
                    }
                    _ => {
                        warn!("Unknown streaming message type: 0x{:02x}", msg_type);
                    }
                }
            }
            Ok(MessageReadResult::Raw(payload)) => {
                #[cfg(any(test, feature = "test-helpers", debug_assertions))]
                {
                    if std::env::var("KAMEO_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
                        crate::test_helpers::record_raw_payload(payload.clone());
                    }
                }
                debug!(peer_addr = %peer_addr, "Ignoring raw message payload");
            }
            Err(e) => {
                debug!(error = %e, "TLS connection closed or error reading message");
                break;
            }
        }
    }

    ConnectionCloseOutcome::Normal {
        node_id: Some(sender_node_id),
    }
}

/// Result type for message reading that can handle gossip, actor, and streaming messages
pub(crate) enum MessageReadResult {
    Gossip(RegistryMessage, Option<u16>),
    AskRaw {
        correlation_id: u16,
        payload: bytes::Bytes,
    },
    Response {
        correlation_id: u16,
        payload: bytes::Bytes,
    },
    Raw(bytes::Bytes),
    Actor {
        msg_type: u8,
        correlation_id: u16,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    },
    Streaming {
        msg_type: u8,
        stream_header: crate::StreamHeader,
        chunk_data: bytes::Bytes,
    },
}

pub(crate) async fn handle_raw_ask_request(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: &[u8],
) {
    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    {
        let response = if std::env::var("KAMEO_REMOTE_TYPED_ECHO").is_ok() && payload.len() >= 8 {
            payload.to_vec()
        } else {
            crate::connection_pool::process_mock_request_payload(payload)
        };

        let conn = {
            let pool = registry.connection_pool.lock().await;
            pool.get_connection_by_addr(&peer_addr)
        };

        if let Some(conn) = conn {
            if let Some(ref stream_handle) = conn.stream_handle {
                let header = crate::framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    response.len(),
                );

                if let Err(e) = stream_handle
                    .write_header_and_payload_control(
                        bytes::Bytes::copy_from_slice(&header),
                        bytes::Bytes::from(response),
                    )
                    .await
                {
                    warn!(peer = %peer_addr, error = %e, "Failed to send Ask response");
                } else {
                    debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent Ask response");
                }
            } else {
                warn!(peer = %peer_addr, "No stream handle for Ask response");
            }
        } else {
            warn!(peer = %peer_addr, "No connection found for Ask response");
        }
    }
    #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
    {
        let _ = registry;
        let _ = payload;
        warn!(
            peer = %peer_addr,
            correlation_id = correlation_id,
            "Received raw Ask request - not supported"
        );
    }
}

pub(crate) async fn handle_response_message(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: bytes::Bytes,
) {
    let conn = {
        let pool = registry.connection_pool.lock().await;
        pool.get_connection_by_addr(&peer_addr)
    };

    if let Some(conn) = conn {
        if let Some(ref correlation) = conn.correlation {
            if correlation.has_pending(correlation_id) {
                correlation.complete(correlation_id, payload);
                debug!(
                    peer = %peer_addr,
                    correlation_id = correlation_id,
                    "Delivered response via correlation tracker"
                );
            } else {
                debug!(
                    peer = %peer_addr,
                    correlation_id = correlation_id,
                    "Response received with no pending request"
                );
            }
        } else {
            warn!(peer = %peer_addr, "Connection has no correlation tracker for response");
        }
    } else {
        warn!(peer = %peer_addr, "No connection found for response delivery");
    }
}

/// Read a message from a TLS reader
pub(crate) async fn read_message_from_tls_reader<R>(
    reader: &mut R,
    max_message_size: usize,
) -> Result<MessageReadResult>
where
    R: AsyncReadExt + Unpin,
{
    // Read the message length (4 bytes)
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;

    if msg_len > max_message_size {
        return Err(crate::GossipError::MessageTooLarge {
            size: msg_len,
            max: max_message_size,
        });
    }

    // Read the message data into a buffer that keeps the length prefix for alignment.
    let mut msg_vec = vec![0u8; msg_len + crate::framing::LENGTH_PREFIX_LEN];
    msg_vec[..crate::framing::LENGTH_PREFIX_LEN].copy_from_slice(&len_buf);
    reader
        .read_exact(&mut msg_vec[crate::framing::LENGTH_PREFIX_LEN..])
        .await?;
    let msg_buf = bytes::Bytes::from(msg_vec);
    let msg_data = msg_buf.slice(crate::framing::LENGTH_PREFIX_LEN..);

    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    {
        if std::env::var("KAMEO_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
            crate::test_helpers::record_raw_payload(msg_data.clone());
        }
    }

    // Check if this is an Ask message with envelope
    if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::Ask as u8
    {
        // This is an Ask message with envelope format:
        // [type:1][correlation_id:2][pad:1][payload:N]

        // Extract correlation ID (bytes 1-2)
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);

        // The actual RegistryMessage starts at byte 8
        // Create a properly aligned buffer for the payload
        let payload = msg_data.slice(crate::framing::ASK_RESPONSE_HEADER_LEN..);

        // Try to deserialize as RegistryMessage first (Ask wrapper for gossip)
        match rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(payload.as_ref()) {
            Ok(msg) => {
                debug!(
                    correlation_id = correlation_id,
                    "Received Ask message with correlation ID"
                );
                Ok(MessageReadResult::Gossip(msg, Some(correlation_id)))
            }
            Err(_) => {
                debug!(
                    correlation_id = correlation_id,
                    payload_len = payload.len(),
                    "Received raw Ask payload"
                );
                Ok(MessageReadResult::AskRaw {
                    correlation_id,
                    payload,
                })
            }
        }
    } else if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data[0] == crate::MessageType::Response as u8
    {
        // Response message format:
        // [type:1][correlation_id:2][pad:1][payload:N]
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload = msg_data.slice(crate::framing::ASK_RESPONSE_HEADER_LEN..);
        Ok(MessageReadResult::Response {
            correlation_id,
            payload,
        })
    } else {
        // Check if this is a Gossip message with type prefix
        if msg_len >= 1 {
            let first_byte = msg_data[0];
            // Check if it's a known message type
            if let Some(msg_type) = crate::MessageType::from_byte(first_byte) {
                match msg_type {
                    crate::MessageType::Gossip => {
                        // This is a gossip message with type prefix, skip the type byte
                        if msg_data.len() >= crate::framing::GOSSIP_HEADER_LEN {
                            // Create a properly aligned buffer for the payload
                            let payload = msg_data.slice(crate::framing::GOSSIP_HEADER_LEN..);
                            match rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(
                                payload.as_ref(),
                            ) {
                                Ok(msg) => return Ok(MessageReadResult::Gossip(msg, None)),
                                Err(_) => return Ok(MessageReadResult::Raw(msg_data)),
                            }
                        } else {
                            return Ok(MessageReadResult::Raw(msg_data));
                        }
                    }
                    crate::MessageType::ActorTell | crate::MessageType::ActorAsk => {
                        // This is an actor message with envelope format:
                        // [type:1][correlation_id:2][pad:1][actor_id:8][type_hash:4][payload_len:4][payload:N]
                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN {
                            // Need at least 24 bytes for header
                            return Ok(MessageReadResult::Raw(msg_data));
                        }

                        // Parse the actor message envelope
                        let msg_type_byte = msg_data[0];
                        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                        let actor_id = u64::from_be_bytes(msg_data[4..12].try_into().unwrap());
                        let type_hash = u32::from_be_bytes(msg_data[12..16].try_into().unwrap());
                        let payload_len =
                            u32::from_be_bytes(msg_data[16..20].try_into().unwrap()) as usize;

                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
                            return Ok(MessageReadResult::Raw(msg_data));
                        }

                        let payload = msg_data.slice(
                            crate::framing::ACTOR_HEADER_LEN
                                ..crate::framing::ACTOR_HEADER_LEN + payload_len,
                        );

                        return Ok(MessageReadResult::Actor {
                            msg_type: msg_type_byte,
                            correlation_id,
                            actor_id,
                            type_hash,
                            payload,
                        });
                    }
                    crate::MessageType::StreamStart
                    | crate::MessageType::StreamData
                    | crate::MessageType::StreamEnd => {
                        // Handle streaming messages
                        // Message format: [type:1][correlation_id:2][reserved:5][stream_header:36][chunk_data:N]
                        if msg_data.len()
                            < crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE
                        {
                            return Ok(MessageReadResult::Raw(msg_data));
                        }

                        // Parse the stream header (36 bytes starting at offset 8)
                        let header_bytes = &msg_data[crate::framing::STREAM_HEADER_PREFIX_LEN
                            ..crate::framing::STREAM_HEADER_PREFIX_LEN
                                + crate::StreamHeader::SERIALIZED_SIZE];
                        let stream_header = match crate::StreamHeader::from_bytes(header_bytes) {
                            Some(header) => header,
                            None => return Ok(MessageReadResult::Raw(msg_data)),
                        };

                        // Extract chunk data (everything after the header)
                        let chunk_data =
                            if msg_data.len()
                                > crate::framing::STREAM_HEADER_PREFIX_LEN
                                    + crate::StreamHeader::SERIALIZED_SIZE
                            {
                                msg_data.slice(
                                    crate::framing::STREAM_HEADER_PREFIX_LEN
                                        + crate::StreamHeader::SERIALIZED_SIZE..,
                                )
                            } else {
                                bytes::Bytes::new()
                            };

                        return Ok(MessageReadResult::Streaming {
                            msg_type: first_byte,
                            stream_header,
                            chunk_data,
                        });
                    }
                    _ => {
                        // Unknown message type, treat as raw payload.
                        return Ok(MessageReadResult::Raw(msg_data));
                    }
                }
            }
        }

        Ok(MessageReadResult::Raw(msg_data))
    }
}

#[cfg(test)]
mod framing_tests {
    use super::{read_message_from_tls_reader, MessageReadResult};
    use crate::{framing, MessageType};
    use tokio::io::AsyncWriteExt;

    async fn read_frame(frame: Vec<u8>) -> MessageReadResult {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            writer.write_all(&frame).await.unwrap();
        });
        read_message_from_tls_reader(&mut reader, 1024 * 1024)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn ask_raw_parses_with_padded_header() {
        let payload_bytes = b"hello";
        let header =
            framing::write_ask_response_header(MessageType::Ask, 42, payload_bytes.len());
        let mut frame = Vec::with_capacity(header.len() + payload_bytes.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(payload_bytes);

        match read_frame(frame).await {
            MessageReadResult::AskRaw {
                correlation_id,
                payload: body,
            } => {
                assert_eq!(correlation_id, 42);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn response_parses_with_padded_header() {
        let payload_bytes = b"world";
        let header =
            framing::write_ask_response_header(MessageType::Response, 7, payload_bytes.len());
        let mut frame = Vec::with_capacity(header.len() + payload_bytes.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(payload_bytes);

        match read_frame(frame).await {
            MessageReadResult::Response {
                correlation_id,
                payload: body,
            } => {
                assert_eq!(correlation_id, 7);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn actor_tell_parses_with_reordered_header() {
        let payload_bytes = b"actor_payload";
        let actor_id = 0x0102030405060708u64;
        let type_hash = 0x11223344u32;

        let total_len = framing::ACTOR_HEADER_LEN + payload_bytes.len();
        let mut frame = Vec::with_capacity(framing::LENGTH_PREFIX_LEN + total_len);
        frame.extend_from_slice(&(total_len as u32).to_be_bytes());
        frame.push(MessageType::ActorTell as u8);
        frame.extend_from_slice(&0u16.to_be_bytes());
        frame.push(0u8);
        frame.extend_from_slice(&actor_id.to_be_bytes());
        frame.extend_from_slice(&type_hash.to_be_bytes());
        frame.extend_from_slice(&(payload_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload_bytes);

        match read_frame(frame).await {
            MessageReadResult::Actor {
                msg_type,
                correlation_id,
                actor_id: parsed_actor_id,
                type_hash: parsed_type_hash,
                payload: body,
            } => {
                assert_eq!(msg_type, MessageType::ActorTell as u8);
                assert_eq!(correlation_id, 0);
                assert_eq!(parsed_actor_id, actor_id);
                assert_eq!(parsed_type_hash, type_hash);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }
}

/// Zero-copy gossip message sender - eliminates bottlenecks in serialization and connection handling
async fn send_gossip_message_zero_copy(
    mut task: GossipTask,
    registry: Arc<GossipRegistry>,
) -> Result<()> {
    // Check if this is a retry attempt
    let is_retry = {
        let gossip_state = registry.gossip_state.lock().await;
        gossip_state
            .peers
            .get(&task.peer_addr)
            .map(|p| p.failures > 0)
            .unwrap_or(false)
    };

    if is_retry {
        info!(
            peer = %task.peer_addr,
            "🔄 GOSSIP RETRY: Attempting to reconnect to previously failed peer"
        );
    }

    // Get connection with minimal lock contention
    let conn = {
        let mut pool = registry.connection_pool.lock().await;
        debug!(
            "GOSSIP: Pool has {} connections before get_connection",
            pool.connection_count()
        );
        match pool.get_connection(task.peer_addr).await {
            Ok(conn) => {
                if is_retry {
                    info!(
                        peer = %task.peer_addr,
                        "✅ GOSSIP RETRY: Successfully reconnected to peer"
                    );
                }
                conn
            }
            Err(e) => {
                if is_retry {
                    info!(
                        peer = %task.peer_addr,
                        error = %e,
                        "❌ GOSSIP RETRY: Failed to reconnect to peer"
                    );
                }
                return Err(e);
            }
        }
    };

    if matches!(
        task.message,
        crate::registry::RegistryMessage::PeerListGossip { .. }
    ) && !registry.peer_supports_peer_list(&task.peer_addr).await
    {
        debug!(
            peer = %task.peer_addr,
            "Skipping PeerListGossip send - peer lacks negotiated capability"
        );
        return Ok(());
    }

    // CRITICAL: Set precise timing RIGHT BEFORE TCP write to exclude all scheduling delays
    // Update wall_clock_time in delta changes to current time for accurate propagation measurement
    let _current_time_secs = crate::current_timestamp();
    let current_time_nanos = crate::current_timestamp_nanos();

    // Debug: Check if there's a delay in the task creation vs sending
    if let crate::registry::RegistryMessage::DeltaGossip { delta } = &task.message {
        for change in &delta.changes {
            if let crate::registry::RegistryChange::ActorAdded { location, .. } = change {
                let creation_time_nanos = location.wall_clock_time as u128 * 1_000_000_000;
                let delay_nanos = current_time_nanos as u128 - creation_time_nanos;
                let _delay_ms = delay_nanos as f64 / 1_000_000.0;
                // eprintln!("🔍 DELTA_SEND_DELAY: {}ms between delta creation and sending", delay_ms);
            }
        }
    }

    match &mut task.message {
        crate::registry::RegistryMessage::DeltaGossip { delta } => {
            delta.precise_timing_nanos = current_time_nanos;
            // Update wall_clock_time in all changes to current time for accurate propagation measurement
            for change in &mut delta.changes {
                match change {
                    crate::registry::RegistryChange::ActorAdded { location, .. } => {
                        // Set wall_clock_time to nanoseconds for consistent timing measurements
                        location.wall_clock_time = current_time_nanos / 1_000_000_000;
                    }
                    crate::registry::RegistryChange::ActorRemoved { .. } => {
                        // No wall_clock_time to update
                    }
                }
            }
        }
        crate::registry::RegistryMessage::FullSync { .. } => {
            // Full sync doesn't use precise timing
        }
        _ => {}
    }

    // Serialize the message AFTER updating timing
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&task.message)?;

    // Create message with Gossip type prefix
    let mut msg_with_type = Vec::with_capacity(crate::framing::GOSSIP_HEADER_LEN + data.len());
    msg_with_type.push(crate::MessageType::Gossip as u8);
    msg_with_type.extend_from_slice(&[0u8; 3]);
    msg_with_type.extend_from_slice(&data);

    // Use zero-copy tell() which uses try_send() internally for max performance
    // This completely bypasses async overhead when the channel has capacity
    let tcp_start = std::time::Instant::now();
    conn.tell(msg_with_type.as_slice()).await?;
    let _tcp_elapsed = tcp_start.elapsed();
    // eprintln!("🔍 TCP_WRITE_TIME: {:?}", tcp_elapsed);
    Ok(())
}
