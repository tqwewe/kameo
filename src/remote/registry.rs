use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    fmt,
    num::NonZero,
    str::{self, FromStr},
    task,
    time::{Duration, Instant},
};

use libp2p::{
    kad::{self, store::RecordStore, StoreInserts},
    swarm::{
        ConnectionDenied, ConnectionId, DialError, DialFailure, FromSwarm, NetworkBehaviour,
        NewExternalAddrOfPeer, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId, StreamProtocol,
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    actor::{ActorId, ActorIdFromBytesError},
    error::RegistryError,
};

const PROTO_NAME: StreamProtocol = StreamProtocol::new("/kameo/registry/1.0.0");

type RegisterResult = Result<(), RegistryError>;
pub(super) type LookupResult = Result<ActorRegistration<'static>, RegistryError>;
type LookupLocalResult = Result<Option<ActorRegistration<'static>>, RegistryError>;

pub(super) type RegisterReply = oneshot::Sender<RegisterResult>;
pub(super) type LookupReply = mpsc::UnboundedSender<LookupResult>;
pub(super) type LookupLocalReply = oneshot::Sender<LookupLocalResult>;
pub(super) type UnregisterReply = oneshot::Sender<()>;

/// The events produced by the `Registry` behaviour.
///
/// See [`NetworkBehaviour::poll`].
#[derive(Debug)]
pub enum Event {
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

    /// The routing table has been updated with a new peer and / or
    /// address, thereby possibly evicting another peer.
    RoutingUpdated {
        /// The ID of the peer that was added or updated.
        peer: PeerId,
        /// Whether this is a new peer and was thus just added to the routing
        /// table, or whether it is an existing peer who's addresses changed.
        is_new_peer: bool,
        /// The full list of known addresses of `peer`.
        addresses: kad::Addresses,
        /// Returns the minimum inclusive and maximum inclusive distance for
        /// the bucket of the peer.
        bucket_range: (kad::KBucketDistance, kad::KBucketDistance),
        /// The ID of the peer that was evicted from the routing table to make
        /// room for the new peer, if any.
        old_peer: Option<PeerId>,
    },

    /// A peer has connected for whom no listen address is known.
    ///
    /// If the peer is to be added to the routing table, a known
    /// listen address for the peer must be provided via [`Behaviour::add_address`].
    UnroutablePeer {
        /// Peer ID.
        peer: PeerId,
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer has not been added to the routing table either
    /// because [`BucketInserts::Manual`] is configured or because
    /// the corresponding bucket is full.
    ///
    /// If the peer is to be included in the routing table, it must
    /// must be explicitly added via [`Behaviour::add_address`], possibly after
    /// removing another peer.
    ///
    /// See [`Behaviour::kbucket`] for insight into the contents of
    /// the k-bucket of `peer`.
    RoutablePeer {
        /// Peer ID.
        peer: PeerId,
        /// Address.
        address: Multiaddr,
    },

    /// A connection to a peer has been established for whom a listen address
    /// is known but the peer is only pending insertion into the routing table
    /// if the least-recently disconnected peer is unresponsive, i.e. the peer
    /// may not make it into the routing table.
    ///
    /// If the peer is to be unconditionally included in the routing table,
    /// it should be explicitly added via [`Behaviour::add_address`] after
    /// removing another peer.
    ///
    /// See [`Behaviour::kbucket`] for insight into the contents of
    /// the k-bucket of `peer`.
    PendingRoutablePeer {
        /// Peer ID.
        peer: PeerId,
        /// Address.
        address: Multiaddr,
    },
}

/// `Behaviour` is a `NetworkBehaviour` that implements the kameo registry behaviour
/// on top of the Kademlia protocol.
#[allow(missing_debug_implementations)]
pub struct Behaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    local_peer_id: PeerId,
    pending_events: VecDeque<Event>,
    registration_queries: HashMap<kad::QueryId, RegistrationQuery>,
    lookup_queries: HashMap<kad::QueryId, LookupQuery>,
}

impl Behaviour {
    /// Creates a new registry behaviour.
    pub fn new(local_peer_id: PeerId) -> Self {
        let mut config = kad::Config::new(PROTO_NAME);

        config.set_periodic_bootstrap_interval(Some(Duration::from_secs(15)));

        // Faster lookups for responsive actor discovery
        config.set_query_timeout(Duration::from_secs(10)); // Default: 60s

        // Lower replication for efficiency while maintaining availability
        config.set_replication_factor(NonZero::new(5).unwrap()); // Default: 20

        // Shorter TTL since actors are more dynamic than files
        config.set_record_ttl(Some(Duration::from_secs(3600))); // 1 hour, Default: 36 hours

        // More frequent re-publication for dynamic actors
        config.set_publication_interval(Some(Duration::from_secs(1800))); // 30 minutes, Default: 24 hours

        // Filter records to prevent registry pollution
        config.set_record_filtering(StoreInserts::FilterBoth); // Default: Unfiltered

        let mut kademlia = kad::Behaviour::with_config(
            local_peer_id,
            kad::store::MemoryStore::new(local_peer_id),
            config,
        );
        kademlia.set_mode(Some(kad::Mode::Server));

        Behaviour {
            kademlia,
            local_peer_id,
            pending_events: VecDeque::new(),
            registration_queries: HashMap::new(),
            lookup_queries: HashMap::new(),
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

    pub(super) fn register_with_reply(
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

    pub(super) fn lookup_with_reply(
        &mut self,
        name: String,
        reply: Option<LookupReply>,
    ) -> kad::QueryId {
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

    fn handle_kademlia_event(&mut self, ev: kad::Event) -> (bool, Option<Event>) {
        match ev {
            kad::Event::InboundRequest { request } => {
                match request {
                    kad::InboundRequest::AddProvider { record } => {
                        let record =
                            record.expect("filtering is enabled, so the record should be present");

                        if self.validate_provider_registration(&record) {
                            // Accept the provider registration
                            if let Err(err) = self.kademlia.store_mut().add_provider(record) {
                                #[cfg(feature = "tracing")]
                                tracing::warn!("failed to store provider: {err}");
                            }
                        } else {
                            println!("ignoring provider registration");
                        }

                        (false, None)
                    }
                    kad::InboundRequest::PutRecord { source, record, .. } => {
                        let record =
                            record.expect("filtering is enabled, so the record should be present");

                        if self.validate_metadata_record(&source, &record) {
                            // Store the metadata record
                            if let Err(err) = self.kademlia.store_mut().put(record) {
                                #[cfg(feature = "tracing")]
                                tracing::warn!("failed to store metadata record: {err}");
                            }
                        } else {
                            println!("ignoring record");
                        }

                        (false, None)
                    }
                    _ => (false, None),
                }
            }
            kad::Event::OutboundQueryProgressed {
                id,
                result,
                stats: _,
                step,
            } => match result {
                kad::QueryResult::Bootstrap(res) => {
                    #[cfg(feature = "tracing")]
                    if let Err(err) = res {
                        tracing::warn!("bootstrap failed: {err}");
                    }

                    (false, None)
                }
                kad::QueryResult::GetClosestPeers(_) => (false, None),
                // Getting the providers has progressed
                kad::QueryResult::GetProviders(res) => {
                    let Entry::Occupied(mut lookup_query_entry) = self.lookup_queries.entry(id)
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("ignoring GetProviders event for unknown lookup query");
                        return (false, None);
                    };
                    let lookup_query = lookup_query_entry.get_mut();

                    match res {
                        Ok(kad::GetProvidersOk::FoundProviders { providers, .. }) => {
                            let mut wake = false;
                            lookup_query.providers_finished = step.last;

                            for provider in providers {
                                wake |=
                                    lookup_query.get_metadata_record(&mut self.kademlia, &provider);
                            }

                            let last = step.last && lookup_query.is_finished();
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: id,
                                    });
                                }
                                lookup_query_entry.remove();
                            }

                            (wake, None)
                        }
                        Ok(kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. }) => {
                            lookup_query.providers_finished = step.last;
                            let last = step.last && lookup_query.is_finished();
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: id,
                                    });
                                }
                                lookup_query_entry.remove();
                            }

                            (false, None)
                        }
                        Err(kad::GetProvidersError::Timeout { .. }) => {
                            lookup_query.providers_finished = step.last;
                            let last = step.last && lookup_query.is_finished();
                            match &lookup_query.reply {
                                Some(tx) => {
                                    let _ = tx.send(Err(RegistryError::Timeout));
                                }
                                None => {
                                    self.pending_events.push_back(Event::LookupTimeout {
                                        provider_query_id: id,
                                    });
                                }
                            }
                            if last {
                                if lookup_query.reply.is_none() {
                                    self.pending_events.push_back(Event::LookupCompleted {
                                        provider_query_id: id,
                                    });
                                }
                                lookup_query_entry.remove();
                            }

                            (false, None)
                        }
                    }
                }
                // Registering an actor has progressed
                kad::QueryResult::StartProviding(res) => {
                    let Entry::Occupied(mut registration_query_entry) =
                        self.registration_queries.entry(id)
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            "ignoring StartProviding event for unknown registration query"
                        );
                        return (false, None);
                    };
                    let registration_query = registration_query_entry.get_mut();

                    match res {
                        Ok(kad::AddProviderOk { .. }) => {
                            let Some(registration) = registration_query.registration.take() else {
                                panic!("the registration should exist here");
                            };
                            registration_query.provider_result = Some(res.clone());

                            // Store the metadata record
                            let key =
                                format!("{}:meta:{}", registration_query.name, self.local_peer_id);
                            let registration_bytes = registration.into_bytes();
                            let record = kad::Record::new(key.into_bytes(), registration_bytes);

                            match self.kademlia.put_record(record, kad::Quorum::One) {
                                Ok(put_query_id) => {
                                    registration_query.put_query_id = Some(put_query_id);
                                }
                                Err(err) => {
                                    // Put record failed immediately
                                    match registration_query_entry.remove().reply {
                                        Some(tx) => {
                                            let _ = tx.send(Err(err.into()));
                                        }
                                        None => {
                                            self.pending_events.push_back(
                                                Event::RegistrationFailed {
                                                    provider_query_id: id,
                                                    error: err.into(),
                                                },
                                            );
                                        }
                                    }
                                }
                            }

                            (true, None)
                        }
                        Err(kad::AddProviderError::Timeout { .. }) => {
                            match registration_query_entry.remove().reply {
                                Some(tx) => {
                                    let _ = tx.send(Err(RegistryError::Timeout));
                                }
                                None => {
                                    self.pending_events.push_back(Event::RegistrationFailed {
                                        provider_query_id: id,
                                        error: RegistryError::Timeout,
                                    });
                                }
                            }

                            (false, None)
                        }
                    }
                }
                kad::QueryResult::RepublishProvider(_) => (false, None),
                // Getting a metadata record has progressed
                kad::QueryResult::GetRecord(res) => {
                    let Some((provider_query_id, lookup_query)) = self
                        .lookup_queries
                        .iter_mut()
                        .find(|(_, lookup_query)| lookup_query.has_metadata_query(&id))
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("ignoring GetRecord event for unknown lookup query");
                        return (false, None);
                    };
                    let provider_query_id = *provider_query_id;

                    match res {
                        Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                            record: kad::Record { value, .. },
                            ..
                        })) => {
                            let result = ActorRegistration::from_bytes(&value)
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
                                            get_query_id: id,
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
                                    let _ = tx.send(Err(RegistryError::QuorumFailed { quorum }));
                                }
                                None => {
                                    self.pending_events.push_back(Event::LookupProgressed {
                                        provider_query_id,
                                        get_query_id: id,
                                        result: Err(RegistryError::QuorumFailed { quorum }),
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
                                    get_query_id: id,
                                    result: Err(RegistryError::Timeout),
                                });
                            }
                        },
                    }

                    if step.last {
                        lookup_query.metadata_query_finished(&id);
                        let last = lookup_query.is_finished();

                        if last {
                            if lookup_query.reply.is_none() {
                                self.pending_events
                                    .push_back(Event::LookupCompleted { provider_query_id });
                            }
                            self.lookup_queries.remove(&provider_query_id);
                        }
                    }

                    (false, None)
                }
                // Putting a metadata record has progressed
                kad::QueryResult::PutRecord(res) => {
                    let Some(provider_query_id) =
                        self.registration_queries
                            .iter()
                            .find_map(|(query_id, reg)| {
                                if reg.put_query_id == Some(id) {
                                    Some(*query_id)
                                } else {
                                    None
                                }
                            })
                    else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("ignoring PutRecord event for unknown registration query");
                        return (false, None);
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
                                        metadata_query_id: id,
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

                    (false, None)
                }
                kad::QueryResult::RepublishRecord(_) => (false, None),
            },
            kad::Event::RoutingUpdated {
                peer,
                is_new_peer,
                addresses,
                bucket_range,
                old_peer,
            } => (
                false,
                Some(Event::RoutingUpdated {
                    peer,
                    is_new_peer,
                    addresses,
                    bucket_range,
                    old_peer,
                }),
            ),
            kad::Event::UnroutablePeer { peer } => (false, Some(Event::UnroutablePeer { peer })),
            kad::Event::RoutablePeer { peer, address } => {
                (false, Some(Event::RoutablePeer { peer, address }))
            }
            kad::Event::PendingRoutablePeer { peer, address } => {
                (false, Some(Event::PendingRoutablePeer { peer, address }))
            }
            kad::Event::ModeChanged { .. } => (false, None),
        }
    }

    fn validate_provider_registration(&mut self, provider: &kad::ProviderRecord) -> bool {
        let source = &provider.provider;

        // Should be valid UTF8
        let key_str = match std::str::from_utf8(provider.key.as_ref()) {
            Ok(s) => s,
            Err(_) => {
                tracing::warn!("invalid UTF-8 in provider key from {source}");
                return false;
            }
        };

        if !self.is_valid_actor_name(key_str) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid actor name in provider registration from {source}: {key_str}");
            return false;
        }

        if !self.is_valid_provider_record(provider) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid provider record from {source}");
            return false;
        }

        #[cfg(feature = "tracing")]
        tracing::debug!("validated provider registration for {key_str} from {source}");
        true
    }

    fn is_valid_provider_record(&self, provider: &kad::ProviderRecord) -> bool {
        if let Some(expires) = provider.expires {
            // Must not be expired
            if expires < Instant::now() {
                #[cfg(feature = "tracing")]
                tracing::warn!("provider record is already expired");
                return false;
            }
        }

        true
    }

    fn validate_metadata_record(&mut self, source: &PeerId, record: &kad::Record) -> bool {
        // Validate key format: "{name}:meta:{peer_id}"
        let key_str = match str::from_utf8(record.key.as_ref()) {
            Ok(s) => s,
            Err(_) => {
                #[cfg(feature = "tracing")]
                tracing::warn!("invalid UTF-8 in metadata record key from {source}");
                return false;
            }
        };

        let parts: Vec<&str> = key_str.splitn(2, ":meta:").collect();
        if parts.len() != 2 {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid metadata key format from {source}: {key_str}");
            return false;
        }

        let actor_name = parts[0];
        let peer_id_str = parts[1];

        if !self.is_valid_actor_name(actor_name) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid actor name from {source}: {actor_name}");
            return false;
        }

        // Parse and validate peer ID from key
        let record_peer_id = match PeerId::from_str(peer_id_str) {
            Ok(id) => id,
            Err(_) => {
                #[cfg(feature = "tracing")]
                tracing::warn!("invalid peer ID in metadata key from {source}: {peer_id_str}");
                return false;
            }
        };

        // Verify the peer_id in key matches the source (prevents impersonation)
        if record_peer_id != *source {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "peer ID mismatch: source {source} trying to register metadata for peer {record_peer_id}"
            );
            return false;
        }

        // Validate metadata format and size
        if !self.is_valid_metadata(&record.value) {
            #[cfg(feature = "tracing")]
            tracing::warn!("invalid metadata format from {source}");
            return false;
        }

        // Verify the source peer is actually providing this actor
        let actor_key = kad::RecordKey::new(&actor_name);
        let providers = self.kademlia.store_mut().providers(&actor_key);

        let is_provider = providers
            .iter()
            .any(|provider_record| provider_record.provider == *source);

        if !is_provider {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "peer {source} trying to register metadata for {actor_name} without being a provider"
            );
            return false;
        }

        #[cfg(feature = "tracing")]
        tracing::debug!("Validated metadata record for {actor_name} from {source}");

        true
    }

    fn is_valid_actor_name(&self, name: &str) -> bool {
        !name.is_empty()
    }

    fn is_valid_metadata(&self, data: &[u8]) -> bool {
        // Size check
        if data.len() > 64 * 1024 {
            // 64KB limit
            return false;
        }

        ActorRegistration::from_bytes(data).is_ok()
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = THandler<kad::Behaviour<kad::store::MemoryStore>>;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: libp2p::swarm::ConnectionId,
        peer: PeerId,
        local_addr: &libp2p::Multiaddr,
        remote_addr: &libp2p::Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.kademlia.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &libp2p::Multiaddr,
        role_override: libp2p::core::Endpoint,
        port_use: libp2p::core::transport::PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.kademlia.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        // We need to manually add the address, because kademlia doesn't do this by default (yet)
        // https://github.com/libp2p/rust-libp2p/issues/5313
        match event {
            FromSwarm::NewExternalAddrOfPeer(NewExternalAddrOfPeer { peer_id, addr }) => {
                self.kademlia.add_address(&peer_id, addr.clone());
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id: Some(peer_id),
                error: DialError::Transport(errors),
                ..
            }) => {
                for (addr, _err) in errors {
                    self.kademlia.remove_address(&peer_id, addr);
                }
            }
            _ => {}
        }

        self.kademlia.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.kademlia
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn poll(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        loop {
            // First priority: return any pending events
            if let Some(ev) = self.pending_events.pop_front() {
                // Wake if we have more pending events to process
                if !self.pending_events.is_empty() {
                    cx.waker().wake_by_ref();
                }
                return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
            }

            // Second priority: poll Kademlia for new events
            match self.kademlia.poll(cx) {
                task::Poll::Ready(ToSwarm::GenerateEvent(ev)) => {
                    let (wake, ev) = self.handle_kademlia_event(ev);

                    // If we have an immediate event to return, return it
                    if let Some(ev) = ev {
                        // Wake if requested by handler OR if we have pending events
                        if wake || !self.pending_events.is_empty() {
                            cx.waker().wake_by_ref();
                        }
                        return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
                    }

                    // No immediate event, but if wake was requested or we have pending events,
                    // continue the loop to process them
                    if wake || !self.pending_events.is_empty() {
                        continue;
                    }

                    // No immediate event and no wake needed, continue polling Kademlia
                    // in case it has more events queued
                    continue;
                }
                task::Poll::Ready(other_ev) => {
                    // Non-GenerateEvent from Kademlia (dial events, etc.)
                    if !self.pending_events.is_empty() {
                        cx.waker().wake_by_ref();
                    }

                    return task::Poll::Ready(
                        other_ev.map_out(|_| unreachable!("we handled GenerateEvent above")),
                    );
                }
                task::Poll::Pending => {
                    // Kademlia has no more work ready
                    // Final check: do we have any pending events that were added by the last handler call?
                    if !self.pending_events.is_empty() {
                        continue; // Go back to the top to process them
                    }

                    // Nothing left to do
                    return task::Poll::Pending;
                }
            }
        }
    }
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

/// Represents an actor registration in the distributed registry.
///
/// Contains the actor's unique ID and its remote type identifier,
/// which together allow remote peers to locate and communicate
/// with the actor.
#[derive(Debug)]
pub struct ActorRegistration<'a> {
    /// The unique identifier of the actor.
    pub actor_id: ActorId,
    /// The remote type identifier for the actor.
    pub remote_id: Cow<'a, str>,
}

impl<'a> ActorRegistration<'a> {
    /// Creates a new actor registration.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - The unique identifier of the actor
    /// * `remote_id` - The remote type identifier for the actor
    pub fn new(actor_id: ActorId, remote_id: Cow<'a, str>) -> Self {
        ActorRegistration {
            actor_id,
            remote_id,
        }
    }

    /// Serializes the actor registration into bytes for storage in the DHT.
    ///
    /// The format includes the peer ID length, actor ID bytes, and remote ID string.
    pub fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + 8 + 42 + self.remote_id.len());
        let actor_id_bytes = self.actor_id.to_bytes();
        let peer_id_len = (actor_id_bytes.len() - 8) as u8;
        bytes.extend_from_slice(&peer_id_len.to_le_bytes());
        bytes.extend_from_slice(&actor_id_bytes);
        bytes.extend_from_slice(self.remote_id.as_bytes());
        bytes
    }

    /// Deserializes an actor registration from bytes retrieved from the DHT.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The serialized registration data
    ///
    /// # Returns
    ///
    /// The deserialized actor registration, or an error if the data is invalid.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, InvalidActorRegistration> {
        if bytes.is_empty() {
            return Err(InvalidActorRegistration::EmptyActorRegistration);
        }

        let peer_id_bytes_len = u8::from_le_bytes(bytes[..1].try_into().unwrap()) as usize;
        let actor_id = ActorId::from_bytes(&bytes[1..1 + 8 + peer_id_bytes_len])?;
        let remote_id = std::str::from_utf8(&bytes[1 + 8 + peer_id_bytes_len..])
            .map_err(InvalidActorRegistration::InvalidRemoteIDUtf8)?;

        Ok(ActorRegistration::new(actor_id, Cow::Borrowed(remote_id)))
    }

    /// Converts a borrowed actor registration into an owned one.
    ///
    /// This is useful when you need to store the registration beyond
    /// the lifetime of the original borrowed data.
    pub fn into_owned(self) -> ActorRegistration<'static> {
        ActorRegistration::new(self.actor_id, Cow::Owned(self.remote_id.into_owned()))
    }
}

/// Errors that can occur when deserializing an actor registration.
#[derive(Debug)]
pub enum InvalidActorRegistration {
    /// The registration data is empty.
    EmptyActorRegistration,
    /// Failed to parse the actor ID from bytes.
    ActorId(ActorIdFromBytesError),
    /// The remote ID contains invalid UTF-8.
    InvalidRemoteIDUtf8(str::Utf8Error),
}

impl From<ActorIdFromBytesError> for InvalidActorRegistration {
    fn from(err: ActorIdFromBytesError) -> Self {
        InvalidActorRegistration::ActorId(err)
    }
}

impl fmt::Display for InvalidActorRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidActorRegistration::EmptyActorRegistration => {
                write!(f, "empty actor registration")
            }
            InvalidActorRegistration::ActorId(err) => err.fmt(f),
            InvalidActorRegistration::InvalidRemoteIDUtf8(err) => err.fmt(f),
        }
    }
}
