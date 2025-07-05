use std::task;

use either::Either;
use futures::{stream::FuturesUnordered, StreamExt};
use libp2p::{
    core::{transport::PortUse, Endpoint},
    swarm::{
        ConnectionClosed, ConnectionDenied, ConnectionHandler, ConnectionHandlerSelect,
        ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent, THandlerOutEvent,
        ToSwarm,
    },
    Multiaddr, PeerId,
};
use tokio::sync::mpsc;

use crate::error::{ActorStopReason, SwarmAlreadyBootstrappedError};

use super::{
    messaging, registry, ActorSwarm, RemoteRegistryActorRef, SwarmCommand, REMOTE_REGISTRY,
};

/// A network behaviour that combines messaging and registry capabilities for remote actor communication.
///
/// This behaviour integrates Kameo's remote actor functionality with libp2p, providing:
/// - Actor registration and discovery through a Kademlia-based registry
/// - Remote message passing between actors across the network
/// - Automatic lifecycle management for remote connections
///
/// # Example
///
/// ```rust
/// use kameo::remote;
/// use libp2p::{swarm::NetworkBehaviour, PeerId};
///
/// #[derive(NetworkBehaviour)]
/// struct MyBehaviour {
///     kameo: remote::Behaviour,
///     // other behaviours...
/// }
///
/// let peer_id = PeerId::random();
/// let behaviour = remote::Behaviour::new(peer_id, remote::messaging::Config::default());
/// ```
#[allow(missing_debug_implementations)]
pub struct Behaviour {
    /// Messaging behaviour for sending and receiving actor messages.
    pub messaging: messaging::Behaviour,
    /// Registry behaviour for actor registration and discovery.
    pub registry: registry::Behaviour,
    local_peer_id: PeerId,
    cmd_tx: mpsc::UnboundedSender<SwarmCommand>,
    cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
}

impl Behaviour {
    /// Creates a new remote behaviour with the specified peer ID and messaging configuration.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The peer ID of the local node
    /// * `messaging_config` - Configuration for the messaging subsystem
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::remote;
    /// use libp2p::PeerId;
    ///
    /// let peer_id = PeerId::random();
    /// let config = remote::messaging::Config::default();
    /// let behaviour = remote::Behaviour::new(peer_id, config);
    /// ```
    pub fn new(local_peer_id: PeerId, messaging_config: messaging::Config) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let messaging = messaging::Behaviour::new(local_peer_id, messaging_config);
        let registry = registry::Behaviour::new(local_peer_id);

        Behaviour {
            messaging,
            registry,
            local_peer_id,
            cmd_tx,
            cmd_rx,
        }
    }

    /// Initializes the global actor swarm for this behaviour, panicking if its already been initialized.
    ///
    /// This method sets up the global communication channel that allows local actors
    /// to interact with the remote system. It must be called before any remote actor
    /// operations can be performed.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kameo::remote;
    /// use libp2p::PeerId;
    ///
    /// let peer_id = PeerId::random();
    /// let behaviour = remote::Behaviour::new(peer_id, remote::messaging::Config::default());
    ///
    /// // Initialize the global swarm
    /// behaviour.init_global();
    /// ```
    ///
    /// # Panics
    ///
    /// This method panics if another behaviour has already been initialized.
    pub fn init_global(&self) {
        self.try_init_global().unwrap()
    }

    /// Initializes the global actor swarm for this behaviour, returning an error if its already been initialized.
    ///
    /// # Returns
    ///
    /// Returns `Ok` if initialization succeeds, or `Err` if the global
    /// swarm has already been initialized by another behaviour instance.
    ///
    /// # Errors
    ///
    /// This method will return an error if `init_global()` has already been called
    /// on another `Behaviour` instance in the same process.
    pub fn try_init_global(&self) -> Result<(), SwarmAlreadyBootstrappedError> {
        ActorSwarm::set(self.cmd_tx.clone(), self.local_peer_id)
            .map_err(|_| SwarmAlreadyBootstrappedError)?;
        Ok(())
    }

    fn handle_command(&mut self, cmd: SwarmCommand) -> bool {
        match cmd {
            SwarmCommand::Lookup { name, reply } => {
                self.registry.lookup_with_reply(name, Some(reply));
                true // We triggered a get_providers on kad, we need to call the waker
            }
            SwarmCommand::LookupLocal { name, reply } => {
                let _ = reply.send(self.registry.lookup_local(&name));
                false // Local lookups don't need to be polled again
            }
            SwarmCommand::Register {
                name,
                registration,
                reply,
            } => match self
                .registry
                .register_with_reply(name, registration, Some(reply))
            {
                Ok(_) => true, // We started a new lookup
                Err((Some(reply), err)) => {
                    let _ = reply.send(Err(err.into()));
                    false
                }
                Err((None, _)) => unreachable!("we should have the reply type here"),
            },
            SwarmCommand::Unregister { name, reply } => {
                self.registry.unregister(&name);
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
                self.messaging.ask_with_reply(
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
                self.messaging.tell_with_reply(
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
                self.messaging.link_with_reply(
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
                self.messaging.unlink_with_reply(
                    sibbling_id,
                    sibbling_remote_id,
                    actor_id,
                    Some(reply),
                );
                true
            }
            SwarmCommand::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
                reply,
            } => {
                self.messaging.signal_link_died_with_reply(
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
}

/// Events emitted by the remote behaviour.
///
/// These events are generated by the underlying messaging and registry behaviours
/// and provide information about remote actor operations, network changes, and
/// communication status.
///
/// # Example
///
/// ```no_run
/// use kameo::remote;
/// use libp2p::swarm::SwarmEvent;
/// #
/// # let swarm_event: SwarmEvent<remote::Event> = todo!();
///
/// // In your swarm event loop:
/// match swarm_event {
///     SwarmEvent::Behaviour(remote::Event::Registry(registry_event)) => {
///         // Handle registry events (actor registration, lookup, etc.)
///     }
///     SwarmEvent::Behaviour(remote::Event::Messaging(messaging_event)) => {
///         // Handle messaging events (message delivery, timeouts, etc.)
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug)]
pub enum Event {
    /// An event from the messaging subsystem.
    ///
    /// These events relate to sending and receiving messages between remote actors,
    /// including delivery confirmations, timeouts, and connection status updates.
    Messaging(messaging::Event),

    /// An event from the registry subsystem.
    ///
    /// These events relate to actor registration and discovery operations,
    /// including successful registrations, lookup results, and network topology changes.
    Registry(registry::Event),
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler =
        ConnectionHandlerSelect<THandler<messaging::Behaviour>, THandler<registry::Behaviour>>;
    type ToSwarm = Event;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.messaging
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)?;

        self.registry
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)?;

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
            self.messaging.handle_established_inbound_connection(
                connection_id,
                peer,
                local_addr,
                remote_addr,
            )?,
            self.registry.handle_established_inbound_connection(
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

        combined_addresses.extend(self.messaging.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )?);

        combined_addresses.extend(self.registry.handle_pending_outbound_connection(
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
            self.messaging.handle_established_outbound_connection(
                connection_id,
                peer,
                addr,
                role_override,
                port_use,
            )?,
            self.registry.handle_established_outbound_connection(
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
            Either::Left(ev) => {
                self.messaging
                    .on_connection_handler_event(peer_id, connection_id, ev)
            }
            Either::Right(ev) => {
                self.registry
                    .on_connection_handler_event(peer_id, connection_id, ev)
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        match self.cmd_rx.poll_recv(cx) {
            task::Poll::Ready(Some(cmd)) => {
                if self.handle_command(cmd) {
                    cx.waker().wake_by_ref();
                }
            }
            task::Poll::Ready(None) => {}
            task::Poll::Pending => {}
        }

        match self.messaging.poll(cx) {
            task::Poll::Ready(ev) => {
                return task::Poll::Ready(ev.map_in(Either::Left).map_out(Event::Messaging))
            }
            task::Poll::Pending => {}
        }

        match self.registry.poll(cx) {
            task::Poll::Ready(ev) => {
                return task::Poll::Ready(ev.map_in(Either::Right).map_out(Event::Registry))
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

        self.messaging.on_swarm_event(event);
        self.registry.on_swarm_event(event);
    }
}
