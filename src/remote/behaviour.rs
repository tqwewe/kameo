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

use crate::error::ActorStopReason;

use super::{
    messaging, registry, ActorSwarm, RemoteRegistryActorRef, SwarmCommand, REMOTE_REGISTRY,
};

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
    /// Messaging behaviour.
    pub messaging: messaging::Behaviour,
    /// Registry behaviour.
    pub registry: registry::Behaviour,
    local_peer_id: PeerId,
    cmd_tx: mpsc::UnboundedSender<SwarmCommand>,
    cmd_rx: mpsc::UnboundedReceiver<SwarmCommand>,
}

impl Behaviour {
    /// Creates a new swarm behavior for Kameo actor communication.
    ///
    /// This initializes the libp2p network behavior with Kademlia DHT for actor
    /// discovery and request-response protocol for actor messaging.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The peer ID for this node
    /// * `messaging` - Messaging behaviour
    /// * `registry` - Registry behaviour
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

    /// Initialize this behavior as the global remote handler.
    pub fn init_global(&self) -> Result<(), ActorSwarm> {
        ActorSwarm::set(self.cmd_tx.clone(), self.local_peer_id)
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

/// Events emitted by the Kameo swarm behavior.
///
/// These events represent the results of network operations, actor lookups,
/// registrations, and message passing between remote actors.
#[derive(Debug)]
pub enum Event {
    /// Messaging event.
    Messaging(messaging::Event),
    /// Registry event.
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
