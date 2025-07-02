use libp2p::{
    kad::{self, store::MemoryStore},
    request_response,
    swarm::NetworkBehaviour,
};

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    /// Kademlia network for actor registration.
    pub kademlia: kad::Behaviour<MemoryStore>,
    /// Request response for actor messaging.
    pub request_response: request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>,
}

// impl NetworkBehaviour for Behaviour {
//     type ConnectionHandler;

//     type ToSwarm;

//     fn handle_established_inbound_connection(
//         &mut self,
//         _connection_id: libp2p::swarm::ConnectionId,
//         peer: libp2p::PeerId,
//         local_addr: &libp2p::Multiaddr,
//         remote_addr: &libp2p::Multiaddr,
//     ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
//         todo!()
//     }

//     fn handle_established_outbound_connection(
//         &mut self,
//         _connection_id: libp2p::swarm::ConnectionId,
//         peer: libp2p::PeerId,
//         addr: &libp2p::Multiaddr,
//         role_override: libp2p::core::Endpoint,
//         port_use: libp2p::core::transport::PortUse,
//     ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
//         todo!()
//     }

//     fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
//         todo!()
//     }

//     fn on_connection_handler_event(
//         &mut self,
//         _peer_id: libp2p::PeerId,
//         _connection_id: libp2p::swarm::ConnectionId,
//         _event: libp2p::swarm::THandlerOutEvent<Self>,
//     ) {
//         todo!()
//     }

//     fn poll(&mut self, cx: &mut std::task::Context<'_>)
//         -> std::task::Poll<libp2p::swarm::ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
//         todo!()
//     }
// }
