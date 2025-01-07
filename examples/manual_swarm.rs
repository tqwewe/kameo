use std::{borrow::Cow, env::args, time::Duration};

use kameo::{
    actor::ActorID,
    error::RemoteSendError,
    remote::{
        ActorSwarm, ActorSwarmBehaviour, ActorSwarmEvent, SwarmBehaviour, SwarmReq, SwarmResp,
    },
};
use libp2p::{
    kad::{
        self,
        store::{MemoryStore, RecordStore},
    },
    mdns,
    request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = args()
        .nth(1)
        .expect("expected the listen port as an argument")
        .parse()
        .expect("invalid listen port");

    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let kademlia = kad::Behaviour::new(
                key.public().to_peer_id(),
                MemoryStore::new(key.public().to_peer_id()),
            );
            Ok(CustomBehaviour {
                kademlia,
                actor_request_response: request_response::cbor::Behaviour::new(
                    [(StreamProtocol::new("/kameo/1"), ProtocolSupport::Full)],
                    request_response::Config::default(),
                ),
                custom_request_response: request_response::cbor::Behaviour::new(
                    [(StreamProtocol::new("/custom/1"), ProtocolSupport::Full)],
                    request_response::Config::default(),
                ),
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    let (actor_swarm, mut actor_behaviour) =
        ActorSwarm::bootstrap_manual(swarm.local_peer_id().clone()).unwrap();

    actor_swarm.listen_on(format!("/ip4/0.0.0.0/udp/{port}/quic-v1").parse()?);

    loop {
        tokio::select! {
            Some(cmd) = actor_behaviour.next_command() => actor_behaviour.handle_command(&mut swarm, cmd),
            Some(event) = swarm.next() => {
                handle_event(&mut swarm, &mut actor_behaviour, event);
            }
        }
    }
}

fn handle_event(
    swarm: &mut Swarm<CustomBehaviour>,
    actor_behaviour: &mut ActorSwarmBehaviour,
    event: SwarmEvent<CustomBehaviourEvent>,
) {
    match event {
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            let name = swarm.local_peer_id().to_string();
            swarm
                .behaviour_mut()
                .custom_request_response
                .send_request(&peer_id, CustomReq::Greet { name });
        }
        SwarmEvent::Behaviour(event) => match event {
            CustomBehaviourEvent::Kademlia(event) => {
                actor_behaviour.handle_event(swarm, ActorSwarmEvent::Kademlia(event))
            }
            CustomBehaviourEvent::ActorRequestResponse(event) => {
                actor_behaviour.handle_event(swarm, ActorSwarmEvent::RequestResponse(event))
            }
            CustomBehaviourEvent::Mdns(event) => {
                actor_behaviour.handle_event(swarm, ActorSwarmEvent::Mdns(event))
            }
            CustomBehaviourEvent::CustomRequestResponse(request_response::Event::Message {
                message,
                ..
            }) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => match request {
                    CustomReq::Greet { name } => {
                        swarm
                            .behaviour_mut()
                            .custom_request_response
                            .send_response(
                                channel,
                                CustomResp::Greeted {
                                    msg: format!("Hello, {name}"),
                                },
                            )
                            .unwrap();
                    }
                },
                request_response::Message::Response { response, .. } => match response {
                    CustomResp::Greeted { msg } => {
                        println!("Greeted: {msg}");
                    }
                },
            },
            _ => {}
        },
        _ => {}
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum CustomReq {
    Greet { name: String },
}

#[derive(Debug, Serialize, Deserialize)]
enum CustomResp {
    Greeted { msg: String },
}

#[derive(NetworkBehaviour)]
struct CustomBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    actor_request_response: request_response::cbor::Behaviour<SwarmReq, SwarmResp>,
    custom_request_response: request_response::cbor::Behaviour<CustomReq, CustomResp>,
    mdns: mdns::tokio::Behaviour,
}

impl SwarmBehaviour for CustomBehaviour {
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
        self.actor_request_response.send_request(
            peer,
            SwarmReq::Ask {
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
        self.actor_request_response.send_request(
            peer,
            SwarmReq::Tell {
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
        channel: ResponseChannel<kameo::remote::SwarmResp>,
        result: Result<Vec<u8>, RemoteSendError<Vec<u8>>>,
    ) -> Result<(), SwarmResp> {
        self.actor_request_response
            .send_response(channel, SwarmResp::Ask(result))
    }

    fn send_tell_response(
        &mut self,
        channel: ResponseChannel<kameo::remote::SwarmResp>,
        result: Result<(), RemoteSendError<Vec<u8>>>,
    ) -> Result<(), SwarmResp> {
        self.actor_request_response
            .send_response(channel, SwarmResp::Tell(result))
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
