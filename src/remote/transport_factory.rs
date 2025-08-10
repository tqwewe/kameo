//! Transport factory for creating kameo_remote transport

use super::transport::{RemoteTransport, TransportConfig};
use super::message_handler::RemoteMessageHandler;
use super::kameo_transport::KameoTransport;

/// Creates a kameo_remote transport instance
/// 
/// This sets up the transport with the message handler for distributed actors.
pub fn create_transport(config: TransportConfig) -> Box<KameoTransport> {
    let mut transport = Box::new(KameoTransport::new(config));
    
    // Set up the message handler
    let handler = Box::new(RemoteMessageHandler::new());
    transport.set_message_handler(handler);
    
    transport
}