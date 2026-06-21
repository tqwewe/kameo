mod poller;
mod tui;

use std::time::Instant;

pub use poller::spawn_poller;
pub use tui::App;

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Disconnected { error: String, since: Instant },
}
