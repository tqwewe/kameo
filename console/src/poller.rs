use std::{
    io::{self, Read, Write},
    mem,
    net::{Shutdown, SocketAddr, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use kameo::console::wire::{Message, Snapshot};

use crate::ConnectionState;

/// Caps a snapshot frame so a misbehaving or wrong-protocol peer can't make us allocate
/// unbounded memory.
const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

pub fn spawn_poller(
    addr: SocketAddr,
    interval: Arc<AtomicU64>,
    connection_timeout: Duration,
    snapshot: Arc<Mutex<Option<Snapshot>>>,
    connection_state: Arc<Mutex<ConnectionState>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let poller = connect_loop(&addr, connection_timeout, &snapshot, &connection_state);
            poll_loop(poller, &interval, &connection_state);
        }
    })
}

fn connect_loop(
    addr: &SocketAddr,
    connection_timeout: Duration,
    snapshot: &Arc<Mutex<Option<Snapshot>>>,
    connection_state: &Arc<Mutex<ConnectionState>>,
) -> Poller {
    loop {
        *connection_state.lock().unwrap() = ConnectionState::Connecting;
        match Poller::connect(addr, connection_timeout, Arc::clone(snapshot)) {
            Ok(poller) => {
                *connection_state.lock().unwrap() = ConnectionState::Connected;
                return poller;
            }
            Err(err) => {
                *connection_state.lock().unwrap() = ConnectionState::Disconnected {
                    error: format!("{err}"),
                    since: Instant::now(),
                };
                thread::sleep(Duration::from_secs(5));
            }
        }
    }
}

fn poll_loop(
    mut poller: Poller,
    interval: &Arc<AtomicU64>,
    connection_state: &Arc<Mutex<ConnectionState>>,
) {
    loop {
        let start = Instant::now();
        match poller.poll() {
            Ok(()) => {
                let interval = Duration::from_millis(interval.load(Ordering::Relaxed));
                let sleep_duration = interval.saturating_sub(start.elapsed());
                thread::sleep(sleep_duration);
            }
            Err(err) => {
                *connection_state.lock().unwrap() = ConnectionState::Disconnected {
                    error: format!("{err}"),
                    since: Instant::now(),
                };
                let _ = poller.disconnect();
                mem::drop(poller);
                return;
            }
        }
    }
}

struct Poller {
    stream: TcpStream,
    snapshot: Arc<Mutex<Option<Snapshot>>>,
}

impl Poller {
    fn connect(
        addr: &SocketAddr,
        connection_timeout: Duration,
        snapshot: Arc<Mutex<Option<Snapshot>>>,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect_timeout(addr, connection_timeout)?;
        stream.set_read_timeout(Some(connection_timeout.max(Duration::from_secs(1))))?;
        stream.set_write_timeout(Some(connection_timeout.max(Duration::from_secs(1))))?;
        Ok(Poller { stream, snapshot })
    }

    fn disconnect(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }

    fn poll(&mut self) -> io::Result<()> {
        self.stream.write_all(&[0])?;

        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len)?;
        let len = u32::from_be_bytes(len);
        if len > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot frame too large ({len} bytes)"),
            ));
        }

        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf)?;

        let Message::Snapshot(snapshot) = rmp_serde::from_slice(&buf)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        *self.snapshot.lock().unwrap() = Some(snapshot);

        Ok(())
    }
}
