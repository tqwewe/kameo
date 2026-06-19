use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Duration,
};

use clap::Parser;
use kameo_console::{App, ConnectionState, spawn_poller};

/// Terminal monitor for kameo actor systems.
#[derive(Debug, Parser)]
#[command(name = "kameo-console", version, about, long_about = None)]
pub struct Args {
    /// Address of the kameo app's console collector to connect to.
    #[arg(default_value = "127.0.0.1:9999")]
    pub addr: SocketAddr,

    /// Initial interval between snapshot polls (adjustable at runtime with -/+).
    #[arg(short, long, default_value = "500ms", value_parser = humantime::parse_duration)]
    pub interval: Duration,

    /// Max time to wait when establishing/re-establishing the connection.
    #[arg(long, default_value = "2s", value_parser = humantime::parse_duration)]
    pub connect_timeout: Duration,

    /// Render hardcoded sample data instead of connecting to a running app.
    #[arg(long)]
    pub demo: bool,
}

fn main() -> color_eyre::Result<()> {
    let args = Args::parse();

    color_eyre::install()?;

    // In demo mode, host the example actor system in-process and connect to it over loopback,
    // so the console drives a real, live system instead of static sample data.
    let addr = if args.demo {
        spawn_demo_server()
    } else {
        args.addr
    };

    let snapshot = Arc::new(Mutex::new(None));
    let connection = Arc::new(Mutex::new(ConnectionState::Connecting));
    let interval = Arc::new(AtomicU64::new(args.interval.as_millis() as u64));
    spawn_poller(
        addr,
        Arc::clone(&interval),
        args.connect_timeout,
        Arc::clone(&snapshot),
        Arc::clone(&connection),
    );

    ratatui::run(|terminal| {
        // Installed here, inside `run`, so it wraps the panic hook ratatui set up in `init`.
        // Otherwise ratatui's hook restores the terminal (disabling raw mode) on the demo's
        // intentional actor panics and tears down the live UI out from under us.
        if args.demo {
            suppress_demo_panics();
        }
        App::live(addr, snapshot, connection, interval).run(terminal)
    })?;

    Ok(())
}

/// Thread-name prefix for the demo runtime's workers, used to filter intentional panics.
const DEMO_THREAD_PREFIX: &str = "kameo-demo-worker";

/// The demo deliberately panics actors (the console catches and restarts them via supervision)
/// to show off restarts. Those panics fire on the demo runtime's worker threads, and since the
/// demo shares this process with the TUI, the installed panic hook would otherwise run for them:
/// ratatui's hook restores the terminal (disabling raw mode) and color_eyre prints its report,
/// either of which tears down the live UI. Wrap the hook so panics from demo threads are
/// swallowed entirely, while a genuine crash on the main thread still restores and reports.
///
/// Must be installed *after* ratatui's hook (i.e. inside `ratatui::run`) so this wrapper sits
/// outermost and can short-circuit before ratatui restores the terminal.
fn suppress_demo_panics() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let from_demo = std::thread::current()
            .name()
            .is_some_and(|name| name.starts_with(DEMO_THREAD_PREFIX));
        if !from_demo {
            prev(info);
        }
    }));
}

/// Runs the demo actor system on a background tokio runtime, serving snapshots on an ephemeral
/// loopback port, and returns that address for the console to connect to.
fn spawn_demo_server() -> SocketAddr {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            // Named so the panic hook can tell intentional demo-actor panics apart from a
            // genuine console crash (see `suppress_demo_panics`).
            .thread_name(DEMO_THREAD_PREFIX)
            .build()
            .expect("failed to build demo runtime");
        runtime.block_on(async move {
            let console = kameo::console::serve("127.0.0.1:0")
                .await
                .expect("failed to start demo console server");
            addr_tx
                .send(console.local_addr())
                .expect("console exited before receiving the demo address");
            let _system = kameo::console::demo::spawn().await;
            // Keep the runtime (and the actor system) alive for the life of the process.
            std::future::pending::<()>().await;
        });
    });
    addr_rx.recv().expect("demo server failed to start")
}
