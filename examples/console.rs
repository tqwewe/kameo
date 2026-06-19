//! Demonstrates the `console` feature: a busy, instrumented actor system serving live snapshots
//! to the kameo console over TCP.
//!
//! The actor system itself lives in `kameo::console::demo` so the `kameo_console --demo` mode can
//! run the exact same thing in-process. It exercises a supervision tree, varied throughput,
//! mailbox backpressure, restarts, a slow handler, and a deadlock.
//!
//! Run with:
//!
//! ```not_rust
//! cargo run --example console --features console
//! ```
//!
//! then, in another terminal, point the console at it:
//!
//! ```not_rust
//! cargo run -p kameo_console
//! ```

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _console = kameo::console::serve("127.0.0.1:9999").await?;
    println!("serving console on 127.0.0.1:9999 — connect with `cargo run -p kameo_console`");

    // Spawn the demo actor system and keep it alive until ctrl-c.
    let _system = kameo::console::demo::spawn().await;

    tokio::signal::ctrl_c().await?;
    Ok(())
}
