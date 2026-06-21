# kameo console

A terminal UI for monitoring a running [kameo](https://github.com/tqwewe/kameo) actor system in
real time. It connects to your application over TCP and shows the live supervision tree, message
throughput, mailbox backpressure, restarts, and deadlocks.

![kameo console screenshot](https://github.com/tqwewe/kameo/blob/main/console/screenshot.png?raw=true)

## How it works

The console has two halves:

1. Your kameo application enables the `console` feature, which instruments every actor with a
   lightweight monitor, and calls `kameo::console::serve(...)` to expose snapshots over TCP.
2. The `kameo_console` binary connects to that address and renders the snapshots. Polling is
   pull based and happens on demand, so an idle application does no extra work.

The instrumentation lives behind the `console` cargo feature and has no cost when the feature is
turned off.

## Enabling it in your app

Add the feature:

```toml
[dependencies]
kameo = { version = "0.21", features = ["console"] }
```

Start the collector once, early in `main`:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let console = kameo::console::serve("127.0.0.1:9999").await?;

    // spawn and run your actors as usual...

    // Keep `console` in scope for as long as you want monitoring. Dropping the handle
    // detaches the server, which keeps running; call `console.shutdown()` to stop it.
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

`serve` returns a `ConsoleHandle`. For custom settings, use the builder:

```rust
let console = kameo::console::Console::builder()
    .grave_window(std::time::Duration::from_secs(10))
    .serve("127.0.0.1:9999")
    .await?;
```

The `grave_window` controls how long a stopped actor lingers in snapshots before it is dropped,
so short lived actors stay visible for at least one poll.

## Installing

Install the console from crates.io:

```sh
cargo install kameo_console
```

That puts a `kameo-console` binary on your `PATH`. Point it at your application's collector
address:

```sh
kameo-console 127.0.0.1:9999
```

Or change the poll interval at startup, and try it without a running application using the built
in demo data:

```sh
kameo-console 127.0.0.1:9999 --interval 250ms
kameo-console --demo
```

## Running from source

If you are working inside the kameo repository, run it with cargo instead of installing:

```sh
cargo run -p kameo_console
cargo run -p kameo_console -- 127.0.0.1:9999 --interval 250ms
cargo run -p kameo_console -- --demo
```

### Options

| Option | Default | Description |
| --- | --- | --- |
| `<addr>` (positional) | `127.0.0.1:9999` | Address of the application's console collector |
| `-i`, `--interval` | `500ms` | Snapshot poll interval, adjustable at runtime with `-` and `+` |
| `--connect-timeout` | `2s` | Maximum time to wait when establishing the connection |
| `--demo` | | Render built in sample data instead of connecting |

## What it shows

- **Supervision tree:** actors nested under their supervisors, collapsible per node.
- **Status:** Running, Restarting, and Dead, plus Busy and Stuck for slow handlers, and Deadlock
  for actors caught in a wait-for cycle.
- **Mailbox depth** with backpressure coloring: white when low, then yellow and red as a bounded
  mailbox fills.
- **Throughput:** messages per second, with a braille sparkline of each actor's recent activity.
- **Restarts and panics**, plus each supervised child's restart policy and limits.
- **Deadlock detection:** cycles in the wait-for graph, where actors block on each other.
- **Inspect panel** with per actor detail: current handler, mailbox, throughput history, links,
  supervisor, and a breakdown of messages by type.

## Keybindings

Press `?` inside the console for the full list. The essentials:

| Keys | Action |
| --- | --- |
| `↑` `k` / `↓` `j` | Move the selection |
| `Home` / `End` | Jump to first / last |
| `Space` | Collapse or expand the selected node |
| `c` / `e` | Collapse all / expand all |
| `←` `h` / `→` `l` | Collapse / expand the selected node |
| `Enter` | Open or close the inspect panel |
| `Tab` | Move focus into the inspect panel to scroll it |
| `/` | Filter actors by name |
| `i` `n` `s` `m` `g` `r` | Sort by the matching column (the highlighted letter in each header) |
| `-` / `+` | Decrease / increase the poll interval |
| `?` | Toggle this help |
| `q` / `Esc` | Quit |

## Try the example

The kameo repository ships a lively example that exercises every feature, with a supervision
tree, varied throughput, backpressure, restarts, and a deadlock:

```sh
# terminal 1: the instrumented application
cargo run --example console --features console

# terminal 2: the console
cargo run -p kameo_console
```

## Stability

The wire protocol between the application and the console is unstable and may change between
releases. Run a console build that matches the kameo version your application links against.

## License

`kameo_console` is dual-licensed under either:

- MIT License ([LICENSE-MIT](../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

You may choose either license to use this software.
