# Kameo 🎬

[![Crates.io Version](https://img.shields.io/crates/v/kameo)](https://crates.io/crates/kameo)
[![docs.rs](https://img.shields.io/docsrs/kameo)](https://docs.rs/kameo)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/kameo)](https://crates.io/crates/kameo)
[![Crates.io License](https://img.shields.io/crates/l/kameo)](https://crates.io/crates/kameo)
[![GitHub Repo stars](https://img.shields.io/github/stars/tqwewe/kameo)](https://github.com/tqwewe/kameo)

[![Kameo banner image](https://github.com/tqwewe/kameo/blob/main/banner.png?raw=true)](https://github.com/tqwewe/kameo)

## What is Kameo

Kameo is a Rust library to build fault-tolerant async actors in Rust.

## Feature highlights

* Async Rust: tokio task per actor
* Supervision: link actors together
* Remote messaging: message actors between nodes
* Panic safe: gracefully handled panics
* Back pressure: supports bounded & unbounded mpsc messaging


## Reasons to use Kameo

* **Distributed Systems**: Use Kameo when building distributed systems that require seamless communication between actors across multiple nodes, leveraging its built-in support for remote actor messaging and registration.
* **Concurrency with Simplicity**: Choose Kameo for projects that need efficient concurrency management without the complexity of manually handling threads, as it provides a straightforward API for spawning and managing actors with Tokio.
* **Scalability**: Kameo is ideal for applications that need to scale horizontally, thanks to its actor pools and remote actor support, making it easy to scale workloads across multiple machines.


# Getting Started

Add kameo as a dependency to your crate.

```bash
cargo add kameo
```

Define an actor and some messages.

```rust
use kameo::Actor;
use kameo::message::{Context, Message};
use kameo::request::MessageSend;

#[derive(Actor)]
struct Counter {
    count: i64,
}

struct Inc { amount: u32 }

impl Message<Inc> for Counter {
    type Reply = i64;

    async fn handle(&mut self, msg: Counter, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.count += msg.0 as i64;
        self.count
    }
}
```

Spawn and message the actor.

```rust
let actor_ref = kameo::spawn(Counter { count: 0 });
let count = actor_ref.ask(Inc { amount: 42 }).send().await?;
assert_eq!(count, 42);
```

## Benchmarks

**13x higher throughput when compared with Actix**

![benchmark](https://github.com/tqwewe/kameo/raw/main/benchmark.svg)

Above shows a basic benchmark for sending a message to an actor in Kameo and Actix.
Always benchmark for yourself.

## Contributing

Contributions are welcome! Feel free to submit pull requests, create issues, or suggest improvements.

## License

`kameo` is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.
