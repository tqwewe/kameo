# Kameo 🎬

[![Discord](https://img.shields.io/badge/Discord-5868e4?logo=discord&logoColor=white)](https://discord.gg/GMX4DV9fbk)
[![Book](https://img.shields.io/badge/Book-0B0d0e?logo=mdbook)](https://docs.page/tqwewe/kameo)
[![Sponsor](https://img.shields.io/badge/sponsor-ffffff?logo=githubsponsors)](https://github.com/sponsors/tqwewe)
[![Crates.io Version](https://img.shields.io/crates/v/kameo)](https://crates.io/crates/kameo)
[![docs.rs](https://img.shields.io/docsrs/kameo)](https://docs.rs/kameo)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/kameo)](https://crates.io/crates/kameo)
[![Crates.io License](https://img.shields.io/crates/l/kameo)](https://crates.io/crates/kameo)
[![GitHub Contributors](https://img.shields.io/github/contributors-anon/tqwewe/kameo)](https://github.com/tqwewe/kameo/graphs/contributors)

[![Kameo banner image](https://github.com/tqwewe/kameo/blob/main/docs/banner.png?raw=true)](https://github.com/tqwewe/kameo)

## Introduction

**Kameo** is a high-performance, lightweight Rust library for building fault-tolerant, asynchronous actor-based systems. Designed to scale from small, local applications to large, distributed systems, Kameo simplifies concurrent programming by providing a robust actor model that seamlessly integrates with Rust's async ecosystem.

Whether you're building a microservice, a real-time application, or an embedded system, Kameo offers the tools you need to manage concurrency, recover from failures, and scale efficiently.

## Key Features

- **Lightweight Actors**: Create actors that run in their own asynchronous tasks, leveraging Tokio for efficient concurrency.
- **Fault Tolerance**: Build resilient systems with supervision strategies that automatically recover from actor failures.
- **Flexible Messaging**: Supports both bounded and unbounded message channels, with backpressure management for load control.
- **Local and Distributed Communication**: Seamlessly send messages between actors, whether they're on the same node or across the network.
- **Panic Safety**: Actors are isolated; a panic in one actor doesn't bring down the whole system.
- **Type-Safe Interfaces**: Strong typing for messages and replies ensures compile-time correctness.
- **Easy Integration**: Compatible with existing Rust async code, and can be integrated into larger systems effortlessly.

## Why Kameo?

Kameo is designed to make concurrent programming in Rust approachable and efficient. By abstracting the complexities of async and concurrent execution, Kameo lets you focus on writing the business logic of your actors without worrying about the underlying mechanics.

Kameo is not just for distributed applications; it's equally powerful for local concurrent systems. Its flexible design allows you to start with a simple, single-node application and scale up to a distributed architecture when needed.

## Use Cases

- **Concurrent Applications**: Simplify the development of applications that require concurrency, such as web servers, data processors, or simulation engines.
- **Distributed Systems**: Build scalable microservices, distributed databases, or message brokers that require robust communication across nodes.
- **Real-Time Systems**: Ideal for applications where low-latency communication is critical, such as gaming servers, chat applications, or monitoring dashboards.
- **Embedded and IoT Devices**: Deploy lightweight actors on resource-constrained devices for efficient and reliable operation.
- **Fault-Tolerant Services**: Create services that need to remain operational even when parts of the system fail.

## Getting Started

### Installation

Add Kameo as a dependency in your `Cargo.toml` file:

```toml
[dependencies]
kameo = "0.19"
```

Alternatively, you can add it via command line:

```bash
cargo add kameo
```

## Basic Example

### Defining an Actor

```rust,ignore
use kameo::Actor;
use kameo::actor::Spawn;
use kameo::message::{Context, Message};

// Implement the actor
#[derive(Actor)]
struct Counter {
    count: i64,
}

// Define message
struct Inc { amount: i64 }

// Implement message handler
impl Message<Inc> for Counter {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.count += msg.amount;
        self.count
    }
}
```

### Spawning and Interacting with the Actor

```rust,ignore
// Spawn the actor and obtain its reference
let actor_ref = Counter::spawn(Counter { count: 0 });

// Send messages to the actor
let count = actor_ref.ask(Inc { amount: 42 }).await?;
assert_eq!(count, 42);
```

## Distributed Actor Communication

Kameo provides built-in support for distributed actors, allowing you to send messages across network boundaries as if they were local.

### Registering an Actor

```rust,ignore
// Spawn and register the actor
let actor_ref = MyActor::spawn(MyActor::default());
actor_ref.register("my_actor").await?;
```

### Looking Up and Messaging a Remote Actor

```rust,ignore
// Lookup the remote actor
if let Some(remote_actor_ref) = RemoteActorRef::<MyActor>::lookup("my_actor").await? {
    let count = remote_actor_ref.ask(&Inc { amount: 10 }).await?;
    println!("Incremented! Count is {count}");
}
```

### Under the Hood

Kameo uses [libp2p](https://libp2p.io) for peer-to-peer networking, enabling actors to communicate over various protocols (TCP/IP, WebSockets, QUIC, etc.) using multiaddresses. This abstraction allows you to focus on your application's logic without worrying about the complexities of network programming.

## Documentation and Resources

- **[API Documentation](https://docs.rs/kameo)**: Detailed information on Kameo's API.
- **[The Kameo Book](https://docs.page/tqwewe/kameo)**: Comprehensive guide with tutorials and advanced topics.
- **[Crate on Crates.io](https://crates.io/crates/kameo)**: Latest releases and version information.
- **[Community Discord](https://discord.gg/GMX4DV9fbk)**: Join the discussion, ask questions, and share your experiences.
- **[Comparing Rust Actor Libraries](https://theari.dev/blog/comparing-rust-actor-libraries/)**: Read a blog post comparing Actix, Coerce, Kameo, Ractor, and Xtra.

## Examples

Explore more examples in the [examples](https://github.com/tqwewe/kameo/tree/main/examples) directory of the repository.

- **Basic Actor**: How to define and interact with a simple actor.
- **Distributed Actors**: Setting up actors that communicate over the network.
- **Actor Pools**: Using an actor pool with the `ActorPool` actor.
- **PubSub Actors**: Using a pubsub pattern with the `PubSub` actor.
- **Attaching Streams**: Attaching streams to an actor.

## Contributing

We welcome contributions from the community! Here are ways you can contribute:

- **Reporting Issues**: Found a bug or have a feature request? [Open an issue](https://github.com/tqwewe/kameo/issues).
- **Improving Documentation**: Help make our documentation better by submitting pull requests.
- **Contributing Code**: Check out the [CONTRIBUTING.md](https://github.com/tqwewe/kameo/blob/main/CONTRIBUTING.md) for guidelines.
- **Join the Discussion**: Participate in discussions on [Discord](https://discord.gg/GMX4DV9fbk).

## Support

[![Sponsor](https://img.shields.io/badge/sponsor-ffffff?logo=githubsponsors)](https://github.com/sponsors/tqwewe)

If you find Kameo useful and would like to support its development, please consider [sponsoring me on GitHub](https://github.com/sponsors/tqwewe). Your support helps me maintain and improve the project!

### Special Thanks to Our Sponsors

A huge thank you to [**Caido Community**], [**vanhouc**], [**cawfeecoder**], and [**JaniM**] for supporting Kameo's development! 💖

<a href="https://github.com/caido-community"><img src="https://avatars.githubusercontent.com/u/168573261?s=100&v=4" width="100" height="100" alt="Caido Community"/></a>
&nbsp;&nbsp;&nbsp;
<a href="https://github.com/vanhouc"><img src="https://avatars.githubusercontent.com/u/3475140?s=100&v=4" width="100" height="100" alt="vanhouc"/></a>
&nbsp;&nbsp;&nbsp;
<a href="https://github.com/cawfeecoder"><img src="https://avatars.githubusercontent.com/u/10661697?s=100&v=4" width="100" height="100" alt="cawfeecoder"/></a>
&nbsp;&nbsp;&nbsp;
<a href="https://github.com/JaniM"><img src="https://avatars.githubusercontent.com/u/908675?s=100&v=4" width="100" height="100" alt="JaniM"/></a>

[**Caido Community**]: https://github.com/caido-community
[**vanhouc**]: https://github.com/vanhouc
[**cawfeecoder**]: https://github.com/cawfeecoder
[**JaniM**]: https://github.com/JaniM

## License

`kameo` is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

You may choose either license to use this software.

---

[Introduction](#introduction) | [Key Features](#key-features) | [Why Kameo?](#why-kameo) | [Use Cases](#use-cases) | [Getting Started](#getting-started) | [Basic Example](#basic-example) | [Distributed Actor Communication](#distributed-actor-communication) | [Examples](#examples) | [Documentation](#documentation-and-resources) | [Contributing](#contributing) | [Support](#support) | [License](#license)

