# Kameo 🎬

[![Discord](https://img.shields.io/badge/Discord-5868e4?logo=discord&logoColor=white)](https://discord.gg/GMX4DV9fbk)
[![Book](https://img.shields.io/badge/Book-0B0d0e?logo=mdbook)](https://docs.page/tqwewe/kameo)
[![Sponsor](https://img.shields.io/badge/sponsor-ffffff?logo=githubsponsors)](https://github.com/sponsor/tqwewe)
[![Crates.io Version](https://img.shields.io/crates/v/kameo)](https://crates.io/crates/kameo)
[![docs.rs](https://img.shields.io/docsrs/kameo)](https://docs.rs/kameo)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/kameo)](https://crates.io/crates/kameo)
[![Crates.io License](https://img.shields.io/crates/l/kameo)](https://crates.io/crates/kameo)
[![GitHub Contributors](https://img.shields.io/github/contributors-anon/tqwewe/kameo)](https://github.com/tqwewe/kameo/graphs/contributors)
[![GitHub Stars](https://img.shields.io/github/stars/tqwewe/kameo)](https://github.com/tqwewe/kameo/stargazers)

[![Kameo banner image](https://github.com/tqwewe/kameo/blob/main/banner.png?raw=true)](https://github.com/tqwewe/kameo)

## What is Kameo

Kameo is a lightweight Rust library for building fault-tolerant, distributed, and asynchronous actors. It allows seamless communication between actors across nodes, providing [scalability](#use-cases), [backpressure](#feature-highlights), and panic recovery for robust distributed systems.

## Feature Highlights

- **Async Rust**: Each actor runs as a separate Tokio task, making concurrency easy and efficient.
- **Supervision**: Link actors to create a fault-tolerant, self-healing actor hierarchy.
- **Remote Messaging**: Send messages to actors on different nodes seamlessly.
- **Panic Safety**: Panics are gracefully handled, allowing the system to recover and continue running.
- **Backpressure Management**: Supports both bounded and unbounded mpsc messaging for handling load effectively.

## Use Cases

Kameo is versatile and can be applied in various domains, such as:

- **Distributed Microservices**: Build resilient microservices that communicate reliably over a distributed network.
- **Real-Time Systems**: Ideal for building chat systems, multiplayer games, or real-time monitoring dashboards where low-latency communication is essential.
- **IoT Devices**: Deploy lightweight actors on low-resource IoT devices for seamless networked communication.

# Getting Started

### Prerequisites

- Rust installed (check [rustup](https://rustup.rs) for installation instructions)
- A basic understanding of asynchronous programming in Rust

## Installation

Add kameo as a dependency in your `Cargo.toml` file:

```bash
cargo add kameo
```

## Example: Defining an Actor

```rust
use kameo::Actor;
use kameo::message::{Context, Message};
use kameo::request::MessageSend;

// Define an actor that will keep a count
#[derive(Actor)]
struct Counter {
    count: i64,
}

// Define the message for incrementing the count
struct Inc { amount: i64 }

// Implement how the actor will handle incoming messages
impl Message<Inc> for Counter {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.count += msg.amount;
        self.count
    }
}
```

Spawn and message the actor.

```rust
// Spawn the actor and get a reference to it
let actor_ref = kameo::spawn(Counter { count: 0 });

// Use the actor reference to send a message and receive a reply
let count = actor_ref.ask(Inc { amount: 42 }).send().await?;
assert_eq!(count, 42);
```

## Distributed Actor Communication

Kameo supports seamless messaging between actors across distributed nodes. This makes it easy to build distributed systems with actors that can communicate over the network.

### How Distributed Actors Work

Distributed actors in Kameo are designed to interact with each other as if they were local, but messages are sent over the network. This is achieved through the following steps:

1. **Actor Registration:** Actors can be registered under a unique name using `ActorRef::register`. Once registered, other actors or systems can look up this actor from a remote node. Under the hood, Kameo uses a **Kademlia Distributed Hash Table (DHT)**, provided by libp2p, to handle actor registration and lookup across nodes in a distributed manner.

   ```rust
   // On the host node, register the actor
   let actor_ref = kameo::spawn(MyActor::default());
   actor_ref.register("my_actor").await?;
   ```

2. **Actor Lookup:** On remote nodes, actors can look up registered actors using `RemoteActorRef::lookup`. This returns a reference to the remote actor that can be messaged.

   ```rust
   // On a guest node, lookup the remote actor
   let remote_actor_ref = RemoteActorRef::<MyActor>::lookup("my_actor").await?;
   ```

3. **Message Passing:** Once the remote actor reference is obtained, you can send messages to it just like with local actors. The message is serialized, sent over the network, deserialized on the remote node, and handled by the actor.

   ```rust
   if let Some(remote_actor_ref) = remote_actor_ref {
       let count = remote_actor_ref.ask(&Inc { amount: 10 }).send().await?;
       println!("Incremented! Count is {count}");
   }
   ```

4. **Message Registration:** In order to send messages between nodes, the message type must implement `Serialize` and `Deserialize`. Additionally, it needs to be annotated with the `#[remote_message("uuid")]` macro, where the `uuid` is a unique identifier for the message type. This UUID helps identify which message implementation to use when sending and receiving messages over the network. It's important to ensure that the UUID does not conflict with other registered messages in your crate.

   ```rust
   #[remote_message("3b9128f1-0593-44a0-b83a-f4188baa05bf")]
   impl Message<Inc> for MyActor {
       type Reply = i64;

       async fn handle(&mut self, msg: Inc, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
           self.count += msg.amount as i64;
           self.count
       }
   }
   ```

5. **Network Protocol:** Kameo uses the [libp2p](https://libp2p.io) library for networking, allowing actors to communicate over a variety of protocols. libp2p uses **multiaddresses**, which encode different addressing schemes such as TCP/IP, WebSockets, or QUIC. This flexibility allows Kameo to work seamlessly across different network layers.

    - **Multiaddresses**: A multiaddress specifies how to reach a peer using different protocols. For example, a TCP/IP multiaddress might look like this: `/ip4/198.51.100.0/tcp/6543`.
      Multiaddresses also support peer identity, preventing impersonation during communication, such as `/ip4/192.0.2.0/tcp/4321/p2p/QmcEPrat8ShnCph8WjkREzt5CPXF2RwhYxYBALDcLC1iV6`.

    Kameo actors use these multiaddresses when communicating across nodes. The `ActorSwarm` component handles networking, allowing actors to register, look up, and send messages to remote actors, abstracting away the underlying complexity.

    ```rust
    // Bootstrap the actor swarm and listen on a UDP port 8020
    ActorSwarm::bootstrap()?
        .listen_on("/ip4/0.0.0.0/udp/8020/quic-v1".parse()?)
        .await?;
    ```

See the [kameo book](https://docs.page/tqwewe/kameo/distributed-actors) for in-depth information on distributed actors in kameo.
Additionally, a full example can be found in [examples/remote.rs](examples/remote.rs).

## Additional Resources

- [Documentation](https://docs.rs/kameo)
- [The Kameo Book](https://docs.page/tqwewe/kameo)
- [Crates.io](https://crates.io/crates/kameo)
- [Discord](https://discord.gg/GMX4DV9fbk)

## Contributing

Contributions are welcome! Whether you are a beginner or an experienced Rust developer, there are many ways to contribute:

- Report issues or bugs
- Improve documentation
- Submit pull requests for new features or bug fixes
- Suggest new ideas in discussions

Join our community on [Discord](https://discord.gg/GMX4DV9fbk) to connect with fellow contributors!

## Support

[![Sponsor](https://img.shields.io/badge/sponsor-ffffff?logo=githubsponsors)](https://github.com/sponsor/tqwewe)

If you find Kameo useful and would like to support its development, please consider [sponsoring me on GitHub](https://github.com/sponsors/tqwewe). Your support helps me maintain and improve the project!

Thank you for your support! 💖

## License

`kameo` is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.

---

[Home](#kameo-) | [Features](#feature-highlights) | [Use Cases](#use-cases) | [Get Started](#getting-started) | [Distributed Actor Communication](#distributed-actor-communication) | [Additional Resources](#additional-resources) | [Contributing](#contributing) | [Support](#support) | [License](#license)
