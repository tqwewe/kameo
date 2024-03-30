# Kameo

**Simple tokio actors**

- ✅ Async Support
- ✅ Links Between Actors (`spawn_link`/`spawn_child`)
- ✅ MPSC Unbounded Channel for Messaging
- ✅ Concurrent Queries
- ✅ Panic Safe

---

### Installing

Stable

```toml
[dependencies]
kameo = "0.1"
```

Nightly

```toml
[dependencies]
kameo = { version = "0.1", features = ["nightly"] }
```

### Defining an Actor without Macros

```rust
// Define the actor state
struct Counter {
    count: i64,
}

impl Actor for Counter {}

// Define messages
struct Inc { amount: u32 }

impl Message<Counter> for Inc {
    type Reply = Result<i64, Infallible>;

    async fn handle(self, state: &mut Counter) -> Self::Reply {
        state.count += self.0 as i64;
        Ok(state.count)
    }
}
```

### Defining an Actor with Macros

```rust
// Define the actor state
#[derive(Actor)]
struct Counter {
    count: i64,
}

// Define messages
#[actor]
impl Counter {
    #[message]
    fn inc(&mut self, amount: u32) -> Result<i64, Infallible> {
        self.count += amount as i64;
        Ok(self.count)
    }
}
```

<details>
  <summary>See generated macro code</summary>

```rust
// Derive Actor
impl kameo::Actor for Counter {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("Counter")
    }
}

// Messages
struct Inc { amount: u32 }

impl kameo::Message<Counter> for Inc {
    type Reply = Result<i64, Infallible>;

    async fn handle(self, state: &mut Counter) -> Self::Reply {
        state.inc(self.amount)
    }
}
```
</details>

<sup>
<a href="https://docs.rs/kameo/latest/kameo/derive.Actor.html" target="_blank">Actor</a>
 • 
<a href="https://docs.rs/kameo/latest/kameo/attr.actor.html" target="_blank">#[actor]</a>
</sup>

### Spawning an Actor & Messaging

```rust
use kameo::{Spawn, ActorRef};

let counter_ref: ActorRef<Counter> = Counter { count: 0 }.spawn();

let count = counter_ref.send(Inc(42)).await?;
println!("Count is {count}");
```

<sup>
<a href="https://docs.rs/kameo/latest/kameo/trait.Spawn.html#method.spawn" target="_blank">Spawn::spawn</a>
 • 
<a href="https://docs.rs/kameo/latest/kameo/trait.ActorRef.html#method.send" target="_blank">ActorRef::send</a>
</sup>

## Contributing

Contributions are welcome! Feel free to submit pull requests, create issues, or suggest improvements.

## License

`kameo` is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.
