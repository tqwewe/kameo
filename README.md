# Kameo

**Simple tokio actors**

- ✅ No Macros
- ✅ Async Support
- ✅ Links Between Actors (`start_link`/`start_child`)
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
kameo = { version = "0.1", features = ["nightly"] }
```

### Defining an Actor

```rust
// Define the actor state
struct Counter {
  count: i64,
}

impl Actor for Counter {}

// Define messages
struct Inc(u32);

impl Message<Counter> for Inc {
    type Reply = Result<i64, Infallible>;

    async fn handle(self, state: &mut Counter) -> Self::Reply {
        state.count += self.0 as i64;
        Ok(state.count)
    }
}
```

<sup>
<a href="https://docs.rs/kameo/latest/kameo/trait.Actor.html" target="_blank">Actor</a>
 • 
<a href="https://docs.rs/kameo/latest/kameo/trait.Message.html" target="_blank">Message</a>
</sup>

Note, with the `nightly` feature flag enabled, this reply type can be `i64` directly without the result.


### Starting an Actor & Messaging

```rust
let counter_ref: ActorRef<Counter> = Counter { count: 0 }.start();

let count = counter_ref.send(Inc(42)).await?;
println!("Count is {count}");
```

<sup>
<a href="https://docs.rs/kameo/latest/kameo/trait.Actor.html#method.start" target="_blank">Actor::start</a>
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
