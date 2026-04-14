/// Demonstrates dispatching a generated message enum to an actor via `ask`,
/// receiving a unified response enum back.
use kameo::prelude::*;

#[derive(Actor, Default)]
pub struct Counter {
    count: i64,
}

#[messages(enum = CounterMessage, replies = CounterResponse)]
impl Counter {
    /// Increment the counter and return the new value.
    #[message]
    pub fn inc(&mut self, amount: u32) -> i64 {
        self.count += i64::from(amount);
        self.count
    }

    /// Decrement the counter and return the new value.
    #[message]
    pub fn dec(&mut self, amount: u32) -> i64 {
        self.count -= i64::from(amount);
        self.count
    }

    /// Reset the counter to zero.
    #[message]
    pub fn reset(&mut self) {
        self.count = 0;
    }

    /// Return the current value without changing it.
    #[message]
    pub fn get(&self) -> i64 {
        self.count
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actor_ref = Counter::spawn(Counter::default());

    // Build a list of commands to replay — any variant of `CounterMessage`.
    // `inc` and `dec` carry fields (tuple variants); `reset` and `get` are unit variants.
    let commands: Vec<(&str, CounterMessage)> = vec![
        ("inc(10)", CounterMessage::Inc(Inc { amount: 10 })),
        ("inc(5)",  CounterMessage::Inc(Inc { amount: 5 })),
        ("dec(3)",  CounterMessage::Dec(Dec { amount: 3 })),
        ("get",     CounterMessage::Get),
        ("reset",   CounterMessage::Reset),
        ("get",     CounterMessage::Get),
    ];

    // Send each command through a single call site; handle each response by
    // matching on the unified `CounterResponse` enum.
    for (label, cmd) in commands {
        let response = actor_ref.ask(cmd).send().await?;
        match response {
            CounterResponse::Inc(v) | CounterResponse::Dec(v) | CounterResponse::Get(v) => {
                println!("{label:<10} -> {v}");
            }
            CounterResponse::Reset => {
                println!("{label:<10} -> (reset)");
            }
        }
    }

    Ok(())
}
