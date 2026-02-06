use kameo::prelude::*;

// Define the actor
#[derive(Actor)]
pub struct HelloWorldActor;

// Define the message
pub struct Greet(String);

// Implement the message handling for HelloWorldActor
impl Message<Greet> for HelloWorldActor {
    type Reply = (); // This actor sends no reply

    async fn handle(
        &mut self,
        Greet(greeting): Greet, // Destructure the Greet message to get the greeting string
        _: &mut Context<Self, Self::Reply>, // The message handling context
    ) -> Self::Reply {
        println!("{greeting}"); // Print the greeting to the console
    }
}

#[tokio::main] // Mark the entry point as an asynchronous main function
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a Result return type for error handling

    // Spawn the HelloWorldActor with an unbounded mailbox
    let actor_ref = <HelloWorldActor as Actor>::spawn(HelloWorldActor);

    // Send a Greet message to the actor
    actor_ref.tell(Greet("Hello, world!".to_string())).await?;

    Ok(())
}
