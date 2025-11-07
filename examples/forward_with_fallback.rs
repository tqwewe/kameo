use kameo::{error::Infallible, prelude::*};

#[derive(Debug)]
struct ComputeRequest {
    value: i32,
}

#[derive(Debug)]
struct ComputeError {
    message: String,
}

impl std::fmt::Display for ComputeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ComputeError {}

// Primary compute actor
#[derive(Default)]
struct PrimaryComputeActor;

impl Actor for PrimaryComputeActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

impl Message<ComputeRequest> for PrimaryComputeActor {
    type Reply = Result<i32, ComputeError>;

    async fn handle(
        &mut self,
        msg: ComputeRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Simulate computation
        if msg.value > 0 {
            Ok(msg.value * 2)
        } else {
            Err(ComputeError {
                message: "Cannot compute with negative or zero values".to_string(),
            })
        }
    }
}

// Gateway actor that forwards requests with fallback capability
#[derive(Default)]
struct GatewayActor {
    primary_compute: Option<ActorRef<PrimaryComputeActor>>,
}

impl Actor for GatewayActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

impl Message<ComputeRequest> for GatewayActor {
    type Reply = ForwardedReply<ComputeRequest, Result<i32, ComputeError>>;

    async fn handle(
        &mut self,
        msg: ComputeRequest,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Check if we have a primary compute actor available
        if let Some(primary_actor) = &self.primary_compute {
            // Try to forward to the primary actor
            println!("Forwarding request to primary compute actor: {msg:?}");
            ctx.forward(primary_actor, msg).await
        } else {
            // No primary actor available - provide fallback computation directly
            println!("Primary actor not available, using fallback computation: {msg:?}");

            if msg.value > 0 {
                // Fallback: simple multiplication by 3 instead of 2
                ForwardedReply::from_ok(msg.value * 3)
            } else if msg.value == 0 {
                // Special handling for zero
                ForwardedReply::from_ok(1)
            } else {
                // Error case
                ForwardedReply::from_err(ComputeError {
                    message: "Fallback computation: negative values not supported".to_string(),
                })
            }
        }
    }
}

// Message to configure the gateway
#[derive(Debug)]
struct SetPrimaryActor(Option<ActorRef<PrimaryComputeActor>>);

impl Message<SetPrimaryActor> for GatewayActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SetPrimaryActor,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.primary_compute = msg.0;
        println!(
            "Gateway configured with primary actor: {}",
            self.primary_compute.is_some()
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Forward with Fallback Demo ===\n");

    // Create gateway actor (no primary actor initially)
    let gateway = GatewayActor::spawn(GatewayActor::default());

    // Test 1: Gateway without primary actor (fallback mode)
    println!("1. Testing fallback computation (no primary actor):");
    let result1 = gateway.ask(ComputeRequest { value: 5 }).await?;
    println!("   Result: {result1:?}");

    let result2 = gateway.ask(ComputeRequest { value: 0 }).await?;
    println!("   Result: {result2:?}");

    let result3 = gateway.ask(ComputeRequest { value: -3 }).await;
    println!("   Result: {result3:?}");

    println!();

    // Test 2: Configure gateway with primary actor
    println!("2. Configuring primary compute actor...");
    let primary = PrimaryComputeActor::spawn(PrimaryComputeActor);
    gateway.tell(SetPrimaryActor(Some(primary))).await?;

    println!("3. Testing forwarded computation (with primary actor):");
    let result4 = gateway.ask(ComputeRequest { value: 5 }).await?;
    println!("   Result: {result4:?}");

    let result5 = gateway.ask(ComputeRequest { value: -3 }).await;
    println!("   Result: {result5:?}");

    println!();

    // Test 3: Remove primary actor to show graceful fallback
    println!("4. Removing primary actor (back to fallback mode):");
    gateway.tell(SetPrimaryActor(None)).await?;

    let result6 = gateway.ask(ComputeRequest { value: 7 }).await?;
    println!("   Result: {result6:?}");

    println!("\n=== Demo Complete ===");
    println!("Key benefits:");
    println!("- No panics when forwarding fails or target is unavailable");
    println!("- Graceful fallback to alternative logic");
    println!("- Same error handling semantics throughout the system");
    println!("- Actors can provide direct responses when needed");

    Ok(())
}
