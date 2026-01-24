//! Test the registration system fixes
//! This example demonstrates that local registration works with the remote feature enabled

use kameo::prelude::*;

#[derive(Actor)]
struct TestActor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing registration fixes...\n");

    // Test 1: Local registration works with remote feature
    println!("Test 1: Local registration with remote feature enabled");
    let actor1 = <TestActor as kameo::Actor>::spawn(TestActor);
    match actor1.register_local("test_actor_1") {
        Ok(()) => println!("✅ Registration succeeded"),
        Err(e) => {
            println!("❌ Registration failed: {:?}", e);
            return Err(e.into());
        }
    }

    // Test 2: Local lookup works with remote feature
    println!("\nTest 2: Local lookup with remote feature enabled");
    match ActorRef::<TestActor>::lookup("test_actor_1").await {
        Ok(Some(found_actor)) => {
            if found_actor.id() == actor1.id() {
                println!("✅ Lookup succeeded and found correct actor");
            } else {
                println!("❌ Lookup found wrong actor");
                return Err("Wrong actor found".into());
            }
        }
        Ok(None) => {
            println!("❌ Lookup returned None");
            return Err("Actor not found".into());
        }
        Err(e) => {
            println!("❌ Lookup failed: {:?}", e);
            return Err(e.into());
        }
    }

    // Test 3: Duplicate registration fails correctly
    println!("\nTest 3: Duplicate registration prevention");
    let actor2 = <TestActor as kameo::Actor>::spawn(TestActor);
    match actor2.register_local("test_actor_1") {
        Ok(()) => {
            println!("❌ Duplicate registration succeeded (should have failed)");
            return Err("Duplicate registration should fail".into());
        }
        Err(kameo::error::RegistryError::NameAlreadyRegistered) => {
            println!("✅ Duplicate registration correctly prevented");
        }
        Err(e) => {
            println!("❌ Unexpected error: {:?}", e);
            return Err(e.into());
        }
    }

    // Test 4: Multiple different actors can be registered
    println!("\nTest 4: Multiple actor registration");
    match actor2.register_local("test_actor_2") {
        Ok(()) => println!("✅ Second actor registered successfully"),
        Err(e) => {
            println!("❌ Second actor registration failed: {:?}", e);
            return Err(e.into());
        }
    }

    // Verify both actors can be found
    let found1 = ActorRef::<TestActor>::lookup("test_actor_1")
        .await?
        .unwrap();
    let found2 = ActorRef::<TestActor>::lookup("test_actor_2")
        .await?
        .unwrap();

    if found1.id() == actor1.id() && found2.id() == actor2.id() {
        println!("✅ Both actors found correctly");
    } else {
        println!("❌ Actor lookup mismatch");
        return Err("Actor mismatch".into());
    }

    // Test 5: Non-existent actor lookup returns None
    println!("\nTest 5: Non-existent actor lookup");
    match ActorRef::<TestActor>::lookup("nonexistent").await {
        Ok(None) => println!("✅ Non-existent actor correctly returned None"),
        Ok(Some(_)) => {
            println!("❌ Found non-existent actor");
            return Err("Should not find non-existent actor".into());
        }
        Err(e) => {
            println!("❌ Lookup error: {:?}", e);
            return Err(e.into());
        }
    }

    println!("\n🎉 All registration fix tests passed!");
    println!("\nKey achievements:");
    println!("• Local registration works with remote feature enabled");
    println!("• Local lookup works with remote feature enabled");
    println!("• Duplicate registration is properly prevented");
    println!("• Multiple actors can be registered with different names");
    println!("• Non-existent actor lookup returns None (not error)");
    println!("\n✨ The registration system fixes are working correctly!");

    Ok(())
}
