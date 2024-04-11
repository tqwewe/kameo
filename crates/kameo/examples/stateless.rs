use kameo::actor::spawn_stateless;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actor_ref = spawn_stateless(|n: i32| async move { n * n });
    let res = actor_ref.send(10).await?;
    println!("{res}");

    Ok(())
}
