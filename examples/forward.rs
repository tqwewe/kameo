use std::collections::HashMap;

use kameo::{prelude::*, reply::ForwardedReply};

#[derive(Actor)]
struct PlayersActor {
    player_map: HashMap<u64, ActorRef<Player>>,
}

struct ForwardToPlayer<M> {
    player_id: u64,
    message: M,
}

impl<M> Message<ForwardToPlayer<M>> for PlayersActor
where
    Player: Message<M>,
    M: Send + 'static,
{
    type Reply = ForwardedReply<M, <Player as Message<M>>::Reply>;

    async fn handle(
        &mut self,
        msg: ForwardToPlayer<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let player_ref = self.player_map.get(&msg.player_id).unwrap();
        ctx.forward(player_ref, msg.message).await
    }
}

#[derive(Actor, Default)]
struct Player {
    health: f32,
}

struct Damage {
    amount: f32,
}

impl Message<Damage> for Player {
    type Reply = f32;

    async fn handle(
        &mut self,
        Damage { amount }: Damage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.health -= amount;
        self.health
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let player_ref = Player::spawn(Player { health: 100.0 });

    let mut player_map = HashMap::new();
    player_map.insert(0, player_ref.clone());

    let players_ref = PlayersActor::spawn(PlayersActor { player_map });

    let health = players_ref
        .ask(ForwardToPlayer {
            player_id: 0,
            message: Damage { amount: 38.2 },
        })
        .await?;
    println!("Player health: {health:.1}");

    Ok(())
}
