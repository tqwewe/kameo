use std::{
    collections::HashMap,
    convert::Infallible,
    time::{Duration, Instant},
};

use futures::FutureExt;
use kameo::{actor, Actor};
use kameo_actors::cache::CacheReply;
use tracing_subscriber::EnvFilter;

#[derive(Actor, Default)]
struct DbCache {
    cache: HashMap<String, String>,
}

#[actor]
impl DbCache {
    #[message]
    async fn get_key(&self, key: String) -> Result<CacheReply<String>, Infallible> {
        let cache_reply = match self.cache.get(&key).cloned() {
            Some(value) => CacheReply::hit(value),
            None => {
                CacheReply::miss(
                    self.actor_ref(),
                    key,
                    async move {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        "howdy".to_string()
                    },
                    |state, key, value| {
                        async move {
                            state.cache.insert(key.to_string(), value.to_string());
                        }
                        .boxed()
                    },
                )
                .await
            }
        };

        Ok(cache_reply)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let db_cache = kameo::spawn(DbCache::default());

    let start = Instant::now();
    let (value_1, value_2) = tokio::try_join!(
        tokio::spawn({
            let db_cache = db_cache.clone();
            async move {
                db_cache
                    .send(GetKey {
                        key: "hi".to_string(),
                    })
                    .await
                    .unwrap()
                    .await
            }
        }),
        tokio::spawn({
            let db_cache = db_cache.clone();
            async move {
                db_cache
                    .send(GetKey {
                        key: "hi".to_string(),
                    })
                    .await
                    .unwrap()
                    .await
            }
        }),
    )?;
    let dur = start.elapsed();
    dbg!(value_1?, value_2?, dur.as_millis());

    let start = Instant::now();
    let value = db_cache
        .send(GetKey {
            key: "hi".to_string(),
        })
        .await?
        .await?;
    let dur = start.elapsed();
    dbg!(value, dur.as_millis());

    let start = Instant::now();
    let value = db_cache
        .send(GetKey {
            key: "hi".to_string(),
        })
        .await?
        .await?;
    let dur = start.elapsed();
    dbg!(value, dur.as_millis());

    let start = Instant::now();
    let value = db_cache
        .send(GetKey {
            key: "there".to_string(),
        })
        .await?
        .await?;
    let dur = start.elapsed();
    dbg!(value, dur.as_millis());

    let start = Instant::now();
    let value = db_cache
        .send(GetKey {
            key: "there".to_string(),
        })
        .await?
        .await?;
    let dur = start.elapsed();
    dbg!(value, dur.as_millis());

    let start = Instant::now();
    let value = db_cache
        .send(GetKey {
            key: "there".to_string(),
        })
        .await?
        .await?;
    let dur = start.elapsed();
    dbg!(value, dur.as_millis());

    Ok(())
}
