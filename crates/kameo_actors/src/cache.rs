use std::{
    any,
    collections::HashMap,
    convert::Infallible,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures::future::BoxFuture;
use lazy_static::lazy_static;
use tokio::sync::watch::{self, error::RecvError, Receiver, Sender};
use tokio_util::sync::ReusableBoxFuture;

use kameo::{Actor, ActorRef, Message};

type AnyReceiverValue = Option<Box<dyn any::Any + Send + Sync>>;

lazy_static! {
    static ref CACHE_REGISTRY_ACTOR: ActorRef<CacheRegistry> =
        kameo::spawn(CacheRegistry::default());
}

#[derive(Default)]
struct CacheRegistry {
    futures: HashMap<(&'static str, String), Receiver<AnyReceiverValue>>,
}

impl Actor for CacheRegistry {
    fn name() -> &'static str {
        "CacheRegistry"
    }
}

struct AddCacheReply<T, A, F, F2> {
    actor_ref: ActorRef<A>,
    key: String,
    future: F,
    update_cache: F2,
    phantom: PhantomData<T>,
}

impl<T, A, F, F2> Message<CacheRegistry> for AddCacheReply<T, A, F, F2>
where
    T: Send + Sync + 'static,
    A: Actor + Send + 'static,
    F: Future<Output = T> + Send + 'static,
    F2: for<'a> Fn(&'a mut A, &'a str, &'a T) -> BoxFuture<'a, ()> + Send + 'static,
{
    type Reply = Result<Receiver<AnyReceiverValue>, Infallible>;

    async fn handle(state: &mut CacheRegistry, msg: AddCacheReply<T, A, F, F2>) -> Self::Reply {
        let key = (A::name(), msg.key);
        let rx = match state.futures.get(&key).cloned() {
            Some(rx) => rx,
            None => {
                println!("insert pending cache reply for key {}", &key.1);
                let (tx, rx) = watch::channel(None);
                state.futures.insert(key.clone(), rx.clone());
                tokio::spawn({
                    let key = key.clone();
                    async move {
                        println!("starting future");
                        let value = msg.future.await;
                        println!("future finished");
                        let res = msg
                            .actor_ref
                            .send(CacheReplyFinished {
                                key: key.1.clone(),
                                value,
                                tx,
                                f: Box::new(msg.update_cache),
                            })
                            .await;
                        if res.is_err() {
                            let _ = CACHE_REGISTRY_ACTOR.send_async(RemoveCacheReply {
                                actor: A::name(),
                                key: key.1,
                            });
                        }
                    }
                });

                rx
            }
        };

        Ok(rx)
    }
}

struct RemoveCacheReply {
    actor: &'static str,
    key: String,
}

impl Message<CacheRegistry> for RemoveCacheReply {
    type Reply = Result<Option<Receiver<AnyReceiverValue>>, Infallible>;

    async fn handle(state: &mut CacheRegistry, msg: RemoveCacheReply) -> Self::Reply {
        println!("remove pending cache reply for key {}", &msg.key);
        Ok(state.futures.remove(&(msg.actor, msg.key)))
    }
}

struct CacheReplyFinished<A, T> {
    key: String,
    value: T,
    tx: Sender<AnyReceiverValue>,
    f: Box<dyn for<'a> Fn(&'a mut A, &'a str, &'a T) -> BoxFuture<'a, ()> + Send>,
}

impl<A, T> Message<A> for CacheReplyFinished<A, T>
where
    A: Actor + Send + 'static,
    T: Send + Sync + 'static,
{
    type Reply = Result<(), ()>;

    async fn handle(state: &mut A, msg: CacheReplyFinished<A, T>) -> Self::Reply {
        (msg.f)(state, &msg.key, &msg.value).await;
        let _ = CACHE_REGISTRY_ACTOR
            .send(RemoveCacheReply {
                actor: A::name(),
                key: msg.key,
            })
            .await;
        let _ = msg
            .tx
            .send(Some(Box::new(msg.value) as Box<dyn any::Any + Send + Sync>));

        Ok(())
    }
}

pub struct CacheReply<T> {
    inner: CacheReplyInner<T>,
}

enum CacheReplyInner<T> {
    Hit(T),
    Miss {
        rx_fut: ReusableBoxFuture<'static, (Result<(), RecvError>, Receiver<AnyReceiverValue>)>,
    },
}

impl<T> CacheReply<T> {
    pub fn hit(value: T) -> CacheReply<T> {
        CacheReply {
            inner: CacheReplyInner::Hit(value),
        }
    }

    pub async fn miss<A, F, F2>(
        actor_ref: ActorRef<A>,
        key: String,
        future: F,
        update_cache: F2,
    ) -> CacheReply<T>
    where
        T: Clone + Send + Sync + 'static,
        A: Actor + Send + 'static,
        F: Future<Output = T> + Send + 'static,
        F2: for<'a> Fn(&'a mut A, &'a str, &'a T) -> BoxFuture<'a, ()> + Send + 'static,
    {
        let rx = CACHE_REGISTRY_ACTOR
            .send(AddCacheReply {
                actor_ref,
                key,
                future,
                update_cache,
                phantom: PhantomData,
            })
            .await
            .unwrap();

        CacheReply {
            inner: CacheReplyInner::Miss {
                rx_fut: ReusableBoxFuture::new(make_future(rx)),
            },
        }
    }
}

async fn make_future(
    mut rx: Receiver<AnyReceiverValue>,
) -> (Result<(), RecvError>, Receiver<AnyReceiverValue>) {
    let result = rx.changed().await;
    (result, rx)
}

impl<T> Future for CacheReply<T>
where
    T: Clone + Unpin + 'static,
{
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.inner {
            CacheReplyInner::Hit(value) => Poll::Ready(Ok(value.clone())),
            CacheReplyInner::Miss { rx_fut } => loop {
                let (result, mut rx) = ready!(rx_fut.poll(cx));
                match result {
                    Ok(_) => {
                        let received = rx
                            .borrow_and_update()
                            .as_ref()
                            .map(|value| value.downcast_ref::<T>().unwrap().clone());
                        rx_fut.set(make_future(rx));
                        match received {
                            Some(value) => return Poll::Ready(Ok(value)),
                            None => continue,
                        }
                    }
                    Err(err) => {
                        rx_fut.set(make_future(rx));
                        return Poll::Ready(Err(err));
                    }
                }
            },
        }
    }
}
