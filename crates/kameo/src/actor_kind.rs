use std::{
    any,
    panic::{panic_any, AssertUnwindSafe},
    sync::Arc,
};

use futures::FutureExt;
use tokio::{
    sync::{oneshot, RwLock, Semaphore},
    task::JoinSet,
};

use crate::{
    actor::Actor,
    error::{ActorStopReason, PanicError, SendError},
    message::{BoxReply, DynMessage, DynQuery},
};

pub(crate) trait ActorState<A: Actor>: Sized {
    async fn handle_message(
        &mut self,
        message: Box<dyn DynMessage<A>>,
        reply: Option<oneshot::Sender<BoxReply>>,
    ) -> Option<ActorStopReason>;

    async fn handle_query(
        &mut self,
        query: Box<dyn DynQuery<A>>,
        reply: Option<oneshot::Sender<Result<BoxReply, SendError<Box<dyn any::Any + Send>>>>>,
    ) -> Option<ActorStopReason>;

    async fn handle_link_died(
        &mut self,
        id: u64,
        reason: ActorStopReason,
    ) -> Option<ActorStopReason>;

    async fn handle_stop(&mut self) -> Option<ActorStopReason>;

    async fn on_shutdown(&mut self, reason: ActorStopReason) -> Option<ActorStopReason>;
    async fn shutdown(self) -> A;

    async fn next_pending_task(&mut self) -> Option<ActorStopReason> {
        None
    }
}

pub(crate) struct SyncActor<A> {
    state: Arc<RwLock<A>>,
    semaphore: Arc<Semaphore>,
    concurrent_queries: JoinSet<Option<ActorStopReason>>,
}

impl<A: Actor> SyncActor<A> {
    pub(crate) fn new(state: A) -> Self {
        SyncActor {
            state: Arc::new(RwLock::new(state)),
            semaphore: Arc::new(Semaphore::new(A::max_concurrent_queries())),
            concurrent_queries: JoinSet::new(),
        }
    }

    #[inline]
    async fn wait_concurrent_queries(&mut self) -> Option<ActorStopReason> {
        while let Some(res) = self.concurrent_queries.join_next().await {
            match res {
                Ok(Some(reason)) => return Some(reason),
                Ok(None) => {}
                Err(err) => {
                    return Some(ActorStopReason::Panicked(PanicError::new_boxed(
                        err.into_panic(),
                    )))
                }
            }
        }

        None
    }
}

impl<A> ActorState<A> for SyncActor<A>
where
    A: Actor + Send + Sync + 'static,
{
    #[inline]
    async fn handle_message(
        &mut self,
        message: Box<dyn DynMessage<A>>,
        reply: Option<oneshot::Sender<BoxReply>>,
    ) -> Option<ActorStopReason> {
        if let Some(reason) = self.wait_concurrent_queries().await {
            return Some(reason);
        }

        match reply {
            Some(reply) => {
                let res =
                    AssertUnwindSafe(message.handle_dyn(&mut self.state.try_write().unwrap()))
                        .catch_unwind()
                        .await;
                match res {
                    Ok(res) => {
                        let _ = reply.send(res);
                        None
                    }
                    Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))),
                }
            }
            None => {
                let res = AssertUnwindSafe(
                    message.handle_dyn_async(&mut self.state.try_write().unwrap()),
                )
                .catch_unwind()
                .await;
                match res {
                    Ok(Some(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))),
                    Ok(None) => None,
                    Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))),
                }
            }
        }
    }

    #[inline]
    async fn handle_query(
        &mut self,
        query: Box<dyn DynQuery<A>>,
        reply: Option<oneshot::Sender<Result<BoxReply, SendError<Box<dyn any::Any + Send>>>>>,
    ) -> Option<ActorStopReason> {
        let permit = self.semaphore.clone().acquire_owned().await;
        let state = self.state.clone();
        self.concurrent_queries.spawn(async move {
            let _permit = permit;
            match reply {
                Some(reply) => {
                    let res = AssertUnwindSafe(query.handle_dyn(&state.try_read().unwrap()))
                        .catch_unwind()
                        .await;
                    match res {
                        Ok(res) => {
                            let _ = reply.send(Ok(res));
                        }
                        Err(err) => {
                            return Some(ActorStopReason::Panicked(PanicError::new_boxed(err)))
                        }
                    }
                }
                None => {
                    let res = AssertUnwindSafe(query.handle_dyn_async(&state.try_read().unwrap()))
                        .catch_unwind()
                        .await;
                    match res {
                        Ok(Some(err)) => {
                            return Some(ActorStopReason::Panicked(PanicError::new(err)))
                        }
                        Ok(None) => {}
                        Err(err) => {
                            return Some(ActorStopReason::Panicked(PanicError::new_boxed(err)))
                        }
                    }
                }
            }
            None
        });

        None
    }

    #[inline]
    async fn handle_link_died(
        &mut self,
        id: u64,
        reason: ActorStopReason,
    ) -> Option<ActorStopReason> {
        if let Some(reason) = self.wait_concurrent_queries().await {
            return Some(reason);
        }

        match AssertUnwindSafe(
            self.state
                .try_write()
                .unwrap()
                .on_link_died(id, reason.clone()),
        )
        .catch_unwind()
        .await
        {
            Ok(Ok(Some(reason))) => Some(reason),
            Ok(Ok(None)) => None,
            Ok(Err(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))),
            Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))),
        }
    }

    #[inline]
    async fn handle_stop(&mut self) -> Option<ActorStopReason> {
        if let Some(reason) = self.wait_concurrent_queries().await {
            return Some(reason);
        }

        Some(ActorStopReason::Normal)
    }

    #[inline]
    async fn on_shutdown(&mut self, reason: ActorStopReason) -> Option<ActorStopReason> {
        self.concurrent_queries.shutdown().await;

        match reason {
            ActorStopReason::Normal => Some(ActorStopReason::Normal),
            ActorStopReason::Killed => Some(ActorStopReason::Killed),
            ActorStopReason::Panicked(err) => {
                match self.state.try_write().unwrap().on_panic(err).await {
                    Ok(Some(reason)) => Some(reason),
                    Ok(None) => None,
                    Err(err) => Some(ActorStopReason::Panicked(PanicError::new(err))),
                }
            }
            ActorStopReason::LinkDied { id, reason } => {
                Some(ActorStopReason::LinkDied { id, reason })
            }
        }
    }

    #[inline]
    async fn shutdown(mut self) -> A {
        self.concurrent_queries.shutdown().await;
        Arc::into_inner(self.state)
            .expect("actor's arc contains other strong references - this is a bug")
            .into_inner()
    }

    #[inline]
    async fn next_pending_task(&mut self) -> Option<ActorStopReason> {
        self.concurrent_queries
            .join_next()
            .await
            .and_then(|res| match res {
                Ok(Some(reason)) => Some(reason),
                Ok(None) => None,
                Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(
                    err.into_panic(),
                ))),
            })
    }
}

pub(crate) struct UnsyncActor<A> {
    state: A,
}

impl<A> UnsyncActor<A> {
    pub(crate) fn new(state: A) -> Self {
        UnsyncActor { state }
    }
}

impl<A> ActorState<A> for UnsyncActor<A>
where
    A: Actor + Send,
{
    async fn handle_message(
        &mut self,
        message: Box<dyn DynMessage<A>>,
        reply: Option<oneshot::Sender<BoxReply>>,
    ) -> Option<ActorStopReason> {
        match reply {
            Some(reply) => {
                let res = AssertUnwindSafe(message.handle_dyn(&mut self.state))
                    .catch_unwind()
                    .await;
                match res {
                    Ok(res) => {
                        let _ = reply.send(res);
                        None
                    }
                    Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))),
                }
            }
            None => {
                let res = AssertUnwindSafe(message.handle_dyn_async(&mut self.state))
                    .catch_unwind()
                    .await;
                match res {
                    Ok(Some(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))),
                    Ok(None) => None,
                    Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))),
                }
            }
        }
    }

    async fn handle_query(
        &mut self,
        _query: Box<dyn DynQuery<A>>,
        reply: Option<oneshot::Sender<Result<BoxReply, SendError<Box<dyn any::Any + Send>>>>>,
    ) -> Option<ActorStopReason> {
        match reply {
            Some(reply) => {
                let _ = reply.send(Err(SendError::QueriesNotSupported));
                None
            }
            None => panic_any(SendError::<()>::QueriesNotSupported),
        }
    }

    async fn handle_link_died(
        &mut self,
        id: u64,
        reason: ActorStopReason,
    ) -> Option<ActorStopReason> {
        match AssertUnwindSafe(self.state.on_link_died(id, reason.clone()))
            .catch_unwind()
            .await
        {
            Ok(Ok(Some(reason))) => Some(reason),
            Ok(Ok(None)) => None,
            Ok(Err(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))),
            Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))),
        }
    }

    async fn handle_stop(&mut self) -> Option<ActorStopReason> {
        Some(ActorStopReason::Normal)
    }

    async fn on_shutdown(&mut self, reason: ActorStopReason) -> Option<ActorStopReason> {
        match reason {
            ActorStopReason::Normal => Some(ActorStopReason::Normal),
            ActorStopReason::Killed => Some(ActorStopReason::Killed),
            ActorStopReason::Panicked(err) => match self.state.on_panic(err).await {
                Ok(Some(reason)) => Some(reason),
                Ok(None) => None,
                Err(err) => Some(ActorStopReason::Panicked(PanicError::new(err))),
            },
            ActorStopReason::LinkDied { id, reason } => {
                Some(ActorStopReason::LinkDied { id, reason })
            }
        }
    }

    async fn shutdown(self) -> A {
        self.state
    }
}
