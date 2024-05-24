use std::{
    collections::VecDeque,
    mem,
    panic::{panic_any, AssertUnwindSafe},
    sync::Arc,
};

use futures::{Future, FutureExt};
use tokio::{
    sync::{oneshot, RwLock, Semaphore},
    task::JoinSet,
};

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{ActorStopReason, BoxSendError, PanicError, SendError},
    message::{BoxReply, DynBlockingMessage, DynMessage, DynQuery},
};

use super::Signal;

pub(crate) trait ActorState<A: Actor>: Sized {
    fn new_from_actor(actor: A, actor_ref: WeakActorRef<A>) -> Self;

    fn handle_startup_finished(&mut self) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_message(
        &mut self,
        message: Box<dyn DynMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_blocking_message(
        &mut self,
        message: Box<dyn DynBlockingMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_query(
        &mut self,
        query: Box<dyn DynQuery<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_link_died(
        &mut self,
        id: u64,
        reason: ActorStopReason,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_stop(&mut self) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn on_shutdown(
        &mut self,
        reason: ActorStopReason,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn shutdown(self) -> impl Future<Output = A> + Send;

    fn next_pending_task(&mut self) -> impl Future<Output = Option<ActorStopReason>> + Send {
        async { None }
    }
}

pub(crate) struct SyncActor<A> {
    actor_ref: WeakActorRef<A>,
    state: Arc<RwLock<A>>,
    finished_startup: bool,
    startup_buffer: VecDeque<Signal<A>>,
    semaphore: Arc<Semaphore>,
    concurrent_queries: JoinSet<Option<ActorStopReason>>,
}

impl<A: Actor> SyncActor<A> {
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
    fn new_from_actor(actor: A, actor_ref: WeakActorRef<A>) -> Self {
        SyncActor {
            actor_ref,
            state: Arc::new(RwLock::new(actor)),
            finished_startup: false,
            startup_buffer: VecDeque::new(),
            semaphore: Arc::new(Semaphore::new(A::max_concurrent_queries())),
            concurrent_queries: JoinSet::new(),
        }
    }

    async fn handle_startup_finished(&mut self) -> Option<ActorStopReason> {
        self.finished_startup = true;
        for signal in mem::take(&mut self.startup_buffer).drain(..) {
            match signal {
                Signal::Message {
                    message,
                    actor_ref,
                    reply,
                    sent_within_actor,
                } => {
                    self.handle_message(message, actor_ref, reply, sent_within_actor)
                        .await?;
                }
                Signal::BlockingMessage {
                    message,
                    actor_ref,
                    reply,
                    sent_within_actor,
                } => {
                    self.handle_blocking_message(message, actor_ref, reply, sent_within_actor)
                        .await?;
                }
                Signal::Query {
                    query,
                    actor_ref,
                    reply,
                    sent_within_actor,
                } => {
                    self.handle_query(query, actor_ref, reply, sent_within_actor)
                        .await?;
                }
                _ => unreachable!(),
            }
        }

        None
    }

    #[inline]
    async fn handle_message(
        &mut self,
        message: Box<dyn DynMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    ) -> Option<ActorStopReason> {
        if !sent_within_actor && !self.finished_startup {
            // The actor is still starting up, so we'll push this message to a buffer to be processed upon startup
            self.startup_buffer.push_back(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
            });
            return None;
        }

        if let Some(reason) = self.wait_concurrent_queries().await {
            return Some(reason);
        }

        let res = AssertUnwindSafe(message.handle_dyn(
            &mut self.state.try_write().unwrap(),
            actor_ref,
            reply,
        ))
        .catch_unwind()
        .await;
        match res {
            Ok(None) => None,
            Ok(Some(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))), // The reply was an error
            Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))), // The handler panicked
        }
    }

    #[inline]
    async fn handle_blocking_message(
        &mut self,
        message: Box<dyn DynBlockingMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    ) -> Option<ActorStopReason> {
        if !sent_within_actor && !self.finished_startup {
            // The actor is still starting up, so we'll push this message to a buffer to be processed upon startup
            self.startup_buffer.push_back(Signal::BlockingMessage {
                message,
                actor_ref,
                reply,
                sent_within_actor,
            });
            return None;
        }

        if let Some(reason) = self.wait_concurrent_queries().await {
            return Some(reason);
        }

        let state = self.state.clone().try_write_owned().unwrap();
        let res = AssertUnwindSafe(message.handle_dyn(state, actor_ref, reply))
            .catch_unwind()
            .await;
        match res {
            Ok(None) => None,
            Ok(Some(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))), // The reply was an error
            Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))), // The handler panicked
        }
    }

    #[inline]
    async fn handle_query(
        &mut self,
        query: Box<dyn DynQuery<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    ) -> Option<ActorStopReason> {
        if !sent_within_actor && !self.finished_startup {
            // The actor is still starting up, so we'll push this query to a buffer to be processed upon startup
            self.startup_buffer.push_back(Signal::Query {
                query,
                actor_ref,
                reply,
                sent_within_actor,
            });
            return None;
        }

        let permit = self.semaphore.clone().acquire_owned().await;
        let state = self.state.clone();
        self.concurrent_queries.spawn(async move {
            let _permit = permit;
            let res =
                AssertUnwindSafe(query.handle_dyn(&state.try_write().unwrap(), actor_ref, reply))
                    .catch_unwind()
                    .await;
            match res {
                Ok(None) => None,
                Ok(Some(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))), // The reply was an error
                Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))), // The handler panicked
            }
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

        match AssertUnwindSafe(self.state.try_write().unwrap().on_link_died(
            self.actor_ref.clone(),
            id,
            reason.clone(),
        ))
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
                match self
                    .state
                    .try_write()
                    .unwrap()
                    .on_panic(self.actor_ref.clone(), err)
                    .await
                {
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
    actor_ref: WeakActorRef<A>,
    state: A,
    finished_startup: bool,
    startup_buffer: VecDeque<Signal<A>>,
}

impl<A> ActorState<A> for UnsyncActor<A>
where
    A: Actor + Send,
{
    #[inline]
    fn new_from_actor(actor: A, actor_ref: WeakActorRef<A>) -> Self {
        UnsyncActor {
            actor_ref,
            state: actor,
            finished_startup: false,
            startup_buffer: VecDeque::new(),
        }
    }

    async fn handle_startup_finished(&mut self) -> Option<ActorStopReason> {
        self.finished_startup = true;
        for signal in mem::take(&mut self.startup_buffer).drain(..) {
            match signal {
                Signal::Message {
                    message,
                    actor_ref,
                    reply,
                    sent_within_actor,
                } => {
                    self.handle_message(message, actor_ref, reply, sent_within_actor)
                        .await?;
                }
                Signal::Query {
                    query,
                    actor_ref,
                    reply,
                    sent_within_actor,
                } => {
                    self.handle_query(query, actor_ref, reply, sent_within_actor)
                        .await?;
                }
                _ => unreachable!(),
            }
        }

        None
    }

    #[inline]
    async fn handle_message(
        &mut self,
        message: Box<dyn DynMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    ) -> Option<ActorStopReason> {
        if !sent_within_actor && !self.finished_startup {
            // The actor is still starting up, so we'll push this message to a buffer to be processed upon startup
            self.startup_buffer.push_back(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
            });
            return None;
        }

        let res = AssertUnwindSafe(message.handle_dyn(&mut self.state, actor_ref, reply))
            .catch_unwind()
            .await;
        match res {
            Ok(None) => None,
            Ok(Some(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))), // The reply was an error
            Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))), // The handler panicked
        }
    }

    #[inline]
    async fn handle_blocking_message(
        &mut self,
        _message: Box<dyn DynBlockingMessage<A>>,
        _actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        _sent_within_actor: bool,
    ) -> Option<ActorStopReason> {
        match reply {
            Some(reply) => {
                let _ = reply.send(Err(SendError::QueriesNotSupported));
                None
            }
            None => panic_any(SendError::<(), ()>::QueriesNotSupported),
        }
    }

    #[inline]
    async fn handle_query(
        &mut self,
        _query: Box<dyn DynQuery<A>>,
        _actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        _sent_within_actor: bool,
    ) -> Option<ActorStopReason> {
        match reply {
            Some(reply) => {
                let _ = reply.send(Err(SendError::QueriesNotSupported));
                None
            }
            None => panic_any(SendError::<(), ()>::QueriesNotSupported),
        }
    }

    #[inline]
    async fn handle_link_died(
        &mut self,
        id: u64,
        reason: ActorStopReason,
    ) -> Option<ActorStopReason> {
        match AssertUnwindSafe(
            self.state
                .on_link_died(self.actor_ref.clone(), id, reason.clone()),
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
        Some(ActorStopReason::Normal)
    }

    #[inline]
    async fn on_shutdown(&mut self, reason: ActorStopReason) -> Option<ActorStopReason> {
        match reason {
            ActorStopReason::Normal => Some(ActorStopReason::Normal),
            ActorStopReason::Killed => Some(ActorStopReason::Killed),
            ActorStopReason::Panicked(err) => {
                match self.state.on_panic(self.actor_ref.clone(), err).await {
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
    async fn shutdown(self) -> A {
        self.state
    }
}
