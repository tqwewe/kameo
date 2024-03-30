use std::{
    collections::HashMap,
    convert,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use futures::{
    stream::{AbortHandle, AbortRegistration, Abortable},
    FutureExt,
};
use tokio::{
    sync::{mpsc, RwLock, Semaphore},
    task::JoinSet,
};
use tracing::{error, trace};

use crate::{
    actor::Actor,
    actor_ref::{ActorRef, Ctx, Links, Signal, CURRENT_CTX},
    error::PanicError,
    stop_reason::ActorStopReason,
};

/// Functionality to spawn an actor.
///
/// # Example
///
/// ```
/// use kameo::Spawn;
///
/// let actor = MyActor::new();
/// let actor_ref = actor.spawn();
/// ```
pub trait Spawn: Sized {
    /// Retrieves a reference to the current actor.
    ///
    /// # Panics
    ///
    /// This function will panic if called outside the scope of an actor.
    ///
    /// # Returns
    /// A reference to the actor of type `Self::Ref`.
    fn actor_ref(&self) -> ActorRef<Self>;

    /// Retrieves a reference to the current actor, if available.
    ///
    /// # Returns
    /// An `Option` containing a reference to the actor of type `Self::Ref` if available,
    /// or `None` if the actor reference is not available.
    fn try_actor_ref() -> Option<ActorRef<Self>>;

    /// Spawns the actor in a tokio::task.
    ///
    /// This calls the `Actor::on_start` hook, then processes messages/queries/signals in a loop,
    /// and finally calls the `Actor::on_stop` hook.
    ///
    /// Messages are sent to the actor through a `mpsc::unbounded_channel`.
    fn spawn(self) -> ActorRef<Self>;

    /// Spawns the actor with a bidirectional link between the current actor and the one being spawned.
    ///
    /// If either actor dies, [Actor::on_link_died] will be called on the other actor.
    fn spawn_link(self) -> ActorRef<Self>;

    /// Spawns the actor with a unidirectional link between the current actor and the child.
    ///
    /// If the current actor dies, [Actor::on_link_died] will be called on the spawned one,
    /// however if the spawned actor dies, Actor::on_link_died will not be called.
    fn spawn_child(self) -> ActorRef<Self>;
}

impl<T> Spawn for T
where
    T: Actor + Send + Sync + Sized + 'static,
{
    fn actor_ref(&self) -> ActorRef<Self> {
        match Self::try_actor_ref() {
            Some(actor_ref) => actor_ref,
            None => panic!("actor_ref called outside the scope of an actor"),
        }
    }

    fn try_actor_ref() -> Option<ActorRef<Self>> {
        ActorRef::current()
    }

    fn spawn(self) -> ActorRef<Self> {
        let (mailbox, mailbox_rx) = mpsc::unbounded_channel::<Signal<Self>>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let links = Arc::new(Mutex::new(HashMap::new()));
        let actor_ref = ActorRef::new(mailbox, abort_handle, links.clone());
        let id = actor_ref.id();
        let ctx = Ctx {
            id,
            actor_ref: Box::new(actor_ref.clone()),
            signal_mailbox: actor_ref.signal_mailbox(),
            links: links.clone(),
        };

        tokio::spawn(CURRENT_CTX.scope(ctx, async move {
            run_actor_lifecycle(id, self, mailbox_rx, abort_registration, links).await
        }));

        actor_ref
    }

    fn spawn_link(self) -> ActorRef<Self> {
        let actor_ref = self.spawn();
        let (sibbling_id, sibbling_signal_mailbox, sibbling_links) = CURRENT_CTX
            .try_with(|ctx| (ctx.id, ctx.signal_mailbox.clone(), ctx.links.clone()))
            .expect("spawn_link cannot be called outside any actors");

        let actor_ref_links = actor_ref.links();
        let (mut this_links, mut sibbling_links) = (
            actor_ref_links.lock().unwrap(),
            sibbling_links.lock().unwrap(),
        );
        this_links.insert(sibbling_id, sibbling_signal_mailbox);
        sibbling_links.insert(actor_ref.id(), actor_ref.signal_mailbox());

        actor_ref
    }

    fn spawn_child(self) -> ActorRef<Self> {
        let actor_ref = self.spawn();
        let parent_links = CURRENT_CTX
            .try_with(|ctx| ctx.links.clone())
            .expect("spawn_child cannot be called outside any actors");

        parent_links
            .lock()
            .unwrap()
            .insert(actor_ref.id(), actor_ref.signal_mailbox());

        actor_ref
    }
}

async fn run_actor_lifecycle<A>(
    id: u64,
    mut actor: A,
    mailbox_rx: mpsc::UnboundedReceiver<Signal<A>>,
    abort_registration: AbortRegistration,
    links: Links,
) where
    A: Actor,
{
    let name = actor.name().into_owned();
    trace!(%id, %name, "actor started");

    let start_res = AssertUnwindSafe(actor.on_start())
        .catch_unwind()
        .await
        .map(|res| res.map_err(|err| PanicError::new(err)))
        .map_err(PanicError::new_boxed)
        .and_then(convert::identity);
    if let Err(err) = start_res {
        let reason = ActorStopReason::Panicked(err);
        actor.on_stop(reason.clone()).await.unwrap();
        log_actor_stop_reason(id, &name, &reason);
        return;
    }

    let actor = Arc::new(RwLock::new(actor));
    let mut concurrent_queries: JoinSet<Option<ActorStopReason>> = JoinSet::new();

    let reason = Abortable::new(
        abortable_actor_loop(&actor, mailbox_rx, &mut concurrent_queries),
        abort_registration,
    )
    .await
    .unwrap_or(ActorStopReason::Killed);

    concurrent_queries.shutdown().await;

    if let Ok(mut links) = links.lock() {
        for (_, actor_ref) in links.drain() {
            let _ = actor_ref.signal_link_died(id, reason.clone());
        }
    }

    Arc::into_inner(actor)
        .expect("actor's arc contains other strong references")
        .into_inner()
        .on_stop(reason.clone())
        .await
        .unwrap();
    log_actor_stop_reason(id, &name, &reason);
}

async fn abortable_actor_loop<A>(
    actor: &Arc<RwLock<A>>,
    mut mailbox_rx: mpsc::UnboundedReceiver<Signal<A>>,
    concurrent_queries: &mut JoinSet<Option<ActorStopReason>>,
) -> ActorStopReason
where
    A: Actor + Send + Sync + 'static,
{
    loop {
        let res = recv_mailbox_loop(actor, &mut mailbox_rx, concurrent_queries).await;

        concurrent_queries.shutdown().await;

        match res {
            ActorStopReason::Normal => break ActorStopReason::Normal,
            ActorStopReason::Killed => break ActorStopReason::Killed,
            ActorStopReason::Panicked(err) => {
                match actor.try_write().unwrap().on_panic(err).await {
                    Ok(Some(reason)) => break reason,
                    Ok(None) => {}
                    Err(err) => break ActorStopReason::Panicked(PanicError::new(err)),
                }
            }
            ActorStopReason::LinkDied { id, reason } => {
                break ActorStopReason::LinkDied { id, reason }
            }
        }
    }
}

async fn recv_mailbox_loop<A>(
    actor: &Arc<RwLock<A>>,
    mailbox_rx: &mut mpsc::UnboundedReceiver<Signal<A>>,
    concurrent_queries: &mut JoinSet<Option<ActorStopReason>>,
) -> ActorStopReason
where
    A: Actor + Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(A::max_concurrent_queries()));
    macro_rules! wait_concurrent_queries {
        () => {
            while let Some(res) = concurrent_queries.join_next().await {
                match res {
                    Ok(Some(reason)) => return reason,
                    Ok(None) => {}
                    Err(err) => {
                        return ActorStopReason::Panicked(PanicError::new_boxed(err.into_panic()))
                    }
                }
            }
        };
    }

    loop {
        tokio::select! {
            biased;
            Some(res) = concurrent_queries.join_next() => {
                match res {
                    Ok(Some(reason)) => return reason,
                    Ok(None) => {}
                    Err(err) => {
                        return ActorStopReason::Panicked(PanicError::new_boxed(err.into_panic()))
                    }
                }
            }
            signal = mailbox_rx.recv() => match signal {
                Some(Signal::Message { message, reply }) => {
                    wait_concurrent_queries!();
                    match reply {
                        Some(reply) => {
                            let res = AssertUnwindSafe(message.handle_dyn(&mut actor.try_write().unwrap())).catch_unwind().await;
                            match res {
                                Ok(res) => {
                                    let _ = reply.send(res);
                                }
                                Err(err) => {
                                    return ActorStopReason::Panicked(PanicError::new_boxed(err));
                                }
                            }

                        }
                        None => {
                            let res = AssertUnwindSafe(message.handle_dyn_async(&mut actor.try_write().unwrap())).catch_unwind().await;
                            match res {
                                Ok(Some(err)) => {
                                    return ActorStopReason::Panicked(PanicError::new(err))
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    return ActorStopReason::Panicked(PanicError::new_boxed(err))
                                }
                            }
                        }
                    }
                }
                Some(Signal::Query { message, reply }) => {
                    let permit = semaphore.clone().acquire_owned().await;
                    let actor = actor.clone();
                    concurrent_queries.spawn(async move {
                        let _permit = permit;
                        match reply {
                            Some(reply) => {
                                let res = AssertUnwindSafe(message.handle_dyn(&mut actor.try_read().unwrap())).catch_unwind().await;
                                match res {
                                    Ok(res) => {
                                        let _ = reply.send(res);
                                    }
                                    Err(err) => {
                                        return Some(ActorStopReason::Panicked(PanicError::new_boxed(err)))
                                    }
                                }
                            }
                            None => {
                                let res = AssertUnwindSafe(message.handle_dyn_async(&mut actor.try_read().unwrap())).catch_unwind().await;
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
                }
                Some(Signal::Stop) | None => {
                    wait_concurrent_queries!();
                    return ActorStopReason::Normal;
                }
                Some(Signal::LinkDied { id, reason }) => {
                    wait_concurrent_queries!();
                    match AssertUnwindSafe(actor.try_write().unwrap().on_link_died(id, reason.clone())).catch_unwind().await {
                        Ok(Ok(Some(reason))) => return reason,
                        Ok(Ok(None)) => {}
                        Ok(Err(err)) => return ActorStopReason::Panicked(PanicError::new(err)),
                        Err(err) => return ActorStopReason::Panicked(PanicError::new_boxed(err)),
                    }
                }
            }
        }
    }
}

fn log_actor_stop_reason(id: u64, name: &str, reason: &ActorStopReason) {
    match reason {
        reason @ ActorStopReason::Normal
        | reason @ ActorStopReason::Killed
        | reason @ ActorStopReason::LinkDied { .. } => {
            trace!(%id, %name, %reason, "actor stopped");
        }
        reason @ ActorStopReason::Panicked(_) => {
            error!(%id, %name, %reason, "actor stopped")
        }
    }
}
