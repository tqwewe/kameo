use std::{
    any,
    borrow::Cow,
    collections::HashMap,
    convert,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
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
    actor_ref::{ActorRef, Ctx, Links, Signal, CURRENT_CTX},
    error::{BoxError, PanicError},
    stop_reason::ActorStopReason,
};

/// Functionality for an actor including lifecycle hooks.
///
/// Methods in this trait that return `BoxError` will stop the actor with the reason
/// `ActorReason::Panicked` containing the error.
#[async_trait]
pub trait Actor: Send + Sync + Sized + 'static {
    /// Actor name, useful for logging.
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(any::type_name::<Self>())
    }

    /// Retrieves a reference to the current actor.
    ///
    /// # Panics
    ///
    /// This function will panic if called outside the scope of an actor.
    ///
    /// # Returns
    /// A reference to the actor of type `Self::Ref`.
    fn actor_ref(&self) -> ActorRef<Self> {
        match Self::try_actor_ref() {
            Some(actor_ref) => actor_ref,
            None => panic!("actor_ref called outside the scope of an actor"),
        }
    }

    /// Retrieves a reference to the current actor, if available.
    ///
    /// # Returns
    /// An `Option` containing a reference to the actor of type `Self::Ref` if available,
    /// or `None` if the actor reference is not available.
    fn try_actor_ref() -> Option<ActorRef<Self>> {
        ActorRef::current()
    }

    /// The maximum number of concurrent queries to handle at a time.
    ///
    /// This defaults to the number of cpus on the system.
    fn max_concurrent_queries() -> usize {
        num_cpus::get()
    }

    /// Hook that is called before the actor starts processing messages.
    ///
    /// This asynchronous method allows for initialization tasks to be performed
    /// before the actor starts receiving messages.
    ///
    /// # Returns
    /// A result indicating successful initialization or an error if initialization fails.
    async fn on_start(&mut self) -> Result<(), BoxError> {
        Ok(())
    }

    /// Hook that is called when an actor panicked or returns an error during an async message.
    ///
    /// This method provides an opportunity to clean up or reset state.
    /// It can also determine whether the actor should be killed or if it should continue processing messages by returning `None`.
    ///
    /// # Parameters
    /// - `err`: The error that occurred.
    ///
    /// # Returns
    /// Whether the actor should continue processing, or be stopped by returning a stop reason.
    async fn on_panic(&mut self, err: PanicError) -> Result<Option<ActorStopReason>, BoxError> {
        Ok(Some(ActorStopReason::Panicked(err)))
    }

    /// Hook that is called before the actor is stopped.
    ///
    /// This method allows for cleanup and finalization tasks to be performed before the
    /// actor is fully stopped. It can be used to release resources, notify other actors,
    /// or complete any final tasks.
    ///
    /// # Parameters
    /// - `reason`: The reason why the actor is being stopped.
    async fn on_stop(self, _reason: ActorStopReason) -> Result<(), BoxError> {
        Ok(())
    }

    /// Hook that is called when a linked actor dies.
    ///
    /// By default, the current actor will be stopped if the reason is anything other than normal.
    ///
    /// # Returns
    /// Whether the actor should continue processing, or be stopped by returning a stop reason.
    #[allow(unused_variables)]
    async fn on_link_died(
        &mut self,
        id: u64,
        reason: ActorStopReason,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        match &reason {
            ActorStopReason::Normal => Ok(None),
            ActorStopReason::Killed
            | ActorStopReason::Panicked(_)
            | ActorStopReason::LinkDied { .. } => Ok(Some(ActorStopReason::LinkDied {
                id,
                reason: Box::new(reason),
            })),
        }
    }

    /// Starts the actor in a tokio::task
    ///
    /// This called `Actor::on_start`, then processes messages/queries/signals in a loop,
    /// and finally calls `Actor::on_stop`.
    ///
    /// Messages are sent to the actor through a `mpsc::unbounded_channel`.
    fn start(self) -> ActorRef<Self> {
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

    /// Starts the actor with a bidirectional link between the current actor and the one being spawned.
    ///
    /// If either actor dies, [Actor::on_link_died](crate::actor::Actor::on_link_died) will be called on the other actor.
    fn start_link(self) -> ActorRef<Self> {
        let actor_ref = self.start();
        let (sibbling_id, sibbling_signal_mailbox, sibbling_links) = CURRENT_CTX
            .try_with(|ctx| (ctx.id, ctx.signal_mailbox.clone(), ctx.links.clone()))
            .expect("start_link cannot be called outside any actors");

        let actor_ref_links = actor_ref.links();
        let (mut this_links, mut sibbling_links) = (
            actor_ref_links.lock().unwrap(),
            sibbling_links.lock().unwrap(),
        );
        this_links.insert(sibbling_id, sibbling_signal_mailbox);
        sibbling_links.insert(actor_ref.id(), actor_ref.signal_mailbox());

        actor_ref
    }

    /// Starts the actor with a unidirectional link between the current actor and the child.
    ///
    /// If the current actor dies, [Actor::on_link_died](crate::actor::Actor::on_link_died) will be called on the spawned one,
    /// however if the spawned actor dies, Actor::on_link_died will not be called.
    fn start_child(self) -> ActorRef<Self> {
        let actor_ref = self.start();
        let parent_links = CURRENT_CTX
            .try_with(|ctx| ctx.links.clone())
            .expect("start_child cannot be called outside any actors");

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
