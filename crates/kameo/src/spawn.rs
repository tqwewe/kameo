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
use tokio::sync::mpsc;
use tracing::{error, trace};

use crate::{
    actor::Actor,
    actor_kind::{ActorState, SyncActor, UnsyncActor},
    actor_ref::{ActorRef, Links, Signal, CURRENT_ACTOR_REF},
    error::{ActorStopReason, PanicError},
};

/// Spawns an actor in a `tokio::task`.
///
/// # Example
///
/// ```no_run
/// use kameo::Actor;
///
/// #[derive(Actor)]
/// struct MyActor;
///
/// kameo::spawn(MyActor);
/// ```
pub fn spawn<A>(actor: A) -> ActorRef<A>
where
    A: Actor + Send + Sync + 'static,
{
    spawn_inner::<A, SyncActor<A>>(actor)
}

/// Spawns an `!Sync` actor in a `tokio::task`.
///
/// Unsync actors cannot handle queries, as this would require the actor be to `Sync` since queries are procesed
/// concurrently.
///
/// # Example
///
/// ```no_run
/// use kameo::Actor;
///
/// #[derive(Actor, Default)]
/// struct MyUnsyncActor {
///     data: RefCell<()>, // RefCell is !Sync
/// }
///
/// kameo::spawn_unsync(MyUnsyncActor::default());
/// ```
pub fn spawn_unsync<A>(actor: A) -> ActorRef<A>
where
    A: Actor + Send + 'static,
{
    spawn_inner::<A, UnsyncActor<A>>(actor)
}

/// Spawns a stateless anonymous actor in a `tokio::task` using a function which processes messages of a single type.
///
/// # Example
///
/// ```no_run
/// use kameo::spawn_stateless;
///
/// let actor_ref = kameo::spawn_stateless(|n: i32| async move { n * n });
/// let res = actor_ref.send(10).await?;
/// assert_eq!(res, 100);
/// ```
pub fn spawn_stateless<M, R>(f: fn(M) -> R) -> ActorRef<fn(M) -> R>
where
    M: 'static,
    R: 'static,
{
    spawn_unsync(f as fn(_) -> _)
}

#[inline]
fn spawn_inner<A, S>(actor: A) -> ActorRef<A>
where
    A: Actor + Send + 'static,
    S: ActorState<A> + Send,
{
    let (mailbox, mailbox_rx) = mpsc::unbounded_channel::<Signal<A>>();
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let links = Arc::new(Mutex::new(HashMap::new()));
    let actor_ref = ActorRef::new(mailbox, abort_handle, links.clone());
    let id = actor_ref.id();

    tokio::spawn(
        CURRENT_ACTOR_REF.scope(Box::new(actor_ref.clone()), async move {
            run_actor_lifecycle::<A, S>(id, actor, mailbox_rx, abort_registration, links).await
        }),
    );

    actor_ref
}

#[inline]
async fn run_actor_lifecycle<A, S>(
    id: u64,
    mut actor: A,
    mailbox_rx: mpsc::UnboundedReceiver<Signal<A>>,
    abort_registration: AbortRegistration,
    links: Links,
) where
    A: Actor,
    S: ActorState<A>,
{
    let name = A::name();
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

    let mut state = S::new_from_actor(actor);

    let reason = Abortable::new(
        abortable_actor_loop(&mut state, mailbox_rx),
        abort_registration,
    )
    .await
    .unwrap_or(ActorStopReason::Killed);

    let actor = state.shutdown().await;

    if let Ok(mut links) = links.lock() {
        for (_, actor_ref) in links.drain() {
            let _ = actor_ref.signal_link_died(id, reason.clone());
        }
    }

    let on_stop_res = actor.on_stop(reason.clone()).await;
    log_actor_stop_reason(id, &name, &reason);
    on_stop_res.unwrap();
}

async fn abortable_actor_loop<A, S>(
    state: &mut S,
    mut mailbox_rx: mpsc::UnboundedReceiver<Signal<A>>,
) -> ActorStopReason
where
    A: Actor,
    S: ActorState<A>,
{
    loop {
        let reason = recv_mailbox_loop(state, &mut mailbox_rx).await;
        if let Some(reason) = state.on_shutdown(reason).await {
            return reason;
        }
    }
}

async fn recv_mailbox_loop<A, S>(
    state: &mut S,
    mailbox_rx: &mut mpsc::UnboundedReceiver<Signal<A>>,
) -> ActorStopReason
where
    A: Actor,
    S: ActorState<A>,
{
    loop {
        tokio::select! {
            biased;
            Some(reason) = state.next_pending_task() => {
                return reason
            }
            signal = mailbox_rx.recv() => match signal {
                Some(Signal::Message { message, reply }) => {
                    if let Some(reason) = state.handle_message(message, reply).await {
                        return reason;
                    }
                }
                Some(Signal::Query { query, reply }) => {
                    if let Some(reason) = state.handle_query(query, reply).await {
                        return reason;
                    }
                }
                Some(Signal::LinkDied { id, reason }) => {
                    if let Some(reason) = state.handle_link_died(id, reason).await {
                        return reason;
                    }
                }
                Some(Signal::Stop) | None => {
                    if let Some(reason) = state.handle_stop().await {
                        return reason;
                    }
                }
            }
        }
    }
}

#[inline]
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
