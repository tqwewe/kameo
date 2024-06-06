use std::{convert, mem, panic::AssertUnwindSafe};

use futures::{
    stream::{AbortHandle, AbortRegistration, Abortable},
    FutureExt,
};
use tracing::{error, trace};

use crate::{
    actor::{
        kind::{ActorState, SyncActor, UnsyncActor},
        Actor, ActorRef, Links, Signal, CURRENT_ACTOR_ID,
    },
    error::{ActorStopReason, PanicError},
};

use super::MailboxReceiver;

/// Spawns an actor in a tokio task.
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

/// Spawns an `!Sync` actor in a tokio task.
///
/// Unsync actors cannot handle queries, as this would require the actor be to `Sync` since queries are procesed
/// concurrently.
///
/// # Example
///
/// ```no_run
/// use kameo::Actor;
/// use kameo::actor::spawn_unsync;
///
/// #[derive(Actor, Default)]
/// struct MyUnsyncActor {
///     data: RefCell<()>, // RefCell is !Sync
/// }
///
/// spawn_unsync(MyUnsyncActor::default());
/// ```
pub fn spawn_unsync<A>(actor: A) -> ActorRef<A>
where
    A: Actor + Send + 'static,
{
    spawn_inner::<A, UnsyncActor<A>>(actor)
}

#[inline]
fn spawn_inner<A, S>(actor: A) -> ActorRef<A>
where
    A: Actor + Send + 'static,
    S: ActorState<A> + Send,
{
    let (mailbox, mailbox_rx) = actor.new_mailbox();
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let links = Links::default();
    let actor_ref = ActorRef::new(mailbox, abort_handle, links.clone());
    let id = actor_ref.id();

    tokio::spawn(CURRENT_ACTOR_ID.scope(id, {
        let actor_ref = actor_ref.clone();
        async move {
            run_actor_lifecycle::<A, S>(actor_ref, actor, mailbox_rx, abort_registration, links)
                .await
        }
    }));

    actor_ref
}

#[inline]
async fn run_actor_lifecycle<A, S>(
    actor_ref: ActorRef<A>,
    mut actor: A,
    mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
    links: Links,
) where
    A: Actor + 'static,
    S: ActorState<A>,
{
    let id = actor_ref.id();
    let name = A::name();
    trace!(%id, %name, "actor started");

    let start_res = AssertUnwindSafe(actor.on_start(actor_ref.clone()))
        .catch_unwind()
        .await
        .map(|res| res.map_err(PanicError::new))
        .map_err(PanicError::new_boxed)
        .and_then(convert::identity);

    let _ = actor_ref.signal_mailbox().signal_startup_finished().await;
    let actor_ref = {
        // Downgrade actor ref
        let weak_actor_ref = actor_ref.downgrade();
        mem::drop(actor_ref);
        weak_actor_ref
    };

    if let Err(err) = start_res {
        let reason = ActorStopReason::Panicked(err);
        actor
            .on_stop(actor_ref.clone(), reason.clone())
            .await
            .unwrap();
        log_actor_stop_reason(id, name, &reason);
        return;
    }

    let mut state = S::new_from_actor(actor, actor_ref.clone());

    let reason = Abortable::new(
        abortable_actor_loop(&mut state, mailbox_rx),
        abort_registration,
    )
    .await
    .unwrap_or(ActorStopReason::Killed);

    let actor = state.shutdown().await;

    {
        let mut links = links.lock().await;
        for (_, actor_ref) in links.drain() {
            let _ = actor_ref.signal_link_died(id, reason.clone()).await;
        }
    }

    let on_stop_res = actor.on_stop(actor_ref, reason.clone()).await;
    log_actor_stop_reason(id, name, &reason);
    on_stop_res.unwrap();
}

async fn abortable_actor_loop<A, S>(
    state: &mut S,
    mut mailbox_rx: MailboxReceiver<A>,
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
    mailbox_rx: &mut MailboxReceiver<A>,
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
            signal = mailbox_rx.recv() => {
                match signal {
                    Some(Signal::StartupFinished) => {
                        if let Some(reason) = state.handle_startup_finished().await {
                            return reason;
                        }
                    }
                    Some(Signal::Message { message, actor_ref, reply, sent_within_actor }) => {
                        if let Some(reason) = state.handle_message(message, actor_ref, reply, sent_within_actor).await {
                            return reason;
                        }
                    }
                    Some(Signal::Query { query, actor_ref, reply, sent_within_actor }) => {
                        if let Some(reason) = state.handle_query(query, actor_ref, reply, sent_within_actor).await {
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
