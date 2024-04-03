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
    actor_ref::{ActorRef, Ctx, Links, Signal, CURRENT_CTX},
    error::{ActorStopReason, PanicError},
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

    /// Spawns an actor in a `tokio::task`.
    ///
    /// This calls the `Actor::on_start` hook, then processes messages/queries/signals in a loop,
    /// and finally calls the `Actor::on_stop` hook.
    ///
    /// Messages are sent to the actor through a `mpsc::unbounded_channel`.
    #[cfg(not(feature = "nightly"))]
    fn spawn(self) -> ActorRef<Self>
    where
        Self: Send + Sync;

    /// Spawns an actor in a `tokio::task`.
    ///
    /// This calls the `Actor::on_start` hook, then processes messages/queries/signals in a loop,
    /// and finally calls the `Actor::on_stop` hook.
    ///
    /// Messages are sent to the actor through a `mpsc::unbounded_channel`.
    #[cfg(feature = "nightly")]
    fn spawn(self) -> ActorRef<Self>
    where
        Self: Send;

    /// Spawns an `!Sync` actor in a `tokio::task`.
    ///
    /// This calls the `Actor::on_start` hook, then processes messages/queries/signals in a loop,
    /// and finally calls the `Actor::on_stop` hook.
    ///
    /// Messages are sent to the actor through a `mpsc::unbounded_channel`.
    #[cfg(not(feature = "nightly"))]
    fn spawn_unsync(self) -> ActorRef<Self>
    where
        Self: Send;

    /// Spawns an actor with a bidirectional link between the current actor and the one being spawned.
    ///
    /// If either actor dies, [Actor::on_link_died] will be called on the other actor.
    fn spawn_link(self) -> ActorRef<Self>
    where
        Self: Send + Sync;

    /// Spawns an `!Sync` actor with a bidirectional link between the current actor and the one being spawned.
    ///
    /// If either actor dies, [Actor::on_link_died] will be called on the other actor.
    #[cfg(not(feature = "nightly"))]
    fn spawn_unsync_link(self) -> ActorRef<Self>
    where
        Self: Send;

    /// Spawns an actor with a unidirectional link between the current actor and the child.
    ///
    /// If the current actor dies, [Actor::on_link_died] will be called on the spawned one,
    /// however if the spawned actor dies, Actor::on_link_died will not be called.
    fn spawn_child(self) -> ActorRef<Self>
    where
        Self: Send + Sync;

    /// Spawns an `!Sync` actor with a unidirectional link between the current actor and the child.
    ///
    /// If the current actor dies, [Actor::on_link_died] will be called on the spawned one,
    /// however if the spawned actor dies, Actor::on_link_died will not be called.
    #[cfg(not(feature = "nightly"))]
    fn spawn_unsync_child(self) -> ActorRef<Self>
    where
        Self: Send;
}

macro_rules! impl_spawn {
    (
        main_bounds = ($($main_bounds:tt)*),
        spawn_bounds = ($($spawn_bounds:tt)?),
        default = ($($default:tt)*),
        kind = $kind:ident,
    ) => {
        impl<T> Spawn for T where T: Actor + 'static $(+ $main_bounds)* {
            $($default)* fn actor_ref(&self) -> ActorRef<Self> {
                match Self::try_actor_ref() {
                    Some(actor_ref) => actor_ref,
                    None => panic!("actor_ref called outside the scope of an actor"),
                }
            }

            $($default)* fn try_actor_ref() -> Option<ActorRef<Self>> {
                ActorRef::current()
            }

            $($default)* fn spawn(self) -> ActorRef<Self>
            where
                Self: Send $(+ $spawn_bounds)?,
            {
                spawn(|ctx, id, mailbox_rx, abort_registration, links| {
                    tokio::spawn(CURRENT_CTX.scope(ctx, async move {
                        run_actor_lifecycle(
                            id,
                            self,
                            $kind::new,
                            mailbox_rx,
                            abort_registration,
                            links,
                        )
                        .await
                    }));
                })
            }

            #[cfg(not(feature = "nightly"))]
            fn spawn_unsync(self) -> ActorRef<Self>
            where
                Self: Send,
            {
                spawn(|ctx, id, mailbox_rx, abort_registration, links| {
                    tokio::spawn(CURRENT_CTX.scope(ctx, async move {
                        run_actor_lifecycle(
                            id,
                            self,
                            UnsyncActor::new,
                            mailbox_rx,
                            abort_registration,
                            links,
                        )
                        .await
                    }));
                })
            }

            $($default)* fn spawn_link(self) -> ActorRef<Self>
            where
                Self: Send $(+ $spawn_bounds)?,
            {
                spawn_link(|| self.spawn())
            }

            #[cfg(not(feature = "nightly"))]
            fn spawn_unsync_link(self) -> ActorRef<Self>
            where
                Self: Send,
            {
                spawn_link(|| self.spawn_unsync())
            }

            $($default)* fn spawn_child(self) -> ActorRef<Self>
            where
                Self: Send $(+ $spawn_bounds)?,
            {
                spawn_child(|| self.spawn())
            }

            #[cfg(not(feature = "nightly"))]
            fn spawn_unsync_child(self) -> ActorRef<Self>
            where
                Self: Send,
            {
                spawn_child(|| self.spawn_unsync())
            }
        }
    };
}

#[cfg(not(feature = "nightly"))]
impl_spawn!(
    main_bounds = (),
    spawn_bounds = (Sync),
    default = (),
    kind = SyncActor,
);

#[cfg(feature = "nightly")]
impl_spawn!(
    main_bounds = (),
    spawn_bounds = (),
    default = (default),
    kind = UnsyncActor,
);

#[cfg(feature = "nightly")]
impl_spawn!(
    main_bounds = (Sync),
    spawn_bounds = (),
    default = (),
    kind = SyncActor,
);

#[inline]
fn spawn<A>(
    spawn: impl FnOnce(Ctx, u64, mpsc::UnboundedReceiver<Signal<A>>, AbortRegistration, Links),
) -> ActorRef<A>
where
    A: Actor + 'static,
{
    let (mailbox, mailbox_rx) = mpsc::unbounded_channel::<Signal<A>>();
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

    spawn(ctx, id, mailbox_rx, abort_registration, links);

    actor_ref
}

#[inline]
fn spawn_link<A>(spawn: impl FnOnce() -> ActorRef<A>) -> ActorRef<A>
where
    A: 'static,
{
    let actor_ref = spawn();
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

#[inline]
fn spawn_child<A>(spawn: impl FnOnce() -> ActorRef<A>) -> ActorRef<A>
where
    A: 'static,
{
    let actor_ref = spawn();
    let parent_links = CURRENT_CTX
        .try_with(|ctx| ctx.links.clone())
        .expect("spawn_child cannot be called outside any actors");

    parent_links
        .lock()
        .unwrap()
        .insert(actor_ref.id(), actor_ref.signal_mailbox());

    actor_ref
}

async fn run_actor_lifecycle<A, S>(
    id: u64,
    mut actor: A,
    new_state: impl Fn(A) -> S,
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

    let mut state = new_state(actor);

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
