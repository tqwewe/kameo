use std::{convert, mem, panic::AssertUnwindSafe, sync::Arc};

use futures::{
    stream::{AbortHandle, AbortRegistration, Abortable},
    Future, FutureExt,
};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::Semaphore,
};
use tracing::{error, trace};

use crate::{
    actor::{
        kind::{ActorBehaviour, ActorState},
        Actor, ActorRef, Links, CURRENT_ACTOR_ID,
    },
    error::{ActorStopReason, PanicError},
    mailbox::{Mailbox, MailboxReceiver, Signal},
};

use super::ActorID;

/// Spawns an actor in a Tokio task, running asynchronously.
///
/// This function spawns the actor in a non-blocking Tokio task, making it suitable for actors that need to
/// perform asynchronous operations. The actor runs in the background and can be interacted with through
/// the returned [`ActorRef`].
///
/// # Example
///
/// ```
/// use kameo::Actor;
///
/// #[derive(Actor)]
/// struct MyActor;
///
/// # tokio_test::block_on(async {
/// let actor_ref = kameo::spawn(MyActor);
/// # })
/// ```
///
/// The actor will continue running in the background, and messages can be sent to it via `actor_ref`.
pub fn spawn<A>(actor: A) -> ActorRef<A>
where
    A: Actor,
{
    spawn_inner::<A, ActorBehaviour<A>, _, _>(|_| async move { actor })
        .now_or_never()
        .unwrap()
}

/// Spawns an actor in a Tokio task, using a factory function that provides access to the [`ActorRef`].
///
/// This function is useful when the actor requires access to its own reference during initialization. The
/// factory function is provided with the actor reference and is responsible for returning the initialized actor.
///
/// # Example
///
/// ```
/// use kameo::Actor;
/// use kameo::actor::ActorRef;
///
/// #[derive(Actor)]
/// struct MyActor { actor_ref: ActorRef<Self> }
///
/// # tokio_test::block_on(async {
/// let actor_ref = kameo::actor::spawn_with(|actor_ref| {
///     let actor = MyActor {
///         actor_ref: actor_ref.clone(),
///     };
///     async { actor }
/// })
/// .await;
/// # })
/// ```
///
/// This allows the actor to have access to its own `ActorRef` during creation, which can be useful for actors
/// that need to communicate with themselves or manage internal state more effectively.
pub async fn spawn_with<A, F, Fu>(f: F) -> ActorRef<A>
where
    A: Actor,
    F: FnOnce(&ActorRef<A>) -> Fu,
    Fu: Future<Output = A>,
{
    spawn_inner::<A, ActorBehaviour<A>, _, _>(f).await
}

/// Spawns an actor in its own dedicated thread, allowing for blocking operations.
///
/// This function spawns the actor in a separate thread, making it suitable for actors that perform blocking
/// operations, such as file I/O or other tasks that cannot be efficiently executed in an asynchronous context.
/// Despite running in a blocking thread, the actor can still communicate asynchronously with other actors.
///
/// # Example
///
/// ```no_run
/// use std::io::{self, Write};
/// use std::fs::File;
///
/// use kameo::Actor;
/// use kameo::message::{Context, Message};
/// use kameo::request::MessageSendSync;
///
/// #[derive(Actor)]
/// struct MyActor {
///     file: File,
/// }
///
/// struct Flush;
/// impl Message<Flush> for MyActor {
///     type Reply = io::Result<()>;
///
///     async fn handle(&mut self, _: Flush, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
///         self.file.flush() // This blocking operation is handled in its own thread
///     }
/// }
///
/// let actor_ref = kameo::actor::spawn_in_thread(MyActor { file: File::create("output.txt").unwrap() });
/// actor_ref.tell(Flush).send_sync()?;
/// # Ok::<(), kameo::error::SendError<Flush, io::Error>>(())
/// ```
///
/// This function is useful for actors that require or benefit from running blocking operations while still
/// enabling asynchronous functionality.
pub fn spawn_in_thread<A>(actor: A) -> ActorRef<A>
where
    A: Actor,
{
    spawn_in_thread_inner::<A, ActorBehaviour<A>>(actor)
}

#[inline]
async fn spawn_inner<A, S, F, Fu>(f: F) -> ActorRef<A>
where
    A: Actor,
    S: ActorState<A> + Send,
    F: FnOnce(&ActorRef<A>) -> Fu,
    Fu: Future<Output = A>,
{
    let (mailbox, mailbox_rx) = A::new_mailbox();
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let links = Links::default();
    let startup_semaphore = Arc::new(Semaphore::new(0));
    let actor_ref = ActorRef::new(
        mailbox,
        abort_handle,
        links.clone(),
        startup_semaphore.clone(),
    );
    let id = actor_ref.id();
    let actor = f(&actor_ref).await;

    #[cfg(not(tokio_unstable))]
    {
        tokio::spawn(CURRENT_ACTOR_ID.scope(id, {
            let actor_ref = actor_ref.clone();
            async move {
                run_actor_lifecycle::<A, S>(
                    actor_ref,
                    actor,
                    mailbox_rx,
                    abort_registration,
                    links,
                    startup_semaphore,
                )
                .await
            }
        }));
    }

    #[cfg(tokio_unstable)]
    {
        tokio::task::Builder::new()
            .name(A::name())
            .spawn(CURRENT_ACTOR_ID.scope(id, {
                let actor_ref = actor_ref.clone();
                async move {
                    run_actor_lifecycle::<A, S>(
                        actor_ref,
                        actor,
                        mailbox_rx,
                        abort_registration,
                        links,
                        startup_semaphore,
                    )
                    .await
                }
            }))
            .unwrap();
    }

    actor_ref
}

#[inline]
fn spawn_in_thread_inner<A, S>(actor: A) -> ActorRef<A>
where
    A: Actor,
    S: ActorState<A> + Send,
{
    let handle = Handle::current();
    if matches!(handle.runtime_flavor(), RuntimeFlavor::CurrentThread) {
        panic!("threaded actors are not supported in a single threaded tokio runtime");
    }

    let (mailbox, mailbox_rx) = A::new_mailbox();
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let links = Links::default();
    let startup_semaphore = Arc::new(Semaphore::new(0));
    let actor_ref = ActorRef::new(
        mailbox,
        abort_handle,
        links.clone(),
        startup_semaphore.clone(),
    );
    let id = actor_ref.id();

    std::thread::Builder::new()
        .name(A::name().to_string())
        .spawn({
            let actor_ref = actor_ref.clone();
            move || {
                handle.block_on(CURRENT_ACTOR_ID.scope(
                    id,
                    run_actor_lifecycle::<A, S>(
                        actor_ref,
                        actor,
                        mailbox_rx,
                        abort_registration,
                        links,
                        startup_semaphore,
                    ),
                ))
            }
        })
        .unwrap();

    actor_ref
}

#[inline]
async fn run_actor_lifecycle<A, S>(
    actor_ref: ActorRef<A>,
    mut actor: A,
    mailbox_rx: <A::Mailbox as Mailbox<A>>::Receiver,
    abort_registration: AbortRegistration,
    links: Links,
    startup_semaphore: Arc<Semaphore>,
) where
    A: Actor,
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

    let _ = actor_ref
        .weak_signal_mailbox()
        .signal_startup_finished()
        .await;
    let actor_ref = {
        // Downgrade actor ref
        let weak_actor_ref = actor_ref.downgrade();
        mem::drop(actor_ref);
        weak_actor_ref
    };

    if let Err(err) = start_res {
        let reason = ActorStopReason::Panicked(err);
        let mut state = S::new_from_actor(actor, actor_ref.clone());
        let reason = state.on_shutdown(reason.clone()).await.unwrap_or(reason);
        let actor = state.shutdown().await;
        actor
            .on_stop(actor_ref.clone(), reason.clone())
            .await
            .unwrap();
        log_actor_stop_reason(id, name, &reason);
        return;
    }

    let mut state = S::new_from_actor(actor, actor_ref.clone());

    let reason = Abortable::new(
        abortable_actor_loop(&mut state, mailbox_rx, startup_semaphore),
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
    mut mailbox_rx: <A::Mailbox as Mailbox<A>>::Receiver,
    startup_semaphore: Arc<Semaphore>,
) -> ActorStopReason
where
    A: Actor,
    S: ActorState<A>,
{
    loop {
        let reason = recv_mailbox_loop(state, &mut mailbox_rx, &startup_semaphore).await;
        if let Some(reason) = state.on_shutdown(reason).await {
            return reason;
        }
    }
}

async fn recv_mailbox_loop<A, S>(
    state: &mut S,
    mailbox_rx: &mut <A::Mailbox as Mailbox<A>>::Receiver,
    startup_semaphore: &Semaphore,
) -> ActorStopReason
where
    A: Actor,
    S: ActorState<A>,
{
    loop {
        match mailbox_rx.recv().await {
            Some(Signal::StartupFinished) => {
                startup_semaphore.add_permits(Semaphore::MAX_PERMITS);
                if let Some(reason) = state.handle_startup_finished().await {
                    return reason;
                }
            }
            Some(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
            }) => {
                if let Some(reason) = state
                    .handle_message(message, actor_ref, reply, sent_within_actor)
                    .await
                {
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

#[inline]
fn log_actor_stop_reason(id: ActorID, name: &str, reason: &ActorStopReason) {
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
