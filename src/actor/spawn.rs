use std::{
    convert,
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
    thread,
};

use futures::{
    stream::{AbortHandle, AbortRegistration, Abortable, FuturesUnordered},
    FutureExt, StreamExt,
};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::Semaphore,
    task::JoinHandle,
};
#[cfg(feature = "tracing")]
use tracing::{error, trace};

#[cfg(feature = "remote")]
use crate::remote;

use crate::{
    actor::{
        kind::{ActorBehaviour, ActorState},
        Actor, ActorRef, Link, Links, CURRENT_ACTOR_ID,
    },
    error::{ActorStopReason, PanicError, SendError},
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
    let prepared_actor = PreparedActor::new();
    let actor_ref = prepared_actor.actor_ref().clone();
    prepared_actor.spawn(actor);
    actor_ref
}

/// Spawns and links an actor in a Tokio task, running asynchronously.
///
/// This function is used to ensure an actor is linked with another actor before its truly spawned,
/// which avoids possible edge cases where the actor could die before having the chance to be linked.
///
/// # Example
///
/// ```
/// use kameo::Actor;
///
/// #[derive(Actor)]
/// struct FooActor;
///
/// #[derive(Actor)]
/// struct BarActor;
///
/// # tokio_test::block_on(async {
/// let link_ref = kameo::spawn(FooActor);
/// let actor_ref = kameo::actor::spawn_link(&link_ref, BarActor).await;
/// # })
/// ```
///
/// The actor will continue running in the background, and messages can be sent to it via `actor_ref`.
pub async fn spawn_link<A, L>(link_ref: &ActorRef<L>, actor: A) -> ActorRef<A>
where
    A: Actor,
    L: Actor,
{
    let prepared_actor = PreparedActor::new();
    let actor_ref = prepared_actor.actor_ref().clone();
    actor_ref.link(link_ref).await;
    prepared_actor.spawn(actor);
    actor_ref
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
    let prepared_actor = PreparedActor::new();
    let actor_ref = prepared_actor.actor_ref().clone();
    prepared_actor.spawn_in_thread(actor);
    actor_ref
}

/// A `PreparedActor` represents an actor that has been initialized and is ready to be either run
/// in the current task or spawned into a new task.
///
/// The `PreparedActor` provides access to the actor's [`ActorRef`] for interacting with the actor
/// before it starts running. It allows for flexible execution, either by running the actor
/// synchronously in the current task or spawning it in a separate task or thread.
#[allow(missing_debug_implementations)]
pub struct PreparedActor<A: Actor> {
    actor_ref: ActorRef<A>,
    mailbox_rx: <A::Mailbox as Mailbox<A>>::Receiver,
    abort_registration: AbortRegistration,
}

impl<A: Actor> PreparedActor<A> {
    /// Creates a new prepared actor, allowing access to its [`ActorRef`] before spawning.
    ///
    /// # Example
    ///
    /// ```
    /// # use kameo::Actor;
    /// # use kameo::actor::PreparedActor;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # tokio_test::block_on(async {
    /// # let other_actor = kameo::spawn(MyActor);
    /// let prepared_actor = PreparedActor::new();
    /// prepared_actor.actor_ref().link(&other_actor).await;
    /// let actor_ref = prepared_actor.spawn(MyActor);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub fn new() -> Self {
        let (mailbox, mailbox_rx) = A::new_mailbox();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let links = Links::default();
        let startup_semaphore = Arc::new(Semaphore::new(0));
        let startup_error = Arc::new(OnceLock::new());
        let actor_ref = ActorRef::new(
            mailbox,
            abort_handle,
            links,
            startup_semaphore,
            startup_error,
        );

        PreparedActor {
            actor_ref,
            mailbox_rx,
            abort_registration,
        }
    }

    /// Returns a reference to the [`ActorRef`], which can be used to send messages to the actor.
    ///
    /// The `ActorRef` can be used for interaction before the actor starts processing its event loop.
    pub fn actor_ref(&self) -> &ActorRef<A> {
        &self.actor_ref
    }

    /// Runs the actor in the current context **without** spawning a separate task, until the actor is stopped.
    ///
    /// This is useful when you need to run an actor synchronously in the current context,
    /// without background execution, and when the actor is expected to be short-lived.
    ///
    /// Note that the actor's mailbox may already contain messages before `run` is called.
    /// In this case, the actor will process all pending messages in the mailbox before completing.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kameo::Actor;
    /// # use kameo::actor::PreparedActor;
    /// # use kameo::message::{Context, Message};
    /// # use kameo::request::MessageSend;
    /// #
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # impl Message<&'static str> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: &'static str, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let prepared_actor = PreparedActor::new();
    /// // Send it a message before it runs
    /// prepared_actor.actor_ref().tell("hello!").send().await?;
    /// prepared_actor.run(MyActor).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn run(self, actor: A) -> (A, ActorStopReason) {
        run_actor_lifecycle::<A, ActorBehaviour<A>>(
            actor,
            self.actor_ref,
            self.mailbox_rx,
            self.abort_registration,
        )
        .await
    }

    /// Spawns the actor in a new background tokio task, returning the `JoinHandle`.
    ///
    /// See [`spawn`] for more information.
    pub fn spawn(self, actor: A) -> JoinHandle<(A, ActorStopReason)> {
        #[cfg(not(tokio_unstable))]
        {
            tokio::spawn(CURRENT_ACTOR_ID.scope(self.actor_ref.id(), self.run(actor)))
        }

        #[cfg(tokio_unstable)]
        {
            tokio::task::Builder::new()
                .name(A::name())
                .spawn(CURRENT_ACTOR_ID.scope(self.actor_ref.id(), self.run(actor)))
                .unwrap()
        }
    }

    /// Spawns the actor in a new background thread, returning the `JoinHandle`.
    ///
    /// See [`spawn_in_thread`] for more information.
    pub fn spawn_in_thread(self, actor: A) -> thread::JoinHandle<(A, ActorStopReason)> {
        let handle = Handle::current();
        if matches!(handle.runtime_flavor(), RuntimeFlavor::CurrentThread) {
            panic!("threaded actors are not supported in a single threaded tokio runtime");
        }

        std::thread::Builder::new()
            .name(A::name().to_string())
            .spawn({
                let actor_ref = self.actor_ref.clone();
                move || handle.block_on(CURRENT_ACTOR_ID.scope(actor_ref.id(), self.run(actor)))
            })
            .unwrap()
    }
}

impl<A: Actor> Default for PreparedActor<A> {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
async fn run_actor_lifecycle<A, S>(
    mut actor: A,
    actor_ref: ActorRef<A>,
    mailbox_rx: <A::Mailbox as Mailbox<A>>::Receiver,
    abort_registration: AbortRegistration,
) -> (A, ActorStopReason)
where
    A: Actor,
    S: ActorState<A>,
{
    #[allow(unused_mut)]
    let mut id = actor_ref.id();
    let name = A::name();
    #[cfg(feature = "tracing")]
    trace!(%id, %name, "actor started");

    let start_res = AssertUnwindSafe(actor.on_start(actor_ref.clone()))
        .catch_unwind()
        .await
        .map(|res| res.map_err(|err| PanicError::new(Box::new(err))))
        .map_err(PanicError::new_from_panic_any)
        .and_then(convert::identity);
    match &start_res {
        Ok(_) => actor_ref
            .startup_error
            .set(None)
            .expect("nothing else should set the startup error"),
        Err(err) => actor_ref
            .startup_error
            .set(Some(err.clone()))
            .expect("nothing else should set the startup error"),
    }

    let mut startup_finished = false;
    if let Err(SendError::MailboxFull(())) =
        actor_ref.weak_signal_mailbox().signal_startup_finished()
    {
        startup_finished = true;
    }

    let (actor_ref, links, startup_semaphore) = {
        // Downgrade actor ref
        let weak_actor_ref = actor_ref.downgrade();
        (weak_actor_ref, actor_ref.links, actor_ref.startup_semaphore)
    };

    if let Err(err) = start_res {
        startup_semaphore.add_permits(Semaphore::MAX_PERMITS);
        let reason = ActorStopReason::Panicked(err);
        let mut state = S::new_from_actor(actor, actor_ref.clone());
        let reason = state.on_shutdown(reason.clone()).await.unwrap_or(reason);
        let mut actor = state.shutdown().await;
        actor
            .on_stop(actor_ref.clone(), reason.clone())
            .await
            .unwrap();
        log_actor_stop_reason(id, name, &reason);
        return (actor, reason);
    }

    let mut state = S::new_from_actor(actor, actor_ref.clone());

    let reason = Abortable::new(
        abortable_actor_loop(&mut state, mailbox_rx, startup_semaphore, startup_finished),
        abort_registration,
    )
    .await
    .unwrap_or(ActorStopReason::Killed);

    let mut actor = state.shutdown().await;

    let mut link_notification_futures = FuturesUnordered::new();
    {
        let mut links = links.lock().await;
        #[allow(unused_variables)]
        for (link_actor_id, link) in links.drain() {
            match link {
                Link::Local(mailbox) => {
                    let reason = reason.clone();
                    link_notification_futures.push(
                        async move {
                            if let Err(err) = mailbox.signal_link_died(id, reason).await {
                                #[cfg(feature = "tracing")]
                                error!("failed to notify actor a link died: {err}");
                            }
                        }
                        .boxed(),
                    );
                }
                #[cfg(feature = "remote")]
                Link::Remote(notified_actor_remote_id) => {
                    if let Some(swarm) = remote::ActorSwarm::get() {
                        let reason = reason.clone();
                        link_notification_futures.push(
                            async move {
                                let res = swarm
                                    .signal_link_died(
                                        id,
                                        link_actor_id,
                                        notified_actor_remote_id,
                                        reason,
                                    )
                                    .await;
                                if let Err(err) = res {
                                    #[cfg(feature = "tracing")]
                                    error!("failed to notify actor a link died: {err}");
                                }
                            }
                            .boxed(),
                        );
                    }
                }
            }
        }
    }

    let on_stop_res = actor.on_stop(actor_ref, reason.clone()).await;
    log_actor_stop_reason(id, name, &reason);

    while let Some(()) = link_notification_futures.next().await {}
    #[cfg(feature = "remote")]
    remote::REMOTE_REGISTRY.lock().await.remove(&id);

    on_stop_res.unwrap();

    (actor, reason)
}

async fn abortable_actor_loop<A, S>(
    state: &mut S,
    mut mailbox_rx: <A::Mailbox as Mailbox<A>>::Receiver,
    startup_semaphore: Arc<Semaphore>,
    startup_finished: bool,
) -> ActorStopReason
where
    A: Actor,
    S: ActorState<A>,
{
    if startup_finished {
        if let Some(reason) = state.handle_startup_finished().await {
            return reason;
        }
    }
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
#[cfg(feature = "tracing")]
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
        #[cfg(feature = "remote")]
        reason @ ActorStopReason::PeerDisconnected => {
            trace!(%id, %name, %reason, "actor stopped");
        }
    }
}

#[cfg(not(feature = "tracing"))]
fn log_actor_stop_reason(_id: ActorID, _name: &str, _reason: &ActorStopReason) {}
