use std::{
    convert,
    ops::ControlFlow,
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
    error::{invoke_actor_error_hook, ActorStopReason, PanicError, SendError},
    mailbox::{MailboxReceiver, MailboxSender, Signal},
};

use super::ActorID;

/// A `PreparedActor` represents an actor that has been initialized and is ready to be either run
/// in the current task or spawned into a new task.
///
/// The `PreparedActor` provides access to the actor's [`ActorRef`] for interacting with the actor
/// before it starts running. It allows for flexible execution, either by running the actor
/// synchronously in the current task or spawning it in a separate task or thread.
#[allow(missing_debug_implementations)]
#[must_use = "the prepared actor needs to be ran/spawned"]
pub struct PreparedActor<A: Actor> {
    actor_ref: ActorRef<A>,
    mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
}

impl<A: Actor> PreparedActor<A> {
    /// Creates a new prepared actor with a specific mailbox configuration, allowing access to its [`ActorRef`] before spawning.
    ///
    /// This function allows you to explicitly specify a mailbox when preparing an actor.
    /// Use this when you need custom mailbox behavior or capacity.
    ///
    /// This is typically created though [`Actor::prepare`] and [`Actor::prepare_with_mailbox`].
    pub fn new((mailbox_tx, mailbox_rx): (MailboxSender<A>, MailboxReceiver<A>)) -> Self {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let links = Links::default();
        let startup_semaphore = Arc::new(Semaphore::new(0));
        let startup_error = Arc::new(OnceLock::new());
        let shutdown_error = Arc::new(OnceLock::new());
        let actor_ref = ActorRef::new(
            mailbox_tx,
            abort_handle,
            links,
            startup_semaphore,
            startup_error,
            shutdown_error,
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
    ///
    /// # #[derive(Actor)]
    /// # struct MyActor;
    /// #
    /// # impl Message<&'static str> for MyActor {
    /// #     type Reply = ();
    /// #     async fn handle(&mut self, msg: &'static str, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply { }
    /// # }
    /// #
    /// # tokio_test::block_on(async {
    /// let prepared_actor = MyActor::prepare();
    /// // Send it a message before it runs
    /// prepared_actor.actor_ref().tell("hello!").await?;
    /// prepared_actor.run(MyActor).await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn run(self, args: A::Args) -> Result<(A, ActorStopReason), PanicError> {
        run_actor_lifecycle::<A, ActorBehaviour<A>>(
            args,
            self.actor_ref,
            self.mailbox_rx,
            self.abort_registration,
        )
        .await
    }

    /// Spawns the actor in a new background tokio task, returning the `JoinHandle`.
    ///
    /// See [`Actor::spawn`] for more information.
    pub fn spawn(self, args: A::Args) -> JoinHandle<Result<(A, ActorStopReason), PanicError>> {
        #[cfg(not(all(tokio_unstable, feature = "tracing")))]
        {
            tokio::spawn(CURRENT_ACTOR_ID.scope(self.actor_ref.id(), self.run(args)))
        }

        #[cfg(all(tokio_unstable, feature = "tracing"))]
        {
            tokio::task::Builder::new()
                .name(A::name())
                .spawn(CURRENT_ACTOR_ID.scope(self.actor_ref.id(), self.run(args)))
                .unwrap()
        }
    }

    /// Spawns the actor in a new background thread, returning the `JoinHandle`.
    ///
    /// See [`Actor::spawn_in_thread`] for more information.
    pub fn spawn_in_thread(
        self,
        args: A::Args,
    ) -> thread::JoinHandle<Result<(A, ActorStopReason), PanicError>> {
        let handle = Handle::current();
        if matches!(handle.runtime_flavor(), RuntimeFlavor::CurrentThread) {
            panic!("threaded actors are not supported in a single threaded tokio runtime");
        }

        std::thread::Builder::new()
            .name(A::name().to_string())
            .spawn({
                let actor_ref = self.actor_ref.clone();
                move || handle.block_on(CURRENT_ACTOR_ID.scope(actor_ref.id(), self.run(args)))
            })
            .unwrap()
    }
}

#[inline]
async fn run_actor_lifecycle<A, S>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
) -> Result<(A, ActorStopReason), PanicError>
where
    A: Actor,
    S: ActorState<A>,
{
    #[allow(unused_mut)]
    let mut id = actor_ref.id();
    let name = A::name();
    #[cfg(feature = "tracing")]
    trace!(%id, %name, "actor started");

    let start_res = AssertUnwindSafe(A::on_start(args, actor_ref.clone()))
        .catch_unwind()
        .await
        .map(|res| res.map_err(|err| PanicError::new(Box::new(err))))
        .map_err(PanicError::new_from_panic_any)
        .and_then(convert::identity);
    match &start_res {
        Ok(actor) => {
            actor_ref
                .startup_error
                .set(None)
                .expect("nothing else should set the startup error");
            Some(actor)
        }
        Err(err) => {
            invoke_actor_error_hook(err);
            actor_ref
                .startup_error
                .set(Some(err.clone()))
                .expect("nothing else should set the startup error");
            None
        }
    };

    let mut startup_finished = false;
    if let Err(SendError::MailboxFull(())) =
        actor_ref.weak_signal_mailbox().signal_startup_finished()
    {
        startup_finished = true;
    }

    let (actor_ref, links, startup_semaphore) = {
        //shadow actor_ref so it will be dropped at the end of the scope
        let actor_ref = actor_ref;
        // Downgrade actor ref
        let weak_actor_ref = actor_ref.downgrade();
        (weak_actor_ref, actor_ref.links, actor_ref.startup_semaphore)
    };

    let mut state = match start_res {
        Ok(actor) => S::new_from_actor(actor, actor_ref.clone()),
        Err(err) => {
            startup_semaphore.add_permits(Semaphore::MAX_PERMITS);
            let reason = ActorStopReason::Panicked(err);
            log_actor_stop_reason(id, name, &reason);
            let ActorStopReason::Panicked(err) = reason else {
                unreachable!()
            };
            return Err(err);
        }
    };

    let reason = Abortable::new(
        abortable_actor_loop(
            &mut state,
            &mut mailbox_rx,
            startup_semaphore,
            startup_finished,
        ),
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

    let on_stop_res = actor.on_stop(actor_ref.clone(), reason.clone()).await;
    log_actor_stop_reason(id, name, &reason);

    while let Some(()) = link_notification_futures.next().await {}
    #[cfg(not(feature = "remote"))]
    crate::registry::ACTOR_REGISTRY.lock().unwrap().remove(name);
    #[cfg(feature = "remote")]
    remote::REMOTE_REGISTRY.lock().await.remove(&id);

    match on_stop_res {
        Ok(()) => {
            if let Some(actor_ref) = actor_ref.upgrade() {
                actor_ref
                    .shutdown_error
                    .set(None)
                    .expect("nothing else should set the shutdown error");
            }
        }
        Err(err) => {
            let err = PanicError::new(Box::new(err));
            invoke_actor_error_hook(&err);
            if let Some(actor_ref) = actor_ref.upgrade() {
                actor_ref
                    .shutdown_error
                    .set(Some(err))
                    .expect("nothing else should set the shutdown error");
            }
        }
    }

    Ok((actor, reason))
}

async fn abortable_actor_loop<A, S>(
    state: &mut S,
    mailbox_rx: &mut MailboxReceiver<A>,
    startup_semaphore: Arc<Semaphore>,
    startup_finished: bool,
) -> ActorStopReason
where
    A: Actor,
    S: ActorState<A>,
{
    if startup_finished {
        if let ControlFlow::Break(reason) = state.handle_startup_finished().await {
            return reason;
        }
    }
    loop {
        let reason = recv_mailbox_loop(state, mailbox_rx, &startup_semaphore).await;
        if let ControlFlow::Break(reason) = state.on_shutdown(reason).await {
            return reason;
        }
    }
}

async fn recv_mailbox_loop<A, S>(
    state: &mut S,
    mailbox_rx: &mut MailboxReceiver<A>,
    startup_semaphore: &Semaphore,
) -> ActorStopReason
where
    A: Actor,
    S: ActorState<A>,
{
    loop {
        match state.next(mailbox_rx).await {
            Some(Signal::StartupFinished) => {
                startup_semaphore.add_permits(Semaphore::MAX_PERMITS);
                if let ControlFlow::Break(reason) = state.handle_startup_finished().await {
                    return reason;
                }
            }
            Some(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
            }) => {
                if let ControlFlow::Break(reason) = state
                    .handle_message(message, actor_ref, reply, sent_within_actor)
                    .await
                {
                    return reason;
                }
            }
            Some(Signal::LinkDied { id, reason }) => {
                if let ControlFlow::Break(reason) = state.handle_link_died(id, reason).await {
                    return reason;
                }
            }
            Some(Signal::Stop) | None => {
                if let ControlFlow::Break(reason) = state.handle_stop().await {
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
