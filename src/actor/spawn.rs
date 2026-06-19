use std::{
    collections::VecDeque, convert, ops::ControlFlow, panic::AssertUnwindSafe, sync::Arc, thread,
};

use futures::{
    FutureExt,
    stream::{AbortHandle, AbortRegistration, Abortable},
};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::SetOnce,
    task::JoinHandle,
};
#[cfg(feature = "tracing")]
use tracing::{Instrument, error, trace};

#[cfg(feature = "remote")]
use crate::remote;

use crate::{
    actor::{Actor, ActorRef, CURRENT_ACTOR_ID, kind::ActorBehaviour},
    error::{ActorStopReason, PanicError, PanicReason, SendError, invoke_actor_error_hook},
    links::Links,
    mailbox::{MailboxReceiver, MailboxSender, Signal},
};

use super::ActorId;

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
    #[cfg(feature = "console")]
    monitor: Arc<crate::console::registry::ActorMonitor>,
}

impl<A: Actor> PreparedActor<A> {
    /// Creates a new prepared actor with a specific mailbox configuration, allowing access to its [`ActorRef`] before spawning.
    ///
    /// This function allows you to explicitly specify a mailbox when preparing an actor.
    /// Use this when you need custom mailbox behavior or capacity.
    ///
    /// This is typically created though [`Actor::prepare`](crate::actor::Spawn::prepare) and [`Actor::prepare_with_mailbox`](crate::actor::Spawn::prepare_with_mailbox).
    pub fn new((mailbox_tx, mailbox_rx): (MailboxSender<A>, MailboxReceiver<A>)) -> Self {
        Self::new_with(
            ActorId::generate(),
            (mailbox_tx, mailbox_rx),
            Links::default(),
        )
    }

    pub(crate) fn new_with(
        actor_id: ActorId,
        (mailbox_tx, mailbox_rx): (MailboxSender<A>, MailboxReceiver<A>),
        links: Links,
    ) -> Self {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let startup_result = Arc::new(SetOnce::new());
        let shutdown_result = Arc::new(SetOnce::new());
        let actor_ref = ActorRef::new(
            actor_id,
            mailbox_tx,
            abort_handle,
            links,
            startup_result,
            shutdown_result,
        );

        #[cfg(feature = "console")]
        let monitor = crate::console::registry::register_or_get::<A>(
            actor_id,
            actor_ref.mailbox_sender(),
            &actor_ref.links,
        );

        PreparedActor {
            actor_ref,
            mailbox_rx,
            abort_registration,
            #[cfg(feature = "console")]
            monitor,
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
    /// # use kameo::actor::{PreparedActor, Spawn};
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
        run_actor_lifecycle::<A>(
            args,
            self.actor_ref,
            self.mailbox_rx,
            self.abort_registration,
            #[cfg(feature = "console")]
            self.monitor,
        )
        .await
    }

    /// Spawns the actor in a new background tokio task, returning the `JoinHandle`.
    ///
    /// See [`Spawn::spawn`](crate::actor::Spawn::spawn) for more information.
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
    /// See [`Spawn::spawn_in_thread`](crate::actor::Spawn::spawn_in_thread) for more information.
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

async fn run_actor_lifecycle<A>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
    #[cfg(feature = "console")] monitor: Arc<crate::console::registry::ActorMonitor>,
) -> Result<(A, ActorStopReason), PanicError>
where
    A: Actor,
{
    #[allow(unused_mut)]
    let mut id = actor_ref.id();
    let name = A::name();

    #[cfg(feature = "console")]
    let monitor_scope = Arc::clone(&monitor);

    let task = async move {
        #[cfg(feature = "tracing")]
        trace!(%id, %name, "actor started");

        let start_res = AssertUnwindSafe(A::on_start(args, actor_ref.clone()))
            .catch_unwind()
            .await
            .map(|res| res.map_err(|err| PanicError::new(Box::new(err), PanicReason::OnStart)))
            .map_err(|err| PanicError::new_from_panic_any(err, PanicReason::OnStart))
            .and_then(convert::identity);
        let startup_finished = matches!(
            actor_ref.weak_signal_mailbox().signal_startup_finished(),
            Err(SendError::MailboxFull(()))
        );

        let actor_ref = actor_ref.into_downgrade();

        match start_res {
            Ok(actor) => {
                let mut state = ActorBehaviour::new_from_actor(actor, actor_ref.clone());

                #[cfg(feature = "console")]
                monitor.set_running();

                let reason = Abortable::new(
                    abortable_actor_loop(
                        &mut state,
                        &mut mailbox_rx,
                        &actor_ref.startup_result,
                        startup_finished,
                        #[cfg(feature = "console")]
                        &monitor,
                    ),
                    abort_registration,
                )
                .await
                .unwrap_or(ActorStopReason::Killed);

                #[cfg(feature = "console")]
                monitor.set_stopping();

                let mut actor = state.shutdown().await;

                actor_ref.links.set_children_parent_shutdown().await;
                actor_ref.links.send_children_shutdown().await;
                drain_until_children_closed(&actor_ref.links, &mut mailbox_rx).await;
                actor_ref
                    .links
                    .lock()
                    .await
                    .notify_links(id, reason.clone(), mailbox_rx);

                log_actor_stop_reason(id, name, &reason);
                let on_stop_res =
                    AssertUnwindSafe(actor.on_stop(actor_ref.clone(), reason.clone()))
                        .catch_unwind()
                        .await
                        .map(|res| {
                            res.map_err(|err| PanicError::new(Box::new(err), PanicReason::OnStop))
                        })
                        .map_err(|err| PanicError::new_from_panic_any(err, PanicReason::OnStop))
                        .and_then(convert::identity);

                #[cfg(feature = "console")]
                monitor.set_stopped(&reason);

                unregister_actor(&id).await;

                match on_stop_res {
                    Ok(()) => {
                        actor_ref
                            .shutdown_result
                            .set(Ok(reason.clone()))
                            .expect("nothing else should set the shutdown result");
                    }
                    Err(err) => {
                        invoke_actor_error_hook(&err);

                        actor_ref
                            .shutdown_result
                            .set(Err(err))
                            .expect("nothing else should set the shutdown result");
                    }
                }

                Ok((actor, reason))
            }
            Err(err) => {
                actor_ref
                    .startup_result
                    .set(Err(err.clone()))
                    .expect("nothing should set the startup result");

                let reason = ActorStopReason::Panicked(err);
                log_actor_stop_reason(id, name, &reason);

                actor_ref.links.set_children_parent_shutdown().await;
                actor_ref.links.send_children_shutdown().await;
                drain_until_children_closed(&actor_ref.links, &mut mailbox_rx).await;
                actor_ref
                    .links
                    .lock()
                    .await
                    .notify_links(id, reason.clone(), mailbox_rx);

                #[cfg(feature = "console")]
                monitor.set_stopped(&reason);

                unregister_actor(&id).await;

                let ActorStopReason::Panicked(err) = reason else {
                    unreachable!()
                };

                actor_ref
                    .shutdown_result
                    .set(Err(err.clone()))
                    .expect("nothing should set the startup result");

                Err(err)
            }
        }
    };

    #[cfg(feature = "console")]
    let task = crate::console::registry::with_monitor(monitor_scope, task);

    #[cfg(not(feature = "tracing"))]
    {
        task.await
    }

    #[cfg(feature = "tracing")]
    {
        let actor_span = tracing::info_span!("actor.lifecycle", actor.name = name, actor.id = %id);
        task.instrument(actor_span).await
    }
}

/// Waits for all child actors to close while keeping the mailbox drained, so a child
/// notifying us cannot deadlock on a full mailbox. Pending messages pulled during the wait
/// are re-queued afterwards so they survive a supervisor restart rather than being dropped.
async fn drain_until_children_closed<A>(links: &Links, mailbox_rx: &mut MailboxReceiver<A>)
where
    A: Actor,
{
    let mut preserved = VecDeque::new();
    let wait = links.wait_children_closed();
    tokio::pin!(wait);
    loop {
        tokio::select! {
            biased;
            _ = &mut wait => break,
            signal = mailbox_rx.recv() => {
                if let Some(signal @ Signal::Message { .. }) = signal {
                    preserved.push_back(signal);
                }
            }
        }
    }
    mailbox_rx.push_front(preserved);
}

async fn abortable_actor_loop<A>(
    state: &mut ActorBehaviour<A>,
    mailbox_rx: &mut MailboxReceiver<A>,
    startup_result: &SetOnce<Result<(), PanicError>>,
    startup_finished: bool,
    #[cfg(feature = "console")] monitor: &Arc<crate::console::registry::ActorMonitor>,
) -> ActorStopReason
where
    A: Actor,
{
    if startup_finished && let ControlFlow::Break(reason) = state.handle_startup_finished().await {
        return reason;
    }
    loop {
        let reason = recv_mailbox_loop(
            state,
            mailbox_rx,
            startup_result,
            #[cfg(feature = "console")]
            monitor,
        )
        .await;
        if let ControlFlow::Break(reason) = state.on_shutdown(reason).await {
            return reason;
        }
    }
}

async fn recv_mailbox_loop<A>(
    state: &mut ActorBehaviour<A>,
    mailbox_rx: &mut MailboxReceiver<A>,
    startup_result: &SetOnce<Result<(), PanicError>>,
    #[cfg(feature = "console")] monitor: &Arc<crate::console::registry::ActorMonitor>,
) -> ActorStopReason
where
    A: Actor,
{
    loop {
        let next = state.next(mailbox_rx).await;

        #[cfg(feature = "console")]
        {
            monitor.set_mailbox_len(mailbox_rx.len());
            if let ControlFlow::Continue(signal) = &next {
                monitor.record_received(signal);
            }
        }

        match next {
            ControlFlow::Continue(Signal::StartupFinished) => {
                if startup_result.set(Ok(())).is_err() {
                    #[cfg(feature = "tracing")]
                    error!("received startup finished signal after already being started up");
                }
                if let ControlFlow::Break(reason) = state.handle_startup_finished().await {
                    return reason;
                }
            }
            ControlFlow::Continue(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
                message_name,
                #[cfg(feature = "tracing")]
                caller_span,
            }) => {
                #[cfg(feature = "console")]
                monitor.begin_handler(message_name);
                let result = state
                    .handle_message(
                        message,
                        actor_ref,
                        reply,
                        sent_within_actor,
                        message_name,
                        #[cfg(feature = "tracing")]
                        caller_span,
                    )
                    .await;
                #[cfg(feature = "console")]
                monitor.end_handler();
                if let ControlFlow::Break(reason) = result {
                    return reason;
                }
            }
            ControlFlow::Continue(Signal::LinkDied {
                id,
                reason,
                mailbox_rx,
                dead_actor_sibblings,
            }) => {
                if let ControlFlow::Break(reason) = state
                    .handle_link_died(id, reason, mailbox_rx, dead_actor_sibblings)
                    .await
                {
                    return reason;
                }
            }
            ControlFlow::Continue(Signal::Stop | Signal::SupervisorRestart) => {
                if let ControlFlow::Break(reason) = state.handle_stop().await {
                    return reason;
                }
            }
            ControlFlow::Break(reason) => return reason,
        }
    }
}

#[allow(unused_variables)]
async fn unregister_actor(id: &ActorId) {
    #[cfg(not(feature = "remote"))]
    crate::registry::ACTOR_REGISTRY
        .lock()
        .unwrap()
        .remove_by_id(id);
    #[cfg(feature = "remote")]
    if let Some(entry) = remote::REMOTE_REGISTRY.lock().await.remove(id)
        && let Some(registered_name) = entry.name
        && let Some(swarm) = remote::ActorSwarm::get()
    {
        _ = swarm.unregister(registered_name);
    }
}

#[inline]
#[cfg(feature = "tracing")]
fn log_actor_stop_reason(id: ActorId, name: &str, reason: &ActorStopReason) {
    match reason {
        reason @ (ActorStopReason::Normal
        | ActorStopReason::SupervisorRestart
        | ActorStopReason::Killed
        | ActorStopReason::LinkDied { .. }) => {
            trace!(%id, %name, ?reason, "actor stopped");
        }
        reason @ ActorStopReason::Panicked(_) => {
            error!(%id, %name, ?reason, "actor stopped")
        }
        #[cfg(feature = "remote")]
        reason @ ActorStopReason::PeerDisconnected => {
            trace!(%id, %name, ?reason, "actor stopped");
        }
    }
}

#[cfg(not(feature = "tracing"))]
fn log_actor_stop_reason(_id: ActorId, _name: &str, _reason: &ActorStopReason) {}
