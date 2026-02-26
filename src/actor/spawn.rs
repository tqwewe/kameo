use std::{convert, ops::ControlFlow, panic::AssertUnwindSafe, sync::Arc, thread};

use futures::{
    FutureExt, StreamExt,
    future::BoxFuture,
    stream::{AbortHandle, AbortRegistration, Abortable, FuturesUnordered},
};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::SetOnce,
    task::JoinHandle,
};
#[cfg(feature = "tracing")]
use tracing::{error, trace};

#[cfg(feature = "remote")]
use crate::remote;

use crate::{
    actor::{Actor, ActorRef, CURRENT_ACTOR_ID, Link, Links, kind::ActorBehaviour},
    error::{ActorStopReason, PanicError, PanicReason, SendError, invoke_actor_error_hook},
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
}

impl<A: Actor> PreparedActor<A> {
    /// Creates a new prepared actor with a specific mailbox configuration, allowing access to its [`ActorRef`] before spawning.
    ///
    /// This function allows you to explicitly specify a mailbox when preparing an actor.
    /// Use this when you need custom mailbox behavior or capacity.
    ///
    /// This is typically created though [`Actor::prepare`](crate::actor::Spawn::prepare) and [`Actor::prepare_with_mailbox`](crate::actor::Spawn::prepare_with_mailbox).
    pub fn new((mailbox_tx, mailbox_rx): (MailboxSender<A>, MailboxReceiver<A>)) -> Self {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let links = Links::default();
        let startup_result = Arc::new(SetOnce::new());
        let shutdown_result = Arc::new(SetOnce::new());
        let actor_ref = ActorRef::new(
            mailbox_tx,
            abort_handle,
            links,
            startup_result,
            shutdown_result,
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

#[inline]
async fn run_actor_lifecycle<A>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
) -> Result<(A, ActorStopReason), PanicError>
where
    A: Actor,
{
    #[allow(unused_mut)]
    let mut id = actor_ref.id();
    let name = A::name();
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

            let reason = Abortable::new(
                abortable_actor_loop(
                    &mut state,
                    mailbox_rx,
                    &actor_ref.startup_result,
                    startup_finished,
                ),
                abort_registration,
            )
            .await
            .unwrap_or(ActorStopReason::Killed);

            let mut actor = state.shutdown().await;

            let mut notify_futs = notify_links(id, &actor_ref.links, &reason).await;

            log_actor_stop_reason(id, name, &reason);
            let on_stop_res = actor.on_stop(actor_ref.clone(), reason.clone()).await;
            while let Some(()) = notify_futs.next().await {}

            unregister_actor(&id).await;

            match on_stop_res {
                Ok(()) => {
                    actor_ref
                        .shutdown_result
                        .set(Ok(()))
                        .expect("nothing else should set the shutdown result");
                }
                Err(err) => {
                    let err = PanicError::new(Box::new(err), PanicReason::OnStop);
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

            let mut notify_futs = notify_links(id, &actor_ref.links, &reason).await;
            while let Some(()) = notify_futs.next().await {}

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
}

async fn abortable_actor_loop<A>(
    state: &mut ActorBehaviour<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    startup_result: &SetOnce<Result<(), PanicError>>,
    startup_finished: bool,
) -> ActorStopReason
where
    A: Actor,
{
    if startup_finished && let ControlFlow::Break(reason) = state.handle_startup_finished().await {
        return reason;
    }
    loop {
        let reason = recv_mailbox_loop(state, &mut mailbox_rx, startup_result).await;
        if let ControlFlow::Break(reason) = state.on_shutdown(reason).await {
            return reason;
        }
    }
}

async fn recv_mailbox_loop<A>(
    state: &mut ActorBehaviour<A>,
    mailbox_rx: &mut MailboxReceiver<A>,
    startup_result: &SetOnce<Result<(), PanicError>>,
) -> ActorStopReason
where
    A: Actor,
{
    loop {
        match state.next(mailbox_rx).await {
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
            }) => {
                if let ControlFlow::Break(reason) = state
                    .handle_message(message, actor_ref, reply, sent_within_actor)
                    .await
                {
                    return reason;
                }
            }
            ControlFlow::Continue(Signal::LinkDied { id, reason }) => {
                if let ControlFlow::Break(reason) = state.handle_link_died(id, reason).await {
                    return reason;
                }
            }
            ControlFlow::Continue(Signal::Stop) => {
                if let ControlFlow::Break(reason) = state.handle_stop().await {
                    return reason;
                }
            }
            ControlFlow::Break(reason) => return reason,
        }
    }
}

async fn notify_links(
    id: ActorId,
    links: &Links,
    reason: &ActorStopReason,
) -> FuturesUnordered<BoxFuture<'static, ()>> {
    let futs = FuturesUnordered::new();
    {
        let mut links = links.lock().await;
        if links.is_empty() {
            return futs;
        }
        let reason = Arc::new(reason.clone()); // Single clone into Arc
        #[allow(unused_variables)]
        for (link_actor_id, link) in links.drain() {
            match link {
                Link::Local(mailbox) => {
                    let reason = Arc::clone(&reason);
                    futs.push(
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
                        let reason = (*reason).clone(); // Full clone for wire serialization
                        futs.push(
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

    futs
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
        reason @ ActorStopReason::Normal
        | reason @ ActorStopReason::Killed
        | reason @ ActorStopReason::LinkDied { .. } => {
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

// ---------------------------------------------------------------------------
// Tests — Arc<ActorStopReason> signal chain
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::{
        Actor,
        actor::{ActorRef, Spawn, WeakActorRef},
        error::{ActorStopReason, Infallible},
        mailbox,
    };

    /// An actor that records its stop reason via a shared slot.
    struct ReasonTracker {
        slot: Arc<Mutex<Option<ActorStopReason>>>,
    }

    impl Actor for ReasonTracker {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(state)
        }

        fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            reason: ActorStopReason,
        ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
            let slot = self.slot.clone();
            async move {
                *slot.lock().unwrap() = Some(reason);
                Ok(())
            }
        }
    }

    /// A minimal actor with no tracking, used as the "dying" side of a link.
    struct SimpleActor;

    impl Actor for SimpleActor {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    // ── kill propagation through link ────────────────────────────────

    #[tokio::test]
    async fn linked_actor_receives_killed_reason() {
        let reason_slot = Arc::new(Mutex::new(None));

        let actor_a = SimpleActor::spawn(SimpleActor);
        let actor_b = ReasonTracker::spawn_with_mailbox(
            ReasonTracker { slot: reason_slot.clone() },
            mailbox::unbounded(),
        );

        actor_a.link(&actor_b).await;
        actor_a.kill();
        actor_b.wait_for_shutdown().await;

        let reason = reason_slot.lock().unwrap().take()
            .expect("on_stop should have been called");
        match reason {
            ActorStopReason::LinkDied { reason, .. } => {
                assert!(
                    matches!(*reason, ActorStopReason::Killed),
                    "inner reason should be Killed, got {reason:?}",
                );
            }
            other => panic!("expected LinkDied, got {other:?}"),
        }
    }

    // ── multiple links all receive death notification ────────────────

    #[tokio::test]
    async fn multiple_links_all_receive_death_notification() {
        let slot_b = Arc::new(Mutex::new(None));
        let slot_c = Arc::new(Mutex::new(None));

        let actor_a = SimpleActor::spawn(SimpleActor);
        let actor_b = ReasonTracker::spawn_with_mailbox(
            ReasonTracker { slot: slot_b.clone() },
            mailbox::unbounded(),
        );
        let actor_c = ReasonTracker::spawn_with_mailbox(
            ReasonTracker { slot: slot_c.clone() },
            mailbox::unbounded(),
        );

        actor_a.link(&actor_b).await;
        actor_a.link(&actor_c).await;

        actor_a.kill();
        actor_b.wait_for_shutdown().await;
        actor_c.wait_for_shutdown().await;

        for (name, slot) in [("B", &slot_b), ("C", &slot_c)] {
            let reason = slot.lock().unwrap().take()
                .unwrap_or_else(|| panic!("actor {name} on_stop was not called"));
            assert!(
                matches!(reason, ActorStopReason::LinkDied { .. }),
                "actor {name} should stop with LinkDied, got {reason:?}",
            );
        }
    }

    // ── normal stop does NOT propagate through link ──────────────────

    #[tokio::test]
    async fn normal_stop_does_not_kill_linked_actor() {
        let slot_b = Arc::new(Mutex::new(None));

        let actor_a = SimpleActor::spawn_with_mailbox(SimpleActor, mailbox::unbounded());
        let actor_b = ReasonTracker::spawn_with_mailbox(
            ReasonTracker { slot: slot_b.clone() },
            mailbox::unbounded(),
        );

        actor_a.link(&actor_b).await;

        // Graceful stop sends Signal::Stop → on_link_died receives Normal → Continue
        actor_a.stop_gracefully().await.unwrap();
        actor_a.wait_for_shutdown().await;

        // Give B a moment to process any signals.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // B should still be alive — on_link_died default for Normal is Continue.
        assert!(
            slot_b.lock().unwrap().is_none(),
            "actor B should NOT have stopped (Normal death does not propagate)",
        );

        // Clean up: kill B so the test exits.
        actor_b.kill();
        actor_b.wait_for_shutdown().await;
    }
}
