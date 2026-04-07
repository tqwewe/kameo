use std::{
    any::Any,
    collections::{HashMap, HashSet, VecDeque},
    mem,
    ops::ControlFlow,
    panic::AssertUnwindSafe,
    sync::Arc,
};

use futures::FutureExt;
#[cfg(feature = "tracing")]
use tracing::Instrument;

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{ActorStopReason, PanicError, PanicReason},
    links::{BoxMailboxReceiver, ShutdownFn},
    mailbox::{MailboxReceiver, Signal},
    message::BoxMessage,
    reply::BoxReplySender,
    supervision::SupervisionStrategy,
};

use super::ActorId;

pub(crate) struct ActorBehaviour<A: Actor> {
    actor_ref: WeakActorRef<A>,
    state: A,
    finished_startup: bool,
    startup_buffer: VecDeque<Signal<A>>,
    coordination: CoordinationState,
}

enum CoordinationState {
    Idle,
    Coordinating {
        waiting_for: HashSet<ActorId>,
        pending_mailboxes: HashMap<ActorId, BoxMailboxReceiver>,
    },
}

impl<A> ActorBehaviour<A>
where
    A: Actor,
{
    #[inline]
    pub(crate) fn new_from_actor(actor: A, actor_ref: WeakActorRef<A>) -> Self {
        ActorBehaviour {
            actor_ref,
            state: actor,
            finished_startup: false,
            startup_buffer: VecDeque::new(),
            coordination: CoordinationState::Idle,
        }
    }

    pub(crate) async fn next(
        &mut self,
        mailbox_rx: &mut MailboxReceiver<A>,
    ) -> ControlFlow<ActorStopReason, Signal<A>> {
        let res = AssertUnwindSafe(self.state.next(self.actor_ref.clone(), mailbox_rx))
            .catch_unwind()
            .await;
        match res {
            Ok(Ok(Some(signal))) => ControlFlow::Continue(signal),
            Ok(Ok(None)) => ControlFlow::Break(ActorStopReason::Normal),
            Ok(Err(err)) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::new(
                Box::new(err),
                PanicReason::Next,
            ))),
            Err(err) => ControlFlow::Break(ActorStopReason::Panicked(
                PanicError::new_from_panic_any(err, PanicReason::Next),
            )), // The handler panicked
        }
    }

    pub(crate) async fn handle_startup_finished(&mut self) -> ControlFlow<ActorStopReason> {
        self.finished_startup = true;
        for signal in mem::take(&mut self.startup_buffer).drain(..) {
            match signal {
                Signal::Message {
                    message,
                    actor_ref,
                    reply,
                    sent_within_actor,
                    message_name,
                    #[cfg(feature = "tracing")]
                    caller_span,
                } => {
                    self.handle_message(
                        message,
                        actor_ref,
                        reply,
                        sent_within_actor,
                        message_name,
                        #[cfg(feature = "tracing")]
                        caller_span,
                    )
                    .await?;
                }
                _ => unreachable!(),
            }
        }

        ControlFlow::Continue(())
    }

    pub(crate) async fn handle_message(
        &mut self,
        message: BoxMessage<A>,
        actor_ref: ActorRef<A>,
        reply: Option<BoxReplySender>,
        sent_within_actor: bool,
        message_name: &'static str,
        #[cfg(feature = "tracing")] caller_span: tracing::Span,
    ) -> ControlFlow<ActorStopReason> {
        if !sent_within_actor && !self.finished_startup {
            // The actor is still starting up, so we'll push this message to a buffer to be processed upon startup
            self.startup_buffer.push_back(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
                message_name,
                #[cfg(feature = "tracing")]
                caller_span,
            });
            return ControlFlow::Continue(());
        }

        #[cfg(feature = "tracing")]
        let handler_span = {
            let actor_id = self.actor_ref.id();

            #[cfg(not(feature = "otel"))]
            {
                tracing::info_span!(
                    parent: &caller_span,
                    "actor.handle_message",
                    actor.name = A::name(),
                    actor.id = %actor_id,
                    message = message_name,
                )
            }

            #[cfg(feature = "otel")]
            {
                use opentelemetry::trace::TraceContextExt;
                use tracing_opentelemetry::OpenTelemetrySpanExt;

                let span_name = format!("{}.{message_name}", A::name());
                let span = tracing::info_span!(
                    parent: &caller_span,
                    "actor.handle_message",
                    otel.name = %span_name,
                    actor.name = A::name(),
                    actor.id = %actor_id,
                    message = message_name,
                );

                let actor_span_ctx = tracing::Span::current()
                    .context()
                    .span()
                    .span_context()
                    .clone();
                span.add_link(actor_span_ctx);

                span
            }
        };

        let mut stop = false;

        #[cfg(feature = "tracing")]
        let res = AssertUnwindSafe(
            self.state
                .on_message(message, actor_ref, reply, &mut stop)
                .instrument(handler_span),
        )
        .catch_unwind()
        .await;

        #[cfg(not(feature = "tracing"))]
        let res = AssertUnwindSafe(self.state.on_message(message, actor_ref, reply, &mut stop))
            .catch_unwind()
            .await;

        match res {
            Ok(Ok(())) => {
                if stop {
                    ControlFlow::Break(ActorStopReason::Normal)
                } else {
                    ControlFlow::Continue(())
                }
            }
            Ok(Err(err)) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::new(
                err,
                PanicReason::OnMessage,
            ))), // The reply was an error
            Err(err) => ControlFlow::Break(ActorStopReason::Panicked(
                PanicError::new_from_panic_any(err, PanicReason::HandlerPanic),
            )), // The handler panicked
        }
    }

    pub(crate) async fn handle_link_died(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        mailbox_rx: Option<Box<dyn Any + Send>>,
    ) -> ControlFlow<ActorStopReason> {
        {
            let mut links = self.actor_ref.links.lock().await;

            // Check if we're already coordinating a restart
            if let CoordinationState::Coordinating {
                waiting_for,
                pending_mailboxes,
            } = &mut self.coordination
                && waiting_for.remove(&id)
            {
                let mailbox_rx =
                    mailbox_rx.expect("mailbox receiver should be sent - this is a bug");
                pending_mailboxes.insert(id, mailbox_rx);

                if waiting_for.is_empty() {
                    let mailboxes = mem::take(pending_mailboxes);
                    self.coordination = CoordinationState::Idle;
                    for (child_id, rx) in mailboxes {
                        let spec = links.children.get(&child_id).unwrap();
                        (spec.factory)(rx).await;
                    }
                }
                return ControlFlow::Continue(());
            }

            if let Some(spec) = links.children.get_mut(&id) {
                let should_restart = spec.should_restart(&reason);
                let factory = Arc::clone(&spec.factory);
                #[cfg(feature = "tracing")]
                let restart_count = spec.restart_count;

                // Extract what we need for coordination before dropping mutable borrow
                let strategy_info = match should_restart {
                    ControlFlow::Continue(()) => match A::supervision_strategy() {
                        SupervisionStrategy::OneForOne => None,
                        SupervisionStrategy::OneForAll => {
                            let others: Vec<(ActorId, Arc<ShutdownFn>)> = links
                                .children
                                .iter()
                                .filter(|(child_id, _)| *child_id != &id)
                                .map(|(child_id, child_spec)| {
                                    (*child_id, Arc::clone(&child_spec.shutdown))
                                })
                                .collect();
                            Some(others)
                        }
                        SupervisionStrategy::RestForOne => {
                            let others: Vec<(ActorId, Arc<ShutdownFn>)> = links
                                .children
                                .iter()
                                .filter(|(child_id, _)| *child_id > &id)
                                .map(|(child_id, child_spec)| {
                                    (*child_id, Arc::clone(&child_spec.shutdown))
                                })
                                .collect();
                            Some(others)
                        }
                    },
                    ControlFlow::Break(_) => None,
                };

                match should_restart {
                    ControlFlow::Continue(()) => {
                        let mailbox_rx =
                            mailbox_rx.expect("mailbox receiver should be sent - this is a bug");

                        match strategy_info {
                            None => {
                                // OneForOne
                                #[cfg(feature = "tracing")]
                                tracing::debug!(
                                    %id,
                                    name = A::name(),
                                    ?reason,
                                    restart_count,
                                    "actor restarting"
                                );
                                factory(mailbox_rx).await;
                            }
                            Some(others) if others.is_empty() => {
                                // OneForAll/RestForOne but no other children
                                factory(mailbox_rx).await;
                            }
                            Some(others) => {
                                // OneForAll/RestForOne with other children to coordinate
                                let mut waiting_for = HashSet::new();
                                let mut pending_mailboxes = HashMap::new();

                                pending_mailboxes.insert(id, mailbox_rx);

                                for (child_id, shutdown) in others {
                                    waiting_for.insert(child_id);
                                    tokio::spawn(shutdown());
                                }

                                self.coordination = CoordinationState::Coordinating {
                                    waiting_for,
                                    pending_mailboxes,
                                };
                            }
                        }

                        return ControlFlow::Continue(());
                    }
                    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                    ControlFlow::Break(no_restart_reason) => {
                        #[cfg(feature = "tracing")]
                        match no_restart_reason {
                            crate::links::NoRestartReason::NormalExitUnderTransientPolicy => {
                                tracing::debug!(
                                    %id,
                                    name = A::name(),
                                    ?reason,
                                    decision = %no_restart_reason,
                                    "actor not restarted"
                                );
                            }
                            crate::links::NoRestartReason::MaxRestartsExceeded { .. } => {
                                tracing::warn!(
                                    %id,
                                    name = A::name(),
                                    ?reason,
                                    decision = %no_restart_reason,
                                    "actor not restarted"
                                );
                            }
                            crate::links::NoRestartReason::NeverPolicy => {
                                tracing::debug!(
                                    %id,
                                    name = A::name(),
                                    ?reason,
                                    decision = %no_restart_reason,
                                    "actor not restarted"
                                );
                            }
                        }
                        }
                        links.notify_sibblings(id, &reason);
                    }
                }
            }
        }

        let res = AssertUnwindSafe(self.state.on_link_died(
            self.actor_ref.clone(),
            id,
            reason.clone(),
        ))
        .catch_unwind()
        .await;
        self.actor_ref.links.lock().await.sibblings.remove(&id);
        match res {
            Ok(Ok(flow)) => flow,
            Ok(Err(err)) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::new(
                Box::new(err),
                PanicReason::OnLinkDied,
            ))),
            Err(err) => ControlFlow::Break(ActorStopReason::Panicked(
                PanicError::new_from_panic_any(err, PanicReason::OnLinkDied),
            )),
        }
    }

    pub(crate) async fn handle_stop(&mut self) -> ControlFlow<ActorStopReason> {
        match self.handle_startup_finished().await {
            ControlFlow::Continue(_) => ControlFlow::Break(ActorStopReason::Normal),
            ControlFlow::Break(reason) => ControlFlow::Break(reason),
        }
    }

    pub(crate) async fn on_shutdown(
        &mut self,
        reason: ActorStopReason,
    ) -> ControlFlow<ActorStopReason> {
        match reason {
            ActorStopReason::Normal => ControlFlow::Break(ActorStopReason::Normal),
            ActorStopReason::SupervisorRestart => {
                ControlFlow::Break(ActorStopReason::SupervisorRestart)
            }
            ActorStopReason::Killed => ControlFlow::Break(ActorStopReason::Killed),
            ActorStopReason::Panicked(err) => {
                match AssertUnwindSafe(self.state.on_panic(self.actor_ref.clone(), err))
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(ControlFlow::Continue(()))) => ControlFlow::Continue(()),
                    Ok(Ok(ControlFlow::Break(reason))) => ControlFlow::Break(reason),
                    Ok(Err(err)) => ControlFlow::Break(ActorStopReason::Panicked(PanicError::new(
                        Box::new(err),
                        PanicReason::OnPanic,
                    ))),
                    Err(err) => ControlFlow::Break(ActorStopReason::Panicked(
                        PanicError::new_from_panic_any(err, PanicReason::OnPanic),
                    )),
                }
            }
            ActorStopReason::LinkDied { id, reason } => {
                ControlFlow::Break(ActorStopReason::LinkDied { id, reason })
            }
            #[cfg(feature = "remote")]
            ActorStopReason::PeerDisconnected => {
                ControlFlow::Break(ActorStopReason::PeerDisconnected)
            }
        }
    }

    #[inline]
    pub(crate) async fn shutdown(self) -> A {
        self.state
    }
}
