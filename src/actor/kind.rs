use std::{collections::VecDeque, mem, ops::ControlFlow, panic::AssertUnwindSafe, sync::Arc};

use futures::FutureExt;

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{MailboxReceiver, Signal},
    message::BoxMessage,
    reply::BoxReplySender,
};

use super::ActorId;

pub(crate) struct ActorBehaviour<A: Actor> {
    actor_ref: WeakActorRef<A>,
    state: A,
    finished_startup: bool,
    startup_buffer: VecDeque<Signal<A>>,
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
            Ok(Some(signal)) => ControlFlow::Continue(signal),
            Ok(None) => ControlFlow::Break(ActorStopReason::Normal),
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
                } => {
                    self.handle_message(message, actor_ref, reply, sent_within_actor)
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
    ) -> ControlFlow<ActorStopReason> {
        if !sent_within_actor && !self.finished_startup {
            // The actor is still starting up, so we'll push this message to a buffer to be processed upon startup
            self.startup_buffer.push_back(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
            });
            return ControlFlow::Continue(());
        }

        let mut stop = false;
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
        reason: Arc<ActorStopReason>,
    ) -> ControlFlow<ActorStopReason> {
        let res = AssertUnwindSafe(self.state.on_link_died(
            self.actor_ref.clone(),
            id,
            Arc::unwrap_or_clone(reason),
        ))
        .catch_unwind()
        .await;
        self.actor_ref.links.lock().await.remove(&id);
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
