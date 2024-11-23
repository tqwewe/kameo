use std::{collections::VecDeque, mem, panic::AssertUnwindSafe};

use futures::{Future, FutureExt};

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{ActorStopReason, PanicError},
    mailbox::Signal,
    message::BoxMessage,
    reply::BoxReplySender,
};

use super::ActorID;

pub(crate) trait ActorState<A: Actor>: Sized {
    fn new_from_actor(actor: A, actor_ref: WeakActorRef<A>) -> Self;

    fn handle_startup_finished(&mut self) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_message(
        &mut self,
        message: BoxMessage<A>,
        actor_ref: ActorRef<A>,
        reply: Option<BoxReplySender>,
        sent_within_actor: bool,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_link_died(
        &mut self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn handle_stop(&mut self) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn on_shutdown(
        &mut self,
        reason: ActorStopReason,
    ) -> impl Future<Output = Option<ActorStopReason>> + Send;

    fn shutdown(self) -> impl Future<Output = A> + Send;
}

pub(crate) struct ActorBehaviour<A: Actor> {
    actor_ref: WeakActorRef<A>,
    state: A,
    finished_startup: bool,
    startup_buffer: VecDeque<Signal<A>>,
}

impl<A> ActorState<A> for ActorBehaviour<A>
where
    A: Actor,
{
    #[inline]
    fn new_from_actor(actor: A, actor_ref: WeakActorRef<A>) -> Self {
        ActorBehaviour {
            actor_ref,
            state: actor,
            finished_startup: false,
            startup_buffer: VecDeque::new(),
        }
    }

    async fn handle_startup_finished(&mut self) -> Option<ActorStopReason> {
        self.finished_startup = true;
        for signal in mem::take(&mut self.startup_buffer).drain(..) {
            match signal {
                Signal::Message {
                    message,
                    actor_ref,
                    reply,
                    sent_within_actor,
                } => {
                    if let Some(reason) = self
                        .handle_message(message, actor_ref, reply, sent_within_actor)
                        .await
                    {
                        return Some(reason);
                    }
                }
                _ => unreachable!(),
            }
        }

        None
    }

    #[inline]
    async fn handle_message(
        &mut self,
        message: BoxMessage<A>,
        actor_ref: ActorRef<A>,
        reply: Option<BoxReplySender>,
        sent_within_actor: bool,
    ) -> Option<ActorStopReason> {
        if !sent_within_actor && !self.finished_startup {
            // The actor is still starting up, so we'll push this message to a buffer to be processed upon startup
            self.startup_buffer.push_back(Signal::Message {
                message,
                actor_ref,
                reply,
                sent_within_actor,
            });
            return None;
        }

        let res = AssertUnwindSafe(self.state.on_message(message, actor_ref, reply))
            .catch_unwind()
            .await;
        match res {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))), // The reply was an error
            Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))), // The handler panicked
        }
    }

    #[inline]
    async fn handle_link_died(
        &mut self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> Option<ActorStopReason> {
        match AssertUnwindSafe(
            self.state
                .on_link_died(self.actor_ref.clone(), id, reason.clone()),
        )
        .catch_unwind()
        .await
        {
            Ok(Ok(Some(reason))) => Some(reason),
            Ok(Ok(None)) => None,
            Ok(Err(err)) => Some(ActorStopReason::Panicked(PanicError::new(err))),
            Err(err) => Some(ActorStopReason::Panicked(PanicError::new_boxed(err))),
        }
    }

    #[inline]
    async fn handle_stop(&mut self) -> Option<ActorStopReason> {
        Some(ActorStopReason::Normal)
    }

    #[inline]
    async fn on_shutdown(&mut self, reason: ActorStopReason) -> Option<ActorStopReason> {
        match reason {
            ActorStopReason::Normal => Some(ActorStopReason::Normal),
            ActorStopReason::Killed => Some(ActorStopReason::Killed),
            ActorStopReason::Panicked(err) => {
                match self.state.on_panic(self.actor_ref.clone(), err).await {
                    Ok(Some(reason)) => Some(reason),
                    Ok(None) => None,
                    Err(err) => Some(ActorStopReason::Panicked(PanicError::new(err))),
                }
            }
            ActorStopReason::LinkDied { id, reason } => {
                Some(ActorStopReason::LinkDied { id, reason })
            }
        }
    }

    #[inline]
    async fn shutdown(self) -> A {
        self.state
    }
}
