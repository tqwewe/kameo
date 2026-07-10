//! An actor that runs a future to completion as its behaviour.

use std::{future::Future, pin::Pin};

use kameo::{mailbox::Signal, prelude::*};

/// An actor that runs a future to completion as its behaviour.
///
/// The future is driven on the actor's own task, so stopping the actor drops the future and cancels
/// it. When the future completes, the actor stops: an `Ok` (or non-`Result`) output stops it with
/// [`ActorStopReason::Normal`], while an `Err` output stops it with [`ActorStopReason::Panicked`],
/// letting supervisors and linked actors observe the failure.
///
/// # Example
///
/// ```
/// use kameo::prelude::*;
/// use kameo_actors::future::FutureActor;
///
/// # tokio_test::block_on(async {
/// let actor_ref = FutureActor::spawn(async {
///     // background work
/// });
/// # })
/// ```
#[derive(Debug)]
pub struct FutureActor<F>(Pin<Box<F>>);

impl<F> Actor for FutureActor<F>
where
    F: Future + Send + 'static,
    F::Output: Reply,
{
    type Args = F;
    type Error = <F::Output as Reply>::Error;

    async fn on_start(future: F, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(FutureActor(Box::pin(future)))
    }

    async fn next(
        &mut self,
        _: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Result<Option<Signal<Self>>, Self::Error> {
        tokio::select! {
            biased;
            out = &mut self.0 => match out.to_result() {
                Ok(_) => Ok(None),
                Err(err) => Err(err),
            },
            signal = mailbox_rx.recv() => Ok(signal),
        }
    }
}
