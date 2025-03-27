//! Behaviour for actor mailboxes.
//!
//! An actor mailbox is a channel which stores pending messages and signals for an actor to process sequentially.

use std::{
    fmt,
    task::{Context, Poll},
};

use dyn_clone::DynClone;
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::{
    actor::{ActorID, ActorRef},
    error::{ActorStopReason, SendError},
    message::DynMessage,
    reply::BoxReplySender,
    Actor,
};

pub fn bounded<A: Actor>(buffer: usize) -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::channel(buffer);
    (MailboxSender::Bounded(tx), MailboxReceiver::Bounded(rx))
}

pub fn unbounded<A: Actor>() -> (MailboxSender<A>, MailboxReceiver<A>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (MailboxSender::Unbounded(tx), MailboxReceiver::Unbounded(rx))
}

pub enum MailboxSender<A: Actor> {
    Bounded(mpsc::Sender<Signal<A>>),
    Unbounded(mpsc::UnboundedSender<Signal<A>>),
}

impl<A: Actor> MailboxSender<A> {
    pub async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        match self {
            MailboxSender::Bounded(tx) => tx.send(signal).await,
            MailboxSender::Unbounded(tx) => tx.send(signal),
        }
    }

    pub async fn closed(&self) {
        match self {
            MailboxSender::Bounded(tx) => tx.closed().await,
            MailboxSender::Unbounded(tx) => tx.closed().await,
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            MailboxSender::Bounded(tx) => tx.is_closed(),
            MailboxSender::Unbounded(tx) => tx.is_closed(),
        }
    }

    pub fn same_channel(&self, other: &MailboxSender<A>) -> bool {
        match (self, other) {
            (MailboxSender::Bounded(a), MailboxSender::Bounded(b)) => a.same_channel(b),
            (MailboxSender::Bounded(_), MailboxSender::Unbounded(_)) => false,
            (MailboxSender::Unbounded(_), MailboxSender::Bounded(_)) => false,
            (MailboxSender::Unbounded(a), MailboxSender::Unbounded(b)) => a.same_channel(b),
        }
    }

    pub fn downgrade(&self) -> WeakMailboxSender<A> {
        match self {
            MailboxSender::Bounded(tx) => WeakMailboxSender::Bounded(tx.downgrade()),
            MailboxSender::Unbounded(tx) => WeakMailboxSender::Unbounded(tx.downgrade()),
        }
    }

    pub fn strong_count(&self) -> usize {
        match self {
            MailboxSender::Bounded(tx) => tx.strong_count(),
            MailboxSender::Unbounded(tx) => tx.strong_count(),
        }
    }

    pub fn weak_count(&self) -> usize {
        match self {
            MailboxSender::Bounded(tx) => tx.weak_count(),
            MailboxSender::Unbounded(tx) => tx.weak_count(),
        }
    }
}

impl<A: Actor> Clone for MailboxSender<A> {
    fn clone(&self) -> Self {
        match self {
            MailboxSender::Bounded(tx) => MailboxSender::Bounded(tx.clone()),
            MailboxSender::Unbounded(tx) => MailboxSender::Unbounded(tx.clone()),
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MailboxSender::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            MailboxSender::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

pub enum WeakMailboxSender<A: Actor> {
    Bounded(mpsc::WeakSender<Signal<A>>),
    Unbounded(mpsc::WeakUnboundedSender<Signal<A>>),
}

impl<A: Actor> WeakMailboxSender<A> {
    pub fn upgrade(&self) -> Option<MailboxSender<A>> {
        match self {
            WeakMailboxSender::Bounded(tx) => tx.upgrade().map(MailboxSender::Bounded),
            WeakMailboxSender::Unbounded(tx) => tx.upgrade().map(MailboxSender::Unbounded),
        }
    }

    pub fn strong_count(&self) -> usize {
        match self {
            WeakMailboxSender::Bounded(tx) => tx.strong_count(),
            WeakMailboxSender::Unbounded(tx) => tx.strong_count(),
        }
    }

    pub fn weak_count(&self) -> usize {
        match self {
            WeakMailboxSender::Bounded(tx) => tx.weak_count(),
            WeakMailboxSender::Unbounded(tx) => tx.weak_count(),
        }
    }
}

impl<A: Actor> Clone for WeakMailboxSender<A> {
    fn clone(&self) -> Self {
        match self {
            WeakMailboxSender::Bounded(tx) => WeakMailboxSender::Bounded(tx.clone()),
            WeakMailboxSender::Unbounded(tx) => WeakMailboxSender::Unbounded(tx.clone()),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakMailboxSender<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WeakMailboxSender::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            WeakMailboxSender::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

pub enum MailboxReceiver<A: Actor> {
    Bounded(mpsc::Receiver<Signal<A>>),
    Unbounded(mpsc::UnboundedReceiver<Signal<A>>),
}

impl<A: Actor> MailboxReceiver<A> {
    pub async fn recv(&mut self) -> Option<Signal<A>> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.recv().await,
            MailboxReceiver::Unbounded(rx) => rx.recv().await,
        }
    }

    pub async fn recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.recv_many(buffer, limit).await,
            MailboxReceiver::Unbounded(rx) => rx.recv_many(buffer, limit).await,
        }
    }

    pub fn try_recv(&mut self) -> Result<Signal<A>, TryRecvError> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.try_recv(),
            MailboxReceiver::Unbounded(rx) => rx.try_recv(),
        }
    }

    pub fn blocking_recv(&mut self) -> Option<Signal<A>> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.blocking_recv(),
            MailboxReceiver::Unbounded(rx) => rx.blocking_recv(),
        }
    }

    pub fn blocking_recv_many(&mut self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.blocking_recv_many(buffer, limit),
            MailboxReceiver::Unbounded(rx) => rx.blocking_recv_many(buffer, limit),
        }
    }

    pub fn close(&mut self) {
        match self {
            MailboxReceiver::Bounded(rx) => rx.close(),
            MailboxReceiver::Unbounded(rx) => rx.close(),
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            MailboxReceiver::Bounded(rx) => rx.is_closed(),
            MailboxReceiver::Unbounded(rx) => rx.is_closed(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            MailboxReceiver::Bounded(rx) => rx.is_empty(),
            MailboxReceiver::Unbounded(rx) => rx.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.len(),
            MailboxReceiver::Unbounded(rx) => rx.len(),
        }
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Signal<A>>> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.poll_recv(cx),
            MailboxReceiver::Unbounded(rx) => rx.poll_recv(cx),
        }
    }

    pub fn poll_recv_many(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut Vec<Signal<A>>,
        limit: usize,
    ) -> Poll<usize> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.poll_recv_many(cx, buffer, limit),
            MailboxReceiver::Unbounded(rx) => rx.poll_recv_many(cx, buffer, limit),
        }
    }

    pub fn sender_strong_count(&self) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.sender_strong_count(),
            MailboxReceiver::Unbounded(rx) => rx.sender_strong_count(),
        }
    }

    pub fn sender_weak_count(&self) -> usize {
        match self {
            MailboxReceiver::Bounded(rx) => rx.sender_weak_count(),
            MailboxReceiver::Unbounded(rx) => rx.sender_weak_count(),
        }
    }
}

impl<A: Actor> fmt::Debug for MailboxReceiver<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MailboxReceiver::Bounded(tx) => f.debug_tuple("Bounded").field(tx).finish(),
            MailboxReceiver::Unbounded(tx) => f.debug_tuple("Unbounded").field(tx).finish(),
        }
    }
}

#[allow(missing_debug_implementations)]
#[doc(hidden)]
pub enum Signal<A: Actor> {
    StartupFinished,
    Message {
        message: Box<dyn DynMessage<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<BoxReplySender>,
        sent_within_actor: bool,
    },
    LinkDied {
        id: ActorID,
        reason: ActorStopReason,
    },
    Stop,
}

impl<A: Actor> Signal<A> {
    pub(crate) fn downcast_message<M>(self) -> Option<M>
    where
        M: 'static,
    {
        match self {
            Signal::Message { message, .. } => message.as_any().downcast().ok().map(|v| *v),
            _ => None,
        }
    }
}

#[doc(hidden)]
pub trait SignalMailbox: DynClone + Send + Sync {
    fn signal_startup_finished(&self) -> Result<(), SendError>;
    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
}

impl<A> SignalMailbox for MailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self {
            MailboxSender::Bounded(tx) => {
                tx.try_send(Signal::StartupFinished)
                    .map_err(|err| match err {
                        mpsc::error::TrySendError::Full(_) => SendError::MailboxFull(()),
                        mpsc::error::TrySendError::Closed(_) => SendError::ActorNotRunning(()),
                    })
            }
            MailboxSender::Unbounded(tx) => tx
                .send(Signal::StartupFinished)
                .map_err(|_| SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        match self {
            MailboxSender::Bounded(tx) => async move {
                tx.send(Signal::LinkDied { id, reason })
                    .await
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
            MailboxSender::Unbounded(tx) => async move {
                tx.send(Signal::LinkDied { id, reason })
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
        }
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        match self {
            MailboxSender::Bounded(tx) => async move {
                tx.send(Signal::Stop)
                    .await
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
            MailboxSender::Unbounded(tx) => async move {
                tx.send(Signal::Stop)
                    .map_err(|_| SendError::ActorNotRunning(()))
            }
            .boxed(),
        }
    }
}

impl<A> SignalMailbox for WeakMailboxSender<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        match self.upgrade() {
            Some(tx) => tx.signal_startup_finished(),
            None => Err(SendError::ActorNotRunning(())),
        }
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => tx.signal_link_died(id, reason).await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(tx) => tx.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }
}

dyn_clone::clone_trait_object!(SignalMailbox);
