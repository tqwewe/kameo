mod bounded;
mod broadcast;
mod unbounded;

pub use bounded::*;
pub use broadcast::*;
pub use unbounded::*;

use dyn_clone::DynClone;
use futures::{future::BoxFuture, Future, FutureExt};
use tokio::sync::oneshot;

use crate::{
    error::{ActorStopReason, BoxSendError, SendError},
    message::{BoxReply, DynBroadcast, DynMessage},
    Actor,
};

use super::{ActorID, ActorRef};

#[doc(hidden)]
pub trait Mailbox<A: Actor>: SignalMailbox + Clone + Send + Sync {
    type Receiver: MailboxReceiver<SignalMessage = Self::SignalMessage>;
    type SignalMessage: Send;
    type WeakMailbox: WeakMailbox<StrongMailbox = Self>;

    fn default_mailbox() -> (Self, Self::Receiver);
    fn send<M, E>(
        &self,
        signal: Signal<Self::SignalMessage>,
    ) -> impl Future<Output = Result<(), SendError<M, E>>> + Send + '_
    where
        M: 'static,
        E: 'static;
    fn signal_mailbox(&self) -> Box<dyn SignalMailbox>;
    fn downgrade(&self) -> Self::WeakMailbox;
}

#[doc(hidden)]
pub trait MailboxReceiver: Send + 'static {
    type SignalMessage: Send;

    fn recv(&mut self) -> impl Future<Output = Option<Signal<Self::SignalMessage>>> + Send + '_;
}

#[doc(hidden)]
pub trait WeakMailbox: SignalMailbox + Clone + Send + Sync {
    type StrongMailbox;

    fn upgrade(&self) -> Option<Self::StrongMailbox>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

#[doc(hidden)]
pub trait MpscMailbox {
    fn closed(&self) -> impl Future<Output = ()> + '_;
    fn is_closed(&self) -> bool;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

#[allow(missing_debug_implementations)]
#[doc(hidden)]
#[derive(Clone)]
pub enum Signal<M> {
    StartupFinished,
    Message(M),
    LinkDied {
        id: ActorID,
        reason: ActorStopReason,
    },
    Stop,
}

impl<A: Actor> Signal<SignalMessage<A>> {
    pub(crate) fn downcast_message<M>(self) -> Option<M>
    where
        M: 'static,
    {
        match self {
            Signal::Message(SignalMessage { message, .. }) => {
                message.as_any().downcast().ok().map(|v| *v)
            }
            _ => None,
        }
    }
}

impl<A: Actor> Signal<SignalBroadcast<A>> {
    pub(crate) fn downcast_message<M>(self) -> Option<M>
    where
        M: 'static,
    {
        match self {
            Signal::Message(SignalBroadcast { message, .. }) => {
                message.as_any().downcast().ok().map(|v| *v)
            }
            _ => None,
        }
    }
}

#[allow(missing_debug_implementations)]
#[doc(hidden)]
pub struct SignalMessage<A: Actor> {
    pub message: Box<dyn DynMessage<A>>,
    pub actor_ref: ActorRef<A>,
    pub reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
    pub sent_within_actor: bool,
}

#[allow(missing_debug_implementations)]
#[doc(hidden)]
pub struct SignalBroadcast<A: Actor> {
    pub message: Box<dyn DynBroadcast<A>>,
    pub sent_from_actor: Option<ActorID>,
}

impl<A: Actor> Clone for SignalBroadcast<A> {
    fn clone(&self) -> Self {
        SignalBroadcast {
            message: self.message.clone(),
            sent_from_actor: self.sent_from_actor.clone(),
        }
    }
}

#[doc(hidden)]
pub trait SignalMailbox: DynClone + Send {
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>>;
}

dyn_clone::clone_trait_object!(SignalMailbox);

impl<A> SignalMailbox for BoundedMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::StartupFinished)
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::LinkDied { id, reason })
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::Stop)
                .await
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }
}

impl<A> SignalMailbox for WeakBoundedMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(mb) => mb.signal_startup_finished().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(mb) => mb.signal_link_died(id, reason).await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(mb) => mb.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }
}

impl<A> SignalMailbox for UnboundedMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::StartupFinished)
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::LinkDied { id, reason })
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.0
                .send(Signal::Stop)
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }
}

impl<A> SignalMailbox for WeakUnboundedMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(mb) => mb.signal_startup_finished().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(mb) => mb.signal_link_died(id, reason).await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            match self.upgrade() {
                Some(mb) => mb.signal_stop().await,
                None => Err(SendError::ActorNotRunning(())),
            }
        }
        .boxed()
    }
}

impl<A> SignalMailbox for BroadcastMailbox<A>
where
    A: Actor,
{
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.tx
                .send(Signal::StartupFinished)
                .map(|_| ())
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_link_died(
        &self,
        id: ActorID,
        reason: ActorStopReason,
    ) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.tx
                .send(Signal::LinkDied { id, reason })
                .map(|_| ())
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }

    fn signal_stop(&self) -> BoxFuture<'_, Result<(), SendError>> {
        async move {
            self.tx
                .send(Signal::Stop)
                .map(|_| ())
                .map_err(|_| SendError::ActorNotRunning(()))
        }
        .boxed()
    }
}
