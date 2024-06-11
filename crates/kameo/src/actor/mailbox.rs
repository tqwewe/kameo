use dyn_clone::DynClone;
use futures::{future::BoxFuture, Future, FutureExt};
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::{ActorStopReason, BoxSendError, SendError},
    message::{BoxReply, DynMessage, DynQuery},
    Actor,
};

use super::ActorRef;

#[doc(hidden)]
pub trait Mailbox<A: Actor>: SignalMailbox + Clone + Send + Sync + Sized {
    type WeakMailbox: WeakMailbox<StrongMailbox = Self>;

    fn default_mailbox() -> (Self, MailboxReceiver<A>);
    fn send(
        &self,
        signal: Signal<A>,
    ) -> impl Future<Output = Result<(), mpsc::error::SendError<Signal<A>>>> + Send + '_;
    fn closed(&self) -> impl Future<Output = ()> + '_;
    fn is_closed(&self) -> bool;
    fn downgrade(&self) -> Self::WeakMailbox;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

#[doc(hidden)]
pub trait WeakMailbox: Clone + Send {
    type StrongMailbox;

    fn upgrade(&self) -> Option<Self::StrongMailbox>;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

/// An unbounded mailbox, where the sending messages to a full mailbox causes backpressure.
#[allow(missing_debug_implementations)]
pub struct BoundedMailbox<A: Actor>(pub(crate) mpsc::Sender<Signal<A>>);

impl<A: Actor> BoundedMailbox<A> {
    /// Creates a new bounded mailbox with a given capacity.
    #[inline]
    pub fn new(capacity: usize) -> (Self, MailboxReceiver<A>) {
        let (tx, rx) = mpsc::channel(capacity);
        (BoundedMailbox(tx), MailboxReceiver::Bounded(rx))
    }
}

impl<A: Actor> Mailbox<A> for BoundedMailbox<A> {
    type WeakMailbox = WeakBoundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, MailboxReceiver<A>) {
        BoundedMailbox::new(1000)
    }

    #[inline]
    async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        self.0.send(signal).await
    }

    #[inline]
    async fn closed(&self) {
        self.0.closed().await
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        WeakBoundedMailbox(self.0.downgrade())
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.0.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.0.weak_count()
    }
}

impl<A: Actor> Clone for BoundedMailbox<A> {
    fn clone(&self) -> Self {
        BoundedMailbox(self.0.clone())
    }
}

/// A weak bounded mailbox that does not prevent the actor from being stopped.
#[allow(missing_debug_implementations)]
pub struct WeakBoundedMailbox<A: Actor>(pub(crate) mpsc::WeakSender<Signal<A>>);

impl<A: Actor> WeakMailbox for WeakBoundedMailbox<A> {
    type StrongMailbox = BoundedMailbox<A>;

    #[inline]
    fn upgrade(&self) -> Option<Self::StrongMailbox> {
        self.0.upgrade().map(BoundedMailbox)
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.0.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.0.weak_count()
    }
}

impl<A: Actor> Clone for WeakBoundedMailbox<A> {
    fn clone(&self) -> Self {
        WeakBoundedMailbox(self.0.clone())
    }
}

/// An unbounded mailbox, where the number of messages queued can grow infinitely.
#[allow(missing_debug_implementations)]
pub struct UnboundedMailbox<A: Actor>(pub(crate) mpsc::UnboundedSender<Signal<A>>);

impl<A: Actor> UnboundedMailbox<A> {
    /// Creates a new unbounded mailbox.
    #[inline]
    pub fn new() -> (Self, MailboxReceiver<A>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (UnboundedMailbox(tx), MailboxReceiver::Unbounded(rx))
    }
}

impl<A: Actor> Mailbox<A> for UnboundedMailbox<A> {
    type WeakMailbox = WeakUnboundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, MailboxReceiver<A>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (UnboundedMailbox(tx), MailboxReceiver::Unbounded(rx))
    }

    #[inline]
    async fn send(&self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> {
        self.0.send(signal)
    }

    #[inline]
    async fn closed(&self) {
        self.0.closed().await
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        WeakUnboundedMailbox(self.0.downgrade())
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.0.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.0.weak_count()
    }
}

impl<A: Actor> Clone for UnboundedMailbox<A> {
    fn clone(&self) -> Self {
        UnboundedMailbox(self.0.clone())
    }
}

/// A weak unbounded mailbox that does not prevent the actor from being stopped.
#[allow(missing_debug_implementations)]
pub struct WeakUnboundedMailbox<A: Actor>(pub(crate) mpsc::WeakUnboundedSender<Signal<A>>);

impl<A: Actor> WeakMailbox for WeakUnboundedMailbox<A> {
    type StrongMailbox = UnboundedMailbox<A>;

    #[inline]
    fn upgrade(&self) -> Option<Self::StrongMailbox> {
        self.0.upgrade().map(UnboundedMailbox)
    }

    #[inline]
    fn strong_count(&self) -> usize {
        self.0.strong_count()
    }

    #[inline]
    fn weak_count(&self) -> usize {
        self.0.weak_count()
    }
}

impl<A: Actor> Clone for WeakUnboundedMailbox<A> {
    fn clone(&self) -> Self {
        WeakUnboundedMailbox(self.0.clone())
    }
}

/// A mailbox receiver, either bounded or unbounded.
#[allow(missing_debug_implementations)]
pub enum MailboxReceiver<A: Actor> {
    /// A bounded mailbox receiver.
    Bounded(mpsc::Receiver<Signal<A>>),
    /// An unbounded mailbox receiver.
    Unbounded(mpsc::UnboundedReceiver<Signal<A>>),
}

impl<A: Actor> MailboxReceiver<A> {
    pub(crate) async fn recv(&mut self) -> Option<Signal<A>> {
        match self {
            MailboxReceiver::Bounded(rx) => rx.recv().await,
            MailboxReceiver::Unbounded(rx) => rx.recv().await,
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
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    },
    Query {
        query: Box<dyn DynQuery<A>>,
        actor_ref: ActorRef<A>,
        reply: Option<oneshot::Sender<Result<BoxReply, BoxSendError>>>,
        sent_within_actor: bool,
    },
    LinkDied {
        id: u64,
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
            Signal::Query { query: message, .. } => message.as_any().downcast().ok().map(|v| *v),
            _ => None,
        }
    }
}

#[doc(hidden)]
pub trait SignalMailbox: DynClone + Send {
    fn signal_startup_finished(&self) -> BoxFuture<'_, Result<(), SendError>>;
    fn signal_link_died(
        &self,
        id: u64,
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
        id: u64,
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
        id: u64,
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
