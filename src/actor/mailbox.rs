use dyn_clone::DynClone;
use futures::{future::BoxFuture, stream::AbortHandle, Future, FutureExt};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    error::{ActorStopReason, BoxSendError, SendError},
    message::{BoxReply, DynBroadcast, DynMessage, Message},
    Actor,
};

use super::{
    kind::ActorBroadcastBehaviour, run_actor_lifecycle, ActorID, ActorRef, Links, CURRENT_ACTOR_ID,
};

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

/// A bounded mailbox, where the sending messages to a full mailbox causes backpressure.
#[allow(missing_debug_implementations)]
pub struct BoundedMailbox<A: Actor>(pub(crate) mpsc::Sender<Signal<SignalMessage<A>>>);

#[allow(missing_debug_implementations)]
pub struct BoundedMailboxReceiver<A: Actor>(pub(crate) mpsc::Receiver<Signal<SignalMessage<A>>>);

impl<A: Actor> BoundedMailbox<A> {
    /// Creates a new bounded mailbox with a given capacity.
    #[inline]
    pub fn new(capacity: usize) -> (Self, BoundedMailboxReceiver<A>) {
        let (tx, rx) = mpsc::channel(capacity);
        (BoundedMailbox(tx), BoundedMailboxReceiver(rx))
    }
}

impl<A: Actor> Mailbox<A> for BoundedMailbox<A> {
    type Receiver = BoundedMailboxReceiver<A>;
    type SignalMessage = SignalMessage<A>;
    type WeakMailbox = WeakBoundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, Self::Receiver) {
        BoundedMailbox::new(1000)
    }

    #[inline]
    async fn send<M, E>(&self, signal: Signal<Self::SignalMessage>) -> Result<(), SendError<M, E>>
    where
        M: 'static,
        E: 'static,
    {
        self.0.send(signal).await?;
        Ok(())
    }

    #[inline]
    fn signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.downgrade())
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        WeakBoundedMailbox(self.0.downgrade())
    }
}

impl<A: Actor> Clone for BoundedMailbox<A> {
    fn clone(&self) -> Self {
        BoundedMailbox(self.0.clone())
    }
}

impl<A: Actor> MailboxReceiver for BoundedMailboxReceiver<A> {
    type SignalMessage = SignalMessage<A>;

    async fn recv(&mut self) -> Option<Signal<Self::SignalMessage>> {
        self.0.recv().await
    }
}

/// A weak bounded mailbox that does not prevent the actor from being stopped.
#[allow(missing_debug_implementations)]
pub struct WeakBoundedMailbox<A: Actor>(pub(crate) mpsc::WeakSender<Signal<SignalMessage<A>>>);

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
pub struct UnboundedMailbox<A: Actor>(pub(crate) mpsc::UnboundedSender<Signal<SignalMessage<A>>>);

#[allow(missing_debug_implementations)]
pub struct UnboundedMailboxReceiver<A: Actor>(
    pub(crate) mpsc::UnboundedReceiver<Signal<SignalMessage<A>>>,
);

impl<A: Actor> UnboundedMailbox<A> {
    /// Creates a new unbounded mailbox.
    #[inline]
    pub fn new() -> (Self, UnboundedMailboxReceiver<A>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (UnboundedMailbox(tx), UnboundedMailboxReceiver(rx))
    }
}

impl<A: Actor> Mailbox<A> for UnboundedMailbox<A> {
    type Receiver = UnboundedMailboxReceiver<A>;
    type SignalMessage = SignalMessage<A>;
    type WeakMailbox = WeakUnboundedMailbox<A>;

    #[inline]
    fn default_mailbox() -> (Self, Self::Receiver) {
        UnboundedMailbox::new()
    }

    #[inline]
    async fn send<M, E>(&self, signal: Signal<Self::SignalMessage>) -> Result<(), SendError<M, E>>
    where
        M: 'static,
        E: 'static,
    {
        self.0.send(signal)?;
        Ok(())
    }

    #[inline]
    fn signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.downgrade())
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        WeakUnboundedMailbox(self.0.downgrade())
    }
}

impl<A: Actor> Clone for UnboundedMailbox<A> {
    fn clone(&self) -> Self {
        UnboundedMailbox(self.0.clone())
    }
}

impl<A: Actor> MailboxReceiver for UnboundedMailboxReceiver<A> {
    type SignalMessage = SignalMessage<A>;

    async fn recv(&mut self) -> Option<Signal<Self::SignalMessage>> {
        self.0.recv().await
    }
}

/// A weak unbounded mailbox that does not prevent the actor from being stopped.
#[allow(missing_debug_implementations)]
pub struct WeakUnboundedMailbox<A: Actor>(
    pub(crate) mpsc::WeakUnboundedSender<Signal<SignalMessage<A>>>,
);

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

#[doc(hidden)]
pub trait MpscMailbox {
    fn closed(&self) -> impl Future<Output = ()> + '_;
    fn is_closed(&self) -> bool;
    fn strong_count(&self) -> usize;
    fn weak_count(&self) -> usize;
}

impl<A: Actor> MpscMailbox for BoundedMailbox<A> {
    #[inline]
    async fn closed(&self) {
        self.0.closed().await
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.0.is_closed()
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

impl<A: Actor> MpscMailbox for UnboundedMailbox<A> {
    #[inline]
    async fn closed(&self) {
        self.0.closed().await
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.0.is_closed()
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

/// A broadcast mailbox which can be shared between multiple actors.
#[allow(missing_debug_implementations)]
pub struct BroadcastMailbox<A: Actor> {
    pub(crate) tx: broadcast::Sender<Signal<SignalBroadcast<A>>>,
    pub(crate) rx: broadcast::Receiver<Signal<SignalBroadcast<A>>>,
}

pub struct BroadcastMailboxReceiver<A: Actor>(
    pub(crate) broadcast::Receiver<Signal<SignalBroadcast<A>>>,
);

impl<A: Actor> BroadcastMailbox<A> {
    pub fn new(capacity: usize) -> BroadcastMailbox<A> {
        let (tx, rx) = broadcast::channel(capacity);
        BroadcastMailbox { tx, rx }
    }

    pub fn spawn(&self, actor: A) -> ActorRef<A>
    where
        A: Actor<Mailbox = BroadcastMailbox<A>>,
    {
        let (mailbox, mailbox_rx) = (
            self.clone(),
            BroadcastMailboxReceiver(self.rx.resubscribe()),
        );
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let links = Links::default();
        let actor_ref = ActorRef::new(mailbox, abort_handle, links.clone());
        let id = actor_ref.id();

        tokio::spawn(CURRENT_ACTOR_ID.scope(id, {
            let actor_ref = actor_ref.clone();
            async move {
                run_actor_lifecycle::<A, ActorBroadcastBehaviour<A>>(
                    actor_ref,
                    actor,
                    mailbox_rx,
                    abort_registration,
                    links,
                )
                .await
            }
        }));

        actor_ref
    }

    /// Sends a message to multiple actors in a shared mailbox.
    #[inline]
    pub fn send<M>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: Message<M>,
        M: Clone + Send + 'static,
    {
        self.tx.send(Signal::Message(SignalBroadcast {
            message: Box::new(msg),
            sent_from_actor: CURRENT_ACTOR_ID.try_with(Clone::clone).ok(),
        }))?;
        Ok(())
    }
}

impl<A: Actor> Mailbox<A> for BroadcastMailbox<A> {
    type Receiver = BroadcastMailboxReceiver<A>;
    type SignalMessage = SignalBroadcast<A>;
    type WeakMailbox = Self;

    fn default_mailbox() -> (Self, Self::Receiver) {
        let (tx, rx) = broadcast::channel(1000);
        (
            BroadcastMailbox {
                tx,
                rx: rx.resubscribe(),
            },
            BroadcastMailboxReceiver(rx),
        )
    }

    #[inline]
    async fn send<M, E>(&self, signal: Signal<Self::SignalMessage>) -> Result<(), SendError<M, E>>
    where
        M: 'static,
        E: 'static,
    {
        self.tx.send(signal)?;
        Ok(())
    }

    #[inline]
    fn signal_mailbox(&self) -> Box<dyn SignalMailbox> {
        Box::new(self.clone())
    }

    #[inline]
    fn downgrade(&self) -> Self::WeakMailbox {
        self.clone()
    }
}

impl<A: Actor> WeakMailbox for BroadcastMailbox<A> {
    type StrongMailbox = Self;

    fn upgrade(&self) -> Option<Self::StrongMailbox> {
        Some(self.clone())
    }

    fn strong_count(&self) -> usize {
        self.tx.receiver_count()
    }

    fn weak_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl<A: Actor> Clone for BroadcastMailbox<A> {
    fn clone(&self) -> Self {
        BroadcastMailbox {
            tx: self.tx.clone(),
            rx: self.rx.resubscribe(),
        }
    }
}

impl<A: Actor> MailboxReceiver for BroadcastMailboxReceiver<A> {
    type SignalMessage = SignalBroadcast<A>;

    async fn recv(&mut self) -> Option<Signal<Self::SignalMessage>> {
        self.0.recv().await.ok()
    }
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
