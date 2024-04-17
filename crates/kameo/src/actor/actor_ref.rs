use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, TryLockError,
    },
    time::Duration,
};

use dyn_clone::DynClone;
use futures::{stream::AbortHandle, Stream, StreamExt};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    task_local,
};

use crate::{
    error::{ActorStopReason, BoxSendError, SendError},
    message::{BoxReply, DynMessage, DynQuery, Message, Query, StreamMessage},
    reply::Reply,
};

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(0);
task_local! {
    pub(crate) static CURRENT_ACTOR_ID: u64;
}

type Mailbox<A> = mpsc::UnboundedSender<Signal<A>>;
type WeakMailbox<A> = mpsc::WeakUnboundedSender<Signal<A>>;
pub(crate) type Links = Arc<Mutex<HashMap<u64, Box<dyn SignalMailbox>>>>;

/// A reference to an actor for sending messages/queries and managing the actor.
pub struct ActorRef<A: ?Sized> {
    id: u64,
    mailbox: Mailbox<A>,
    abort_handle: AbortHandle,
    links: Links,
}

impl<A> ActorRef<A> {
    pub(crate) fn new(mailbox: Mailbox<A>, abort_handle: AbortHandle, links: Links) -> Self {
        ActorRef {
            id: ACTOR_COUNTER.fetch_add(1, Ordering::Relaxed),
            mailbox,
            abort_handle,
            links,
        }
    }

    /// Returns the actor identifier.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns whether the actor is currently alive.
    pub fn is_alive(&self) -> bool {
        self.mailbox.is_closed()
    }

    /// Converts the `ActorRef` to a [`WeakActorRef`] that does not count
    /// towards RAII semantics, i.e. if all `ActorRef` instances of the
    /// actor were dropped and only `WeakActorRef` instances remain,
    /// the actor is stopped.
    #[must_use = "Downgrade creates a WeakActorRef without destroying the original non-weak actor ref."]
    pub fn downgrade(&self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.downgrade(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
        }
    }

    /// Returns the number of [`ActorRef`] handles.
    pub fn strong_count(&self) -> usize {
        self.mailbox.strong_count()
    }

    /// Returns the number of [`WeakActorRef`] handles.
    pub fn weak_count(&self) -> usize {
        self.mailbox.weak_count()
    }

    /// Returns true if called from within the actor.
    pub fn is_current(&self) -> bool {
        CURRENT_ACTOR_ID
            .try_with(Clone::clone)
            .map(|current_actor_id| current_actor_id == self.id())
            .unwrap_or(false)
    }

    /// Signals the actor to stop after processing all messages currently in its mailbox.
    ///
    /// This method sends a special stop message to the end of the actor's mailbox, ensuring
    /// that the actor will process all preceding messages before stopping. Any messages sent
    /// after this stop signal will be ignored and dropped. This approach allows for a graceful
    /// shutdown of the actor, ensuring all pending work is completed before termination.
    pub fn stop_gracefully(&self) -> Result<(), SendError>
    where
        A: 'static,
    {
        self.mailbox.signal_stop()
    }

    /// Kills the actor immediately.
    ///
    /// This method aborts the actor immediately. Messages in the mailbox will be ignored and dropped.
    ///
    /// The actors on_stop hook will still be called.
    ///
    /// Note: If the actor is in the middle of processing a message, it will abort processing of that message.
    pub fn kill(&self) {
        self.abort_handle.abort()
    }

    /// Waits for the actor to finish processing and stop.
    ///
    /// This method suspends execution until the actor has stopped, ensuring that any ongoing
    /// processing is completed and the actor has fully terminated. This is particularly useful
    /// in scenarios where it's necessary to wait for an actor to clean up its resources or
    /// complete its final tasks before proceeding.
    ///
    /// Note: This method does not initiate the stop process; it only waits for the actor to
    /// stop. You should signal the actor to stop using [`stop_gracefully`](ActorRef::stop_gracefully) or [`kill`](ActorRef::kill)
    /// before calling this method.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Assuming `actor.stop_gracefully().await` has been called earlier
    /// actor.wait_for_stop().await;
    /// ```
    pub async fn wait_for_stop(&self) {
        self.mailbox.closed().await
    }

    /// Sends a message to the actor, waiting for a reply.
    pub async fn send<M>(
        &self,
        msg: M,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        debug_assert!(
            !self.is_current(),
            "actors cannot send messages syncronously themselves as this would deadlock - use send_async instead\nthis assertion only occurs on debug builds, release builds will deadlock",
        );

        let (reply, rx) = oneshot::channel();
        self.mailbox.send(Signal::Message {
            message: Box::new(msg),
            actor_ref: self.clone(),
            reply: Some(reply),
            sent_within_actor: self.is_current(),
        })?;
        match rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Sends a message to the actor asyncronously without waiting for a reply.
    ///
    /// If the handler for this message returns an error, the actor will panic.
    pub fn send_async<M>(&self, msg: M) -> Result<(), SendError<M>>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        Ok(self.mailbox.send(Signal::Message {
            message: Box::new(msg),
            actor_ref: self.clone(),
            reply: None,
            sent_within_actor: self.is_current(),
        })?)
    }

    /// Sends a message to the actor after a delay.
    ///
    /// This spawns a tokio::task and sleeps for the duration of `delay` before seding the message.
    ///
    /// If the handler for this message returns an error, the actor will panic.
    pub fn send_after<M>(&self, msg: M, delay: Duration) -> JoinHandle<Result<(), SendError<M>>>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        let actor_ref = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            actor_ref.send_async(msg)
        })
    }

    /// Queries the actor for some data.
    ///
    /// Queries can run in parallel if executed in sequence.
    ///
    /// If the actor was spawned as `!Sync` with [spawn_unsync](crate::actor::spawn_unsync),
    /// then queries will not be supported and any query will return an error of [`SendError::QueriesNotSupported`].
    pub async fn query<M>(
        &self,
        msg: M,
    ) -> Result<<A::Reply as Reply>::Ok, SendError<M, <A::Reply as Reply>::Error>>
    where
        A: Query<M>,
        M: Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        self.mailbox.send(Signal::Query {
            query: Box::new(msg),
            actor_ref: self.clone(),
            reply: Some(reply),
            sent_within_actor: self.is_current(),
        })?;
        match rx.await? {
            Ok(val) => Ok(*val.downcast().unwrap()),
            Err(err) => Err(err.downcast()),
        }
    }

    /// Attaches a stream of messages to the actor.
    ///
    /// This spawns a tokio task which forwards the stream to the actor.
    /// The returned `JoinHandle` can be aborted to stop the messages from being forwarded to the actor.
    ///
    /// The `start_value` and `finish_value` can be provided to pass additional context when attaching the stream.
    /// If there's no data to be sent, these can be set to `()`.
    #[allow(clippy::type_complexity)]
    pub fn attach_stream<M, S, T, F>(
        &self,
        mut stream: S,
        start_value: T,
        finish_value: F,
    ) -> JoinHandle<Result<(), SendError<StreamMessage<M, T, F>>>>
    where
        A: Message<StreamMessage<M, T, F>> + Send + 'static,
        S: Stream<Item = M> + Send + Unpin + 'static,
        M: Send + 'static,
        T: Send + 'static,
        F: Send + 'static,
    {
        let actor_ref = self.clone();
        tokio::spawn(async move {
            actor_ref.send_async(StreamMessage::Started(start_value))?;

            while let Some(msg) = stream.next().await {
                actor_ref.send_async(StreamMessage::Next(msg))?;
            }

            actor_ref.send_async(StreamMessage::Finished(finish_value))?;

            Ok(())
        })
    }

    /// Links this actor with a child, making this one the parent.
    ///
    /// If the parent dies, then the child will be notified with a link died signal.
    pub fn link_child<B>(&self, child: &ActorRef<B>)
    where
        B: 'static,
    {
        if self.id == child.id() {
            return;
        }

        let child_id = child.id();
        let child: Box<dyn SignalMailbox> = child.signal_mailbox();
        self.links.lock().unwrap().insert(child_id, child);
    }

    /// Unlinks a previously linked child actor.
    pub fn unlink_child<B>(&self, child: &ActorRef<B>)
    where
        B: 'static,
    {
        if self.id == child.id() {
            return;
        }

        self.links.lock().unwrap().remove(&child.id());
    }

    /// Links this actor with a sibbling, notifying eachother if either one dies.
    pub fn link_together<B>(&self, sibbling: &ActorRef<B>)
    where
        A: 'static,
        B: 'static,
    {
        if self.id == sibbling.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            (self.links.lock().unwrap(), sibbling.links.lock().unwrap());
        this_links.insert(sibbling.id(), sibbling.signal_mailbox());
        sibbling_links.insert(self.id, self.signal_mailbox());
    }

    /// Unlinks previously linked processes from eachother.
    pub fn unlink_together<B>(&self, sibbling: &ActorRef<B>)
    where
        B: 'static,
    {
        if self.id == sibbling.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            (self.links.lock().unwrap(), sibbling.links.lock().unwrap());
        this_links.remove(&sibbling.id());
        sibbling_links.remove(&self.id);
    }

    pub(crate) fn signal_mailbox(&self) -> Box<dyn SignalMailbox>
    where
        A: 'static,
    {
        Box::new(self.mailbox.clone())
    }
}

impl<A: ?Sized> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
        }
    }
}

impl<A: ?Sized> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("ActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(TryLockError::Poisoned(_)) => {
                d.field("links", &format_args!("<poisoned>"));
            }
            Err(TryLockError::WouldBlock) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

/// A actor ref that does not prevent the actor from being stopped.
///
/// If all [`ActorRef`] instances of an actor were dropped and only
/// `WeakActorRef` instances remain, the actor is stopped.
///
/// In order to send messages to an actor, the `WeakActorRef` needs to be upgraded using
/// [`WeakActorRef::upgrade`], which returns `Option<ActorRef>`. It returns `None`
/// if all `ActorRef`s have been dropped, and otherwise it returns an `ActorRef`.
pub struct WeakActorRef<A: ?Sized> {
    id: u64,
    mailbox: WeakMailbox<A>,
    abort_handle: AbortHandle,
    links: Links,
}

impl<A: ?Sized> WeakActorRef<A> {
    /// Returns the actor identifier.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Tries to convert a `WeakActorRef` into a [`ActorRef`]. This will return `Some`
    /// if there are other `ActorRef` instances alive, otherwise `None` is returned.
    pub fn upgrade(&self) -> Option<ActorRef<A>> {
        self.mailbox.upgrade().map(|mailbox| ActorRef {
            id: self.id,
            mailbox,
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
        })
    }

    /// Returns the number of [`ActorRef`] handles.
    pub fn strong_count(&self) -> usize {
        self.mailbox.strong_count()
    }

    /// Returns the number of [`WeakActorRef`] handles.
    pub fn weak_count(&self) -> usize {
        self.mailbox.weak_count()
    }
}

impl<A: ?Sized> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
        }
    }
}

impl<A: ?Sized> fmt::Debug for WeakActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(TryLockError::Poisoned(_)) => {
                d.field("links", &format_args!("<poisoned>"));
            }
            Err(TryLockError::WouldBlock) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

pub(crate) trait SignalMailbox: DynClone + Send {
    fn signal_startup_finished(&self) -> Result<(), SendError>;
    fn signal_link_died(&self, id: u64, reason: ActorStopReason) -> Result<(), SendError>;
    fn signal_stop(&self) -> Result<(), SendError>;
}

dyn_clone::clone_trait_object!(SignalMailbox);

impl<A> SignalMailbox for Mailbox<A> {
    fn signal_startup_finished(&self) -> Result<(), SendError> {
        self.send(Signal::StartupFinished)
            .map_err(|_| SendError::ActorNotRunning(()))
    }

    fn signal_link_died(&self, id: u64, reason: ActorStopReason) -> Result<(), SendError> {
        self.send(Signal::LinkDied { id, reason })
            .map_err(|_| SendError::ActorNotRunning(()))
    }

    fn signal_stop(&self) -> Result<(), SendError> {
        self.send(Signal::Stop)
            .map_err(|_| SendError::ActorNotRunning(()))
    }
}

pub(crate) enum Signal<A: ?Sized> {
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

impl<A> Signal<A> {
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
