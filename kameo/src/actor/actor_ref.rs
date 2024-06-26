use std::{
    cell::Cell,
    collections::HashMap,
    fmt, ops,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::{stream::AbortHandle, Stream, StreamExt};
use tokio::{sync::Mutex, task::JoinHandle, task_local};

use crate::{
    error::SendError,
    message::{Message, Query, StreamMessage},
    reply::Reply,
    request::{AskRequest, QueryRequest, TellRequest, WithoutRequestTimeout},
    Actor,
};

use super::{Mailbox, Signal, SignalMailbox, WeakMailbox};

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(0);
task_local! {
    pub(crate) static CURRENT_ACTOR_ID: u64;
}
thread_local! {
    pub(crate) static CURRENT_THREAD_ACTOR_ID: Cell<Option<u64>> = Cell::new(None);
}

/// A reference to an actor for sending messages/queries and managing the actor.
pub struct ActorRef<A: Actor> {
    id: u64,
    mailbox: A::Mailbox,
    abort_handle: AbortHandle,
    links: Links,
}

impl<A> ActorRef<A>
where
    A: Actor,
{
    pub(crate) fn new(mailbox: A::Mailbox, abort_handle: AbortHandle, links: Links) -> Self {
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
        !self.mailbox.is_closed()
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
    pub async fn stop_gracefully(&self) -> Result<(), SendError>
    where
        A: 'static,
    {
        self.mailbox.signal_stop().await
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
    ///
    /// # Example
    ///
    /// ```
    /// actor_ref.ask(msg).send().await?; // Receive reply asyncronously
    /// actor_ref.ask(msg).blocking_send()?; // Receive reply blocking
    /// actor_ref.ask(msg).mailbox_timeout(Duration::from_secs(1)).send().await?; // Timeout after 1 second
    /// ```
    #[inline]
    pub fn ask<M>(
        &self,
        msg: M,
    ) -> AskRequest<A, A::Mailbox, M, WithoutRequestTimeout, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        AskRequest::new(self, msg)
    }

    /// Sends a message to the actor asyncronously without waiting for a reply.
    ///
    /// # Example
    ///
    /// ```
    /// actor_ref.tell(msg).send().await?; // Send message
    /// actor_ref.tell(msg).timeout(Duration::from_secs(1)).send().await?; // Timeout after 1 second
    /// ```
    #[inline]
    pub fn tell<M>(&self, msg: M) -> TellRequest<A, A::Mailbox, M, WithoutRequestTimeout>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        TellRequest::new(self, msg)
    }

    /// Queries the actor for some data.
    ///
    /// Queries can run in parallel if executed in sequence.
    ///
    /// If the actor was spawned as `!Sync` with [spawn_unsync](crate::actor::spawn_unsync),
    /// then queries will not be supported and any query will return an error of [`SendError::QueriesNotSupported`].
    ///
    /// # Example
    ///
    /// ```
    /// actor_ref.query(msg).send().await?; // Receive reply asyncronously
    /// actor_ref.query(msg).blocking_send()?; // Receive reply blocking
    /// actor_ref.query(msg).mailbox_timeout(Duration::from_secs(1)).send().await?; // Timeout after 1 second
    /// ```
    #[inline]
    pub fn query<M>(
        &self,
        msg: M,
    ) -> QueryRequest<A, A::Mailbox, M, WithoutRequestTimeout, WithoutRequestTimeout>
    where
        A: Query<M>,
        M: Send + 'static,
    {
        QueryRequest::new(self, msg)
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
    ) -> JoinHandle<Result<(), SendError<StreamMessage<M, T, F>, <A::Reply as Reply>::Error>>>
    where
        A: Message<StreamMessage<M, T, F>> + Send + 'static,
        A::Mailbox: Send + Sync,
        S: Stream<Item = M> + Send + Unpin + 'static,
        M: Send + 'static,
        T: Send + 'static,
        F: Send + 'static,
    {
        let actor_ref = self.clone();
        tokio::spawn(async move {
            actor_ref
                .send_msg(StreamMessage::Started(start_value))
                .await?;

            while let Some(msg) = stream.next().await {
                actor_ref.send_msg(StreamMessage::Next(msg)).await?;
            }

            actor_ref
                .send_msg(StreamMessage::Finished(finish_value))
                .await?;

            Ok(())
        })
    }

    /// Links this actor with a child, making this one the parent.
    ///
    /// If the parent dies, then the child will be notified with a link died signal.
    pub async fn link_child<B>(&self, child: &ActorRef<B>)
    where
        B: Actor + 'static,
    {
        if self.id == child.id() {
            return;
        }

        let child_id = child.id();
        let child: Box<dyn SignalMailbox> = child.weak_signal_mailbox();
        self.links.lock().await.insert(child_id, child);
    }

    /// Unlinks a previously linked child actor.
    pub async fn unlink_child<B>(&self, child: &ActorRef<B>)
    where
        B: Actor + 'static,
    {
        if self.id == child.id() {
            return;
        }

        self.links.lock().await.remove(&child.id());
    }

    /// Links this actor with a sibbling, notifying eachother if either one dies.
    pub async fn link_together<B>(&self, sibbling: &ActorRef<B>)
    where
        A: 'static,
        B: Actor + 'static,
    {
        if self.id == sibbling.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling.links.lock());
        this_links.insert(sibbling.id(), sibbling.weak_signal_mailbox());
        sibbling_links.insert(self.id, self.weak_signal_mailbox());
    }

    /// Unlinks previously linked processes from eachother.
    pub async fn unlink_together<B>(&self, sibbling: &ActorRef<B>)
    where
        B: Actor + 'static,
    {
        if self.id == sibbling.id() {
            return;
        }

        let (mut this_links, mut sibbling_links) =
            tokio::join!(self.links.lock(), sibbling.links.lock());
        this_links.remove(&sibbling.id());
        sibbling_links.remove(&self.id);
    }

    pub(crate) fn mailbox(&self) -> &A::Mailbox {
        &self.mailbox
    }

    pub(crate) fn weak_signal_mailbox(&self) -> Box<dyn SignalMailbox>
    where
        A: 'static,
    {
        Box::new(self.mailbox.downgrade())
    }

    async fn send_msg<M>(&self, msg: M) -> Result<(), SendError<M, <A::Reply as Reply>::Error>>
    where
        A: Message<M>,
        M: Send + 'static,
    {
        self.mailbox
            .send(Signal::Message {
                message: Box::new(msg),
                actor_ref: self.clone(),
                reply: None,
                sent_within_actor: false,
            })
            .await?;
        Ok(())
    }
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("ActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(_) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

impl<A: Actor> AsRef<Links> for ActorRef<A> {
    fn as_ref(&self) -> &Links {
        &self.links
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
pub struct WeakActorRef<A: Actor> {
    id: u64,
    mailbox: <A::Mailbox as Mailbox<A>>::WeakMailbox,
    abort_handle: AbortHandle,
    links: Links,
}

impl<A: Actor> WeakActorRef<A> {
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

impl<A: Actor> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("WeakActorRef");
        d.field("id", &self.id);
        match self.links.try_lock() {
            Ok(guard) => {
                d.field("links", &guard.keys());
            }
            Err(_) => {
                d.field("links", &format_args!("<locked>"));
            }
        }
        d.finish()
    }
}

/// A hashmap of linked actors to be notified when the actor dies.
#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
pub struct Links(Arc<Mutex<HashMap<u64, Box<dyn SignalMailbox>>>>);

impl ops::Deref for Links {
    type Target = Mutex<HashMap<u64, Box<dyn SignalMailbox>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
