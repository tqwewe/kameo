use std::{
    any,
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, TryLockError,
    },
    time::Duration,
};

use dyn_clone::DynClone;
use futures::stream::AbortHandle;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    error::{ActorStopReason, SendError},
    message::{BoxReply, DynMessage, DynQuery, Message, Query},
};

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(0);
pub(crate) struct Ctx {
    pub(crate) id: u64,
    pub(crate) actor_ref: Box<dyn any::Any + Send>,
    pub(crate) signal_mailbox: Box<dyn SignalMailbox>,
    pub(crate) links: Links,
}
tokio::task_local! {
    pub(crate) static CURRENT_CTX: Ctx;
}

type Mailbox<A> = mpsc::UnboundedSender<Signal<A>>;
pub(crate) type Links = Arc<Mutex<HashMap<u64, Box<dyn SignalMailbox>>>>;

/// A reference to an actor for sending messages/queries and managing the actor.
pub struct ActorRef<A> {
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

    /// Returns the current actor ref if called within an actor.
    pub fn current() -> Option<ActorRef<A>>
    where
        A: 'static,
    {
        CURRENT_CTX.with(|ctx| ctx.actor_ref.downcast_ref().cloned())
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
    /// stop. You should signal the actor to stop using `stop_gracefully` or `kill`
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
    /// ```
    /// let reply = actor_ref.send(msg).await?;
    /// ```
    pub async fn send<M>(&self, msg: M) -> Result<M::Reply, SendError<M>>
    where
        M: Message<A>,
    {
        let (reply, rx) = oneshot::channel();
        self.mailbox.send(Signal::Message {
            message: Box::new(msg),
            reply: Some(reply),
        })?;
        Ok(rx.await.map(|val| *val.downcast().unwrap())?)
    }

    /// Sends a message to the actor asyncronously without waiting for a reply.
    ///
    /// If the handler for this message returns an error, the actor will panic.
    ///
    /// ```
    /// actor_ref.send_async(msg)?;
    /// ```
    pub fn send_async<M>(&self, msg: M) -> Result<(), SendError<M>>
    where
        M: Message<A>,
    {
        Ok(self.mailbox.send(Signal::Message {
            message: Box::new(msg),
            reply: None,
        })?)
    }

    /// Sends a message to the actor after a delay.
    ///
    /// This spawns a tokio::task and sleeps for the duration of `delay` before seding the message.
    ///
    /// If the handler for this message returns an error, the actor will panic.
    ///
    /// ```
    /// actor_ref.send_after(msg, Duration::from_secs(5));
    /// ```
    pub fn send_after<M>(&self, msg: M, delay: Duration) -> JoinHandle<Result<(), SendError<M>>>
    where
        A: 'static,
        M: Message<A>,
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
    /// If the actor was spawned as `!Sync` with `Spawn::spawn_unsync`, then queries will not be supported
    /// and any query will return `Err(SendError::QueriesNotSupported)`.
    ///
    /// ```
    /// // Query from the actor
    /// let reply = actor_ref.query(msg).await?;
    ///
    /// // Queries run concurrently if no `Message`'s are sent between them
    /// let (a, b, c) = tokio::try_join!(
    ///     actor_ref.query(Foo { ... }),
    ///     actor_ref.query(Foo { ... }),
    ///     actor_ref.query(Foo { ... }),
    /// )?;
    /// ```
    pub async fn query<M>(&self, msg: M) -> Result<M::Reply, SendError<M>>
    where
        A: Sync,
        M: Query<A>,
    {
        let (reply, rx) = oneshot::channel();
        self.mailbox.send(Signal::Query {
            query: Box::new(msg),
            reply: Some(reply),
        })?;
        match rx.await {
            Ok(Ok(val)) => Ok(*val.downcast().unwrap()),
            Ok(Err(SendError::ActorNotRunning(err))) => {
                Err(SendError::ActorNotRunning(*err.downcast().unwrap()))
            }
            Ok(Err(SendError::ActorStopped)) => Err(SendError::QueriesNotSupported),
            Ok(Err(SendError::QueriesNotSupported)) => Err(SendError::QueriesNotSupported),
            Err(err) => Err(err.into()),
        }
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

    pub(crate) fn links(&self) -> Links {
        self.links.clone()
    }
}

impl<A> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            id: self.id,
            mailbox: self.mailbox.clone(),
            abort_handle: self.abort_handle.clone(),
            links: self.links.clone(),
        }
    }
}

impl<A> fmt::Debug for ActorRef<A> {
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

pub(crate) trait SignalMailbox: DynClone + Send {
    fn signal_link_died(&self, id: u64, reason: ActorStopReason) -> Result<(), SendError>;
    fn signal_stop(&self) -> Result<(), SendError>;
}

dyn_clone::clone_trait_object!(SignalMailbox);

impl<A> SignalMailbox for Mailbox<A> {
    fn signal_link_died(&self, id: u64, reason: ActorStopReason) -> Result<(), SendError> {
        self.send(Signal::LinkDied { id, reason })
            .map_err(|_| SendError::ActorNotRunning(()))
    }

    fn signal_stop(&self) -> Result<(), SendError> {
        self.send(Signal::Stop)
            .map_err(|_| SendError::ActorNotRunning(()))
    }
}

pub(crate) enum Signal<A> {
    Message {
        message: Box<dyn DynMessage<A>>,
        reply: Option<oneshot::Sender<BoxReply>>,
    },
    Query {
        query: Box<dyn DynQuery<A>>,
        reply: Option<oneshot::Sender<Result<BoxReply, SendError<Box<dyn any::Any + Send>>>>>,
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
            Signal::Message { message, reply: _ } => message.as_any().downcast().ok().map(|v| *v),
            Signal::Query {
                query: message,
                reply: _,
            } => message.as_any().downcast().ok().map(|v| *v),
            _ => None,
        }
    }
}
