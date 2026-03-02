#![allow(missing_docs)]

use std::{
    any::Any,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{FutureExt, future::BoxFuture};

use crate::{
    actor::{Actor, ActorId, ActorRef, DEFAULT_MAILBOX_CAPACITY, PreparedActor},
    links::{BoxActorRef, ErasedChildSpec, Links, ShutdownFn, SpawnFactory},
    mailbox::{self, MailboxReceiver, MailboxSender, Signal},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum RestartPolicy {
    /// Always restarted, regardless of how it died (panic or normal exit)
    #[default]
    Permanent,
    /// Only restarted if it died abnormally (panic, error).
    /// A normal exit is left alone.
    Transient,
    /// Never restarted. Supervisor just notes it died and moves on.
    Temporary,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum SupervisionStrategy {
    /// Only restart the child that died.
    #[default]
    OneForOne,
    /// Restart all children when any one dies.
    OneForAll,
    /// Restart the child that died, plus all children spawned after it.
    RestForOne,
}

impl<A: Actor> ActorRef<A> {
    pub fn supervise<B: Actor>(&self, args: B::Args) -> SupervisedActorBuilder<'_, A, B>
    where
        B::Args: Clone + Sync,
    {
        SupervisedActorBuilder {
            supervisor: self,
            args_factory: SupervisorFactory::new(args),
            restart_policy: RestartPolicy::default(),
            max_restarts: 5,
            restart_window: Duration::from_secs(5),
        }
    }

    pub fn supervise_with<B: Actor>(
        &self,
        f: impl Fn() -> B::Args + Send + Sync + 'static,
    ) -> SupervisedActorBuilder<'_, A, B>
    where
        B::Args: Sync,
    {
        SupervisedActorBuilder {
            supervisor: self,
            args_factory: SupervisorFactory::new_with(f),
            restart_policy: RestartPolicy::default(),
            max_restarts: 5,
            restart_window: Duration::from_secs(5),
        }
    }
}

#[allow(missing_debug_implementations)]
pub struct SupervisedActorBuilder<'a, S: Actor, C: Actor> {
    supervisor: &'a ActorRef<S>,
    args_factory: SupervisorFactory<C::Args>,
    restart_policy: RestartPolicy,
    max_restarts: u32,
    restart_window: Duration,
}

impl<'a, S: Actor, C: Actor> SupervisedActorBuilder<'a, S, C> {
    pub fn restart(mut self, policy: RestartPolicy) -> Self {
        self.restart_policy = policy;
        self
    }

    pub fn restart_limit(mut self, restarts: u32, within: Duration) -> Self {
        self.max_restarts = restarts;
        self.restart_window = within;
        self
    }

    pub async fn spawn(self) -> ActorRef<C> {
        self.spawn_with_mailbox(mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
            .await
    }

    pub async fn spawn_with_mailbox(
        self,
        (mailbox_tx, mailbox_rx): (MailboxSender<C>, MailboxReceiver<C>),
    ) -> ActorRef<C> {
        let actor_id = ActorId::generate();
        let links = Links::default();
        let restart_policy = self.restart_policy;
        let factory = Arc::new(new_factory(
            actor_id,
            mailbox_tx.clone(),
            links.clone(),
            self.args_factory,
        ));
        let shutdown = Arc::new(Box::new(move || {
            let mailbox_tx = mailbox_tx.clone();
            async move {
                // We can ignore the error here.
                // If this failed, then the supervisor will restart it for its previous stop reason.
                let _ = mailbox_tx.send(Signal::SupervisorRestart).await;
            }
            .boxed()
        }) as ShutdownFn);
        let spec = ErasedChildSpec {
            factory: factory.clone(),
            shutdown,
            restart_policy,
            restart_count: 0,
            last_restart: Instant::now(),
            max_restarts: self.max_restarts,
            restart_window: self.restart_window,
        };
        self.supervisor.link_child(actor_id, &links, spec).await;
        *(*factory)(Box::new(mailbox_rx)).await.downcast().unwrap()
    }
}

fn new_factory<C: Actor>(
    actor_id: ActorId,
    mailbox_tx: MailboxSender<C>,
    links: Links,
    args_factory: SupervisorFactory<C::Args>,
) -> SpawnFactory {
    Box::new(move |rx: Box<dyn Any + Send>| {
        let mailbox_rx = *rx.downcast::<MailboxReceiver<C>>().unwrap();
        let mailbox_tx = mailbox_tx.clone();
        let links = links.clone();
        let args = args_factory.get();

        Box::pin(async move {
            let prepared = PreparedActor::new_with(actor_id, (mailbox_tx, mailbox_rx), links);
            let actor_ref = prepared.actor_ref().clone();
            prepared.spawn(args);
            Box::new(actor_ref) as BoxActorRef
        }) as BoxFuture<'static, BoxActorRef>
    })
}

pub(crate) struct SupervisorFactory<T>(Box<dyn Fn() -> T + Send + Sync>);

impl<T> SupervisorFactory<T> {
    fn new(args: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        SupervisorFactory(Box::new(move || args.clone()))
    }

    fn new_with(f: impl Fn() -> T + Send + Sync + 'static) -> Self {
        SupervisorFactory(Box::new(f))
    }

    fn get(&self) -> T {
        (self.0)()
    }
}
