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
    /// Always restarted, regardless of how it died (panic or normal exit).
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

#[allow(missing_debug_implementations)]
pub struct SupervisedActorBuilder<'a, S: Actor, C: Actor> {
    supervisor_ref: &'a ActorRef<S>,
    args_factory: SupervisorFactory<C::Args>,
    restart_policy: RestartPolicy,
    max_restarts: u32,
    restart_window: Duration,
}

impl<'a, S: Actor, C: Actor> SupervisedActorBuilder<'a, S, C> {
    pub(crate) fn new(supervisor_ref: &'a ActorRef<S>, args: C::Args) -> Self
    where
        C::Args: Clone + Sync,
    {
        SupervisedActorBuilder {
            supervisor_ref,
            args_factory: SupervisorFactory::new(args),
            restart_policy: RestartPolicy::default(),
            max_restarts: 5,
            restart_window: Duration::from_secs(5),
        }
    }

    pub(crate) fn new_with(
        supervisor_ref: &'a ActorRef<S>,
        f: impl Fn() -> C::Args + Send + Sync + 'static,
    ) -> Self
    where
        C::Args: Sync,
    {
        SupervisedActorBuilder {
            supervisor_ref,
            args_factory: SupervisorFactory::new_with(f),
            restart_policy: RestartPolicy::default(),
            max_restarts: 5,
            restart_window: Duration::from_secs(5),
        }
    }

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
        self.spawn_inner((mailbox_tx, mailbox_rx), false).await
    }

    pub async fn spawn_in_thread(self) -> ActorRef<C> {
        self.spawn_with_mailbox(mailbox::bounded(DEFAULT_MAILBOX_CAPACITY))
            .await
    }

    pub async fn spawn_in_thread_with_mailbox(
        self,
        (mailbox_tx, mailbox_rx): (MailboxSender<C>, MailboxReceiver<C>),
    ) -> ActorRef<C> {
        self.spawn_inner((mailbox_tx, mailbox_rx), true).await
    }

    pub async fn spawn_inner(
        self,
        (mailbox_tx, mailbox_rx): (MailboxSender<C>, MailboxReceiver<C>),
        in_thread: bool,
    ) -> ActorRef<C> {
        let actor_id = ActorId::generate();
        let links = Links::default();
        let restart_policy = self.restart_policy;
        let factory = Arc::new(new_factory(
            actor_id,
            mailbox_tx.clone(),
            links.clone(),
            self.args_factory,
            in_thread,
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
        self.supervisor_ref.link_child(actor_id, &links, spec).await;
        *(*factory)(Box::new(mailbox_rx)).await.downcast().unwrap()
    }
}

fn new_factory<C: Actor>(
    actor_id: ActorId,
    mailbox_tx: MailboxSender<C>,
    links: Links,
    args_factory: SupervisorFactory<C::Args>,
    in_thread: bool,
) -> SpawnFactory {
    Box::new(move |rx: Box<dyn Any + Send>| {
        let mailbox_rx = *rx.downcast::<MailboxReceiver<C>>().unwrap();
        let mailbox_tx = mailbox_tx.clone();
        let links = links.clone();
        let args = args_factory.get();

        Box::pin(async move {
            let prepared = PreparedActor::new_with(actor_id, (mailbox_tx, mailbox_rx), links);
            let actor_ref = prepared.actor_ref().clone();
            if in_thread {
                prepared.spawn_in_thread(args);
            } else {
                prepared.spawn(args);
            }
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

#[cfg(test)]
mod tests {
    use std::{
        ops::ControlFlow,
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };

    use crate::{
        actor::{Actor, ActorRef, Spawn, WeakActorRef},
        error::{ActorStopReason, Infallible},
        message::{Context, Message},
        supervision::{RestartPolicy, SupervisionStrategy},
    };

    // ==================== Test Helper Actors ====================

    /// A configurable test child actor that can panic, return errors, or stop normally
    #[derive(Clone)]
    struct TestChild {
        start_count: Arc<AtomicU32>,
    }

    impl TestChild {
        fn new(start_count: Arc<AtomicU32>) -> Self {
            TestChild { start_count }
        }
    }

    impl Actor for TestChild {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            state.start_count.fetch_add(1, Ordering::SeqCst);
            Ok(state)
        }
    }

    /// Message that triggers a panic
    struct TriggerPanic;

    impl Message<TriggerPanic> for TestChild {
        type Reply = ();

        async fn handle(
            &mut self,
            _msg: TriggerPanic,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            panic!("intentional panic for testing");
        }
    }

    /// Message that returns an error (for tell requests, triggers on_panic)
    struct TriggerError;

    impl Message<TriggerError> for TestChild {
        type Reply = Result<(), &'static str>;

        async fn handle(
            &mut self,
            _msg: TriggerError,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            Err("intentional error for testing")
        }
    }

    /// Message that stops the actor gracefully
    struct StopGracefully;

    impl Message<StopGracefully> for TestChild {
        type Reply = ();

        async fn handle(
            &mut self,
            _msg: StopGracefully,
            ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            ctx.stop();
        }
    }

    /// Message to check if actor is alive (used for verification)
    struct Ping;

    impl Message<Ping> for TestChild {
        type Reply = u32;

        async fn handle(
            &mut self,
            _msg: Ping,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            self.start_count.load(Ordering::SeqCst)
        }
    }

    /// A supervisor actor with OneForOne strategy (default)
    #[derive(Clone)]
    struct TestSupervisor;

    impl Actor for TestSupervisor {
        type Args = Self;
        type Error = Infallible;

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::OneForOne
        }

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    /// Supervisor with OneForAll strategy
    struct OneForAllSupervisor;

    impl Actor for OneForAllSupervisor {
        type Args = Self;
        type Error = Infallible;

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::OneForAll
        }

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    /// Supervisor with RestForOne strategy
    struct RestForOneSupervisor;

    impl Actor for RestForOneSupervisor {
        type Args = Self;
        type Error = Infallible;

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::RestForOne
        }

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    /// A sibling actor that tracks link death notifications
    #[derive(Clone)]
    struct LinkTracker {
        link_died_count: Arc<AtomicU32>,
        start_count: Arc<AtomicU32>,
    }

    impl LinkTracker {
        fn new(link_died_count: Arc<AtomicU32>, start_count: Arc<AtomicU32>) -> Self {
            LinkTracker {
                link_died_count,
                start_count,
            }
        }
    }

    impl Actor for LinkTracker {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(
            state: Self::Args,
            _actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            state.start_count.fetch_add(1, Ordering::SeqCst);
            Ok(state)
        }

        async fn on_link_died(
            &mut self,
            _actor_ref: WeakActorRef<Self>,
            _id: crate::actor::ActorId,
            _reason: ActorStopReason,
        ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
            self.link_died_count.fetch_add(1, Ordering::SeqCst);
            Ok(ControlFlow::Continue(()))
        }
    }

    impl Message<Ping> for LinkTracker {
        type Reply = u32;

        async fn handle(
            &mut self,
            _msg: Ping,
            _ctx: &mut Context<Self, Self::Reply>,
        ) -> Self::Reply {
            self.start_count.load(Ordering::SeqCst)
        }
    }

    // ==================== Restart Policy Tests ====================

    #[tokio::test]
    async fn permanent_restarts_on_panic() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger panic
        let _ = child.tell(TriggerPanic).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "actor should have restarted after panic"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn permanent_restarts_on_normal_exit() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Stop gracefully (normal exit)
        let _ = child.tell(StopGracefully).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "permanent policy should restart on normal exit"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn permanent_restarts_on_error() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger error (tell with Result::Err triggers on_panic)
        let _ = child.tell(TriggerError).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "permanent policy should restart on error"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn transient_restarts_on_panic() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Transient)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger panic
        let _ = child.tell(TriggerPanic).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "transient policy should restart on panic"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn transient_no_restart_on_normal_exit() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Transient)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Stop gracefully (normal exit)
        let _ = child.tell(StopGracefully).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "transient policy should NOT restart on normal exit"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn transient_restarts_on_error() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Transient)
            .restart_limit(5, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger error
        let _ = child.tell(TriggerError).await;

        // Wait for restart
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "transient policy should restart on error"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn temporary_never_restarts_on_panic() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Temporary)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger panic
        let _ = child.tell(TriggerPanic).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "temporary policy should NEVER restart"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn temporary_never_restarts_on_normal_exit() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Temporary)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Stop gracefully (normal exit)
        let _ = child.tell(StopGracefully).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "temporary policy should NEVER restart"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn temporary_never_restarts_on_error() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Temporary)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger error
        let _ = child.tell(TriggerError).await;

        // Wait to see if restart happens (it shouldn't)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            1,
            "temporary policy should NEVER restart"
        );

        supervisor.kill();
    }

    // ==================== Restart Limits Tests ====================

    #[tokio::test]
    async fn max_restarts_exceeded_stops_restarting() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .restart_limit(2, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // First crash -> restart (count = 1)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 2);

        // Second crash -> restart (count = 2)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 3);

        // Third crash -> should NOT restart (max_restarts = 2 exceeded)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            3,
            "should not restart after max_restarts exceeded"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn restart_count_resets_after_window() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .restart_limit(2, Duration::from_millis(100)) // Very short window
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // First crash -> restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 2);

        // Second crash -> restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 3);

        // Wait for window to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Third crash -> should restart (window reset)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            4,
            "should restart after window resets"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn custom_restart_limit_respected() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .restart_limit(1, Duration::from_secs(10)) // Only 1 restart allowed
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // First crash -> restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 2);

        // Second crash -> should NOT restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "custom limit of 1 should be respected"
        );

        supervisor.kill();
    }

    // ==================== Supervision Strategy Tests ====================

    #[tokio::test]
    async fn one_for_one_only_restarts_failed_child() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);

        // Crash child1
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            1,
            "child2 should NOT restart"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn one_for_all_restarts_all_children() {
        let supervisor = OneForAllSupervisor::spawn(OneForAllSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child1
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart (OneForAll)"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart (OneForAll)"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn rest_for_one_restarts_later_children() {
        let supervisor = RestForOneSupervisor::spawn(RestForOneSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let _child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child2 (middle child)
        let _ = child2.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            1,
            "child1 should NOT restart (spawned before)"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart (spawned after)"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn rest_for_one_first_child_restarts_all() {
        let supervisor = RestForOneSupervisor::spawn(RestForOneSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child1 (first child) - all children after should restart
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart (spawned after child1)"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart (spawned after child1)"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn rest_for_one_last_child_only_self() {
        let supervisor = RestForOneSupervisor::spawn(RestForOneSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));
        let start_count3 = Arc::new(AtomicU32::new(0));

        let _child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let child3 = TestChild::supervise(&supervisor, TestChild::new(start_count3.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count1.load(Ordering::SeqCst), 1);
        assert_eq!(start_count2.load(Ordering::SeqCst), 1);
        assert_eq!(start_count3.load(Ordering::SeqCst), 1);

        // Crash child3 (last child) - only child3 should restart
        let _ = child3.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            1,
            "child1 should NOT restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            1,
            "child2 should NOT restart"
        );
        assert_eq!(
            start_count3.load(Ordering::SeqCst),
            2,
            "child3 should restart"
        );

        supervisor.kill();
    }

    // ==================== Link Propagation Tests ====================

    #[tokio::test]
    async fn sibling_link_notifies_on_death() {
        let link_died_count = Arc::new(AtomicU32::new(0));
        let tracker_start_count = Arc::new(AtomicU32::new(0));

        let tracker = LinkTracker::spawn(LinkTracker::new(
            link_died_count.clone(),
            tracker_start_count.clone(),
        ));

        let child_start_count = Arc::new(AtomicU32::new(0));
        let child = TestChild::spawn(TestChild::new(child_start_count.clone()));

        // Wait for both to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Link them as siblings
        tracker.link(&child).await;

        // Kill the child
        child.kill();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            link_died_count.load(Ordering::SeqCst),
            1,
            "tracker should receive link_died notification"
        );

        tracker.kill();
    }

    #[tokio::test]
    async fn sibling_unlink_stops_notifications() {
        let link_died_count = Arc::new(AtomicU32::new(0));
        let tracker_start_count = Arc::new(AtomicU32::new(0));

        let tracker = LinkTracker::spawn(LinkTracker::new(
            link_died_count.clone(),
            tracker_start_count.clone(),
        ));

        let child_start_count = Arc::new(AtomicU32::new(0));
        let child = TestChild::spawn(TestChild::new(child_start_count.clone()));

        // Wait for both to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Link and then unlink
        tracker.link(&child).await;
        tracker.unlink(&child).await;

        // Kill the child
        child.kill();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            link_died_count.load(Ordering::SeqCst),
            0,
            "tracker should NOT receive notification after unlink"
        );

        tracker.kill();
    }

    #[tokio::test]
    async fn supervised_child_does_not_notify_siblings_on_restart() {
        // When a supervised child restarts, siblings should NOT be notified
        // (the parent handles the restart internally)
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let link_died_count = Arc::new(AtomicU32::new(0));
        let tracker_start_count = Arc::new(AtomicU32::new(0));
        let child_start_count = Arc::new(AtomicU32::new(0));

        let tracker = LinkTracker::supervise(
            &supervisor,
            LinkTracker::new(link_died_count.clone(), tracker_start_count.clone()),
        )
        .restart(RestartPolicy::Permanent)
        .spawn()
        .await;

        let child = TestChild::supervise(&supervisor, TestChild::new(child_start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for both to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Link them as siblings
        tracker.link(&child).await;

        // Crash the child (will be restarted by supervisor)
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Child should have restarted
        assert_eq!(
            child_start_count.load(Ordering::SeqCst),
            2,
            "child should restart"
        );
        // Tracker should NOT be notified because the child was restarted by supervisor
        assert_eq!(
            link_died_count.load(Ordering::SeqCst),
            0,
            "siblings should NOT be notified when child restarts"
        );

        supervisor.kill();
    }

    // ==================== Edge Cases ====================

    #[tokio::test]
    async fn rapid_successive_restarts() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .restart_limit(10, Duration::from_secs(10))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger multiple rapid crashes
        for _ in 0..5 {
            let _ = child.tell(TriggerPanic).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // All should have restarted
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            6,
            "should handle rapid successive restarts"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn one_for_all_with_single_child() {
        let supervisor = OneForAllSupervisor::spawn(OneForAllSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Crash the only child
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "single child should restart under OneForAll"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn one_for_one_multiple_independent_failures() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        let child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Crash both children independently
        let _ = child1.tell(TriggerPanic).await;
        let _ = child2.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart independently"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            2,
            "child2 should restart independently"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn supervisor_factory_uses_cloned_args() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .restart(RestartPolicy::Permanent)
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Trigger crash to force restart
        let _ = child.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The restarted actor should use cloned args (same Arc)
        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "factory should clone args for restart"
        );

        supervisor.kill();
    }

    // ==================== Default Values Tests ====================

    #[tokio::test]
    async fn default_restart_policy_is_permanent() {
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count = Arc::new(AtomicU32::new(0));

        // Use default (no .restart() call)
        let child = TestChild::supervise(&supervisor, TestChild::new(start_count.clone()))
            .spawn()
            .await;

        // Wait for initial start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(start_count.load(Ordering::SeqCst), 1);

        // Normal exit should trigger restart (Permanent policy)
        let _ = child.tell(StopGracefully).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count.load(Ordering::SeqCst),
            2,
            "default policy (Permanent) should restart on normal exit"
        );

        supervisor.kill();
    }

    #[tokio::test]
    async fn default_supervision_strategy_is_one_for_one() {
        // TestSupervisor uses default strategy (OneForOne)
        let supervisor = TestSupervisor::spawn(TestSupervisor);
        let start_count1 = Arc::new(AtomicU32::new(0));
        let start_count2 = Arc::new(AtomicU32::new(0));

        let child1 = TestChild::supervise(&supervisor, TestChild::new(start_count1.clone()))
            .spawn()
            .await;

        let _child2 = TestChild::supervise(&supervisor, TestChild::new(start_count2.clone()))
            .spawn()
            .await;

        // Wait for initial starts
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Crash child1
        let _ = child1.tell(TriggerPanic).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            start_count1.load(Ordering::SeqCst),
            2,
            "child1 should restart"
        );
        assert_eq!(
            start_count2.load(Ordering::SeqCst),
            1,
            "child2 should NOT restart (OneForOne)"
        );

        supervisor.kill();
    }
}
