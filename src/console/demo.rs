//! A self-contained, lively actor system used to showcase the console.
//!
//! [`spawn`] builds a small supervision tree plus a few standalone actors driven with
//! organic-looking random traffic, exercising every feature the console surfaces: a nested
//! tree, varied throughput, mailbox backpressure, restarts, a slow handler, and a deadlock.
//! Both the `console` example and the `kameo_console --demo` mode run this exact code.

use std::{
    f64::consts::TAU,
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{error::Infallible, prelude::*, supervision::SupervisionStrategy};

/// Spawns the whole demo actor system and returns a handle that keeps it alive.
///
/// The caller is expected to have already started a console collector (e.g. with
/// [`crate::console::serve`]); this only spawns the actors. Hold onto the returned
/// [`DemoSystem`] for as long as the demo should run.
pub async fn spawn() -> DemoSystem {
    let app = App::spawn(());
    let metrics = Metrics::spawn(());

    let session_a = Session::spawn(());
    let session_b = Session::spawn(());
    session_a.link(&session_b).await;

    // Two actors that ask each other inside their handlers: a textbook deadlock cycle.
    let knot_a = Knot::spawn(Knot { peer: None });
    let knot_b = Knot::spawn(Knot { peer: None });
    let _ = knot_a.tell(Tie(knot_b.clone())).await;
    let _ = knot_b.tell(Tie(knot_a.clone())).await;
    let _ = knot_a.tell(Tangle).await; // ignite: a asks b, b asks a, neither can reply

    DemoSystem {
        _app: app,
        _metrics: metrics,
        _session_a: session_a,
        _session_b: session_b,
        _knot_a: knot_a,
        _knot_b: knot_b,
    }
}

/// Keeps the demo's top-level actors alive. Drop it to let the system wind down.
pub struct DemoSystem {
    _app: ActorRef<App>,
    _metrics: ActorRef<Metrics>,
    _session_a: ActorRef<Session>,
    _session_b: ActorRef<Session>,
    _knot_a: ActorRef<Knot>,
    _knot_b: ActorRef<Knot>,
}

// ---------------------------------------------------------------------------------------------
// Randomness + load shaping
// ---------------------------------------------------------------------------------------------

/// A tiny xorshift64* PRNG, so the demo has organic-looking traffic without a `rand` dependency.
struct Rng(u64);

impl Rng {
    fn new() -> Self {
        // Mix the clock with a per-call counter so every actor gets a distinct stream.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Rng((t ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A float in `[0, 1)`.
    fn ratio(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// An integer in `[0, n)`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// A throughput profile: a smooth sine baseline that the driver jitters and spikes randomly.
#[derive(Clone, Copy)]
struct Load {
    base: f64,
    amp: f64,
    period: f64,
}

impl Load {
    /// Messages to send this tick, from the smooth wave plus random jitter and the odd spike.
    fn this_tick(&self, t: f64, rng: &mut Rng) -> u32 {
        let wave = self.base + self.amp * (0.5 + 0.5 * ((t / self.period) * TAU).sin());
        let jitter = rng.ratio() * self.amp * 0.5;
        let spike = if rng.below(45) == 0 { 20.0 } else { 0.0 };
        (wave + jitter + spike).round().max(0.0) as u32
    }
}

const TICK: Duration = Duration::from_millis(120);

/// Spawns a background task that drives `actor` with `load`, calling `send` once per message with
/// a fresh random roll (so handlers can pick a message type). The task owns a clone of the ref,
/// so the actor stays alive for the demo's duration.
fn drive<A, F, Fut>(actor: ActorRef<A>, load: Load, send: F)
where
    A: Actor,
    F: Fn(ActorRef<A>, u64) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let mut rng = Rng::new();
        let start = Instant::now();
        loop {
            let n = load.this_tick(start.elapsed().as_secs_f64(), &mut rng);
            for _ in 0..n {
                send(actor.clone(), rng.next()).await;
            }
            tokio::time::sleep(TICK).await;
        }
    });
}

// ---------------------------------------------------------------------------------------------
// Supervision tree
// ---------------------------------------------------------------------------------------------

/// Root supervisor (one-for-one). Owns the pools and a few standalone workers.
struct App;

impl Actor for App {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "app"
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForOne
    }

    async fn on_start(_: (), app: ActorRef<Self>) -> Result<Self, Self::Error> {
        HttpPool::supervise(&app, ()).spawn().await;
        CachePool::supervise(&app, ()).spawn().await;

        // A bounded, slow writer that's driven harder than it can drain: shows backpressure.
        let writer = DbWriter::supervise(&app, ())
            .spawn_with_mailbox(mailbox::bounded(32))
            .await;
        drive(
            writer,
            Load {
                base: 5.0,
                amp: 3.0,
                period: 6.0,
            },
            |actor, _| async move {
                let _ = actor.tell(Insert).await;
            },
        );

        // A reporter with a multi-second handler: periodically goes Busy, then Stuck.
        let reporter = Reporter::supervise(&app, ()).spawn().await;
        tokio::spawn(async move {
            let mut rng = Rng::new();
            loop {
                tokio::time::sleep(Duration::from_secs(9 + rng.below(7))).await;
                let _ = reporter.tell(Build).await;
            }
        });

        // A worker that panics on a random timer: shows live restarts and Restarting.
        let flaky = Flaky::supervise(&app, ())
            .restart_limit(1_000, Duration::from_secs(1))
            .spawn()
            .await;
        tokio::spawn(async move {
            let mut rng = Rng::new();
            loop {
                tokio::time::sleep(Duration::from_secs(4 + rng.below(5))).await;
                let _ = flaky.tell(Boom).await;
            }
        });

        Ok(App)
    }
}

/// Pool of http workers (one-for-all: a crash takes the whole pool down together).
struct HttpPool;

impl Actor for HttpPool {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "http-pool"
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForAll
    }

    async fn on_start(_: (), pool: ActorRef<Self>) -> Result<Self, Self::Error> {
        let profiles = [
            Load {
                base: 1.0,
                amp: 5.0,
                period: 7.0,
            },
            Load {
                base: 0.0,
                amp: 7.0,
                period: 3.5,
            },
            Load {
                base: 4.0,
                amp: 2.0,
                period: 11.0,
            },
        ];
        for load in profiles {
            let worker = HttpWorker::supervise(&pool, ()).spawn().await;
            drive(worker, load, |actor, roll| async move {
                match roll % 10 {
                    0..=5 => drop(actor.tell(Get).await),
                    6..=8 => drop(actor.tell(Post).await),
                    _ => drop(actor.tell(Delete).await),
                }
            });
        }
        Ok(HttpPool)
    }
}

/// Pool of cache shards (rest-for-one: a crash restarts that shard and younger siblings).
struct CachePool;

impl Actor for CachePool {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "cache-pool"
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::RestForOne
    }

    async fn on_start(_: (), pool: ActorRef<Self>) -> Result<Self, Self::Error> {
        let profiles = [
            Load {
                base: 2.0,
                amp: 1.5,
                period: 9.0,
            },
            Load {
                base: 1.0,
                amp: 2.5,
                period: 5.0,
            },
            Load {
                base: 3.0,
                amp: 1.0,
                period: 13.0,
            },
        ];
        for load in profiles {
            let shard = CacheShard::supervise(&pool, ()).spawn().await;
            drive(shard, load, |actor, roll| async move {
                match roll % 10 {
                    0..=6 => drop(actor.tell(Read).await),
                    7..=8 => drop(actor.tell(Write).await),
                    _ => drop(actor.tell(Evict).await),
                }
            });
        }
        Ok(CachePool)
    }
}

// ---------------------------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------------------------

/// Holds its own `Rng` so handlers can add a touch of random latency: enough that under bursts
/// the mailbox briefly backs up (green dipping into yellow) instead of sitting flat at zero.
struct HttpWorker {
    rng: Rng,
}

impl HttpWorker {
    async fn handle_request(&mut self) {
        tokio::time::sleep(Duration::from_micros(150 + self.rng.below(800))).await;
    }
}

impl Actor for HttpWorker {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "http-worker"
    }

    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(HttpWorker { rng: Rng::new() })
    }
}

struct Get;
struct Post;
struct Delete;

impl Message<Get> for HttpWorker {
    type Reply = ();
    async fn handle(&mut self, _: Get, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_request().await;
    }
}
impl Message<Post> for HttpWorker {
    type Reply = ();
    async fn handle(&mut self, _: Post, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_request().await;
    }
}
impl Message<Delete> for HttpWorker {
    type Reply = ();
    async fn handle(&mut self, _: Delete, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_request().await;
    }
}

struct CacheShard {
    rng: Rng,
}

impl CacheShard {
    async fn handle_op(&mut self) {
        tokio::time::sleep(Duration::from_micros(50 + self.rng.below(350))).await;
    }
}

impl Actor for CacheShard {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "cache-shard"
    }

    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(CacheShard { rng: Rng::new() })
    }
}

struct Read;
struct Write;
struct Evict;

impl Message<Read> for CacheShard {
    type Reply = ();
    async fn handle(&mut self, _: Read, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_op().await;
    }
}
impl Message<Write> for CacheShard {
    type Reply = ();
    async fn handle(&mut self, _: Write, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_op().await;
    }
}
impl Message<Evict> for CacheShard {
    type Reply = ();
    async fn handle(&mut self, _: Evict, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_op().await;
    }
}

/// A bounded, deliberately slow writer: it's fed faster than it can drain, so its mailbox fills
/// up and the console shows yellow/red backpressure.
struct DbWriter;

impl Actor for DbWriter {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "db-writer"
    }

    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(DbWriter)
    }
}

struct Insert;

impl Message<Insert> for DbWriter {
    type Reply = ();
    async fn handle(&mut self, _: Insert, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A reporter whose handler takes several seconds: the console shows it as Busy, then Stuck.
struct Reporter;

impl Actor for Reporter {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "reporter"
    }

    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Reporter)
    }
}

struct Build;

impl Message<Build> for Reporter {
    type Reply = ();
    async fn handle(&mut self, _: Build, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        tokio::time::sleep(Duration::from_secs(7)).await;
    }
}

/// Panics on demand; its supervisor restarts it, driving the restart counter and Restarting state.
struct Flaky;

impl Actor for Flaky {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "flaky"
    }

    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Flaky)
    }
}

struct Boom;

impl Message<Boom> for Flaky {
    type Reply = ();
    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("flaky worker fell over");
    }
}

// ---------------------------------------------------------------------------------------------
// Standalone actors
// ---------------------------------------------------------------------------------------------

/// A steady, low-rate background actor.
struct Metrics;

impl Actor for Metrics {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "metrics"
    }

    async fn on_start(_: (), me: ActorRef<Self>) -> Result<Self, Self::Error> {
        drive(
            me,
            Load {
                base: 2.0,
                amp: 1.0,
                period: 17.0,
            },
            |actor, _| async move {
                let _ = actor.tell(Sample).await;
            },
        );
        Ok(Metrics)
    }
}

struct Sample;

impl Message<Sample> for Metrics {
    type Reply = ();
    async fn handle(&mut self, _: Sample, _: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

/// A pair of these are linked as siblings (the `↔` flag), with light random traffic.
struct Session;

impl Actor for Session {
    type Args = ();
    type Error = Infallible;

    fn name() -> &'static str {
        "session"
    }

    async fn on_start(_: (), me: ActorRef<Self>) -> Result<Self, Self::Error> {
        drive(
            me,
            Load {
                base: 0.0,
                amp: 3.0,
                period: 4.0,
            },
            |actor, _| async move {
                let _ = actor.tell(Event).await;
            },
        );
        Ok(Session)
    }
}

struct Event;

impl Message<Event> for Session {
    type Reply = ();
    async fn handle(&mut self, _: Event, _: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

/// Two `Knot`s point at each other and, on `Tangle`, each `ask`s the other while still inside its
/// own handler, so neither can ever reply. The console shows this as a deadlock cycle.
struct Knot {
    peer: Option<ActorRef<Knot>>,
}

impl Actor for Knot {
    type Args = Self;
    type Error = Infallible;

    fn name() -> &'static str {
        "knot"
    }

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct Tie(ActorRef<Knot>);

impl Message<Tie> for Knot {
    type Reply = ();
    async fn handle(&mut self, msg: Tie, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.peer = Some(msg.0);
    }
}

struct Tangle;

impl Message<Tangle> for Knot {
    type Reply = ();
    async fn handle(&mut self, _: Tangle, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(peer) = self.peer.clone() {
            let _ = peer.ask(Tangle).await;
        }
    }
}
