//! Regression tests for https://github.com/tqwewe/kameo/issues/323 and
//! https://github.com/tqwewe/kameo/issues/381
//!
//! `actor.handle_message` spans used to be children of the sending handler's span (#323), which
//! nested without bound on self-send loops, and later children of `actor.lifecycle` (#381), which
//! accumulated every message of a long-lived actor into one giant trace.
//!
//! Every `actor.handle_message` span must instead be a root span (its own trace), connected to
//! `actor.lifecycle` via a follows-from link.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use kameo::error::Infallible;
use kameo::prelude::*;
use tokio::sync::mpsc;
use tracing::span;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// A span's name paired with its parent span's name.
type SpanRecord = (&'static str, Option<&'static str>);

/// A span's name paired with the name of a span it follows from.
type FollowsRecord = (&'static str, &'static str);

/// Records every span's name and parent name on creation, and every follows-from link.
#[derive(Clone, Default)]
struct SpanCapture {
    records: Arc<Mutex<Vec<SpanRecord>>>,
    follows: Arc<Mutex<Vec<FollowsRecord>>>,
}

impl<S> Layer<S> for SpanCapture
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let parent = span.parent().map(|parent| parent.name());
            self.records.lock().unwrap().push((span.name(), parent));
        }
    }

    fn on_follows_from(&self, id: &span::Id, follows: &span::Id, ctx: LayerContext<'_, S>) {
        if let (Some(span), Some(follows)) = (ctx.span(id), ctx.span(follows)) {
            self.follows
                .lock()
                .unwrap()
                .push((span.name(), follows.name()));
        }
    }
}

impl SpanCapture {
    /// Asserts every `actor.handle_message` span is a root span (no parent) that follows from
    /// `actor.lifecycle`.
    fn assert_handle_message_spans_are_roots(&self) {
        let records = self.records.lock().unwrap();

        let handle_messages = records
            .iter()
            .filter(|(name, _)| *name == "actor.handle_message")
            .count();
        assert!(
            handle_messages > 0,
            "expected some actor.handle_message spans to be recorded, got: {records:?}"
        );

        for (name, parent) in records.iter() {
            if *name != "actor.handle_message" {
                continue;
            }
            assert_eq!(
                *parent, None,
                "actor.handle_message span should be a root span, got parent {parent:?}"
            );
        }

        let follows = self.follows.lock().unwrap();
        let lifecycle_links = follows
            .iter()
            .filter(|(name, from)| *name == "actor.handle_message" && *from == "actor.lifecycle")
            .count();
        assert_eq!(
            lifecycle_links, handle_messages,
            "every actor.handle_message span should follow from actor.lifecycle, got: {follows:?}"
        );
    }
}

// ==================== Looper actor ====================

struct Looper {
    self_ref: Option<ActorRef<Looper>>,
    done_tx: mpsc::UnboundedSender<()>,
    use_send_after: bool,
}

impl Actor for Looper {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut this: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        this.self_ref = Some(actor_ref);
        Ok(this)
    }
}

/// Drives `n` more iterations, each looping a message back to the actor itself.
struct Tick(usize);

impl Message<Tick> for Looper {
    type Reply = ();

    async fn handle(
        &mut self,
        Tick(n): Tick,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if n == 0 {
            let _ = self.done_tx.send(());
            return;
        }

        let self_ref = self.self_ref.clone().unwrap();
        if self.use_send_after {
            self_ref
                .tell(Tick(n - 1))
                .send_after(Duration::from_millis(0));
        } else {
            self_ref.tell(Tick(n - 1)).send().await.unwrap();
        }
    }
}

const ITERATIONS: usize = 20;

async fn run_loop(use_send_after: bool) -> SpanCapture {
    let capture = SpanCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let actor = Looper::spawn(Looper {
        self_ref: None,
        done_tx,
        use_send_after,
    });

    actor.tell(Tick(ITERATIONS)).send().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), done_rx.recv())
        .await
        .expect("timed out waiting for the loop to finish")
        .expect("done channel closed before finishing");

    actor.kill();
    actor.wait_for_shutdown().await;

    capture
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_spans_do_not_nest_on_self_tell() {
    let capture = run_loop(false).await;
    capture.assert_handle_message_spans_are_roots();
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_spans_do_not_nest_on_send_after() {
    let capture = run_loop(true).await;
    capture.assert_handle_message_spans_are_roots();
}
