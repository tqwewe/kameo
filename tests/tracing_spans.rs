//! Regression tests for https://github.com/tqwewe/kameo/issues/323
//!
//! When an actor sends messages that loop back to it (a plain self-`tell`, or a delayed
//! `send_after`), each `actor.handle_message` span used to be created as a child of the
//! sending handler's span. Over a loop this nested the spans without bound, producing log
//! lines like `actor.handle_message:actor.handle_message:actor.handle_message:...`.
//!
//! Every `actor.handle_message` span must instead be a direct child of the actor's
//! `actor.lifecycle` span, so the depth stays constant regardless of how messages chain.

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

/// Records, for every span as it is created, its name and its parent span's name.
#[derive(Clone, Default)]
struct SpanCapture {
    records: Arc<Mutex<Vec<SpanRecord>>>,
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
}

impl SpanCapture {
    /// Asserts no `actor.handle_message` span is nested under another, and that every one is a
    /// direct child of `actor.lifecycle`.
    fn assert_no_handle_message_nesting(&self) {
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
            assert_ne!(
                *parent,
                Some("actor.handle_message"),
                "actor.handle_message span nested under another actor.handle_message span"
            );
            assert_eq!(
                *parent,
                Some("actor.lifecycle"),
                "actor.handle_message span should be a child of actor.lifecycle, got parent {parent:?}"
            );
        }
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
    capture.assert_no_handle_message_nesting();
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_spans_do_not_nest_on_send_after() {
    let capture = run_loop(true).await;
    capture.assert_no_handle_message_nesting();
}
