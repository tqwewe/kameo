use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use kameo::prelude::*;
use kameo_actors::future::FutureActor;
use tokio::sync::oneshot;

#[tokio::test]
async fn runs_future_to_completion() {
    let ran = Arc::new(AtomicBool::new(false));
    let actor_ref = FutureActor::spawn({
        let ran = ran.clone();
        async move {
            ran.store(true, Ordering::SeqCst);
        }
    });

    let reason = actor_ref.wait_for_shutdown_result().await.unwrap();

    assert!(ran.load(Ordering::SeqCst), "the future should have run");
    assert!(matches!(reason, ActorStopReason::Normal));
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestError;

#[tokio::test]
async fn err_output_stops_with_error() {
    let actor_ref = FutureActor::spawn(async { Err::<(), _>(TestError) });

    let reason = actor_ref.wait_for_shutdown_result().await.unwrap();

    let ActorStopReason::Panicked(err) = reason else {
        panic!("expected Panicked, got {reason:?}");
    };
    assert_eq!(err.downcast::<TestError>(), Some(TestError));
}

#[tokio::test]
async fn stopping_cancels_pending_future() {
    struct Guard(Option<oneshot::Sender<()>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }

    let (tx, rx) = oneshot::channel();
    let guard = Guard(Some(tx));
    let actor_ref = FutureActor::spawn(async move {
        let _guard = guard;
        pending::<()>().await
    });

    actor_ref.stop_gracefully().await.unwrap();

    // Stopping the actor drops the future, which drops the guard and fires the channel.
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("future was not cancelled when the actor stopped")
        .unwrap();
}
