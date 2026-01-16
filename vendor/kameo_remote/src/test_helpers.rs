use bytes::Bytes;
use std::sync::{Mutex, OnceLock};
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};

struct RawCapture {
    messages: Mutex<Vec<Bytes>>,
    notify: Notify,
}

fn raw_capture() -> &'static RawCapture {
    static CAPTURE: OnceLock<RawCapture> = OnceLock::new();
    CAPTURE.get_or_init(|| RawCapture {
        messages: Mutex::new(Vec::new()),
        notify: Notify::new(),
    })
}

pub fn record_raw_payload(payload: Bytes) {
    let capture = raw_capture();
    {
        let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
        guard.push(payload);
    }
    capture.notify.notify_waiters();
}

pub fn drain_raw_payloads() -> Vec<Bytes> {
    let capture = raw_capture();
    let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
    guard.drain(..).collect()
}

pub async fn wait_for_raw_payload(timeout: Duration) -> Option<Bytes> {
    let capture = raw_capture();
    {
        let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
        if let Some(payload) = guard.pop() {
            return Some(payload);
        }
    }

    tokio::select! {
        _ = capture.notify.notified() => {
            let mut guard = capture.messages.lock().expect("raw payload mutex poisoned");
            guard.pop()
        }
        _ = sleep(timeout) => None,
    }
}
