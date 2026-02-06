//! Optional helpers for exactly-once ask semantics.
//!
//! This is an opt-in, application-level facility. It does not change the transport
//! or hot-path send/ask performance. Use it when you want retries with dedup.

use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use thiserror::Error;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

use super::type_hash::{HasTypeHash, TypeHash};

/// 128-bit request identifier used for deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RSerialize, RDeserialize)]
pub struct RequestId(pub u128);

impl RequestId {
    /// Create a request id from a raw u128.
    pub fn from_u128(value: u128) -> Self {
        Self(value)
    }

    /// Best-effort unique request id generator (fast, non-cryptographic).
    ///
    /// Composition: [time_micros:64 | random_nonce:64].
    pub fn next() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let nonce = rand::random::<u64>();
        Self(((now as u128) << 64) | (nonce as u128))
    }
}

/// Wrapper message for exactly-once asks.
///
/// The request id is part of the payload to enable deduplication on the receiver.
#[derive(Debug, Archive, RSerialize, RDeserialize)]
pub struct ExactlyOnce<M> {
    /// Unique request identifier for deduplication.
    pub request_id: RequestId,
    /// Inner message payload.
    pub message: M,
}

// Provide a deterministic type hash for the wrapper without requiring downstream
// crates to implement `HasTypeHash` for a foreign type (orphan rules).
//
// This hash is derived from the inner message hash so that different M yield
// different wrapper hashes.
impl<M> HasTypeHash for ExactlyOnce<M>
where
    M: HasTypeHash,
{
    const TYPE_HASH: TypeHash = TypeHash::from_bytes(b"ExactlyOnce").combine(&M::TYPE_HASH);
}

/// Hash of a request payload for duplicate integrity checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PayloadFingerprint([u8; 32]);

impl PayloadFingerprint {
    /// Compute a fingerprint from raw bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let hash = blake3::hash(bytes);
        Self(*hash.as_bytes())
    }
}

/// Errors that can occur during exactly-once resolution.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExactlyOnceError {
    /// The request id already exists but the payload hash does not match.
    #[error("payload hash mismatch for request id")]
    PayloadMismatch,
}

/// Dedup helper for typed replies (requires `R: Clone`).
#[derive(Debug)]
pub struct ExactlyOnceDedup<R> {
    capacity: usize,
    order: VecDeque<RequestId>,
    cached: HashMap<RequestId, (Option<PayloadFingerprint>, R)>,
}

impl<R> ExactlyOnceDedup<R>
where
    R: Clone,
{
    /// Create a dedup cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::with_capacity(capacity.max(1)),
            cached: HashMap::with_capacity(capacity.max(1)),
        }
    }

    /// Resolve a request id with deduplication.
    ///
    /// If the id was seen, returns the cached reply. Otherwise, computes and stores it.
    pub async fn resolve<F, Fut>(&mut self, id: RequestId, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        if let Some((_, value)) = self.cached.get(&id) {
            return value.clone();
        }

        let value = f().await;
        // Insert before cloning for the return value, so that if `Clone` panics
        // the dedup record still exists for retries.
        self.insert(id, None, value);
        self.cached
            .get(&id)
            .expect("just inserted")
            .1
            .clone()
    }

    /// Resolve with payload fingerprint verification.
    ///
    /// If the same request id is seen with a different fingerprint, returns `PayloadMismatch`.
    pub async fn resolve_checked<F, Fut>(
        &mut self,
        id: RequestId,
        fingerprint: PayloadFingerprint,
        f: F,
    ) -> Result<R, ExactlyOnceError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        if let Some((existing, value)) = self.cached.get(&id) {
            // If a caller requests checked semantics, a previous unchecked insert
            // must not bypass payload integrity checks.
            return match existing {
                Some(existing_fp) if *existing_fp == fingerprint => Ok(value.clone()),
                _ => Err(ExactlyOnceError::PayloadMismatch),
            };
        }

        let value = f().await;
        self.insert(id, Some(fingerprint), value);
        Ok(self
            .cached
            .get(&id)
            .expect("just inserted")
            .1
            .clone())
    }

    fn insert(&mut self, id: RequestId, fingerprint: Option<PayloadFingerprint>, value: R) {
        if self.cached.contains_key(&id) {
            return;
        }
        if self.cached.len() >= self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.cached.remove(&evicted);
            }
        }
        self.order.push_back(id);
        self.cached.insert(id, (fingerprint, value));
    }
}

/// Zero-copy dedup helper for replies already in `Bytes`.
#[derive(Debug)]
pub struct ExactlyOnceBytesDedup {
    capacity: usize,
    order: VecDeque<RequestId>,
    cached: HashMap<RequestId, (Option<PayloadFingerprint>, Bytes)>,
}

impl ExactlyOnceBytesDedup {
    /// Create a dedup cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::with_capacity(capacity.max(1)),
            cached: HashMap::with_capacity(capacity.max(1)),
        }
    }

    /// Resolve a request id with deduplication (zero-copy on cached replies).
    pub async fn resolve<F, Fut>(&mut self, id: RequestId, f: F) -> Bytes
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Bytes>,
    {
        if let Some((_, value)) = self.cached.get(&id) {
            return value.clone();
        }

        let value = f().await;
        self.insert(id, None, value);
        self.cached
            .get(&id)
            .expect("just inserted")
            .1
            .clone()
    }

    /// Resolve with payload fingerprint verification.
    ///
    /// If the same request id is seen with a different fingerprint, returns `PayloadMismatch`.
    pub async fn resolve_checked<F, Fut>(
        &mut self,
        id: RequestId,
        fingerprint: PayloadFingerprint,
        f: F,
    ) -> Result<Bytes, ExactlyOnceError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Bytes>,
    {
        if let Some((existing, value)) = self.cached.get(&id) {
            return match existing {
                Some(existing_fp) if *existing_fp == fingerprint => Ok(value.clone()),
                _ => Err(ExactlyOnceError::PayloadMismatch),
            };
        }

        let value = f().await;
        self.insert(id, Some(fingerprint), value);
        Ok(self
            .cached
            .get(&id)
            .expect("just inserted")
            .1
            .clone())
    }

    fn insert(&mut self, id: RequestId, fingerprint: Option<PayloadFingerprint>, value: Bytes) {
        if self.cached.contains_key(&id) {
            return;
        }
        if self.cached.len() >= self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.cached.remove(&evicted);
            }
        }
        self.order.push_back(id);
        self.cached.insert(id, (fingerprint, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn request_id_next_unique() {
        let a = RequestId::next();
        let b = RequestId::next();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn request_id_next_many_unique() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for _ in 0..10_000 {
            let id = RequestId::next();
            assert!(seen.insert(id));
        }
    }

    #[tokio::test]
    async fn dedup_returns_cached_value() {
        let mut dedup = ExactlyOnceDedup::<u32>::new(8);
        let calls = AtomicUsize::new(0);
        let id = RequestId::from_u128(42);

        let v1 = dedup
            .resolve(id, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                123
            })
            .await;
        let v2 = dedup
            .resolve(id, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                999
            })
            .await;

        assert_eq!(v1, 123);
        assert_eq!(v2, 123);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn dedup_different_ids_call_twice() {
        let mut dedup = ExactlyOnceDedup::<u32>::new(8);
        let calls = AtomicUsize::new(0);

        let a = dedup
            .resolve(RequestId::from_u128(1), || async {
                calls.fetch_add(1, Ordering::Relaxed);
                1
            })
            .await;
        let b = dedup
            .resolve(RequestId::from_u128(2), || async {
                calls.fetch_add(1, Ordering::Relaxed);
                2
            })
            .await;

        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn dedup_eviction_recomputes() {
        let mut dedup = ExactlyOnceDedup::<u32>::new(1);
        let calls = AtomicUsize::new(0);
        let id1 = RequestId::from_u128(10);
        let id2 = RequestId::from_u128(11);

        let v1 = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                100
            })
            .await;
        let v2 = dedup
            .resolve(id2, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                200
            })
            .await;
        let v1_again = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                300
            })
            .await;

        assert_eq!(v1, 100);
        assert_eq!(v2, 200);
        assert_eq!(v1_again, 300);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn dedup_capacity_zero_behaves_as_one() {
        let mut dedup = ExactlyOnceDedup::<u32>::new(0);
        let calls = AtomicUsize::new(0);
        let id1 = RequestId::from_u128(1);
        let id2 = RequestId::from_u128(2);

        let _ = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                10
            })
            .await;
        let _ = dedup
            .resolve(id2, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                20
            })
            .await;
        let v1_again = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                30
            })
            .await;

        assert_eq!(v1_again, 30);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert!(dedup.cached.len() <= 1);
    }

    #[tokio::test]
    async fn dedup_fifo_eviction_order() {
        let mut dedup = ExactlyOnceDedup::<u32>::new(2);
        let calls = AtomicUsize::new(0);
        let id1 = RequestId::from_u128(1);
        let id2 = RequestId::from_u128(2);
        let id3 = RequestId::from_u128(3);

        let _ = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                1
            })
            .await;
        let _ = dedup
            .resolve(id2, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                2
            })
            .await;
        let _ = dedup
            .resolve(id3, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                3
            })
            .await;

        let v2_again = dedup
            .resolve(id2, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                22
            })
            .await;
        let v1_again = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                11
            })
            .await;

        assert_eq!(v2_again, 2);
        assert_eq!(v1_again, 11);
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn dedup_caches_error_results() {
        let mut dedup = ExactlyOnceDedup::<Result<u32, &'static str>>::new(8);
        let calls = AtomicUsize::new(0);
        let id = RequestId::from_u128(77);

        let v1 = dedup
            .resolve(id, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err("fail")
            })
            .await;
        let v2 = dedup
            .resolve(id, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(1)
            })
            .await;

        assert_eq!(v1, Err("fail"));
        assert_eq!(v2, Err("fail"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn dedup_rejects_payload_mismatch() {
        let mut dedup = ExactlyOnceDedup::<u32>::new(8);
        let id = RequestId::from_u128(99);
        let a = PayloadFingerprint::from_bytes(b"first");
        let b = PayloadFingerprint::from_bytes(b"second");

        let v1 = dedup
            .resolve_checked(id, a, || async { 10 })
            .await
            .expect("first");
        let err = dedup
            .resolve_checked(id, b, || async { 20 })
            .await
            .expect_err("mismatch");

        assert_eq!(v1, 10);
        assert_eq!(err, ExactlyOnceError::PayloadMismatch);
    }

    #[tokio::test]
    async fn dedup_checked_rejects_unchecked_cache_poison() {
        let mut dedup = ExactlyOnceDedup::<u32>::new(8);
        let id = RequestId::from_u128(123);

        let v1 = dedup.resolve(id, || async { 7 }).await;
        assert_eq!(v1, 7);

        // Previously: unchecked insert stored `(None, value)` which bypassed fingerprint checks.
        let fp = PayloadFingerprint::from_bytes(b"malicious");
        let err = dedup
            .resolve_checked(id, fp, || async { 999 })
            .await
            .expect_err("must not bypass fingerprint on None");
        assert_eq!(err, ExactlyOnceError::PayloadMismatch);
    }

    #[tokio::test]
    async fn dedup_inserts_before_clone_to_survive_clone_panic() {
        #[derive(Debug)]
        struct PanicOnFirstClone {
            value: u32,
            clones: Arc<AtomicUsize>,
        }

        impl Clone for PanicOnFirstClone {
            fn clone(&self) -> Self {
                let n = self.clones.fetch_add(1, Ordering::AcqRel);
                if n == 0 {
                    panic!("boom");
                }
                Self {
                    value: self.value,
                    clones: self.clones.clone(),
                }
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let clones = Arc::new(AtomicUsize::new(0));
        let mut dedup = ExactlyOnceDedup::<PanicOnFirstClone>::new(8);
        let id = RequestId::from_u128(55);

        let calls2 = calls.clone();
        let clones2 = clones.clone();
        let first = std::panic::AssertUnwindSafe(async {
            dedup.resolve(id, || async move {
                calls2.fetch_add(1, Ordering::AcqRel);
                PanicOnFirstClone {
                    value: 10,
                    clones: clones2,
                }
            })
            .await
        })
        .catch_unwind()
        .await;

        assert!(first.is_err(), "expected clone panic on first return");
        assert!(
            dedup.cached.contains_key(&id),
            "dedup record must exist even if Clone panics"
        );

        // Retry should be a cache hit and must not re-run `f`.
        let v2 = dedup
            .resolve(id, || async {
                calls.fetch_add(1, Ordering::AcqRel);
                PanicOnFirstClone {
                    value: 999,
                    clones: clones.clone(),
                }
            })
            .await;
        assert_eq!(v2.value, 10);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(
            clones.load(Ordering::Acquire) >= 2,
            "expected second clone for cache hit"
        );
    }

    #[tokio::test]
    async fn rkyv_roundtrip_exactly_once() {
        #[derive(Debug, Archive, RSerialize, RDeserialize, PartialEq)]
        struct Msg {
            a: u32,
            b: u64,
        }

        let msg = ExactlyOnce {
            request_id: RequestId::from_u128(0xdead_beef),
            message: Msg { a: 7, b: 9 },
        };

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();
        let decoded: ExactlyOnce<Msg> =
            rkyv::from_bytes::<ExactlyOnce<Msg>, rkyv::rancor::Error>(&bytes).unwrap();

        assert_eq!(decoded.request_id, msg.request_id);
        assert_eq!(decoded.message, msg.message);
    }

    #[tokio::test]
    async fn bytes_dedup_is_zero_copy_on_hit() {
        let mut dedup = ExactlyOnceBytesDedup::new(8);
        let calls = AtomicUsize::new(0);
        let id = RequestId::from_u128(7);

        let v1 = dedup
            .resolve(id, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Bytes::from_static(b"hello")
            })
            .await;
        let v2 = dedup
            .resolve(id, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Bytes::from_static(b"world")
            })
            .await;

        assert_eq!(v1, Bytes::from_static(b"hello"));
        assert_eq!(v2, Bytes::from_static(b"hello"));
        assert_eq!(v1.as_ptr(), v2.as_ptr());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn bytes_dedup_eviction() {
        let mut dedup = ExactlyOnceBytesDedup::new(0);
        let calls = AtomicUsize::new(0);
        let id1 = RequestId::from_u128(1);
        let id2 = RequestId::from_u128(2);

        let _ = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Bytes::from_static(b"a")
            })
            .await;
        let _ = dedup
            .resolve(id2, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Bytes::from_static(b"b")
            })
            .await;
        let v1_again = dedup
            .resolve(id1, || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Bytes::from_static(b"c")
            })
            .await;

        assert_eq!(v1_again, Bytes::from_static(b"c"));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn bytes_dedup_rejects_payload_mismatch() {
        let mut dedup = ExactlyOnceBytesDedup::new(8);
        let id = RequestId::from_u128(101);
        let a = PayloadFingerprint::from_bytes(b"alpha");
        let b = PayloadFingerprint::from_bytes(b"beta");

        let v1 = dedup
            .resolve_checked(id, a, || async { Bytes::from_static(b"ok") })
            .await
            .expect("first");
        let err = dedup
            .resolve_checked(id, b, || async { Bytes::from_static(b"nope") })
            .await
            .expect_err("mismatch");

        assert_eq!(v1, Bytes::from_static(b"ok"));
        assert_eq!(err, ExactlyOnceError::PayloadMismatch);
    }

    #[tokio::test]
    async fn bytes_dedup_checked_rejects_unchecked_cache_poison() {
        let mut dedup = ExactlyOnceBytesDedup::new(8);
        let id = RequestId::from_u128(202);

        let v1 = dedup.resolve(id, || async { Bytes::from_static(b"ok") }).await;
        assert_eq!(v1, Bytes::from_static(b"ok"));

        let fp = PayloadFingerprint::from_bytes(b"tamper");
        let err = dedup
            .resolve_checked(id, fp, || async { Bytes::from_static(b"nope") })
            .await
            .expect_err("must not bypass fingerprint on None");
        assert_eq!(err, ExactlyOnceError::PayloadMismatch);
    }
}
