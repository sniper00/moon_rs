//! Shared request-pool helpers for the DB-backed native modules (sqlx,
//! mongodb, pg, redis). Which items are exercised depends on the enabled
//! feature set, so individual members are legitimately unused in some build
//! configurations — allow dead code module-wide rather than annotating each.
#![allow(dead_code)]

use moon_base::laux::{LuaState, LuaTable};
use moon_runtime::context::ActorId;
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};
use tokio::sync::mpsc;

/// Per-worker request accounting shared between the Lua/actor thread (which
/// enqueues) and the worker task (which dequeues). Tracks the live in-flight
/// count plus lifetime totals and a high-water mark for `stats()`.
struct CounterInner {
    /// Requests dispatched but not yet replied to (current backpressure).
    pending: AtomicI64,
    /// Cumulative number of requests ever dispatched (monotonic).
    total: AtomicI64,
    /// Highest `pending` value ever observed (monotonic high-water mark).
    peak: AtomicI64,
}

#[derive(Clone)]
pub(crate) struct PendingCounter {
    inner: Arc<CounterInner>,
}

impl PendingCounter {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(CounterInner {
                pending: AtomicI64::new(0),
                total: AtomicI64::new(0),
                peak: AtomicI64::new(0),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_value(value: i64) -> Self {
        Self {
            inner: Arc::new(CounterInner {
                pending: AtomicI64::new(value),
                total: AtomicI64::new(value),
                peak: AtomicI64::new(value),
            }),
        }
    }

    pub(crate) fn inc(&self) {
        let pending = self.inner.pending.fetch_add(1, Ordering::Release) + 1;
        self.inner.total.fetch_add(1, Ordering::Release);
        // Best-effort high-water mark: bump `peak` up to the new pending value.
        let mut peak = self.inner.peak.load(Ordering::Relaxed);
        while pending > peak {
            match self.inner.peak.compare_exchange_weak(
                peak,
                pending,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => peak = observed,
            }
        }
    }

    pub(crate) fn dec(&self) {
        self.inner.pending.fetch_sub(1, Ordering::Release);
    }

    pub(crate) fn load(&self) -> i64 {
        self.inner.pending.load(Ordering::Acquire)
    }

    /// Cumulative requests ever dispatched on this counter.
    pub(crate) fn total(&self) -> i64 {
        self.inner.total.load(Ordering::Acquire)
    }

    /// Highest simultaneous `pending` ever observed on this counter.
    pub(crate) fn peak(&self) -> i64 {
        self.inner.peak.load(Ordering::Acquire)
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Build a per-connection stats table and leave it on top of the Lua stack.
///
/// Shared by every DB-backed module's `stats()` so the shape stays consistent:
/// `{ pending, total, peak, workers }`. For pooled drivers (redis/pg) the
/// values are summed across workers, so `peak` is the sum of per-worker
/// high-water marks (an upper bound on true simultaneous peak).
pub(crate) fn push_pool_stats(
    state: LuaState,
    pending: i64,
    total: i64,
    peak: i64,
    workers: i64,
) {
    let t = LuaTable::new(state, 0, 4);
    t.insert("pending", pending);
    t.insert("total", total);
    t.insert("peak", peak);
    t.insert("workers", workers);
}

impl Default for PendingCounter {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) trait QueuedRequest {
    fn owner_session(&self) -> Option<(ActorId, i64)>;
}

pub(crate) fn drain_queued_requests<M, F>(
    rx: &mut mpsc::Receiver<M>,
    counter: &PendingCounter,
    mut fail_waiting: F,
) where
    M: QueuedRequest,
    F: FnMut(ActorId, i64),
{
    while let Ok(queued) = rx.try_recv() {
        if let Some((owner, session)) = queued.owner_session() {
            if session != 0 {
                fail_waiting(owner, session);
            }
            counter.dec();
        }
    }
}

pub(crate) struct WorkerHandle<M> {
    tx: mpsc::Sender<M>,
    counter: PendingCounter,
}

impl<M> WorkerHandle<M> {
    pub(crate) fn new(tx: mpsc::Sender<M>, counter: PendingCounter) -> Self {
        Self { tx, counter }
    }

    pub(crate) fn tx(&self) -> &mpsc::Sender<M> {
        &self.tx
    }

    pub(crate) fn counter(&self) -> &PendingCounter {
        &self.counter
    }
}

pub(crate) struct WorkerSet<M> {
    name: String,
    workers: Vec<WorkerHandle<M>>,
    next: AtomicUsize,
}

impl<M> WorkerSet<M> {
    pub(crate) fn new(name: String, workers: Vec<WorkerHandle<M>>) -> Self {
        Self {
            name,
            workers,
            next: AtomicUsize::new(0),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn workers(&self) -> &[WorkerHandle<M>] {
        &self.workers
    }

    pub(crate) fn dispatch(&self, msg: M) -> Result<(), String> {
        let n = self.workers.len();
        if n == 0 {
            return Err("request pool has no workers".to_string());
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % n;
        let worker = &self.workers[idx];
        match worker.tx.try_send(msg) {
            Ok(()) => {
                worker.counter.inc();
                Ok(())
            }
            Err(err) => Err(format!("{}: failed to send message to worker: {}", self.name, err)),
        }
    }

    pub(crate) fn pending(&self) -> i64 {
        self.workers.iter().map(|w| w.counter.load()).sum()
    }

    /// Cumulative requests dispatched across all workers (lifetime).
    pub(crate) fn total(&self) -> i64 {
        self.workers.iter().map(|w| w.counter.total()).sum()
    }

    /// Sum of per-worker pending high-water marks.
    pub(crate) fn peak(&self) -> i64 {
        self.workers.iter().map(|w| w.counter.peak()).sum()
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestRequest {
        owner: ActorId,
        session: i64,
    }

    enum TestMessage {
        Request(TestRequest),
        Shutdown,
    }

    impl QueuedRequest for TestMessage {
        fn owner_session(&self) -> Option<(ActorId, i64)> {
            match self {
                TestMessage::Request(req) => Some((req.owner, req.session)),
                TestMessage::Shutdown => None,
            }
        }
    }

    #[test]
    fn worker_set_round_robin_dispatch_counts_pending() {
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        let c1 = PendingCounter::new();
        let c2 = PendingCounter::new();
        let pool = WorkerSet::new(
            "test".to_string(),
            vec![
                WorkerHandle::new(tx1, c1.clone()),
                WorkerHandle::new(tx2, c2.clone()),
            ],
        );

        pool.dispatch(TestMessage::Request(TestRequest {
            owner: 1,
            session: 1,
        }))
        .unwrap();
        pool.dispatch(TestMessage::Request(TestRequest {
            owner: 1,
            session: 2,
        }))
        .unwrap();

        assert_eq!(c1.load(), 1);
        assert_eq!(c2.load(), 1);
        assert_eq!(pool.pending(), 2);
        assert!(matches!(rx1.try_recv(), Ok(TestMessage::Request(_))));
        assert!(matches!(rx2.try_recv(), Ok(TestMessage::Request(_))));
    }

    #[test]
    fn worker_set_queue_full_does_not_increment_pending() {
        let (tx, _rx) = mpsc::channel(1);
        let counter = PendingCounter::new();
        let pool = WorkerSet::new(
            "test".to_string(),
            vec![WorkerHandle::new(tx, counter.clone())],
        );

        pool.dispatch(TestMessage::Request(TestRequest {
            owner: 1,
            session: 1,
        }))
        .unwrap();
        assert!(
            pool.dispatch(TestMessage::Request(TestRequest {
                owner: 1,
                session: 2,
            }))
            .is_err()
        );

        assert_eq!(counter.load(), 1);
    }

    #[test]
    fn drain_queued_requests_replies_only_waiting_and_decrements_all() {
        let (tx, mut rx) = mpsc::channel(8);
        let counter = PendingCounter::new();
        for session in [11, 0, 12] {
            tx.try_send(TestMessage::Request(TestRequest { owner: 7, session }))
                .unwrap();
            counter.inc();
        }
        tx.try_send(TestMessage::Shutdown).unwrap();

        let mut failed = Vec::new();
        drain_queued_requests(&mut rx, &counter, |owner, session| {
            failed.push((owner, session));
        });

        assert_eq!(failed, vec![(7, 11), (7, 12)]);
        assert_eq!(counter.load(), 0);
    }

    #[test]
    fn pending_counter_identity_guard() {
        let a = PendingCounter::new();
        let b = a.clone();
        let c = PendingCounter::new();

        assert!(a.ptr_eq(&b));
        assert!(!a.ptr_eq(&c));
    }
}
