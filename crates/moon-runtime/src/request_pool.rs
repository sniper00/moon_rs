//! Shared request-pool helpers for the DB-backed native modules (sqlx,
//! mongodb, pg, redis). Which items are exercised depends on the enabled
//! feature set, so individual members are legitimately unused in some build
//! configurations — allow dead code module-wide rather than annotating each.
#![allow(dead_code)]

use moon_base::laux::{self, LuaState, LuaTable};
use moon_runtime::context::{ActorId, CONTEXT};
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

    /// Track one already-counted request until processing (including retries
    /// and streaming) ends. Dropping the worker future also releases its count.
    pub(crate) fn finish_on_drop(&self) -> PendingRequest<'_> {
        PendingRequest(self)
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

#[must_use]
pub(crate) struct PendingRequest<'a>(&'a PendingCounter);

impl Drop for PendingRequest<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Reserve capacity before counting, then publish only after accounting.
/// Failed sends leave all counters unchanged.
pub(crate) fn try_send_counted<M>(
    tx: &mpsc::Sender<M>,
    counter: &PendingCounter,
    msg: M,
) -> Result<(), mpsc::error::TrySendError<M>> {
    let permit = match tx.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(_)) => {
            return Err(mpsc::error::TrySendError::Full(msg));
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            return Err(mpsc::error::TrySendError::Closed(msg));
        }
    };
    counter.inc();
    permit.send(msg);
    Ok(())
}

/// Shared SQLx/MongoDB Lua contract: session for a waiting request, true for
/// fire-and-forget/control messages, or { kind = "ERROR", message = ... }.
pub(crate) fn dispatch_request<M: QueuedRequest>(
    state: LuaState,
    tx: &mpsc::Sender<M>,
    counter: &PendingCounter,
    msg: M,
) -> i32 {
    let owner_session = msg.owner_session();
    let result = if owner_session.is_some() {
        try_send_counted(tx, counter, msg)
    } else {
        tx.try_send(msg)
    };
    match result {
        Ok(()) => match owner_session {
            Some((_, session)) if session != 0 => laux::lua_push(state, session),
            _ => laux::lua_push(state, true),
        },
        Err(err) => {
            LuaTable::new(state, 0, 2)
                .insert("kind", "ERROR")
                .insert("message", err.to_string());
        }
    }
    1
}

/// Best-effort shutdown notification without blocking the caller on a full
/// queue. Control messages never contribute to request counters.
pub(crate) fn notify_shutdown<M: Send + 'static>(tx: &mpsc::Sender<M>, msg: M) {
    if let Err(mpsc::error::TrySendError::Full(msg)) = tx.try_send(msg) {
        let tx = tx.clone();
        CONTEXT.io_runtime().spawn(async move {
            let _ = tx.send(msg).await;
        });
    }
}

/// Build a per-connection stats table and leave it on top of the Lua stack.
///
/// Shared by every DB-backed module's `stats()` so the shape stays consistent:
/// `{ pending, total, peak, workers }`. For pooled drivers (redis/pg) the
/// values are summed across workers, so `peak` is the sum of per-worker
/// high-water marks (an upper bound on true simultaneous peak).
pub(crate) fn push_pool_stats(state: LuaState, pending: i64, total: i64, peak: i64, workers: i64) {
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

pub(crate) async fn drain_queued_requests<M, F>(
    rx: &mut mpsc::Receiver<M>,
    counter: &PendingCounter,
    mut fail_waiting: F,
) where
    M: QueuedRequest,
    F: FnMut(ActorId, i64),
{
    // Closing rejects new reservations. recv() also waits for permits issued
    // before close, so a concurrent accepted request cannot lose its reply.
    rx.close();
    while let Some(queued) = rx.recv().await {
        if let Some((owner, session)) = queued.owner_session() {
            let _pending = counter.finish_on_drop();
            if session != 0 {
                fail_waiting(owner, session);
            }
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
        match try_send_counted(&worker.tx, &worker.counter, msg) {
            Ok(()) => Ok(()),
            Err(err) => Err(format!(
                "{}: failed to send message to worker: {}",
                self.name, err
            )),
        }
    }

    pub(crate) fn notify_shutdown(&self, make_message: impl Fn() -> M)
    where
        M: Send + 'static,
    {
        for worker in &self.workers {
            notify_shutdown(&worker.tx, make_message());
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
    fn lua_dispatch_preserves_results_and_counts_only_requests() {
        let state = laux::LuaState::new(unsafe { moon_base::ffi::luaL_newstate() }).unwrap();
        let _owner = laux::LuaGlobalState::new(state);
        let mut lua = unsafe { laux::LuaStack::from_raw(state) };
        let (tx, mut rx) = mpsc::channel(1);
        let counter = PendingCounter::new();
        for session in [17, 0] {
            lua.set_top(0);
            assert_eq!(
                dispatch_request(
                    state,
                    &tx,
                    &counter,
                    TestMessage::Request(TestRequest { owner: 1, session })
                ),
                1
            );
            if session == 0 {
                assert!(lua.get::<bool>(-1).unwrap());
            } else {
                assert_eq!(lua.get::<i64>(-1).unwrap(), session);
            }
            assert_eq!(counter.load(), 1);
            assert!(matches!(rx.try_recv(), Ok(TestMessage::Request(_))));
            drop(counter.finish_on_drop());
        }
        lua.set_top(0);
        dispatch_request(state, &tx, &counter, TestMessage::Shutdown);
        assert!(lua.get::<bool>(-1).unwrap());
        assert_eq!((counter.load(), counter.total()), (0, 2));

        // A full queue must preserve the existing error table and counters.
        lua.set_top(0);
        dispatch_request(
            state,
            &tx,
            &counter,
            TestMessage::Request(TestRequest {
                owner: 1,
                session: 18,
            }),
        );
        assert_eq!(
            lua.opt_field::<String>(-1, "kind").as_deref(),
            Some("ERROR")
        );
        assert!(
            lua.opt_field::<String>(-1, "message")
                .unwrap()
                .contains("no available capacity")
        );
        assert_eq!((counter.load(), counter.total()), (0, 2));
        drop(rx);
        lua.set_top(0);
        dispatch_request(state, &tx, &counter, TestMessage::Shutdown);
        assert_eq!(
            lua.opt_field::<String>(-1, "kind").as_deref(),
            Some("ERROR")
        );
        assert!(
            lua.opt_field::<String>(-1, "message")
                .unwrap()
                .contains("closed")
        );
    }

    #[tokio::test]
    async fn cancelled_processing_releases_its_pending_count() {
        let counter = PendingCounter::with_value(1);
        let worker_counter = counter.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _pending = worker_counter.finish_on_drop();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        assert_eq!(counter.load(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!((counter.load(), counter.total()), (0, 1));
    }

    #[tokio::test]
    async fn shutdown_reaches_idle_workers_while_another_queue_is_full() {
        let (tx1, mut rx1) = mpsc::channel(1);
        let (tx2, mut rx2) = mpsc::channel(1);
        let pool = WorkerSet::new(
            "shutdown".into(),
            vec![
                WorkerHandle::new(tx1, PendingCounter::new()),
                WorkerHandle::new(tx2, PendingCounter::new()),
            ],
        );
        pool.dispatch(TestMessage::Request(TestRequest {
            owner: 1,
            session: 1,
        }))
        .unwrap();
        pool.notify_shutdown(|| TestMessage::Shutdown);
        assert!(matches!(rx2.try_recv(), Ok(TestMessage::Shutdown)));
        assert!(matches!(rx1.try_recv(), Ok(TestMessage::Request(_))));
        drop(pool.workers()[0].counter().finish_on_drop());
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), rx1.recv())
                .await
                .unwrap(),
            Some(TestMessage::Shutdown)
        ));
        assert_eq!((pool.pending(), pool.total()), (0, 1));
    }

    #[tokio::test]
    async fn shutdown_drain_waits_for_previously_reserved_requests() {
        let (tx, mut rx) = mpsc::channel(1);
        let counter = PendingCounter::new();
        let permit = tx.try_reserve().unwrap();
        counter.inc();
        let mut failed = Vec::new();
        {
            let drain = drain_queued_requests(&mut rx, &counter, |owner, session| {
                failed.push((owner, session))
            });
            tokio::pin!(drain);
            tokio::select! {
                biased;
                _ = &mut drain => panic!("reserved request was skipped"),
                _ = tokio::task::yield_now() => {}
            }
            assert!(tx.is_closed());
            assert!(tx.try_reserve().is_err());
            permit.send(TestMessage::Request(TestRequest {
                owner: 7,
                session: 21,
            }));
            tokio::time::timeout(std::time::Duration::from_secs(2), drain)
                .await
                .unwrap();
        }
        assert_eq!(failed, vec![(7, 21)]);
        assert_eq!(counter.load(), 0);
    }

    #[test]
    fn counted_send_is_visible_before_worker_completion() {
        let (tx, mut rx) = mpsc::channel(1);
        let counter = PendingCounter::new();
        let worker_counter = counter.clone();
        let worker = std::thread::spawn(move || {
            while rx.blocking_recv().is_some() {
                assert!(worker_counter.load() > 0);
                worker_counter.dec();
            }
        });
        for _ in 0..10_000 {
            loop {
                match try_send_counted(&tx, &counter, ()) {
                    Ok(()) => break,
                    Err(mpsc::error::TrySendError::Full(_)) => std::thread::yield_now(),
                    Err(err) => panic!("{err}"),
                }
            }
        }
        drop(tx);
        worker.join().unwrap();
        assert_eq!(counter.load(), 0);
        assert_eq!(counter.total(), 10_000);
        assert!(counter.peak() >= 1);
    }

    #[test]
    fn closed_queue_does_not_change_counts() {
        let (tx, rx) = mpsc::channel(1);
        let counter = PendingCounter::new();
        drop(rx);
        assert!(try_send_counted(&tx, &counter, ()).is_err());
        assert_eq!((counter.load(), counter.total(), counter.peak()), (0, 0, 0));
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

        assert_eq!((counter.load(), counter.total(), counter.peak()), (1, 1, 1));
    }

    #[tokio::test]
    async fn drain_queued_requests_replies_only_waiting_and_decrements_all() {
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
        })
        .await;

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
