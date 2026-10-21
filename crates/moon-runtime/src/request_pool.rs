//! Shared request-pool helpers for the DB-backed native modules (sqlx,
//! mongodb, pg, redis). Which items are exercised depends on the enabled
//! feature set, so individual members are legitimately unused in some build
//! configurations — allow dead code module-wide rather than annotating each.
#![allow(dead_code)]

use moon_lua_sys::laux::{self, LuaState, LuaTable};
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

    pub(crate) fn inc(&self) {
        let pending = self.inner.pending.fetch_add(1, Ordering::Release) + 1;
        self.inner.total.fetch_add(1, Ordering::Release);
        self.inner.peak.fetch_max(pending, Ordering::Release);
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
#[path = "../tests/unit/request_pool.rs"]
mod tests;
