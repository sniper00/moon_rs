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

impl PendingCounter {
    pub(crate) fn with_value(value: i64) -> Self {
        Self {
            inner: Arc::new(CounterInner {
                pending: AtomicI64::new(value),
                total: AtomicI64::new(value),
                peak: AtomicI64::new(value),
            }),
        }
    }
}
