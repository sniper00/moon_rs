use crate::context::ActorId;

use super::*;
use serial_test::{parallel, serial};

fn reg_actor(id: ActorId) -> mpsc::UnboundedReceiver<Message> {
    let (tx, rx) = mpsc::unbounded_channel();
    CONTEXT.register_pseudo_actor(id, tx);
    rx
}

fn insert_outbound(from_addr: u32, session: i64, to_node: u32, cgen: u64) {
    CLUSTER.outbound_calls.insert(
        (from_addr, session),
        OutboundCallInfo {
            to_node,
            cgen,
            timestamp: Instant::now(),
        },
    );
}

fn resp_frame(from_addr: u32, session: i64, body: &[u8]) -> Box<Buffer> {
    let mut v = format!("RESP {} {}\n", from_addr, session).into_bytes();
    v.extend_from_slice(body);
    Box::new(Buffer::from_slice(&v))
}

/// Two actors with the *same* per-actor session id must not clobber each
/// other: closing one node releases only that actor's call.
#[test]
#[parallel]
fn close_releases_matching_actor_not_session_twin() {
    let (from_a, from_b) = (0x7001_0001, 0x7001_0002);
    let (node_x, node_y) = (0x7001_00A1, 0x7001_00A2);

    let mut rx_a = reg_actor(from_a);
    let mut rx_b = reg_actor(from_b);

    insert_outbound(from_a, 1, node_x, 5);
    insert_outbound(from_b, 1, node_y, 5); // same session id, different node

    on_connection_closed(node_x, 5, ClusterCloseReason::Eof);

    let m = rx_a.try_recv().expect("actor A should be released");
    assert_eq!(m.session, 1);
    assert_eq!(m.ptype(), context::PTYPE_ERROR);
    if let MessageBody::Buffer(_, data) = &m.data {
        assert!(data.to_string().contains("(EOF)"));
    } else {
        panic!("expected error buffer");
    }
    assert!(CLUSTER.outbound_calls.get(&(from_a, 1)).is_none());

    assert!(rx_b.try_recv().is_err(), "actor B must be untouched");
    assert!(CLUSTER.outbound_calls.get(&(from_b, 1)).is_some());

    CLUSTER.outbound_calls.remove(&(from_b, 1));
}

/// Closing an old (replaced) connection fails only calls sent on that
/// generation; calls on the current generation and the live connection state
/// are left intact.
#[test]
#[parallel]
fn close_fails_only_matching_generation() {
    let from = 0x7002_0001;
    let node = 0x7002_00A1;
    let mut rx = reg_actor(from);

    let (conn, _rx2) = make_conn_with_cgen(1, 8);
    CLUSTER.connections.insert(node, conn); // current connection is generation 8
    insert_outbound(from, 10, node, 5); // call on the old generation
    insert_outbound(from, 11, node, 8); // call on the current generation

    on_connection_closed(node, 5, ClusterCloseReason::Eof); // the old connection closes

    let m = rx.try_recv().expect("old-generation call should be failed");
    assert_eq!(m.session, 10);
    assert_eq!(m.ptype(), context::PTYPE_ERROR);
    assert!(CLUSTER.outbound_calls.get(&(from, 10)).is_none());

    assert!(
        rx.try_recv().is_err(),
        "current-generation call must survive"
    );
    assert!(CLUSTER.outbound_calls.get(&(from, 11)).is_some());
    assert_eq!(
        CLUSTER.connections.get(&node).map(|e| e.value().cgen()),
        Some(8)
    );

    CLUSTER.outbound_calls.remove(&(from, 11));
    CLUSTER.connections.remove(&node);
}

/// A normal RESP delivers exactly once; a duplicate/late RESP for the same
/// call is dropped (no second wake-up of the coroutine).
#[test]
#[parallel]
fn resp_delivers_once_then_dedups() {
    let from = 0x7003_0001;
    let node = 0x7003_00A1;
    let mut rx = reg_actor(from);

    insert_outbound(from, 42, node, 5);

    dispatch_frame(resp_frame(from, 42, b"payload"), node);
    let m = rx.try_recv().expect("first RESP should be delivered");
    assert_eq!(m.session, 42);
    assert_eq!(m.ptype(), context::PTYPE_LUA);
    assert!(CLUSTER.outbound_calls.get(&(from, 42)).is_none());

    dispatch_frame(resp_frame(from, 42, b"payload2"), node);
    assert!(rx.try_recv().is_err(), "duplicate RESP must be deduped");
}

/// A close failure followed by a late RESP must not double-wake the caller.
#[test]
#[parallel]
fn close_then_late_resp_does_not_double_wake() {
    let from = 0x7004_0001;
    let node = 0x7004_00A1;
    let mut rx = reg_actor(from);

    insert_outbound(from, 7, node, 5);

    on_connection_closed(node, 5, ClusterCloseReason::SocketError);
    let m = rx.try_recv().expect("close should fail the pending call");
    assert_eq!(m.session, 7);
    assert_eq!(m.ptype(), context::PTYPE_ERROR);

    dispatch_frame(resp_frame(from, 7, b"late"), node);
    assert!(rx.try_recv().is_err(), "post-close RESP must be deduped");
}

/// Incoming-call bookkeeping is dropped on close *without* sending a
/// misdirected error to the (remote) caller address: the local handler is the
/// responder, not a waiter.
#[test]
#[parallel]
fn close_drops_incoming_pending_without_error() {
    let remote_caller = 0x7005_0001;
    let node = 0x7005_00A1;
    let mut rx = reg_actor(remote_caller);

    CLUSTER.pending_calls.insert(
        500,
        PendingCallInfo {
            node_id: node,
            from_addr: remote_caller,
            session: 99,
            timestamp: Instant::now(),
        },
    );

    on_connection_closed(node, 1, ClusterCloseReason::Eof);

    assert!(CLUSTER.pending_calls.get(&500).is_none());
    assert!(
        rx.try_recv().is_err(),
        "incoming-call cleanup must not respond_error"
    );
}

/// `fail_outbound_call` releases at most once even if called twice.
#[test]
#[parallel]
fn fail_outbound_call_is_idempotent() {
    let from = 0x7006_0001;
    let mut rx = reg_actor(from);

    insert_outbound(from, 3, 0x7006_00A1, 5);

    fail_outbound_call(from, 3, "boom".to_string());
    let m = rx.try_recv().expect("first failure should notify");
    assert_eq!(m.session, 3);
    assert_eq!(m.ptype(), context::PTYPE_ERROR);

    fail_outbound_call(from, 3, "boom again".to_string());
    assert!(rx.try_recv().is_err(), "second failure must be a no-op");
}

fn make_conn(capacity: usize) -> (ConnectionState, ClusterWriteReceiver) {
    let (tx, rx) = mpsc::unbounded_channel::<ClusterFrame>();
    let conn = Arc::new(ConnectionStateInner {
        tx,
        cgen: AtomicU64::new(0),
        pending_writes: AtomicUsize::new(0),
        write_queue_limit: capacity,
        closing: AtomicBool::new(false),
    });
    (conn, rx)
}

fn make_conn_with_cgen(capacity: usize, cgen: u64) -> (ConnectionState, ClusterWriteReceiver) {
    let (tx, rx) = mpsc::unbounded_channel::<ClusterFrame>();
    let conn = Arc::new(ConnectionStateInner {
        tx,
        cgen: AtomicU64::new(cgen),
        pending_writes: AtomicUsize::new(0),
        write_queue_limit: capacity,
        closing: AtomicBool::new(false),
    });
    (conn, rx)
}

#[test]
#[parallel]
fn enqueue_cluster_frame_reports_full_and_closed() {
    let (conn, _rx) = make_conn(1);

    enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
        .expect("first frame should fit");
    assert_eq!(conn.pending_writes.load(Ordering::Acquire), 1);

    let err = enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
        .expect_err("second frame should hit the cluster's own queue limit");
    assert!(err.contains("write queue full"));
    assert_eq!(conn.pending_writes.load(Ordering::Acquire), 1);

    let (conn, rx) = make_conn(1);
    drop(rx);
    let err = enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
        .expect_err("closed receiver should reject enqueue");
    assert!(err.contains("write queue closed"));
    assert_eq!(conn.pending_writes.load(Ordering::Acquire), 0);
}

#[test]
#[parallel]
fn concurrent_enqueues_respect_cluster_queue_limit() {
    let (conn, rx) = make_conn(32);
    let accepted: usize = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..32)
                        .filter(|_| enqueue_cluster_frame(&conn, make_ping_frame(), "test").is_ok())
                        .count()
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum()
    });
    assert_eq!(accepted, 32);
    assert_eq!(rx.len(), 32);
    assert_eq!(conn.pending_writes.load(Ordering::Acquire), 32);
}

#[test]
#[parallel]
fn enqueue_rejects_when_closing() {
    let (conn, _rx) = make_conn(16);
    conn.closing.store(true, Ordering::Release);

    let err = enqueue_cluster_frame(&conn, ClusterFrame::header_only(b"PING\n".to_vec()), "test")
        .expect_err("should reject when closing");
    assert!(err.contains("closing"));
}

/// Outbound calls that have been pending longer than the timeout threshold
/// are released with a PTYPE_ERROR "timeout" message.
#[test]
#[parallel]
fn timeout_releases_expired_outbound_calls() {
    let from = 0x700B_0001;
    let node = 0x700B_00A1;
    let mut rx = reg_actor(from);

    // Insert an outbound call with a timestamp far in the past
    CLUSTER.outbound_calls.insert(
        (from, 77),
        OutboundCallInfo {
            to_node: node,
            cgen: 1,
            timestamp: Instant::now() - Duration::from_secs(CALL_TIMEOUT_S + 1),
        },
    );

    // Insert another call that is NOT expired
    CLUSTER.outbound_calls.insert(
        (from, 78),
        OutboundCallInfo {
            to_node: node,
            cgen: 1,
            timestamp: Instant::now(),
        },
    );

    check_call_timeouts(CALL_TIMEOUT_S);

    // The expired call should be released
    let m = rx
        .try_recv()
        .expect("expired outbound call should be released");
    assert_eq!(m.session, 77);
    assert_eq!(m.ptype(), context::PTYPE_ERROR);
    if let MessageBody::Buffer(_, data) = &m.data {
        let text = std::str::from_utf8(data.as_slice()).unwrap();
        assert!(text.contains("timeout"), "error should mention timeout");
    }
    assert!(CLUSTER.outbound_calls.get(&(from, 77)).is_none());

    // The non-expired call should still exist
    assert!(CLUSTER.outbound_calls.get(&(from, 78)).is_some());
    assert!(
        rx.try_recv().is_err(),
        "non-expired call must not be released"
    );

    // Cleanup
    CLUSTER.outbound_calls.remove(&(from, 78));
}

/// Expired incoming `pending_calls` are silently removed without sending
/// any error to the (remote) caller.
#[test]
#[parallel]
fn timeout_removes_expired_pending_calls_silently() {
    let remote_caller = 0x700C_0001;
    let node = 0x700C_00A1;
    let mut rx = reg_actor(remote_caller);

    // Expired incoming call
    CLUSTER.pending_calls.insert(
        6000,
        PendingCallInfo {
            node_id: node,
            from_addr: remote_caller,
            session: 50,
            timestamp: Instant::now() - Duration::from_secs(CALL_TIMEOUT_S + 1),
        },
    );

    // Non-expired incoming call
    CLUSTER.pending_calls.insert(
        6001,
        PendingCallInfo {
            node_id: node,
            from_addr: remote_caller,
            session: 51,
            timestamp: Instant::now(),
        },
    );

    check_call_timeouts(CALL_TIMEOUT_S);

    // Expired entry removed
    assert!(CLUSTER.pending_calls.get(&6000).is_none());
    // Non-expired entry retained
    assert!(CLUSTER.pending_calls.get(&6001).is_some());
    // No message sent to any actor (incoming calls don't notify)
    assert!(
        rx.try_recv().is_err(),
        "pending_calls timeout must NOT send error to remote caller"
    );

    // Cleanup
    CLUSTER.pending_calls.remove(&6001);
}

/// Timeout does not double-release a call already resolved by RESP or close.
#[test]
#[parallel]
fn timeout_skips_already_resolved_outbound_calls() {
    let from = 0x700D_0001;
    let node = 0x700D_00A1;
    let mut rx = reg_actor(from);

    // Insert an expired outbound call
    CLUSTER.outbound_calls.insert(
        (from, 90),
        OutboundCallInfo {
            to_node: node,
            cgen: 1,
            timestamp: Instant::now() - Duration::from_secs(CALL_TIMEOUT_S + 1),
        },
    );

    // Simulate RESP arriving first (removes the entry)
    CLUSTER.outbound_calls.remove(&(from, 90));

    check_call_timeouts(CALL_TIMEOUT_S);

    // No message should be sent (the call was already resolved)
    assert!(
        rx.try_recv().is_err(),
        "already-resolved call must not be released again"
    );
}

#[test]
#[parallel]
fn enqueue_failure_close_releases_matching_outbound_wait() {
    let from = 0x7008_0001;
    let node = 0x7008_00A1;
    let cgen = 0x7008_u64;
    let mut rx = reg_actor(from);

    let (conn, _rx2) = make_conn_with_cgen(1, cgen);
    CLUSTER.connections.insert(node, conn);
    insert_outbound(from, 88, node, cgen);

    close_connection_for_enqueue_error(node, "test backpressure");

    let m = rx
        .try_recv()
        .expect("backpressure close should release the pending wait");
    assert_eq!(m.session, 88);
    assert_eq!(m.ptype(), context::PTYPE_ERROR);
    assert!(CLUSTER.outbound_calls.get(&(from, 88)).is_none());
    assert!(CLUSTER.connections.get(&node).is_none());
}

/// End-to-end over a real TCP socket: when the peer drops the connection,
/// `read_task` hits EOF and runs `on_connection_closed`, which must release
/// the caller's pending wait with an error. Exercises the real read loop +
/// close path (not just the synchronous cleanup helper).
#[tokio::test]
#[parallel]
async fn real_tcp_close_releases_outbound_wait() {
    let from = 0x7007_0001;
    let node = 0x7007_00A1;
    let cgen = 0x7007_u64;

    let mut rx = reg_actor(from);

    // A real loopback TCP connection pair within this process.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _peer) = listener.accept().await.unwrap();

    // Register this generation as the current connection and a pending call on it.
    let (conn, _dummy_rx) = make_conn_with_cgen(1, cgen);
    CLUSTER.connections.insert(node, conn.clone());
    insert_outbound(from, 55, node, cgen);

    // Drive the server's read side exactly as production does.
    let (read_half, _write_half) = server.into_split();
    let handle = tokio::spawn(read_task(read_half, node, conn, cgen));

    // Peer disconnects -> EOF -> read_task breaks -> on_connection_closed.
    drop(client);
    handle.await.unwrap();

    let m = rx
        .try_recv()
        .expect("socket close should release the pending wait");
    assert_eq!(m.session, 55);
    assert_eq!(m.ptype(), context::PTYPE_ERROR);
    assert!(CLUSTER.outbound_calls.get(&(from, 55)).is_none());
    assert!(
        CLUSTER.connections.get(&node).is_none(),
        "connection state torn down"
    );
}

#[tokio::test]
#[parallel]
async fn write_cluster_frame_preserves_wire_format() {
    let header = b"CALL echo 1 2 3\n".to_vec();
    let body = Box::new(Buffer::from_slice(b"payload"));
    let frame = ClusterFrame::with_body(header.clone(), body);
    let total_len = frame.total_len() as u32;

    let (mut client, mut server) = tokio::io::duplex(128);
    let writer = tokio::spawn(async move {
        write_cluster_frame(&mut client, &frame, total_len)
            .await
            .unwrap();
    });

    let mut out = vec![0u8; 4 + total_len as usize];
    server.read_exact(&mut out).await.unwrap();
    writer.await.unwrap();

    assert_eq!(&out[..4], &total_len.to_be_bytes());
    assert_eq!(&out[4..4 + header.len()], header.as_slice());
    assert_eq!(&out[4 + header.len()..], b"payload");
}

/// Full `shutdown_cluster` test: exercises the real function that iterates all
/// connections, calls `on_connection_closed` per entry, clears global state,
/// and fails all remaining outbound calls (including "not yet sent" ones).
///
/// Serialized with `#[serial]` because `shutdown_cluster` calls `.clear()` on
/// global `CLUSTER` maps — cannot run in parallel with other tests that
/// depend on CLUSTER state.
#[test]
#[serial]
fn shutdown_cluster_full_teardown() {
    // --- Setup: two connections, two outbound calls (one per connection),
    //     one outbound call still connecting (cgen=MAX), one pending_call ---
    let from_a = 0x700A_0001;
    let from_b = 0x700A_0002;
    let from_c = 0x700A_0003;
    let node_x = 0x700A_00A1;
    let node_y = 0x700A_00A2;
    let cgen_x = 0xA001_u64;
    let cgen_y = 0xA002_u64;

    let mut rx_a = reg_actor(from_a);
    let mut rx_b = reg_actor(from_b);
    let mut rx_c = reg_actor(from_c);

    let (conn_x, _rx_x) = make_conn_with_cgen(4, cgen_x);
    let (conn_y, _rx_y) = make_conn_with_cgen(4, cgen_y);
    CLUSTER.connections.insert(node_x, conn_x.clone());
    CLUSTER.connections.insert(node_y, conn_y.clone());
    CLUSTER.connecting.insert(0x700A_00B1); // a "connecting" node

    // Outbound calls: sent on known cgens
    insert_outbound(from_a, 100, node_x, cgen_x);
    insert_outbound(from_b, 200, node_y, cgen_y);
    // Outbound call still connecting (cgen=MAX, not yet associated with a connection)
    insert_outbound(from_c, 300, 0x700A_00B1, u64::MAX);

    // Pending incoming call from node_x
    CLUSTER.pending_calls.insert(
        9000,
        PendingCallInfo {
            node_id: node_x,
            from_addr: 0xDEAD,
            session: 42,
            timestamp: Instant::now(),
        },
    );

    CLUSTER.initialized.store(true, Ordering::Release);

    // --- Act ---
    shutdown_cluster(ClusterCloseReason::Shutdown);

    // --- Assert ---
    // initialized flag cleared
    assert!(!CLUSTER.initialized.load(Ordering::Acquire));

    // All connections removed
    assert!(CLUSTER.connections.is_empty());
    assert!(CLUSTER.connecting.is_empty());
    assert!(CLUSTER.pending_calls.is_empty());
    assert!(CLUSTER.outbound_calls.is_empty());

    // closing flag set on both connection Arcs
    assert!(conn_x.closing.load(Ordering::Acquire));
    assert!(conn_y.closing.load(Ordering::Acquire));

    // Actor A: outbound call on node_x failed
    let m_a = rx_a
        .try_recv()
        .expect("actor A's outbound call should be failed on shutdown");
    assert_eq!(m_a.session, 100);
    assert_eq!(m_a.ptype(), context::PTYPE_ERROR);
    if let MessageBody::Buffer(_, data) = &m_a.data {
        let text = std::str::from_utf8(data.as_slice()).unwrap();
        assert!(
            text.contains("SHUTDOWN"),
            "error should mention SHUTDOWN reason"
        );
    }

    // Actor B: outbound call on node_y failed
    let m_b = rx_b
        .try_recv()
        .expect("actor B's outbound call should be failed on shutdown");
    assert_eq!(m_b.session, 200);
    assert_eq!(m_b.ptype(), context::PTYPE_ERROR);

    // Actor C: "not yet sent" outbound call (cgen=MAX) also failed in the
    // final sweep of remaining outbound_calls
    let m_c = rx_c
        .try_recv()
        .expect("actor C's connecting call should be failed on shutdown");
    assert_eq!(m_c.session, 300);
    assert_eq!(m_c.ptype(), context::PTYPE_ERROR);
    if let MessageBody::Buffer(_, data) = &m_c.data {
        let text = std::str::from_utf8(data.as_slice()).unwrap();
        assert!(text.contains("shutdown"), "error should mention shutdown");
    }

    // No extra messages
    assert!(rx_a.try_recv().is_err());
    assert!(rx_b.try_recv().is_err());
    assert!(rx_c.try_recv().is_err());
}

/// The `close_after` flag in ClusterFrame causes write_task to exit after
/// writing that frame. Uses a real TCP socket pair.
#[tokio::test]
#[parallel]
async fn write_task_exits_on_close_after_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();

    let (conn, rx) = make_conn(16);
    let (_read_half, write_half) = server.into_split();
    let handle = tokio::spawn(write_task(write_half, rx, conn.clone()));

    // Enqueue a normal frame then a close-after frame
    let normal = ClusterFrame::header_only(b"PING\n".to_vec());
    enqueue_cluster_frame(&conn, normal, "test").unwrap();

    let mut close_frame = ClusterFrame::header_only(b"BYE\n".to_vec());
    close_frame.close_after = true;
    enqueue_cluster_frame(&conn, close_frame, "test").unwrap();

    // write_task should exit after writing the close_after frame
    handle.await.unwrap();
    assert_eq!(conn.pending_writes.load(Ordering::Acquire), 0);

    // Verify both frames were written to the peer. `write_task` has already
    // exited (handle.await above) and dropped its write half, so the peer
    // sees EOF once every byte has arrived. Read to EOF instead of relying on
    // a single `read()` returning both frames — TCP may split them across
    // segments, so one read can legitimately return just the first frame.
    let (mut client_read, _cw) = client.into_split();
    let mut got = Vec::new();
    client_read.read_to_end(&mut got).await.unwrap();
    // PING: 4-byte len (5) + "PING\n" = 9 bytes
    // BYE:  4-byte len (4) + "BYE\n"  = 8 bytes
    assert_eq!(
        got.len(),
        9 + 8,
        "expected both frames written before close"
    );
}
