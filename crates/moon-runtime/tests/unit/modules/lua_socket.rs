use super::*;
use tokio::io::{AsyncWriteExt, duplex};
use tokio::net::{TcpListener, TcpStream};

struct RecordingWriter {
    output: Vec<u8>,
    calls: usize,
    max_iov: usize,
    write_limit: usize,
}

impl RecordingWriter {
    fn new(write_limit: usize) -> Self {
        Self {
            output: Vec::new(),
            calls: 0,
            max_iov: 0,
            write_limit,
        }
    }
}

impl AsyncWrite for RecordingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.calls += 1;
        let len = data.len().min(self.write_limit);
        self.output.extend_from_slice(&data[..len]);
        std::task::Poll::Ready(Ok(len))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.calls += 1;
        self.max_iov = self.max_iov.max(slices.len());
        let mut written = 0;
        for slice in slices {
            let len = slice.len().min(self.write_limit - written);
            self.output.extend_from_slice(&slice[..len]);
            written += len;
            if written == self.write_limit {
                break;
            }
        }
        std::task::Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn handle_write_merges_frames_into_one_vectored_write() {
    let (tx, rx) = socket_write_channel();
    for (payload, close) in [(b"one", false), (b"two", true)] {
        tx.send(WriteOp::Write(
            SocketWriteItem::Frame(Arc::new(Buffer::from_slice(payload))),
            close,
        ))
        .unwrap();
    }
    let mut writer = RecordingWriter::new(usize::MAX);
    assert_eq!(handle_write(&mut writer, rx).await, None);
    assert_eq!(writer.output, b"\x00\x03one\x00\x03two");
    assert_eq!(writer.calls, 1);
    assert_eq!(writer.max_iov, 4);
}

#[tokio::test]
async fn write_batch_preserves_mixed_order_with_short_writes() {
    for write_limit in [usize::MAX, 1, 3] {
        let batch = [
            SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"A"))),
            SocketWriteItem::Frame(Arc::new(Buffer::from_slice(b"one"))),
            SocketWriteItem::Raw(Arc::new(Buffer::new())),
            SocketWriteItem::Frame(Arc::new(Buffer::new())),
            SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"B"))),
            SocketWriteItem::Frame(Arc::new(Buffer::from_slice(b"two"))),
        ];
        let mut writer = RecordingWriter::new(write_limit);
        write_socket_batch(&mut writer, &batch).await.unwrap();
        assert_eq!(writer.output, b"A\x00\x03oneB\x00\x03two");
        assert_eq!(writer.max_iov, 6);
        if write_limit == usize::MAX {
            assert_eq!(writer.calls, 1);
        }
    }
}

#[tokio::test]
async fn write_batch_bounds_iovecs_across_many_frames() {
    let data = Arc::new(Buffer::from_slice(b"x"));
    let count = MAX_SOCKET_WRITE_CHUNKS * 2 + 1;
    let batch: Vec<_> = (0..count)
        .map(|_| SocketWriteItem::Frame(data.clone()))
        .collect();
    let mut writer = RecordingWriter::new(usize::MAX);
    write_socket_batch(&mut writer, &batch).await.unwrap();
    assert_eq!(writer.output, b"\x00\x01x".repeat(count));
    assert_eq!(writer.calls, 3);
    assert_eq!(writer.max_iov, MAX_SOCKET_WRITE_IOV);
}

#[tokio::test]
async fn write_batch_preserves_continuation_end_marker_across_windows() {
    let chunk = vec![b'X'; MESSAGE_CONTINUED_FLAG as usize];
    let payload = chunk.repeat(MAX_SOCKET_WRITE_CHUNKS);
    let batch = [
        SocketWriteItem::Frame(Arc::new(Buffer::from_slice(&payload))),
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"tail"))),
    ];
    let mut expected = Vec::new();
    for _ in 0..MAX_SOCKET_WRITE_CHUNKS {
        expected.extend_from_slice(&MESSAGE_CONTINUED_FLAG.to_be_bytes());
        expected.extend_from_slice(&chunk);
    }
    expected.extend_from_slice(b"\x00\x00tail");
    let mut writer = RecordingWriter::new(usize::MAX);
    write_socket_batch(&mut writer, &batch).await.unwrap();
    assert_eq!(writer.output, expected);
    assert_eq!(writer.calls, 2);
    assert_eq!(writer.max_iov, MAX_SOCKET_WRITE_IOV);
}

#[test]
fn write_batch_caps_empty_items_and_reuses_capacity() {
    let (tx, mut rx) = socket_write_channel();
    let data = Arc::new(Buffer::new());
    for _ in 0..MAX_SOCKET_WRITE_BATCH_ITEMS * 2 - 1 {
        tx.send(WriteOp::Write(SocketWriteItem::Raw(data.clone()), false))
            .unwrap();
    }
    tx.send(WriteOp::Close).unwrap();
    let mut batch = Vec::new();
    assert!(!drain_socket_write_batch(
        SocketWriteItem::Raw(data),
        false,
        &mut rx,
        &mut batch
    ));
    assert_eq!(batch.len(), MAX_SOCKET_WRITE_BATCH_ITEMS);
    let storage = batch.as_ptr();
    let capacity = batch.capacity();
    batch.clear();
    let WriteOp::Write(item, close) = rx.try_recv().unwrap() else {
        panic!("missing write")
    };
    assert!(!drain_socket_write_batch(item, close, &mut rx, &mut batch));
    assert_eq!(batch.len(), MAX_SOCKET_WRITE_BATCH_ITEMS);
    assert_eq!(batch.as_ptr(), storage);
    assert_eq!(batch.capacity(), capacity);
    assert!(matches!(rx.try_recv().unwrap(), WriteOp::Close));
}

#[tokio::test]
async fn handle_write_releases_payloads_before_waiting_for_next_batch() {
    let (tx, rx) = socket_write_channel();
    let data = Arc::new(Buffer::from_slice(b"hello"));
    let payload = Arc::downgrade(&data);
    tx.send(WriteOp::Write(SocketWriteItem::Frame(data), false))
        .unwrap();
    let mut writer = RecordingWriter::new(usize::MAX);
    let mut write = Box::pin(handle_write(&mut writer, rx));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(write.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(
        payload.upgrade().is_none(),
        "idle writer retained the last payload"
    );
    tx.send(WriteOp::Close).unwrap();
    assert_eq!(write.await, None);
}

#[test]
fn lua_close_preserves_success_and_empty_failure_returns() {
    let state = unsafe { LuaState::new(ffi::luaL_newstate()).unwrap() };
    let _guard = moon_base::laux::LuaGlobalState::new(state);
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let fd = next_net_fd();
    laux::lua_push(state, fd);
    assert_eq!(lua_socket_close(&mut lua).unwrap(), 0);

    let (_reader, mut writer) = setup_net_channel(fd);
    assert_eq!(lua_socket_close(&mut lua).unwrap(), 1);
    assert!(lua.get::<bool>(-1).unwrap());
    assert!(matches!(writer.try_recv().unwrap(), WriteOp::Close));
    laux::lua_pop(state, 1);

    drop(writer);
    assert_eq!(lua_socket_close(&mut lua).unwrap(), 0);
    NET.remove(&fd);
}

#[tokio::test]
async fn vectored_write_preserves_data_across_short_writes_and_empty_slices() {
    let (mut writer, mut reader) = duplex(3);
    let task = tokio::spawn(async move {
        let mut slices = [
            IoSlice::new(b""),
            IoSlice::new(b"ab"),
            IoSlice::new(b""),
            IoSlice::new(b"cdef"),
            IoSlice::new(b"ghi"),
            IoSlice::new(b""),
        ];
        write_all_vectored(&mut writer, &mut slices, "short write")
            .await
            .unwrap();
    });
    let mut output = Vec::new();
    timeout(Duration::from_secs(2), reader.read_to_end(&mut output))
        .await
        .expect("vectored write stalled")
        .unwrap();
    task.await.unwrap();
    assert_eq!(output, b"abcdefghi");
}

#[tokio::test]
async fn vectored_write_skips_empty_data_and_preserves_write_zero_errors() {
    let mut writer = std::io::Cursor::new(&mut [][..]);
    let empty = Arc::new(Buffer::new());
    write_socket_batch(
        &mut writer,
        &[
            SocketWriteItem::Raw(empty.clone()),
            SocketWriteItem::Raw(empty),
        ],
    )
    .await
    .unwrap();

    let data = Arc::new(Buffer::from_slice(b"x"));
    let raw_error = write_socket_batch(&mut writer, &[SocketWriteItem::Raw(data.clone())])
        .await
        .unwrap_err();
    assert_eq!(raw_error.kind(), ErrorKind::WriteZero);
    assert_eq!(raw_error.to_string(), "failed to write socket buffers");
    let frame_error = write_socket_batch(&mut writer, &[SocketWriteItem::Frame(data)])
        .await
        .unwrap_err();
    assert_eq!(frame_error.kind(), ErrorKind::WriteZero);
    assert_eq!(frame_error.to_string(), "failed to write socket frame");
}

#[test]
fn lua_read_submission_returns_sessions_for_full_and_closed_queues() {
    for mode in 0..3 {
        let owner = 0x8002_0050 + mode;
        let mut actor = Box::new(LuaActor::new(&context::LuaActorParam {
            id: owner,
            unique: false,
            creator: 0,
            session: 0,
            memlimit: 0,
            name: "socket-read-test".to_string(),
            source: String::new(),
            params: String::new(),
        }));
        let state = unsafe { LuaState::new(ffi::luaL_newstate()).unwrap() };
        actor.set_main_state(state);
        let mut lua = unsafe { LuaStack::from_raw(state) };
        let (actor_tx, mut actor_rx) = mpsc::unbounded_channel();
        CONTEXT.register_pseudo_actor(owner, actor_tx);
        let fd = next_net_fd();
        let (mut read_rx, _write_rx) = setup_net_channel(fd);
        laux::lua_push(state, fd);
        match mode {
            0 => {
                laux::lua_push(state, 5i64);
                laux::lua_push(state, 0i64);
            }
            1 => {
                laux::lua_push(state, "\n");
                laux::lua_push(state, 64i64);
                laux::lua_push(state, 0i64);
            }
            _ => {
                laux::lua_push(state, 0i64);
            }
        }
        let submit = |lua: &mut LuaStack<'_>| {
            if mode == 2 {
                lua_read_frame(lua)
            } else {
                lua_socket_read(lua)
            }
        };
        assert_eq!(submit(&mut lua).unwrap(), 1);
        assert_eq!(lua.get::<i64>(-1).unwrap(), 1);
        laux::lua_pop(state, 1);
        assert!(actor_rx.try_recv().is_err());

        assert_eq!(submit(&mut lua).unwrap(), 1);
        assert_eq!(lua.get::<i64>(-1).unwrap(), 2);
        laux::lua_pop(state, 1);
        let name = if mode == 2 { "read_frame" } else { "read" };
        let message = actor_rx
            .try_recv()
            .expect("full queue must reply with an error");
        assert_eq!(message.session, 2);
        match message.data {
            MessageBody::Buffer(context::PTYPE_ERROR, data) => {
                assert!(
                    data.as_slice()
                        .starts_with(format!("{}: channel full (fd={}): ", name, fd).as_bytes())
                );
            }
            _ => panic!("unexpected full-queue response"),
        }
        let queued = read_rx.try_recv().unwrap();
        assert_eq!((queued.owner, queued.session), (owner, 1));
        assert_eq!(queued.read_timeout, 0);
        match queued.op {
            ReadOp::Bytes(5) if mode == 0 => {}
            ReadOp::Until(64, delim) if mode == 1 => {
                assert_eq!(delim.as_slice(), b"\n")
            }
            ReadOp::Frame if mode == 2 => {}
            _ => panic!("read arguments changed during submission"),
        }
        drop(read_rx);

        assert_eq!(submit(&mut lua).unwrap(), 1);
        assert_eq!(lua.get::<i64>(-1).unwrap(), 3);
        let message = actor_rx
            .try_recv()
            .expect("closed queue must reply with an error");
        assert_eq!(message.session, 3);
        match message.data {
            MessageBody::Buffer(context::PTYPE_ERROR, data) => {
                assert_eq!(
                    data.as_slice(),
                    format!("{}: channel full (fd={}): channel closed", name, fd).as_bytes()
                );
            }
            _ => panic!("unexpected closed-queue response"),
        }
        assert!(actor_rx.try_recv().is_err());
        NET.remove(&fd);
    }
}

#[test]
fn enqueue_write_preserves_error_text_and_backpressure_close() {
    for framed in [false, true] {
        let name = if framed { "write_frame" } else { "write" };
        let item = || {
            let data = Arc::new(Buffer::from_slice(b"hello"));
            if framed {
                SocketWriteItem::Frame(data)
            } else {
                SocketWriteItem::Raw(data)
            }
        };
        let fd = next_net_fd();
        assert_eq!(
            enqueue_socket_write(fd, item(), u16::MAX, false).unwrap_err(),
            format!("{}: fd {} not found", name, fd),
        );

        let (_reader, mut writer) = setup_net_channel(fd);
        enqueue_socket_write(fd, item(), 0, true).unwrap();
        assert_eq!(
            enqueue_socket_write(fd, item(), 0, false).unwrap_err(),
            format!("{}: backpressure (fd={})", name, fd),
        );
        match writer.try_recv().unwrap() {
            WriteOp::Write(SocketWriteItem::Raw(data), close) if !framed => {
                assert_eq!(data.as_slice(), b"hello");
                assert!(close);
            }
            WriteOp::Write(SocketWriteItem::Frame(data), close) if framed => {
                assert_eq!(data.as_slice(), b"hello");
                assert!(close);
            }
            _ => panic!("unexpected write operation"),
        }
        assert!(matches!(writer.try_recv().unwrap(), WriteOp::Close));
        drop(writer);
        let err = enqueue_socket_write(fd, item(), u16::MAX, false).unwrap_err();
        assert_eq!(
            err,
            format!("{}: channel closed (fd={}): channel closed", name, fd)
        );
        NET.remove(&fd);
    }
}

// -----------------------------------------------------------------------
// read_one_frame tests
// -----------------------------------------------------------------------

/// Creates a TCP pair and returns (writer_half, BufReader<OwnedReadHalf>)
async fn tcp_pair() -> (tokio::net::tcp::OwnedWriteHalf, BufReader<OwnedReadHalf>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let (_client_read, client_write) = client.into_split();
    let (server_read, _server_write) = server.into_split();
    (client_write, BufReader::new(server_read))
}

#[tokio::test]
async fn read_one_frame_single_small_message() {
    let (mut writer, mut reader) = tcp_pair().await;
    let payload = b"hello";
    let header = (payload.len() as u16).to_be_bytes();
    writer.write_all(&header).await.unwrap();
    writer.write_all(payload).await.unwrap();
    drop(writer);

    let result = read_one_frame(&mut reader, 0).await;
    let buf = result.unwrap();
    assert_eq!(buf.as_slice(), b"hello");
}

#[tokio::test]
async fn read_one_frame_continuation_then_final() {
    let (mut writer, mut reader) = tcp_pair().await;
    // First chunk: continuation (0xFFFF header) with 10 bytes
    let cont_header = MESSAGE_CONTINUED_FLAG.to_be_bytes();
    let chunk1 = vec![b'A'; MESSAGE_CONTINUED_FLAG as usize];
    writer.write_all(&cont_header).await.unwrap();
    writer.write_all(&chunk1).await.unwrap();
    // Final chunk: 5 bytes
    let final_payload = b"FINAL";
    let final_header = (final_payload.len() as u16).to_be_bytes();
    writer.write_all(&final_header).await.unwrap();
    writer.write_all(final_payload).await.unwrap();
    drop(writer);

    let buf = read_one_frame(&mut reader, 0).await.unwrap();
    assert_eq!(buf.len(), MESSAGE_CONTINUED_FLAG as usize + 5);
    assert_eq!(&buf.as_slice()[..5], &[b'A'; 5]);
    assert_eq!(&buf.as_slice()[buf.len() - 5..], b"FINAL");
}

#[tokio::test]
async fn read_one_frame_zero_end_marker_after_continuation() {
    let (mut writer, mut reader) = tcp_pair().await;
    // Continuation frame
    let cont_header = MESSAGE_CONTINUED_FLAG.to_be_bytes();
    let chunk = vec![b'X'; MESSAGE_CONTINUED_FLAG as usize];
    writer.write_all(&cont_header).await.unwrap();
    writer.write_all(&chunk).await.unwrap();
    // Zero-length end marker
    let end_marker = 0u16.to_be_bytes();
    writer.write_all(&end_marker).await.unwrap();
    drop(writer);

    let buf = read_one_frame(&mut reader, 0).await.unwrap();
    assert_eq!(buf.len(), MESSAGE_CONTINUED_FLAG as usize);
}

#[tokio::test]
async fn read_one_frame_eof_before_header() {
    let (writer, mut reader) = tcp_pair().await;
    drop(writer); // immediate EOF

    let result = read_one_frame(&mut reader, 0).await;
    assert_eq!(result.unwrap_err(), "eof");
}

#[tokio::test]
async fn read_one_frame_eof_mid_body() {
    let (mut writer, mut reader) = tcp_pair().await;
    // Header says 100 bytes but we only write 10
    let header = 100u16.to_be_bytes();
    writer.write_all(&header).await.unwrap();
    writer.write_all(&[b'Z'; 10]).await.unwrap();
    drop(writer);

    let result = read_one_frame(&mut reader, 0).await;
    assert_eq!(result.unwrap_err(), "eof");
}

#[tokio::test]
async fn read_one_frame_timeout() {
    let (_writer, mut reader) = tcp_pair().await;
    // No data written — should timeout
    let result = read_one_frame(&mut reader, 50).await; // 50ms timeout
    assert_eq!(result.unwrap_err(), "read timeout");
}

#[tokio::test]
async fn read_one_frame_timeout_mid_body() {
    let (mut writer, mut reader) = tcp_pair().await;
    writer.write_all(b"\x00\x05h").await.unwrap();
    let result = read_one_frame(&mut reader, 10).await;
    assert_eq!(result.unwrap_err(), "read timeout");
}

// -----------------------------------------------------------------------
// drain_socket_write_batch tests
// -----------------------------------------------------------------------

#[test]
fn write_queue_accepts_more_than_the_former_global_limit_and_then_close() {
    let (tx, mut rx) = socket_write_channel();
    let data = Arc::new(Buffer::from_slice(b"x"));
    let count = 64 * 1024 + 1;
    for _ in 0..count {
        tx.send(WriteOp::Write(SocketWriteItem::Raw(data.clone()), false))
            .unwrap();
    }
    tx.send(WriteOp::Close).unwrap();
    assert_eq!(tx.len(), count + 1);

    for _ in 0..count {
        assert!(matches!(rx.try_recv().unwrap(), WriteOp::Write(..)));
    }
    assert!(matches!(rx.try_recv().unwrap(), WriteOp::Close));
    assert_eq!(tx.len(), 0);

    drop(rx);
    assert!(tx.send(WriteOp::Close).is_err());
    assert_eq!(tx.len(), 0);
}

#[tokio::test]
async fn listener_closes_through_unbounded_write_channel() {
    let fd = listen("127.0.0.1:0", 0, 1).unwrap();
    let tx = {
        let channel = NET.get(&fd).unwrap();
        channel.1.send(WriteOp::Close).unwrap();
        channel.1.tx.clone()
    };
    timeout(Duration::from_secs(2), tx.closed())
        .await
        .expect("listener did not close");
    assert!(!NET.contains_key(&fd));
}

#[test]
fn drain_batch_single_raw() {
    let (tx, mut rx) = socket_write_channel();
    drop(tx); // channel closed, drain won't find more
    let item = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"hello")));
    let mut batch = Vec::new();
    let close = drain_socket_write_batch(item, false, &mut rx, &mut batch);
    assert_eq!(batch.len(), 1);
    assert!(!close);
}

#[test]
fn drain_batch_collects_until_close() {
    let (tx, mut rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"abc"))),
        false,
    ))
    .unwrap();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"def"))),
        true,
    ))
    .unwrap();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"ghi"))),
        false,
    ))
    .unwrap();
    assert_eq!(tx.len(), 3);

    let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"000")));
    let mut batch = Vec::new();
    let close = drain_socket_write_batch(first, false, &mut rx, &mut batch);
    // Should have: first + abc + def, stop at close=true
    assert_eq!(batch.len(), 3);
    assert!(close);
    assert_eq!(tx.len(), 1);
}

#[test]
fn drain_batch_close_op_stops_drain() {
    let (tx, mut rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"abc"))),
        false,
    ))
    .unwrap();
    tx.send(WriteOp::Close).unwrap();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"def"))),
        false,
    ))
    .unwrap();
    drop(tx);

    let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"first")));
    let mut batch = Vec::new();
    let close = drain_socket_write_batch(first, false, &mut rx, &mut batch);
    assert_eq!(batch.len(), 2); // first + abc
    assert!(close); // Close op triggers close_after_batch
}

#[test]
fn drain_batch_respects_byte_limit() {
    let (tx, mut rx) = socket_write_channel();
    // Fill with many small writes that collectively exceed MAX_SOCKET_WRITE_BATCH_BYTES
    let small_buf = Arc::new(Buffer::from_slice(&vec![0u8; 64 * 1024])); // 64KB each
    for _ in 0..8 {
        tx.send(WriteOp::Write(
            SocketWriteItem::Raw(small_buf.clone()),
            false,
        ))
        .unwrap();
    }
    drop(tx);

    let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(&vec![0u8; 64 * 1024])));
    let mut batch = Vec::new();
    let close = drain_socket_write_batch(first, false, &mut rx, &mut batch);
    // MAX is 256KB, first is 64KB, so can fit 3 more (64*4 = 256KB) then stops
    assert!(batch.len() <= 5); // capped by byte limit
    assert!(!close);
}

#[test]
fn drain_batch_mixed_raw_and_frame() {
    let (tx, mut rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Frame(Arc::new(Buffer::from_slice(b"frame1"))),
        false,
    ))
    .unwrap();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"raw"))),
        false,
    ))
    .unwrap();
    drop(tx);

    let first = SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"first_raw")));
    let mut batch = Vec::new();
    let close = drain_socket_write_batch(first, false, &mut rx, &mut batch);
    assert_eq!(batch.len(), 3);
    assert!(!close);
    // Verify types
    assert!(matches!(&batch[0], SocketWriteItem::Raw(_)));
    assert!(matches!(&batch[1], SocketWriteItem::Frame(_)));
    assert!(matches!(&batch[2], SocketWriteItem::Raw(_)));
}

// -----------------------------------------------------------------------
// handle_write error propagation tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn handle_write_returns_error_on_broken_pipe() {
    let (tx, rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"data"))),
        false,
    ))
    .unwrap();
    drop(tx);

    // Create a duplex but drop the reader to simulate broken pipe
    let (writer, _reader) = duplex(1);
    drop(_reader);

    let result = handle_write(writer, rx).await;
    assert!(result.is_some());
    assert!(result.unwrap().contains("write"));
}

#[tokio::test]
async fn handle_write_empty_channel_returns_none() {
    let (_tx, rx) = socket_write_channel();
    drop(_tx); // close the sender immediately

    let (writer, _reader) = duplex(64);
    let result = handle_write(writer, rx).await;
    assert_eq!(result, None);
}

// -----------------------------------------------------------------------
// Frame batch writer tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn write_frame_empty_payload_is_noop() {
    let (mut writer, mut reader) = duplex(64);
    let buf = Buffer::new();
    write_socket_batch(&mut writer, &[SocketWriteItem::Frame(Arc::new(buf))])
        .await
        .unwrap();
    drop(writer);

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert!(out.is_empty());
}

#[tokio::test]
async fn write_frame_small_payload_single_chunk() {
    let (mut writer, mut reader) = duplex(64);
    let buf = Buffer::from_slice(b"test123");
    write_socket_batch(&mut writer, &[SocketWriteItem::Frame(Arc::new(buf))])
        .await
        .unwrap();
    drop(writer);

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    // header (2 bytes: len=7) + "test123"
    assert_eq!(out.len(), 2 + 7);
    assert_eq!(&out[..2], &7u16.to_be_bytes());
    assert_eq!(&out[2..], b"test123");
}

#[tokio::test]
async fn write_frame_multi_chunk_payload() {
    let payload_size = MESSAGE_CONTINUED_FLAG as usize + 100;
    let payload = vec![b'M'; payload_size];
    let (mut writer, mut reader) = duplex(payload_size * 2);
    let buf = Buffer::from_slice(&payload);
    write_socket_batch(&mut writer, &[SocketWriteItem::Frame(Arc::new(buf))])
        .await
        .unwrap();
    drop(writer);

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    // Chunk 1: 0xFFFF header + 65535 bytes
    let mut pos = 0;
    let h1 = u16::from_be_bytes([out[pos], out[pos + 1]]);
    assert_eq!(h1, MESSAGE_CONTINUED_FLAG);
    pos += 2;
    assert_eq!(
        &out[pos..pos + MESSAGE_CONTINUED_FLAG as usize],
        &payload[..MESSAGE_CONTINUED_FLAG as usize]
    );
    pos += MESSAGE_CONTINUED_FLAG as usize;

    // Chunk 2: header (100) + 100 bytes
    let h2 = u16::from_be_bytes([out[pos], out[pos + 1]]);
    assert_eq!(h2, 100);
    pos += 2;
    assert_eq!(
        &out[pos..pos + 100],
        &payload[MESSAGE_CONTINUED_FLAG as usize..]
    );
    pos += 100;
    assert_eq!(pos, out.len());
}

#[tokio::test]
async fn write_frame_chunk_boundaries_roundtrip() {
    let max_chunk = MESSAGE_CONTINUED_FLAG as usize;
    for size in [max_chunk - 1, max_chunk, max_chunk + 1, max_chunk * 2] {
        let payload = vec![b'X'; size];
        let data = Buffer::from_slice(&payload);
        let (mut writer, mut reader) = tcp_pair().await;
        let task = tokio::spawn(async move {
            write_socket_batch(&mut writer, &[SocketWriteItem::Frame(Arc::new(data))])
                .await
                .unwrap();
        });
        let decoded = timeout(Duration::from_secs(2), read_one_frame(&mut reader, 0))
            .await
            .expect("frame did not finish")
            .unwrap();
        assert_eq!(decoded.as_slice(), payload);
        task.await.unwrap();
    }
}

// -----------------------------------------------------------------------
// handle_read via read_until / read_bytes tests (using CONTEXT pseudo-actors)
// -----------------------------------------------------------------------

fn session_read_ops(owner: ActorId) -> [ReadRequest; 3] {
    [
        ReadOp::Bytes(5),
        ReadOp::Until(64, Delimiter::new(b"\n").unwrap()),
        ReadOp::Frame,
    ]
    .map(|op| ReadRequest {
        owner,
        session: 1,
        read_timeout: 0,
        op,
    })
}

#[tokio::test]
async fn handle_read_timeouts_reply_once_and_preserve_close_reason() {
    for index in 0..3 {
        let owner = 0x8002_0020 + index as u32;
        let (actor_tx, mut actor_rx) = mpsc::unbounded_channel();
        CONTEXT.register_pseudo_actor(owner, actor_tx);
        let (_writer, reader) = tcp_pair().await;
        let (tx, rx) = mpsc::channel(1);
        let mut request = session_read_ops(owner).into_iter().nth(index).unwrap();
        request.read_timeout = 10;
        tx.try_send(request).unwrap();
        let reason = timeout(
            Duration::from_secs(2),
            handle_read(reader.into_inner(), 1, rx),
        )
        .await
        .expect("read did not time out");
        assert_eq!(
            reason.as_deref(),
            if index == 2 {
                Some("read timeout")
            } else {
                None
            }
        );
        let message = actor_rx.try_recv().expect("missing timeout response");
        assert_eq!(message.session, 1);
        match message.data {
            MessageBody::Buffer(context::PTYPE_ERROR, buffer) => {
                assert_eq!(buffer.as_slice(), b"read timeout");
            }
            _ => panic!("unexpected timeout response"),
        }
        assert!(matches!(
            actor_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn handle_read_abort_responds_to_in_flight_and_queued_sessions() {
    for index in 0..3 {
        let owner = 0x8002_0000 + index as u32;
        let (actor_tx, mut actor_rx) = mpsc::unbounded_channel();
        CONTEXT.register_pseudo_actor(owner, actor_tx);
        let (_writer, reader) = tcp_pair().await;
        let (read_tx, read_rx) = mpsc::channel(1);
        read_tx
            .try_send(session_read_ops(owner).into_iter().nth(index).unwrap())
            .unwrap();

        let mut read = Box::pin(handle_read(reader.into_inner(), 1, read_rx));
        // Poll once with no peer data so the first request is in flight,
        // rather than depending on timing or cancelling a still-queued read.
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(read.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(read_tx.capacity(), 1);
        read_tx
            .try_send(ReadRequest {
                owner,
                session: 2,
                read_timeout: 0,
                op: ReadOp::Bytes(1),
            })
            .unwrap();

        let task = tokio::spawn(read);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        for session in [1, 2] {
            let message = actor_rx
                .try_recv()
                .expect("missing read cancellation response");
            assert_eq!(message.to, owner);
            assert_eq!(message.session, session);
            match message.data {
                MessageBody::Buffer(context::PTYPE_ERROR, buffer) => {
                    assert_eq!(buffer.as_slice(), b"closed");
                }
                _ => panic!("unexpected cancellation response body"),
            }
        }
        assert!(matches!(
            actor_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(read_tx.is_closed());
    }
}

#[tokio::test]
async fn handle_read_completed_sessions_receive_only_one_response() {
    for fails in [false, true] {
        for index in 0..3 {
            let owner = 0x8002_0010 + (fails as u32) * 3 + index as u32;
            let (actor_tx, mut actor_rx) = mpsc::unbounded_channel();
            CONTEXT.register_pseudo_actor(owner, actor_tx);
            let (mut writer, reader) = tcp_pair().await;
            let (read_tx, read_rx) = mpsc::channel(1);
            read_tx
                .try_send(session_read_ops(owner).into_iter().nth(index).unwrap())
                .unwrap();
            drop(read_tx);

            if !fails {
                let wire: &[u8] = if index == 2 {
                    b"\x00\x05hello"
                } else {
                    b"hello\n"
                };
                writer.write_all(wire).await.unwrap();
            }
            drop(writer);
            timeout(
                Duration::from_secs(2),
                handle_read(reader.into_inner(), 1, read_rx),
            )
            .await
            .expect("read did not finish");

            let message = actor_rx.try_recv().expect("missing read response");
            assert_eq!(message.session, 1);
            match message.data {
                MessageBody::Buffer(ptype, buffer) => {
                    if fails {
                        assert_eq!(ptype, context::PTYPE_ERROR);
                        assert!(!buffer.is_empty());
                        assert_ne!(buffer.as_slice(), b"closed");
                    } else {
                        let expected_ptype = if index == 2 {
                            context::PTYPE_SOCKET_EVENT
                        } else {
                            context::PTYPE_SOCKET_TCP
                        };
                        assert_eq!(ptype, expected_ptype);
                        assert_eq!(buffer.as_slice(), b"hello");
                    }
                }
                _ => panic!("unexpected read response body"),
            }
            assert!(matches!(
                actor_rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
    }
}

// -----------------------------------------------------------------------
// Original write tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn handle_write_batches_contiguous_writes_and_preserves_order() {
    let (tx, rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"abc"))),
        false,
    ))
    .unwrap();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"def"))),
        false,
    ))
    .unwrap();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"ghi"))),
        true,
    ))
    .unwrap();

    let (client, mut server) = tokio::io::duplex(64);
    let writer = tokio::spawn(handle_write(client, rx));

    let mut out = [0u8; 9];
    server.read_exact(&mut out).await.unwrap();
    assert_eq!(&out, b"abcdefghi");
    assert_eq!(writer.await.unwrap(), None);
}

#[tokio::test]
async fn handle_write_close_op_writes_prior_batch_then_closes() {
    let (tx, rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"abc"))),
        false,
    ))
    .unwrap();
    tx.send(WriteOp::Close).unwrap();
    tx.send(WriteOp::Write(
        SocketWriteItem::Raw(Arc::new(Buffer::from_slice(b"def"))),
        false,
    ))
    .unwrap();

    let (client, mut server) = tokio::io::duplex(64);
    let writer = tokio::spawn(handle_write(client, rx));

    let mut out = [0u8; 3];
    server.read_exact(&mut out).await.unwrap();
    assert_eq!(&out, b"abc");

    let mut eof = [0u8; 1];
    assert_eq!(server.read(&mut eof).await.unwrap(), 0);
    assert_eq!(writer.await.unwrap(), None);
}

#[tokio::test]
async fn handle_write_frame_writes_header_and_payload_without_preconcat() {
    let (tx, rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Frame(Arc::new(Buffer::from_slice(b"payload"))),
        true,
    ))
    .unwrap();

    let (client, mut server) = tokio::io::duplex(64);
    let writer = tokio::spawn(handle_write(client, rx));

    let mut out = [0u8; 9];
    server.read_exact(&mut out).await.unwrap();
    assert_eq!(&out[..2], &7u16.to_be_bytes());
    assert_eq!(&out[2..], b"payload");
    assert_eq!(writer.await.unwrap(), None);
}

#[tokio::test]
async fn handle_write_frame_exact_continuation_chunk_writes_end_marker() {
    let payload = vec![b'a'; MESSAGE_CONTINUED_FLAG as usize];
    let (tx, rx) = socket_write_channel();
    tx.send(WriteOp::Write(
        SocketWriteItem::Frame(Arc::new(Buffer::from_slice(&payload))),
        true,
    ))
    .unwrap();

    let (client, mut server) = tokio::io::duplex(8192);
    let writer = tokio::spawn(handle_write(client, rx));

    let mut header = [0u8; 2];
    server.read_exact(&mut header).await.unwrap();
    assert_eq!(header, MESSAGE_CONTINUED_FLAG.to_be_bytes());

    let mut body = vec![0u8; MESSAGE_CONTINUED_FLAG as usize];
    server.read_exact(&mut body).await.unwrap();
    assert_eq!(body, payload);

    let mut end_marker = [0u8; 2];
    server.read_exact(&mut end_marker).await.unwrap();
    assert_eq!(end_marker, 0u16.to_be_bytes());
    assert_eq!(writer.await.unwrap(), None);
}

#[tokio::test]
async fn automatic_reads_dispatch_frames_and_cancel_only_waiting_sessions() {
    for (index, read_timeout) in [0, 1000].into_iter().enumerate() {
        let owner = 0x8002_0040 + index as u32;
        let fd = 42;
        let (actor_tx, mut actor_rx) = mpsc::unbounded_channel();
        CONTEXT.register_pseudo_actor(owner, actor_tx);
        let (mut writer, reader) = tcp_pair().await;
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(ReadRequest {
            owner,
            session: 0,
            read_timeout,
            op: ReadOp::Frame,
        })
        .unwrap();
        let task = tokio::spawn(handle_read(reader.into_inner(), fd, rx));
        writer.write_all(b"\x00\x03one\x00\x03two").await.unwrap();
        for expected in [b"one", b"two"] {
            let message = timeout(Duration::from_secs(2), actor_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.session, 0);
            let MessageBody::Boxed(context::PTYPE_SOCKET_EVENT, event) = message.data else {
                panic!("automatic read returned a session response");
            };
            let SocketEvent::Message(event_fd, data) =
                (unsafe { event.into_inner::<SocketEvent>() })
            else {
                panic!("unexpected socket event");
            };
            assert_eq!(event_fd, fd);
            assert_eq!(data.as_slice(), expected);
        }
        tx.try_send(ReadRequest {
            owner,
            session: 1,
            read_timeout: 0,
            op: ReadOp::Bytes(1),
        })
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let message = actor_rx.try_recv().expect("queued read must be cancelled");
        assert_eq!(message.session, 1);
        let MessageBody::Buffer(context::PTYPE_ERROR, data) = message.data else {
            panic!("unexpected cancellation response");
        };
        assert_eq!(data.as_slice(), b"closed");
        assert!(actor_rx.try_recv().is_err());
        assert!(tx.is_closed());
    }
}

#[tokio::test]
async fn automatic_read_timeout_has_close_reason_without_a_session_reply() {
    let owner = 0x8002_0042;
    let (actor_tx, mut actor_rx) = mpsc::unbounded_channel();
    CONTEXT.register_pseudo_actor(owner, actor_tx);
    let (_writer, reader) = tcp_pair().await;
    let (tx, rx) = mpsc::channel(1);
    tx.try_send(ReadRequest {
        owner,
        session: 0,
        read_timeout: 10,
        op: ReadOp::Frame,
    })
    .unwrap();
    let reason = timeout(
        Duration::from_secs(2),
        handle_read(reader.into_inner(), 42, rx),
    )
    .await
    .unwrap();
    assert_eq!(reason.as_deref(), Some("read timeout"));
    assert!(actor_rx.try_recv().is_err());
    assert!(tx.is_closed());
}
