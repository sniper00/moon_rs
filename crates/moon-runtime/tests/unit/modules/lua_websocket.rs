use super::*;
use tokio_tungstenite::tungstenite::protocol::Role;

#[test]
fn binary_and_ping_keep_the_unread_buffer_allocation() {
    for kind in ["b", "p"] {
        let mut buffer = moon_runtime::buffer::Buffer::from_slice(b"prefix-payload");
        buffer.consume(7);
        let pointer = buffer.data().as_ptr();
        let message = message_from_buffer(buffer, kind);
        let payload = match &message {
            Message::Binary(payload) | Message::Ping(payload) => payload,
            _ => panic!("unexpected message kind"),
        };
        assert_eq!(payload.as_ref(), b"payload");
        assert_eq!(payload.as_ptr(), pointer);
    }
}

#[test]
fn text_and_close_keep_the_unread_buffer_allocation() {
    for kind in ["t", "c"] {
        let mut buffer = moon_runtime::buffer::Buffer::from_slice(b"prefix-payload");
        buffer.consume(7);
        let pointer = buffer.data().as_ptr();
        let message = message_from_buffer(buffer, kind);
        let payload = match &message {
            Message::Text(text) => text.as_bytes(),
            Message::Close(Some(frame)) => frame.reason.as_bytes(),
            _ => panic!("unexpected message kind"),
        };
        assert_eq!(payload, b"payload");
        assert_eq!(payload.as_ptr(), pointer);
    }
}

#[test]
fn text_and_close_keep_lossy_utf8_behavior() {
    let text = b"hello".to_vec();
    let pointer = text.as_ptr();
    let message = to_message(text, "t");
    let Message::Text(payload) = &message else {
        panic!("expected text message");
    };
    assert_eq!(payload.as_bytes().as_ptr(), pointer);
    assert_eq!(message, Message::Text("hello".into()));
    assert_eq!(
        to_message(b"\xff".to_vec(), "t"),
        Message::Text("\u{fffd}".into())
    );
    assert_eq!(
        to_message(b"\xff".to_vec(), "c"),
        Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "\u{fffd}".into(),
        }))
    );
    for kind in ["t", "c"] {
        let mut buffer = moon_runtime::buffer::Buffer::from_slice(b"prefix-\xff");
        buffer.consume(7);
        assert_eq!(
            message_from_buffer(buffer, kind),
            to_message(b"\xff".to_vec(), kind)
        );
    }
}

#[tokio::test]
async fn peer_close_is_acknowledged_before_disconnect() {
    for (local_role, peer_role) in [(Role::Server, Role::Client), (Role::Client, Role::Server)] {
        let (socket, peer) = tokio::io::duplex(4096);
        let stream = WebSocketStream::from_raw_socket(socket, local_role, None).await;
        let mut peer = WebSocketStream::from_raw_socket(peer, peer_role, None).await;
        let (fd, reads, writes) = setup_ws_connection();
        WS_NET
            .get(&fd)
            .unwrap()
            .tx_reader
            .try_send(WsRequest::Read(0, 1, 0))
            .unwrap();
        let task = tokio::spawn(run_connection(stream, fd, reads, writes));
        let frame = Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        });
        peer.send(Message::Close(frame.clone())).await.unwrap();
        let reply = timeout(Duration::from_secs(2), peer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(reply, Message::Close(frame));
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(!WS_NET.contains_key(&fd));
    }
}

#[tokio::test]
async fn local_close_removes_idle_connection() {
    let (socket, _peer) = tokio::io::duplex(4096);
    let stream = WebSocketStream::from_raw_socket(socket, Role::Server, None).await;
    let (fd, reads, writes) = setup_ws_connection();
    let channels = WS_NET.get(&fd).unwrap().clone();
    channels
        .tx_writer
        .try_send(WsRequest::Close(Message::Close(None)))
        .unwrap();
    timeout(
        Duration::from_secs(2),
        run_connection(stream, fd, reads, writes),
    )
    .await
    .unwrap();
    assert!(!WS_NET.contains_key(&fd));
    assert!(channels.tx_reader.is_closed());
    assert!(channels.tx_writer.is_closed());
}

#[tokio::test]
async fn connection_shutdown_releases_active_and_queued_readers() {
    for mode in 0..4 {
        let owner = 0x7200_0030 + mode;
        let (tx, mut responses) = mpsc::unbounded_channel();
        let mut actor = LuaActor::new(&context::LuaActorParam {
            id: owner,
            unique: false,
            creator: 0,
            session: 0,
            memlimit: 0,
            name: String::new(),
            source: String::new(),
            params: String::new(),
        });
        CONTEXT.add_actor(&mut actor, tx).unwrap();
        let (socket, peer) = tokio::io::duplex(4096);
        let stream = WebSocketStream::from_raw_socket(socket, Role::Server, None).await;
        let mut peer = WebSocketStream::from_raw_socket(peer, Role::Client, None).await;
        let (fd, reads, writes) = setup_ws_connection();
        let channels = WS_NET.get(&fd).unwrap().clone();
        channels
            .tx_reader
            .try_send(WsRequest::Read(owner, 1, if mode == 2 { 100 } else { 0 }))
            .unwrap();
        let task = tokio::spawn(run_connection(stream, fd, reads, writes));
        timeout(Duration::from_secs(2), async {
            while channels.tx_reader.capacity() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        channels
            .tx_reader
            .try_send(WsRequest::Read(owner, 2, 0))
            .unwrap();
        match mode {
            0 => channels
                .tx_writer
                .try_send(WsRequest::Close(Message::Close(None)))
                .unwrap(),
            1 => {
                drop(peer);
                channels
                    .tx_writer
                    .try_send(WsRequest::Write(Message::Text("test".into()), false))
                    .unwrap();
            }
            2 => {} // Let the active read time out.
            3 => peer.send(Message::Close(None)).await.unwrap(),
            _ => unreachable!(),
        }
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(!WS_NET.contains_key(&fd));
        for session in [1, 2] {
            let message = loop {
                let message = responses
                    .try_recv()
                    .expect("waiting reader must be released");
                // Other tests can broadcast service exits through CONTEXT.
                if message.ptype() == context::PTYPE_WEBSOCKET {
                    break message;
                }
            };
            assert_eq!(message.session, session);
            let context::MessageBody::Boxed(_, value) = message.data else {
                panic!("expected websocket response")
            };
            let response = unsafe { value.into_inner::<WsResponse>() };
            if mode == 3 && session == 1 {
                assert!(matches!(response, WsResponse::Read(Message::Close(_))));
            } else {
                assert!(matches!(response, WsResponse::Error(_)));
            }
        }
        while let Ok(message) = responses.try_recv() {
            assert_ne!(message.ptype(), context::PTYPE_WEBSOCKET);
        }
        CONTEXT.remove_actor(owner, "");
    }
}
