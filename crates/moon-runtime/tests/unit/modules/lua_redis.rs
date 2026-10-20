use super::*;

#[test]
fn owned_bulk_shares_the_reply_frame() {
    let wire = b"$7\r\npayload\r\n".to_vec();
    let raw = Bytes::from(wire);
    let payload_ptr = raw[4..].as_ptr();
    let RedisReply::Bulk(Some(payload)) = parse_owned_reply(&raw, &mut 0).unwrap() else {
        panic!("expected bulk reply");
    };
    assert_eq!(payload.as_ref(), b"payload");
    assert_eq!(payload.as_ptr(), payload_ptr);
}

// Exercise every split point: RESP boundaries need not align with socket
// reads, and bulk payloads can themselves contain RESP-looking bytes.
#[test]
fn raw_scanner_fragmented_replies_and_pipeline_boundaries() {
    let replies: &[&[u8]] = &[
        b"+OK\r\n",
        b"-ERR wrong type\r\n",
        b":-9223372036854775808\r\n",
        b"$-1\r\n",
        b"*-1\r\n",
        b"*0\r\n",
        b"$0\r\n\r\n",
        b"$8\r\n\0\xff\r\n*1\r\n\r\n",
        b"*3\r\n*2\r\n+OK\r\n:42\r\n*0\r\n$3\r\nfoo\r\n",
    ];
    let mut scanner = RawReplyScanner::new();
    for reply in replies {
        for split in 0..=reply.len() {
            scanner.reset();
            assert_eq!(scanner.consume(&reply[..split]).unwrap(), split);
            assert_eq!(scanner.complete(), split == reply.len());
            let mut tail = reply[split..].to_vec();
            tail.extend_from_slice(b"+NEXT\r\n");
            assert_eq!(scanner.consume(&tail).unwrap(), reply.len() - split);
            assert!(scanner.complete());
        }
        scanner.reset();
        for (i, byte) in reply.iter().enumerate() {
            assert_eq!(scanner.consume(&[*byte]).unwrap(), 1);
            assert_eq!(scanner.complete(), i + 1 == reply.len());
        }
    }
}

#[test]
fn raw_scanner_rejects_invalid_frames_and_limits() {
    let frames = [
        b"+bad\n".to_vec(),
        b"$1\r\nx!!".to_vec(),
        b"$-2\r\n".to_vec(),
        b"*-2\r\n".to_vec(),
        b"$oops\r\n".to_vec(),
        b"?OK\r\n".to_vec(),
        format!("${}\r\n", MAX_MESSAGE_LEN + 1).into_bytes(),
        format!("*{}\r\n", MAX_ARRAY_COUNT + 1).into_bytes(),
        b"$9223372036854775808\r\n".to_vec(),
    ];
    for frame in frames {
        let mut scanner = RawReplyScanner::new();
        assert!(scanner.consume(&frame).is_err(), "accepted {frame:?}");
    }

    let mut nested = b"*1\r\n".repeat(MAX_RESP_DEPTH);
    nested.extend_from_slice(b"+OK\r\n");
    let mut scanner = RawReplyScanner::new();
    assert_eq!(scanner.consume(&nested).unwrap(), nested.len());
    assert!(scanner.complete());
    scanner.reset();
    nested.splice(0..0, b"*1\r\n".iter().copied());
    assert!(scanner.consume(&nested).is_err());
    scanner.reset();
    let mut nested_empty = b"*1\r\n".repeat(MAX_RESP_DEPTH);
    nested_empty.extend_from_slice(b"*0\r\n");
    assert!(scanner.consume(&nested_empty).is_err());
}

#[test]
fn raw_scanner_bounds_aggregate_reply_size() {
    // Reuse the same chunk to simulate a large reply without allocating
    // hundreds of MiB in the test. Individually legal bulk strings must not
    // bypass the aggregate bound when wrapped in an array.
    let mut scanner = RawReplyScanner::new();
    let count = MAX_RAW_REPLY_LEN / MAX_MESSAGE_LEN + 1;
    scanner.consume(format!("*{count}\r\n").as_bytes()).unwrap();
    let header = format!("${MAX_MESSAGE_LEN}\r\n");
    let chunk = [0; 16 * 1024];
    for _ in 0..count {
        scanner.consume(header.as_bytes()).unwrap();
        for _ in 0..MAX_MESSAGE_LEN / chunk.len() {
            match scanner.consume(&chunk) {
                Ok(n) => assert_eq!(n, chunk.len()),
                Err(e) => {
                    assert_eq!(e, "RESP reply too large");
                    return;
                }
            }
        }
        scanner.consume(b"\r\n").unwrap();
    }
    panic!("oversized aggregate reply was accepted");
}

async fn raw_test_connection(read_timeout_ms: u64) -> (RedisConn, TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let params = ConnParams {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
        username: String::new(),
        password: String::new(),
        db: 0,
        read_timeout_ms,
    };
    let (client, server) = tokio::join!(RedisConn::connect(&params, 1000), listener.accept());
    (client.unwrap(), server.unwrap().0)
}

#[tokio::test]
async fn simple_replies_reuse_the_typed_read_buffer() {
    let (mut conn, mut server) = raw_test_connection(1000).await;
    conn.pending_reply.reserve(128);
    let storage = conn.pending_reply.as_ptr();
    server
        .write_all(b"+OK\r\n:42\r\n-ERR failed\r\n")
        .await
        .unwrap();

    assert!(matches!(conn.read_reply().await.unwrap(), RedisReply::Status(s) if s == "OK"));
    assert_eq!(conn.pending_reply.as_ptr(), storage);
    assert!(matches!(
        conn.read_reply().await.unwrap(),
        RedisReply::Integer(42)
    ));
    assert_eq!(conn.pending_reply.as_ptr(), storage);
    assert!(matches!(conn.read_reply().await.unwrap(), RedisReply::Error(e) if e == "ERR failed"));
    assert_eq!(conn.pending_reply.as_ptr(), storage);
}

#[tokio::test]
async fn raw_reader_retains_pipeline_tail_for_typed_reader() {
    let (mut conn, mut server) = raw_test_connection(1000).await;
    // Exceed BufReader's capacity and include binary payload/CRLF bytes.
    let payload = b"\0\xff\r\n".repeat(READ_BUFFER_CAPACITY / 2);
    let mut expected = format!("*2\r\n+OK\r\n${}\r\n", payload.len()).into_bytes();
    expected.extend_from_slice(&payload);
    expected.extend_from_slice(b"\r\n");
    let mut wire = expected.clone();
    wire.extend_from_slice(b"$-1\r\n:42\r\n");
    let writer = tokio::spawn(async move { server.write_all(&wire).await.unwrap() });
    let mut raw = b"prefix".to_vec();
    conn.read_raw_reply(&mut raw).await.unwrap();
    assert_eq!(&raw[6..], &expected);
    conn.read_raw_reply(&mut raw).await.unwrap();
    assert_eq!(&raw[6 + expected.len()..], b"$-1\r\n");
    assert!(matches!(
        conn.read_reply().await.unwrap(),
        RedisReply::Integer(42)
    ));
    writer.await.unwrap();
}

#[tokio::test]
async fn raw_reader_batches_mixed_frames_without_consuming_extra_reply() {
    let (mut conn, mut server) = raw_test_connection(1000).await;
    let frame = b"*3\r\n+OK\r\n$3\r\nfoo\r\n:7\r\n$-1\r\n-ERR test\r\n";
    let expected = frame.repeat(4000);
    let mut wire = expected.clone();
    wire.extend_from_slice(b"+TAIL\r\n");
    let writer = tokio::spawn(async move { server.write_all(&wire).await.unwrap() });
    let mut raw = b"prefix".to_vec();
    conn.read_raw_replies(&mut raw, 12000).await.unwrap();
    assert_eq!(&raw[6..], &expected);
    let mut tail = Vec::new();
    conn.read_raw_reply(&mut tail).await.unwrap();
    assert_eq!(tail, b"+TAIL\r\n");
    writer.await.unwrap();
}

#[tokio::test]
async fn raw_reader_reports_truncated_reply() {
    let (mut conn, mut server) = raw_test_connection(1000).await;
    server.write_all(b"$5\r\nabc").await.unwrap();
    drop(server);
    let err = conn.read_raw_reply(&mut Vec::new()).await.unwrap_err();
    assert!(err.contains("EOF"), "{err}");
}

#[tokio::test]
async fn typed_reader_resumes_partial_frame_after_cancellation() {
    let (mut conn, mut server) = raw_test_connection(1000).await;
    server.write_all(b"*2\r\n$5\r\nhe").await.unwrap();
    // This is what watch_loop's select does when a control message wins
    // while part of a pub/sub delivery has already been received.
    assert!(
        timeout(Duration::from_millis(20), conn.read_reply())
            .await
            .is_err()
    );
    server.write_all(b"llo\r\n:42\r\n+NEXT\r\n").await.unwrap();
    let RedisReply::Array(Some(items)) = conn.read_reply().await.unwrap() else {
        panic!("expected array");
    };
    assert!(matches!(&items[0], RedisReply::Bulk(Some(value)) if value.as_ref() == b"hello"));
    assert!(matches!(&items[1], RedisReply::Integer(42)));
    assert!(matches!(conn.read_reply().await.unwrap(), RedisReply::Status(s) if s == "NEXT"));
}

#[tokio::test]
async fn typed_reader_cancellation_preserves_deadline() {
    let (mut conn, mut server) = raw_test_connection(100).await;
    server.write_all(b"+PARTIAL").await.unwrap();
    assert!(
        timeout(Duration::from_millis(20), conn.read_reply())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    // The old deadline must be observed immediately, not replaced with a
    // fresh 100 ms budget when a canceled read future is created again.
    let result = timeout(Duration::from_millis(40), conn.read_reply())
        .await
        .unwrap();
    assert!(matches!(result, Err(e) if e == "read timeout"));
}

#[tokio::test]
async fn raw_reader_deadline_is_per_reply_not_per_pipeline() {
    let (mut conn, mut server) = raw_test_connection(400).await;
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        server.write_all(b"+ONE\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        server.write_all(b"+TWO\r\n").await.unwrap();
    });
    let mut raw = Vec::new();
    conn.read_raw_replies(&mut raw, 2).await.unwrap();
    assert_eq!(raw, b"+ONE\r\n+TWO\r\n");
    writer.await.unwrap();
}

#[tokio::test]
async fn raw_reader_fragments_do_not_restart_deadline() {
    let (mut conn, mut server) = raw_test_connection(200).await;
    server.write_all(b"$3\r\n").await.unwrap();
    let writer = tokio::spawn(async move {
        for part in [b"a".as_slice(), b"b", b"c\r\n"] {
            tokio::time::sleep(Duration::from_millis(75)).await;
            if server.write_all(part).await.is_err() {
                break;
            }
        }
    });
    let err = conn.read_raw_replies(&mut Vec::new(), 2).await.unwrap_err();
    assert_eq!(err, "read timeout");
    writer.abort();
    let _ = writer.await;
}

// -- RESP encoding -------------------------------------------------------

#[test]
fn array_header_encoding() {
    let mut buf = Vec::new();
    write_resp_array_header(&mut buf, 3);
    assert_eq!(buf, b"*3\r\n");

    buf.clear();
    write_resp_array_header(&mut buf, 0);
    assert_eq!(buf, b"*0\r\n");

    buf.clear();
    write_resp_array_header(&mut buf, 100);
    assert_eq!(buf, b"*100\r\n");
}

#[test]
fn bulk_bytes_encoding() {
    let mut buf = Vec::new();
    write_bulk_bytes(&mut buf, b"SET");
    assert_eq!(buf, b"$3\r\nSET\r\n");

    buf.clear();
    write_bulk_bytes(&mut buf, b"");
    assert_eq!(buf, b"$0\r\n\r\n");

    buf.clear();
    write_bulk_bytes(&mut buf, b"hello world");
    assert_eq!(buf, b"$11\r\nhello world\r\n");
}

#[test]
fn bulk_bytes_binary_data() {
    let mut buf = Vec::new();
    write_bulk_bytes(&mut buf, &[0x00, 0xff, 0x0d, 0x0a]);
    assert_eq!(buf, b"$4\r\n\x00\xff\x0d\x0a\r\n");
}

#[test]
fn encode_resp_strings_set() {
    let mut buf = Vec::new();
    encode_resp_strings(&mut buf, &["SET", "mykey", "myval"]);
    assert_eq!(buf, b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n");
}

#[test]
fn encode_resp_strings_get() {
    let mut buf = Vec::new();
    encode_resp_strings(&mut buf, &["GET", "key"]);
    assert_eq!(buf, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
}

#[test]
fn encode_resp_strings_no_args() {
    let mut buf = Vec::new();
    encode_resp_strings(&mut buf, &["PING"]);
    assert_eq!(buf, b"*1\r\n$4\r\nPING\r\n");
}

// -- find_crlf -----------------------------------------------------------

#[test]
fn find_crlf_basic() {
    assert_eq!(find_crlf(b"+OK\r\n", 1), Ok(3));
    assert_eq!(find_crlf(b"$3\r\nSET\r\n", 1), Ok(2));
}

#[test]
fn find_crlf_from_offset() {
    let data = b"*2\r\n$3\r\nGET\r\n";
    assert_eq!(find_crlf(data, 1), Ok(2));
    assert_eq!(find_crlf(data, 5), Ok(6));
}

#[test]
fn find_crlf_missing() {
    assert!(find_crlf(b"+OK", 1).is_err());
    assert!(find_crlf(b"+OK\r", 1).is_err()); // \r at end, no \n
}

#[test]
fn find_crlf_lone_cr_not_matched() {
    // lone \r without \n is not a valid CRLF — memchr finds the first \r
    // but the next byte is not \n, so it fails
    assert!(find_crlf(b"+O\rK", 1).is_err());
}

// -- ConnParams ----------------------------------------------------------

#[test]
fn conn_params_defaults() {
    let p = ConnParams {
        host: "localhost".into(),
        port: 6379,
        username: String::new(),
        password: String::new(),
        db: 0,
        read_timeout_ms: crate::LIMITS.db_read_timeout_ms,
    };
    assert_eq!(p.host, "localhost");
    assert_eq!(p.port, 6379);
    assert!(p.username.is_empty());
    assert!(p.password.is_empty());
    assert_eq!(p.db, 0);
    assert_eq!(p.read_timeout_ms, crate::LIMITS.db_read_timeout_ms);
}

// -- ConnectConfig::parse -------------------------------------------------

#[test]
fn parse_url_full() {
    let cfg = ConnectConfig::parse(
        "redis://user:pass@redis.host:6380/3?name=main&connect_timeout=3000\
             &pool_size=4&read_timeout=20000&queue_capacity=2048",
    )
    .unwrap();
    assert_eq!(cfg.params.host, "redis.host");
    assert_eq!(cfg.params.port, 6380);
    assert_eq!(cfg.params.username, "user");
    assert_eq!(cfg.params.password, "pass");
    assert_eq!(cfg.params.db, 3);
    assert_eq!(cfg.params.read_timeout_ms, 20000);
    assert_eq!(cfg.name, "main");
    assert_eq!(cfg.connect_timeout_ms, 3000);
    assert_eq!(cfg.pool_size, 4);
    assert_eq!(cfg.queue_capacity, 2048);
}

#[test]
fn parse_url_defaults() {
    let cfg = ConnectConfig::parse("redis://127.0.0.1:6379").unwrap();
    assert_eq!(cfg.params.host, "127.0.0.1");
    assert_eq!(cfg.params.port, 6379);
    assert!(cfg.params.username.is_empty());
    assert!(cfg.params.password.is_empty());
    assert_eq!(cfg.params.db, 0);
    assert_eq!(cfg.name, "default");
    assert_eq!(cfg.connect_timeout_ms, 5000);
    assert_eq!(cfg.pool_size, 1);
}

#[test]
fn parse_url_password_only_and_max_connections_alias() {
    // Redis allows password without a username (`redis://:pass@host`).
    let cfg = ConnectConfig::parse("redis://:secret@localhost/1?max_connections=8").unwrap();
    assert!(cfg.params.username.is_empty());
    assert_eq!(cfg.params.password, "secret");
    assert_eq!(cfg.params.db, 1);
    assert_eq!(cfg.pool_size, 8);
}

#[test]
fn parse_url_percent_encoded_password() {
    let cfg = ConnectConfig::parse("redis://:p%40ss%2Fword@h:6379/0").unwrap();
    assert_eq!(cfg.params.password, "p@ss/word");
}

#[test]
fn parse_url_clamps_pool_and_queue() {
    let cfg = ConnectConfig::parse("redis://h/0?pool_size=0&queue_capacity=0").unwrap();
    assert_eq!(cfg.pool_size, 1);
    assert_eq!(cfg.queue_capacity, 1);
}

#[test]
fn parse_url_errors() {
    assert!(ConnectConfig::parse("http://h/0").is_err()); // bad scheme
    assert!(ConnectConfig::parse("not_a_url").is_err()); // not a url
    assert!(ConnectConfig::parse("redis://h/abc").is_err()); // db not a number
    assert!(ConnectConfig::parse("redis://h/0?sslmode=require").is_err()); // unknown param
    assert!(ConnectConfig::parse("redis://h/0?connect_timeout=abc").is_err()); // bad number
}

// -- Limits --------------------------------------------------------------

#[test]
fn max_constants_are_reasonable() {
    assert_eq!(MAX_MESSAGE_LEN, crate::LIMITS.db_wire_message_bytes);
    assert_eq!(MAX_ARRAY_COUNT, crate::LIMITS.redis_array_items);
}

// -- RESP round-trip via raw bytes ---------------------------------------

#[test]
fn resp_status_raw_format() {
    let raw = b"+OK\r\n";
    let end = find_crlf(raw, 1).unwrap();
    assert_eq!(&raw[1..end], b"OK");
}

#[test]
fn resp_error_raw_format() {
    let raw = b"-ERR unknown command\r\n";
    let end = find_crlf(raw, 1).unwrap();
    assert_eq!(&raw[1..end], b"ERR unknown command");
}

#[test]
fn resp_integer_raw_format() {
    let raw = b":42\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let val = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
    assert_eq!(val, 42);
}

#[test]
fn resp_negative_integer() {
    let raw = b":-1\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let val = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
    assert_eq!(val, -1);
}

#[test]
fn resp_bulk_string_raw_format() {
    let raw = b"$5\r\nhello\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap() as usize;
    let data_start = end + 2;
    assert_eq!(&raw[data_start..data_start + len], b"hello");
}

#[test]
fn resp_null_bulk_string() {
    let raw = b"$-1\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
    assert_eq!(len, -1);
}

#[test]
fn resp_nested_array_raw_format() {
    // *2\r\n*1\r\n:1\r\n:2\r\n
    let raw = b"*2\r\n*1\r\n:1\r\n:2\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let count = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
    assert_eq!(count, 2);
}

// -- Encode then verify --------------------------------------------------

#[test]
fn encode_decode_round_trip() {
    let mut buf = Vec::new();
    encode_resp_strings(&mut buf, &["HSET", "myhash", "field1", "value1"]);
    assert_eq!(&buf[..4], b"*4\r\n");
    assert!(buf.ends_with(b"$6\r\nvalue1\r\n"));
}

#[test]
fn pipeline_encoding() {
    let mut buf = Vec::new();
    encode_resp_strings(&mut buf, &["SET", "a", "1"]);
    encode_resp_strings(&mut buf, &["SET", "b", "2"]);
    let expected = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
                         *3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
    assert_eq!(buf, expected.to_vec());
}

// -- is_pubsub_delivery ---------------------------------------------------

#[test]
fn pubsub_delivery_message() {
    let reply = RedisReply::Array(Some(vec![
        RedisReply::Bulk(Some(Bytes::from_static(b"message"))),
        RedisReply::Bulk(Some(Bytes::from_static(b"chan"))),
        RedisReply::Bulk(Some(Bytes::from_static(b"payload"))),
    ]));
    assert!(is_pubsub_delivery(&reply));
}

#[test]
fn pubsub_delivery_pmessage() {
    let reply = RedisReply::Array(Some(vec![
        RedisReply::Bulk(Some(Bytes::from_static(b"pmessage"))),
        RedisReply::Bulk(Some(Bytes::from_static(b"pattern*"))),
        RedisReply::Bulk(Some(Bytes::from_static(b"chan"))),
        RedisReply::Bulk(Some(Bytes::from_static(b"data"))),
    ]));
    assert!(is_pubsub_delivery(&reply));
}

#[test]
fn pubsub_delivery_subscribe_is_not_delivery() {
    let reply = RedisReply::Array(Some(vec![
        RedisReply::Bulk(Some(Bytes::from_static(b"subscribe"))),
        RedisReply::Bulk(Some(Bytes::from_static(b"chan"))),
        RedisReply::Integer(1),
    ]));
    assert!(!is_pubsub_delivery(&reply));
}

#[test]
fn pubsub_delivery_non_array() {
    assert!(!is_pubsub_delivery(&RedisReply::Status("OK".into())));
    assert!(!is_pubsub_delivery(&RedisReply::Integer(42)));
    assert!(!is_pubsub_delivery(&RedisReply::Bulk(None)));
}

#[test]
fn pubsub_delivery_empty_array() {
    assert!(!is_pubsub_delivery(&RedisReply::Array(Some(vec![]))));
    assert!(!is_pubsub_delivery(&RedisReply::Array(None)));
}

#[test]
fn pubsub_delivery_first_element_integer_not_delivery() {
    let reply = RedisReply::Array(Some(vec![
        RedisReply::Integer(1),
        RedisReply::Bulk(Some(Bytes::from_static(b"message"))),
    ]));
    assert!(!is_pubsub_delivery(&reply));
}

// -- Pool dispatch round-robin --------------------------------------------

#[test]
fn pool_dispatch_round_robin() {
    let mut workers = Vec::new();
    let mut _receivers = Vec::new();
    for _ in 0..3 {
        let (tx, rx) = mpsc::channel::<RedisMessage>(8);
        _receivers.push(rx);
        workers.push(WorkerHandle::new(tx, PendingCounter::new()));
    }
    let pool = RedisPool {
        inner: Arc::new(WorkerSet::new("test".into(), workers)),
    };

    for i in 0..9 {
        pool.dispatch(1, i as i64, vec![0], 1).unwrap();
    }

    // Each worker should have received 3 requests
    for w in pool.inner.workers() {
        assert_eq!(w.counter().load(), 3);
    }
}

#[test]
fn pool_dispatch_closed_worker_returns_error() {
    let (tx, rx) = mpsc::channel::<RedisMessage>(1);
    drop(rx); // close the receiver
    let workers = vec![WorkerHandle::new(tx, PendingCounter::new())];
    let pool = RedisPool {
        inner: Arc::new(WorkerSet::new("dead".into(), workers)),
    };

    let result = pool.dispatch(1, 1, vec![0], 1);
    assert!(result.is_err());
}

#[test]
fn pool_pending_sums_all_workers() {
    let mut workers = Vec::new();
    for i in 0..4 {
        let (tx, _rx) = mpsc::channel::<RedisMessage>(1);
        workers.push(WorkerHandle::new(tx, PendingCounter::with_value(i * 10)));
    }
    let pool = RedisPool {
        inner: Arc::new(WorkerSet::new("test".into(), workers)),
    };
    // 0 + 10 + 20 + 30 = 60
    assert_eq!(pool.inner.pending(), 60);
}

// -- find_crlf edge cases -------------------------------------------------

#[test]
fn find_crlf_at_very_start() {
    assert_eq!(find_crlf(b"\r\n", 0), Ok(0));
}

#[test]
fn find_crlf_first_cr_not_followed_by_lf() {
    // First \r at pos 1, next byte is 'x', so not CRLF.
    // memchr only finds the first \r, so even though \r\n exists later, it fails.
    assert!(find_crlf(b"+\rxOK\r\n", 1).is_err());
}

#[test]
fn find_crlf_at_boundary() {
    // \r is at end of buffer with no following \n
    assert!(find_crlf(b"+OK\r", 1).is_err());
}

// -- RESP encoding edge cases ---------------------------------------------

#[test]
fn encode_resp_strings_empty_args() {
    let mut buf = Vec::new();
    encode_resp_strings(&mut buf, &[]);
    assert_eq!(buf, b"*0\r\n");
}

#[test]
fn encode_resp_strings_large_arg_count() {
    let args: Vec<&str> = (0..256).map(|_| "x").collect();
    let mut buf = Vec::new();
    encode_resp_strings(&mut buf, &args);
    assert!(buf.starts_with(b"*256\r\n"));
}

#[test]
fn bulk_bytes_with_crlf_in_data() {
    let mut buf = Vec::new();
    write_bulk_bytes(&mut buf, b"a\r\nb");
    // Binary-safe: length=4, data contains \r\n
    assert_eq!(buf, b"$4\r\na\r\nb\r\n");
}

// -- RESP raw parsing edge cases ------------------------------------------

#[test]
fn resp_empty_bulk_string() {
    let raw = b"$0\r\n\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap() as usize;
    let data_start = end + 2;
    assert_eq!(len, 0);
    assert_eq!(&raw[data_start..data_start + len], b"");
}

#[test]
fn resp_null_array() {
    let raw = b"*-1\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let count = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
    assert_eq!(count, -1);
}

#[test]
fn resp_empty_array() {
    let raw = b"*0\r\n";
    let end = find_crlf(raw, 1).unwrap();
    let count = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn resp_large_integer() {
    let raw = b":9223372036854775807\r\n"; // i64::MAX
    let end = find_crlf(raw, 1).unwrap();
    let val = lexical_core::parse::<i64>(&raw[1..end]).unwrap();
    assert_eq!(val, i64::MAX);
}

#[test]
fn resp_large_bulk_string_length() {
    // Build a bulk string with 1024 bytes
    let payload = vec![b'x'; 1024];
    let mut raw = format!("${}\r\n", payload.len()).into_bytes();
    raw.extend_from_slice(&payload);
    raw.extend_from_slice(b"\r\n");
    let end = find_crlf(&raw, 1).unwrap();
    let len = lexical_core::parse::<i64>(&raw[1..end]).unwrap() as usize;
    assert_eq!(len, 1024);
    let data_start = end + 2;
    assert_eq!(&raw[data_start..data_start + len], payload.as_slice());
}

// -- ConnParams field coverage --------------------------------------------

#[test]
fn conn_params_with_auth_and_db() {
    let p = ConnParams {
        host: "redis.example.com".into(),
        port: 6380,
        username: "user".into(),
        password: "pass123".into(),
        db: 5,
        read_timeout_ms: 5_000,
    };
    assert_eq!(p.host, "redis.example.com");
    assert_eq!(p.port, 6380);
    assert_eq!(p.username, "user");
    assert_eq!(p.password, "pass123");
    assert_eq!(p.db, 5);
    assert_eq!(p.read_timeout_ms, 5_000);
}
