use super::*;

fn register_array_api(state: LuaState) {
    luaopen_pg(state);
    unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("pg")) };
}

#[test]
fn array_encoder_escapes_strings_and_preserves_nulls_and_shapes() {
    let (state, _owner) = test_lua();
    register_array_api(state);
    check_lua(
        state,
        r#"
            assert(pg.array({}) == '{}')
            assert(pg.array({1, pg.NULL, -3}) == '{1,NULL,-3}')
            assert(pg.array({true, false}) == '{true,false}')
            assert(pg.array({{1,2}, {3,4}}) == '{{1,2},{3,4}}')
            assert(pg.array({'NULL', '', 'a,b', '{x}', 'a"b', 'a\\b'}) ==
                [[{"NULL","","a,b","{x}","a\"b","a\\b"}]])
        "#,
    );
    for (oid, expression, assertion) in [
        (
            1009,
            r#"{'NULL', '', 'a,b', '{x}', 'a"b', 'a\\b', pg.NULL}"#,
            r#"assert(a[1]=='NULL' and a[2]=='' and a[3]=='a,b' and a[4]=='{x}' and a[5]=='a"b' and a[6]=='a\\b' and a[7]==pg.NULL and #a==7)"#,
        ),
        (1007, "{{1,2},{3,4}}", "assert(a[1][2]==2 and a[2][1]==3)"),
        (
            1000,
            "{true, false, pg.NULL}",
            "assert(a[1]==true and a[2]==false and a[3]==pg.NULL)",
        ),
    ] {
        check_lua(state, &format!("return pg.array({expression})"));
        let encoded = unsafe { LuaStack::from_raw(state) }
            .value(-1)
            .as_bytes()
            .unwrap()
            .to_vec();
        laux::lua_pop(state, 1);
        convert_value(state, oid, &encoded).unwrap();
        unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("a")) };
        check_lua(state, assertion);
    }
}

#[test]
fn array_encoder_rejects_holes_maps_cycles_and_ragged_shapes() {
    let (state, _owner) = test_lua();
    register_array_api(state);
    check_lua(
        state,
        r#"
            for _, value in ipairs({
                {1, {2}}, {{1}, {2,3}}, {{}, {}},
                {[1]=1, [3]=3}, {x=1}, {['1']=1}, {function() end}, {'a\0b'}
            }) do
                assert(not pcall(pg.array, value))
            end
            local cycle = {}; cycle[1] = cycle
            assert(not pcall(pg.array, cycle))
            assert(not pcall(pg.array, 'not a table'))
            assert(pg.array({1,2}) == '{1,2}')
        "#,
    );
    assert_eq!(laux::lua_top(state), 0);
}

#[test]
fn array_decoder_handles_bounds_escapes_and_exact_numeric_text() {
    let (state, _owner) = test_lua();
    register_array_api(state);
    for (oid, wire, assertion) in [
        (1007, "[1:2][1:2]={{1,2},{3,4}}", "assert(a[2][2]==4)"),
        (
            1009,
            r#"{NULL,"NULL",null,"",a\,b,\NULL}"#,
            "assert(#a==6 and a[1]==pg.NULL and a[2]=='NULL' and a[3]==pg.NULL and a[4]=='' and a[5]=='a,b' and a[6]=='NULL')",
        ),
        (
            1016,
            "{9223372036854775807,-9223372036854775808}",
            "assert(a[1]==math.maxinteger and a[2]==math.mininteger)",
        ),
        (
            1231,
            "{12345678901234567890.123456789,NaN,NULL}",
            "assert(a[1]=='12345678901234567890.123456789' and a[2]=='NaN' and a[3]==pg.NULL)",
        ),
        (1009, "{}", "assert(type(a)=='table' and #a==0)"),
    ] {
        convert_value(state, oid, wire.as_bytes()).unwrap();
        unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("a")) };
        check_lua(state, assertion);
    }
    convert_value(state, 99999, b"unknown text").unwrap();
    assert_eq!(
        unsafe { LuaStack::from_raw(state) }
            .value(-1)
            .as_bytes()
            .unwrap(),
        b"unknown text"
    );
}

#[test]
fn malformed_arrays_restore_stack_and_surface_protocol_errors() {
    let (state, _owner) = test_lua();
    laux::lua_push(state, 42i64);
    for text in [
        "",
        "{",
        "{1,}",
        "{,1}",
        "{1,,2}",
        "{\"unterminated}",
        "{\"a\"junk}",
        "{1}junk",
        "{{1},{2,3}}",
        "{1,{2}}",
        "{{}}",
        "[0:1]={1,2}",
        "[1:3]={1,2}",
        "[1:9223372036854775808]={1}",
        "{{{{{{{1}}}}}}}",
    ] {
        assert!(
            convert_value(state, 1007, text.as_bytes()).is_err(),
            "{text}"
        );
        assert_eq!(laux::lua_top(state), 1, "{text}");
    }
    let mut rows = DataRows::default();
    let row = data_row(&[Some(b"[0:1]={1,2}")]);
    rows.begin(row.len()).extend_from_slice(&row);
    let stmt = Statement {
        row_desc: Some(description(&[("ids", 1007)])),
        data_rows: rows,
        command_tag: Some(b"SELECT 1\0".to_vec()),
    };
    assert!(push_statement_result(state, &stmt).is_err());
}

#[test]
fn lua_wrapper_exports_arrays_and_forwards_update_types() {
    let (state, _owner) = test_lua();
    register_array_api(state);
    check_lua(
        state,
        r#"
            package.preload['moon'] = function()
                return { PTYPE_PG=1, register_protocol=function() end }
            end
            package.preload['pg.core'] = function() return pg end
        "#,
    );
    check_lua(
        state,
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../lualib/moon/db/pg.lua"
        )),
    );
    unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("wrapper")) };
    check_lua(
        state,
        r#"
            assert(wrapper.NULL == pg.NULL)
            assert(wrapper.array({1, wrapper.NULL}) == '{1,NULL}')
            local calls = 0
            local function update(self, t, k, cols, rows, kt, types)
                assert(t == 'example' and k == 'id' and kt == 'bigint')
                assert(types[1] == 'int4[]' and rows[1][2] == '{1,2}')
                calls = calls + 1
                return {}
            end
            local db = setmetatable({obj={update_many=update, exec_update_many=function(...)
                update(...)
            end}}, wrapper)
            db:update_many('example', 'id', {'ids'}, {{1,wrapper.array({1,2})}}, 'bigint', {'int4[]'})
            db:execute_update_many('example', 'id', {'ids'}, {{1,wrapper.array({1,2})}}, 'bigint', {'int4[]'})
            assert(calls == 2)
        "#,
    );
}

#[test]
fn encoded_arrays_use_existing_bind_parameter_path() {
    let (state, _owner) = test_lua();
    register_array_api(state);
    check_lua(state, "return pg.array({1, pg.NULL, 3})");
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let mut wire = Vec::new();
    write_param(&mut wire, &mut lua, 1, &JsonOptions::default()).unwrap();
    assert_eq!(read_i32(&wire, 0), 10);
    assert_eq!(&wire[4..], b"{1,NULL,3}");
    wire.clear();
    append_statement(
        &mut wire,
        &mut lua,
        b"SELECT $1::int4[]",
        1..2,
        &JsonOptions::default(),
    )
    .unwrap();
    assert!(wire.windows(10).any(|part| part == b"{1,NULL,3}"));
    laux::lua_settop(state, 0);
    check_lua(
        state,
        "return {{pg.array({1, pg.NULL, 3})}, {pg.array({})}}",
    );
    wire.clear();
    encode_many(
        &mut lua,
        &mut wire,
        1,
        2,
        1,
        &JsonOptions::default(),
        &|n| build_insert_sql("example", &["ids".into()], n, None),
    )
    .unwrap();
    assert!(wire.windows(10).any(|part| part == b"{1,NULL,3}"));
    assert!(wire.windows(2).any(|part| part == b"{}"));
    let sql = build_update_sql(
        "example",
        "id",
        &["ids".into()],
        2,
        Some("bigint"),
        Some(&["int4[]".into()]),
    );
    assert!(sql.contains("\"ids\" = _d.\"ids\"::int4[]"));
    assert!(sql.contains("($1,$2),($3,$4)"));
}

fn wire_frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut wire = vec![kind];
    wire.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    wire.extend_from_slice(body);
    wire
}

fn description(fields: &[(&str, i32)]) -> Vec<u8> {
    let mut body = (fields.len() as u16).to_be_bytes().to_vec();
    for (name, oid) in fields {
        write_cstr(&mut body, name.as_bytes());
        body.extend_from_slice(&[0; 6]);
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&[0; 8]);
    }
    body
}

fn data_row(values: &[Option<&[u8]>]) -> Vec<u8> {
    let mut row = (values.len() as u16).to_be_bytes().to_vec();
    for value in values {
        match value {
            Some(value) => {
                row.extend_from_slice(&(value.len() as i32).to_be_bytes());
                row.extend_from_slice(value);
            }
            None => row.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    row
}

async fn test_connection(capacity: usize, millis: u64) -> (PgConn, TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    let read_timeout = Duration::from_millis(millis);
    (
        PgConn {
            stream: BufReader::with_capacity(capacity, client.unwrap()),
            read_timeout,
            read_timer: Box::pin(tokio::time::sleep(read_timeout)),
        },
        server.unwrap().0,
    )
}

#[tokio::test]
async fn buffered_and_fragmented_queries_keep_statement_and_cycle_boundaries() {
    let desc = description(&[("value", 25)]);
    let small = data_row(&[Some(b"small")]);
    let payload = b"\0\xff\r\n".repeat(10_000);
    let large = data_row(&[Some(&payload)]);
    let notify = [42i32.to_be_bytes().as_slice(), b"chan\0payload\0"].concat();
    let mut wire = Vec::new();
    for (kind, body) in [
        (b'1', b"".as_slice()),
        (b'2', b""),
        (b'T', desc.as_slice()),
        (b'D', small.as_slice()),
        (b'D', large.as_slice()),
        (b'N', b"SNOTICE\0Mtest\0\0"),
        (b'A', notify.as_slice()),
        (b'C', b"SELECT 2\0"),
        (b'C', b"UPDATE 3\0"),
        (b'Z', b"I"),
        (b'C', b"DELETE 4\0"),
        (b'Z', b"T"),
    ] {
        wire.extend(wire_frame(kind, body));
    }
    for capacity in [1, 4, 5, 17, 16 * 1024, wire.len()] {
        let (mut conn, mut server) = test_connection(capacity, 5000).await;
        let bytes = wire.clone();
        let writer = tokio::spawn(async move { server.write_all(&bytes).await.unwrap() });
        let result = conn.execute(b"", true).await.unwrap();
        assert!(result.error.is_none());
        assert_eq!(result.txn_status, b'I');
        assert_eq!(result.statements.len(), 2);
        assert_eq!(
            result.statements[0].row_desc.as_deref(),
            Some(desc.as_slice())
        );
        assert_eq!(result.statements[0].data_rows.count, 2);
        assert_eq!(
            result.statements[0].data_rows.iter().collect::<Vec<_>>(),
            [&small[..], &large[..]]
        );
        assert_eq!(
            result.statements[1].command_tag.as_deref(),
            Some(b"UPDATE 3\0".as_slice())
        );
        assert_eq!(result.notifications.len(), 1);
        assert_eq!(result.notifications[0].payload, "payload");
        let next = conn.execute(b"", true).await.unwrap();
        assert_eq!(next.txn_status, b'T');
        assert_eq!(
            next.statements[0].command_tag.as_deref(),
            Some(b"DELETE 4\0".as_slice())
        );
        writer.await.unwrap();
    }
}

#[tokio::test]
async fn discard_result_drains_large_rows_and_preserves_error_and_txn_status() {
    let row = data_row(&[Some(&vec![b'x'; 64 * 1024])]);
    let mut wire = wire_frame(b'T', &description(&[("value", 25)]));
    wire.extend(wire_frame(b'D', &row));
    wire.extend(wire_frame(b'C', b"SELECT 1\0"));
    wire.extend(wire_frame(b'E', b"SERROR\0C23505\0Mduplicate\0\0"));
    wire.extend(wire_frame(b'Z', b"E"));
    wire.extend(wire_frame(b'C', b"ROLLBACK\0"));
    wire.extend(wire_frame(b'Z', b"I"));
    for capacity in [7, wire.len()] {
        let (mut conn, mut server) = test_connection(capacity, 5000).await;
        let bytes = wire.clone();
        let writer = tokio::spawn(async move {
            server.write_all(&bytes).await.unwrap();
            // Keep the peer alive until the explicit rollback arrives.
            let mut rollback = [0u8; 14];
            server.read_exact(&mut rollback).await.unwrap();
            assert_eq!(&rollback, b"Q\0\0\0\x0dROLLBACK\0");
        });
        let result = conn.execute(b"", false).await.unwrap();
        assert!(result.statements.is_empty());
        assert!(result.notifications.is_empty());
        assert_eq!(result.error.unwrap().code.as_deref(), Some("23505"));
        assert_eq!(result.txn_status, b'E');
        conn.rollback().await.unwrap();
        writer.await.unwrap();
    }
}

#[tokio::test]
async fn pooled_requests_rollback_open_transactions_and_validate_cleanup() {
    for (status, rollback_status, rollback_error) in [
        (b'T', b'I', false),
        (b'E', b'I', false),
        (b'T', b'T', false),
        (b'T', b'I', true),
    ] {
        let (mut conn, mut server) = test_connection(128, 5000).await;
        let peer = tokio::spawn(async move {
            let mut query = [0; 14];
            server.read_exact(&mut query).await.unwrap();
            assert_eq!(&query, b"Q\0\0\0\x0dROLLBACK\0");
            if rollback_error {
                server
                    .write_all(&wire_frame(b'E', b"SERROR\0CXX000\0Mfailed\0\0"))
                    .await
                    .unwrap();
            }
            server
                .write_all(&wire_frame(b'Z', &[rollback_status]))
                .await
                .unwrap();
        });
        let mut result = QueryCollector::new(true, 10).finish();
        result.txn_status = status;
        if status == b'E' {
            result.error = Some(Box::new(parse_error(b"SERROR\0C23505\0Mduplicate\0\0")));
        }
        let cleanup = conn.finish_request(&mut result).await;
        assert_eq!(cleanup.is_ok(), rollback_status == b'I' && !rollback_error);
        assert_eq!(
            result.error.as_ref().unwrap().code.as_deref(),
            Some(if status == b'E' { "23505" } else { "25000" })
        );
        peer.await.unwrap();
    }
    let (mut conn, _server) = test_connection(128, 20).await;
    let mut result = QueryCollector::new(true, 10).finish();
    conn.finish_request(&mut result).await.unwrap();
    assert!(result.error.is_none());
}

#[test]
fn row_cap_preserves_completed_statements_and_discards_current_rows() {
    for capture in [true, false] {
        let mut collector = QueryCollector::new(capture, 2);
        collector.accept(b'D', &data_row(&[Some(b"1")])).unwrap();
        collector.accept(b'C', b"SELECT 1\0").unwrap();
        collector.accept(b'D', &data_row(&[Some(b"2")])).unwrap();
        collector.accept(b'D', &data_row(&[Some(b"3")])).unwrap();
        assert!(collector.row_destination(64 * 1024).is_none());
        assert!(collector.current.data_rows.bytes.is_empty());
        collector.accept(b'C', b"SELECT 2\0").unwrap();
        assert!(collector.accept(b'Z', b"I").unwrap());
        let result = collector.finish();
        assert_eq!(result.error.unwrap().code.as_deref(), Some("54000"));
        if capture {
            assert_eq!(result.statements.len(), 2);
            assert_eq!(result.statements[0].data_rows.count, 1);
            assert_eq!(result.statements[1].data_rows.count, 0);
        } else {
            assert!(result.statements.is_empty());
        }
    }
}

#[tokio::test]
async fn reader_rejects_invalid_headers_and_truncated_bodies() {
    let mut oversized = vec![b'D'];
    oversized.extend_from_slice(&((MAX_MESSAGE_LEN + 5) as i32).to_be_bytes());
    for (bytes, expected) in [
        (vec![b'D', 0, 0, 0, 3], "invalid message length"),
        (oversized, "message too large"),
        (vec![b'D', 0, 0], "socket read failed"),
        (vec![b'D', 0, 0, 0, 9, b'x'], "EOF"),
        (wire_frame(b'Z', b""), "invalid ReadyForQuery"),
        (wire_frame(b'Z', b"X"), "invalid ReadyForQuery"),
    ] {
        for capture in [true, false] {
            let (mut conn, mut server) = test_connection(16, 1000).await;
            server.write_all(&bytes).await.unwrap();
            server.shutdown().await.unwrap();
            let error = conn.execute(b"", capture).await.err().unwrap();
            assert!(error.contains(expected), "{error}");
        }
    }
}

#[tokio::test]
async fn startup_auth_and_query_share_buffered_messages() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let params = ConnParams {
        host: "127.0.0.1".to_string(),
        port: listener.local_addr().unwrap().port(),
        user: "tester".to_string(),
        password: String::new(),
        database: "test".to_string(),
        application_name: "pg-test".to_string(),
    };
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let len = stream.read_u32().await.unwrap() as usize;
        let mut startup = vec![0; len - 4];
        stream.read_exact(&mut startup).await.unwrap();
        assert_eq!(read_i32(&startup, 0), 196608);
        assert!(
            startup
                .windows(b"options\0-c array_nulls=on\0".len())
                .any(|part| part == b"options\0-c array_nulls=on\0")
        );
        let mut reply = wire_frame(b'R', &0i32.to_be_bytes());
        reply.extend(wire_frame(b'S', b"client_encoding\0UTF8\0"));
        reply.extend(wire_frame(b'K', &[0; 8]));
        reply.extend(wire_frame(b'Z', b"I"));
        stream.write_all(&reply).await.unwrap();
        let mut query = [0u8; 14];
        stream.read_exact(&mut query).await.unwrap();
        assert_eq!(&query, b"Q\0\0\0\x0dSELECT 1\0");
        let mut reply = wire_frame(b'T', &description(&[("one", 23)]));
        reply.extend(wire_frame(b'D', &data_row(&[Some(b"1")])));
        reply.extend(wire_frame(b'C', b"SELECT 1\0"));
        reply.extend(wire_frame(b'Z', b"I"));
        stream.write_all(&reply).await.unwrap();
    });
    let mut conn = PgConn::connect(&params, 1000, 1000).await.unwrap();
    let result = conn
        .execute(&wire_frame(b'Q', b"SELECT 1\0"), true)
        .await
        .unwrap();
    assert_eq!(result.statements[0].data_rows.count, 1);
    assert_eq!(
        result.statements[0].data_rows.iter().next().unwrap(),
        data_row(&[Some(b"1")])
    );
    server.await.unwrap();
}

#[tokio::test]
async fn parameterized_setup_cycle_is_drained_without_changing_results() {
    for capture in [true, false] {
        let (mut conn, mut server) = test_connection(1024, 1000).await;
        let (state, _owner) = test_lua();
        let mut lua = unsafe { LuaStack::from_raw(state) };
        let mut request = Vec::new();
        append_statement(
            &mut request,
            &mut lua,
            b"SELECT 1",
            0..0,
            &JsonOptions::default(),
        )
        .unwrap();
        append_sync(&mut request);
        let mut reply = wire_frame(b'A', b"\0\0\0\x07events\0setup\0");
        reply.extend(wire_frame(b'C', b"SET\0"));
        reply.extend(wire_frame(b'Z', b"I"));
        reply.extend(wire_frame(b'T', &description(&[("one", 23)])));
        reply.extend(wire_frame(b'D', &data_row(&[Some(b"1")])));
        reply.extend(wire_frame(b'C', b"SELECT 1\0"));
        reply.extend(wire_frame(b'A', b"\0\0\0\x07events\0query\0"));
        reply.extend(wire_frame(b'Z', b"I"));
        server.write_all(&reply).await.unwrap();
        let result = conn.execute_request(&request, capture).await.unwrap();
        assert!(result.error.is_none());
        assert_eq!(result.statements.len(), usize::from(capture));
        assert_eq!(result.notifications.len(), if capture { 2 } else { 0 });
        if capture {
            assert_eq!(result.statements[0].data_rows.count, 1);
            assert_eq!(result.notifications[0].payload, "setup");
            assert_eq!(result.notifications[1].payload, "query");
        }
        let setup = wire_frame(b'Q', b"SET array_nulls = on\0");
        let mut sent = vec![0; setup.len() + request.len()];
        server.read_exact(&mut sent).await.unwrap();
        assert_eq!(&sent[..setup.len()], setup);
        assert_eq!(&sent[setup.len()..], request);
        server.write_all(&wire_frame(b'Z', b"I")).await.unwrap();
        assert!(conn.execute(b"", false).await.unwrap().error.is_none());
    }
}

#[tokio::test]
async fn fully_buffered_response_does_not_rearm_timer() {
    let (mut conn, mut server) = test_connection(1024, 1000).await;
    let mut wire = wire_frame(b'C', b"UPDATE 2\0");
    wire.extend(wire_frame(b'Z', b"I"));
    server.write_all(&wire).await.unwrap();
    assert_eq!(conn.stream.fill_buf().await.unwrap(), wire);
    let deadline = conn.read_timer.deadline();
    assert_eq!(conn.execute(b"", true).await.unwrap().statements.len(), 1);
    assert_eq!(conn.read_timer.deadline(), deadline);
}

#[tokio::test]
async fn body_fragments_do_not_restart_timeout() {
    let (mut conn, mut server) = test_connection(8, 150).await;
    // Header declares a body of 100 bytes; each fragment arrives in less
    // than 150ms, but the body as a whole never completes within its budget.
    server.write_all(&[b'D', 0, 0, 0, 104]).await.unwrap();
    let writer = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if server.write_all(b"x").await.is_err() {
                break;
            }
        }
    });
    let result = timeout(Duration::from_secs(2), conn.execute(b"", true))
        .await
        .unwrap();
    writer.abort();
    let _ = writer.await;
    assert_eq!(result.err().unwrap(), "socket read timed out");
}

fn test_lua() -> (LuaState, laux::LuaGlobalState) {
    let state = LuaState::new(unsafe { ffi::luaL_newstate() }).unwrap();
    let owner = laux::LuaGlobalState::new(state);
    unsafe { ffi::luaL_openlibs(state.as_ptr()) };
    (state, owner)
}

fn check_lua(state: LuaState, code: &str) {
    let code = std::ffi::CString::new(code).unwrap();
    let result = unsafe { ffi::luaL_dostring(state.as_ptr(), code.as_ptr()) };
    assert_eq!(
        result,
        ffi::LUA_OK,
        "{}",
        unsafe { LuaStack::from_raw(state) }.value(-1)
    );
}

#[test]
fn compact_rows_decode_with_cached_keys_nulls_and_returning_counts() {
    let (state, _owner) = test_lua();
    let mut collector = QueryCollector::new(true, 10);
    collector
        .accept(
            b'T',
            &description(&[("id", 20), ("text", 25), ("flag", 16)]),
        )
        .unwrap();
    for values in [
        [
            Some(b"9223372036854775807".as_slice()),
            Some(b"a\0b"),
            Some(b"t"),
        ],
        [Some(b"-42".as_slice()), None, Some(b"f")],
    ] {
        collector.accept(b'D', &data_row(&values)).unwrap();
    }
    collector.accept(b'C', b"INSERT 0 2\0").unwrap();
    collector.accept(b'C', b"BEGIN\0").unwrap();
    collector.accept(b'Z', b"I").unwrap();
    laux::lua_push(state, "sentinel");
    assert_eq!(
        push_pg_response(state, PgResponse::Result(collector.finish())),
        1
    );
    assert_eq!(laux::lua_top(state), 2);
    unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("result")) };
    check_lua(
        state,
        r#"
            assert(result.num_queries == 2)
            assert(result.data[1].affected_rows == 2)
            assert(result.data[1][1].id == math.maxinteger)
            assert(result.data[1][1].text == "a\0b")
            assert(result.data[1][1].flag == true)
            assert(result.data[1][2].id == -42)
            assert(result.data[1][2].text == nil)
            assert(result.data[1][2].flag == false)
            assert(result.data[2] == true)
        "#,
    );
    assert_eq!(laux::lua_top(state), 1);
}

#[test]
fn malformed_rows_return_protocol_error_and_restore_lua_stack() {
    let good = data_row(&[Some(b"ok")]);
    let mut trailing = good.clone();
    trailing.push(0);
    for row in [
        vec![],
        data_row(&[]),
        vec![0, 1, 0, 0, 0],
        vec![0, 1, 0, 0, 0, 9, b'x'],
        vec![0, 1, 255, 255, 255, 254],
        trailing,
    ] {
        let (state, _owner) = test_lua();
        let mut collector = QueryCollector::new(true, 10);
        collector
            .accept(b'T', &description(&[("value", 25)]))
            .unwrap();
        // Two rows force the cached-key path, including its error cleanup.
        collector.accept(b'D', &good).unwrap();
        collector.accept(b'D', &row).unwrap();
        collector.accept(b'C', b"SELECT 2\0").unwrap();
        laux::lua_push(state, "sentinel");
        assert_eq!(
            push_pg_response(state, PgResponse::Result(collector.finish())),
            1
        );
        assert_eq!(laux::lua_top(state), 2);
        unsafe { ffi::lua_setglobal(state.as_ptr(), cstr!("result")) };
        check_lua(
            state,
            "assert(result.code == 'PROTOCOL'); assert(result.data == nil)",
        );
        assert_eq!(laux::lua_top(state), 1);
    }
}

#[test]
fn row_description_checks_every_truncation_and_borrows_valid_names() {
    let body = description(&[("first", 23), ("second", 25)]);
    for len in 0..body.len() {
        assert!(parse_row_desc(&body[..len]).is_err(), "length {len}");
    }
    let fields = parse_row_desc(&body).unwrap();
    assert!(matches!(fields[0].0, Cow::Borrowed("first")));
    let mut invalid_utf8 = description(&[("x", 25)]);
    invalid_utf8[2] = 255;
    assert_eq!(parse_row_desc(&invalid_utf8).unwrap()[0].0, "\u{fffd}");
    assert!(parse_notification(b"\0\0\0\0channel").is_none());
    assert!(parse_notification(b"\0\0\0\0channel\0payload").is_none());
}

#[test]
fn numeric_parameters_preserve_existing_text_format() {
    let (state, _owner) = test_lua();
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let options = JsonOptions::default();
    let mut encoded = Vec::new();
    for number in [i64::MIN, -1, 0, 1, i64::MAX] {
        laux::lua_push(state, number);
        encoded.clear();
        write_param(&mut encoded, &mut lua, -1, &options).unwrap();
        let expected = number.to_string();
        assert_eq!(read_i32(&encoded, 0) as usize, expected.len());
        assert_eq!(&encoded[4..], expected.as_bytes());
        laux::lua_pop(state, 1);
    }
    for number in [
        0.0,
        -0.0,
        1.25,
        1e-300,
        1e300,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        laux::lua_push(state, number);
        encoded.clear();
        write_param(&mut encoded, &mut lua, -1, &options).unwrap();
        let expected = number.to_string();
        assert_eq!(read_i32(&encoded, 0) as usize, expected.len());
        assert_eq!(&encoded[4..], expected.as_bytes());
        laux::lua_pop(state, 1);
    }
    laux::lua_push(state, 42i64);
    laux::lua_push(state, "value");
    encoded.clear();
    append_statement(&mut encoded, &mut lua, b"SELECT $1, $2", 1..3, &options).unwrap();
    let parse_len = read_i32(&encoded, 1) as usize + 1;
    let bind = &encoded[parse_len..];
    assert_eq!(bind[0], b'B');
    assert_eq!(read_u16(bind, 9), 2);
    assert_eq!(&bind[11..26], b"\0\0\0\x0242\0\0\0\x05value");
}

#[test]
fn parse_url_full() {
    let cfg = ConnectConfig::parse(
        "postgres://alice:s3cret@db.host:6543/shop?application_name=svc&name=main\
             &connect_timeout=3000&max_connections=8&read_timeout=20000&queue_capacity=2048",
    )
    .unwrap();
    assert_eq!(cfg.params.host, "db.host");
    assert_eq!(cfg.params.port, 6543);
    assert_eq!(cfg.params.user, "alice");
    assert_eq!(cfg.params.password, "s3cret");
    assert_eq!(cfg.params.database, "shop");
    assert_eq!(cfg.params.application_name, "svc");
    assert_eq!(cfg.name, "main");
    assert_eq!(cfg.connect_timeout_ms, 3000);
    assert_eq!(cfg.max_connections, 8);
    assert_eq!(cfg.read_timeout_ms, 20000);
    assert_eq!(cfg.queue_capacity, 2048);
}

#[test]
fn parse_url_defaults_and_alias() {
    // `postgresql` scheme alias, `pool_size` param alias, defaults elsewhere.
    let cfg =
        ConnectConfig::parse("postgresql://postgres:123456@127.0.0.1/postgres?name=c&pool_size=3")
            .unwrap();
    assert_eq!(cfg.params.port, 5432);
    assert_eq!(cfg.params.application_name, "moon");
    assert_eq!(cfg.name, "c");
    assert_eq!(cfg.connect_timeout_ms, 5000);
    assert_eq!(cfg.max_connections, 3);
}

#[test]
fn parse_url_percent_encoded_password() {
    let cfg = ConnectConfig::parse("postgres://u:p%40ss%2Fword@h/db?name=c").unwrap();
    assert_eq!(cfg.params.password, "p@ss/word");
}

#[test]
fn parse_url_errors() {
    assert!(ConnectConfig::parse("mysql://u:p@h/db?name=c").is_err()); // bad scheme
    assert!(ConnectConfig::parse("postgres://h/db?name=c").is_err()); // no user
    assert!(ConnectConfig::parse("postgres://u@h?name=c").is_err()); // no database
    assert!(ConnectConfig::parse("postgres://u:p@h/db").is_err()); // no name
    assert!(ConnectConfig::parse("postgres://u:p@h/db?name=c&sslmode=require").is_err()); // unknown param
    assert!(ConnectConfig::parse("not_a_url").is_err()); // not a url
    assert!(ConnectConfig::parse("postgres://u:p@h/db?name=c&port=abc").is_err()); // 'port' is not a query param
    assert!(ConnectConfig::parse("postgres://u:p@h/db?name=c&connect_timeout=abc").is_err()); // bad number
}

#[test]
fn md5_known_vector() {
    assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
    assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
}

#[test]
fn command_tag_parsing() {
    assert_eq!(
        parse_command_tag(b"INSERT 0 5\0"),
        ("INSERT".into(), Some(5))
    );
    assert_eq!(parse_command_tag(b"UPDATE 2\0"), ("UPDATE".into(), Some(2)));
    assert_eq!(parse_command_tag(b"SELECT 3\0"), ("SELECT".into(), Some(3)));
    assert_eq!(parse_command_tag(b"BEGIN\0"), ("BEGIN".into(), None));
}

#[test]
fn error_response_parsing() {
    // S<severity>\0 C<code>\0 M<message>\0 \0
    let body = b"SERROR\0C23505\0Mduplicate key\0\0";
    let err = parse_error(body);
    assert_eq!(err.severity.as_deref(), Some("ERROR"));
    assert_eq!(err.code.as_deref(), Some("23505"));
    assert_eq!(err.message.as_deref(), Some("duplicate key"));
    assert_eq!(parse_error_string(body), "duplicate key");
}

#[test]
fn row_description_parsing() {
    // 1 field named "id", type OID 23 (int4).
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes()); // field count
    body.extend_from_slice(b"id\0"); // name
    body.extend_from_slice(&0i32.to_be_bytes()); // table OID
    body.extend_from_slice(&0i16.to_be_bytes()); // column attr
    body.extend_from_slice(&23i32.to_be_bytes()); // type OID
    body.extend_from_slice(&4i16.to_be_bytes()); // type size
    body.extend_from_slice(&(-1i32).to_be_bytes()); // type mod
    body.extend_from_slice(&0i16.to_be_bytes()); // format
    let fields = parse_row_desc(&body).unwrap();
    assert_eq!(fields, vec![(Cow::Borrowed("id"), 23)]);
}

#[test]
fn notification_parsing() {
    let mut body = Vec::new();
    body.extend_from_slice(&42i32.to_be_bytes());
    body.extend_from_slice(b"chan\0payload\0");
    let n = parse_notification(&body).unwrap();
    assert_eq!(n.pid, 42);
    assert_eq!(n.channel, "chan");
    assert_eq!(n.payload, "payload");
}

#[test]
fn conflict_clause_accepts_valid() {
    assert!(
        validate_conflict_clause("ON CONFLICT (uid,key) DO UPDATE SET value = EXCLUDED.value")
            .is_ok()
    );
    // Case-insensitive prefix and surrounding whitespace are allowed.
    assert!(validate_conflict_clause("  on conflict do nothing  ").is_ok());
    assert!(validate_conflict_clause("ON CONFLICT ON CONSTRAINT pk DO NOTHING").is_ok());
}

#[test]
fn conflict_clause_rejects_bad_prefix() {
    assert!(validate_conflict_clause("").is_err());
    assert!(validate_conflict_clause("DO NOTHING").is_err());
    assert!(validate_conflict_clause("ON CONF").is_err()); // shorter than prefix
    // A whole injected statement that doesn't start with ON CONFLICT.
    assert!(validate_conflict_clause("; DROP TABLE users").is_err());
}

#[test]
fn conflict_clause_rejects_injection_tokens() {
    assert!(validate_conflict_clause("ON CONFLICT DO NOTHING; DROP TABLE users").is_err());
    assert!(validate_conflict_clause("ON CONFLICT DO NOTHING -- comment").is_err());
    assert!(validate_conflict_clause("ON CONFLICT DO NOTHING /* block */").is_err());
}

#[test]
fn build_insert_sql_numbers_placeholders_and_appends_conflict() {
    let cols = vec!["a".to_string(), "b".to_string()];
    let sql = build_insert_sql("t", &cols, 2, None);
    assert_eq!(
        sql,
        "INSERT INTO \"t\" (\"a\",\"b\") VALUES ($1,$2),($3,$4)"
    );

    let sql = build_insert_sql("t", &cols, 1, Some("ON CONFLICT DO NOTHING"));
    assert_eq!(
        sql,
        "INSERT INTO \"t\" (\"a\",\"b\") VALUES ($1,$2) ON CONFLICT DO NOTHING"
    );
}

#[test]
fn quote_ident_escapes_double_quotes() {
    assert_eq!(quote_ident("col"), "\"col\"");
    assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
}

// -- quote_ident edge cases -----------------------------------------------

#[test]
fn quote_ident_empty_string() {
    assert_eq!(quote_ident(""), "\"\"");
}

#[test]
fn quote_ident_multiple_double_quotes() {
    assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");
}

#[test]
fn quote_ident_special_chars() {
    assert_eq!(quote_ident("my col"), "\"my col\"");
    assert_eq!(quote_ident("table-name"), "\"table-name\"");
}

// -- validate_type_name ---------------------------------------------------

#[test]
fn validate_type_name_valid() {
    assert!(validate_type_name("bigint").is_ok());
    assert!(validate_type_name("character varying").is_ok());
    assert!(validate_type_name("integer[]").is_ok());
    assert!(validate_type_name("text").is_ok());
    assert!(validate_type_name("double_precision").is_ok());
    for ty in [
        "DOUBLE PRECISION[]",
        "timestamp with time zone[][]",
        "my_enum[]",
        " int4 [] ",
    ] {
        assert!(validate_type_name(ty).is_ok(), "{ty}");
    }
}

#[test]
fn validate_type_name_rejects_empty() {
    assert!(validate_type_name("").is_err());
}

#[test]
fn validate_type_name_rejects_injection() {
    assert!(validate_type_name("int; DROP TABLE").is_err());
    assert!(validate_type_name("int--comment").is_err());
    assert!(validate_type_name("int'").is_err());
    for ty in [
        "bigint OR true",
        "boolean OR true",
        "int4[] IS NULL",
        "int4[1]",
        "[]",
        "1int",
        "text COLLATE default",
        "text FROM users",
    ] {
        assert!(validate_type_name(ty).is_err(), "{ty}");
    }
}

// -- build_update_sql -----------------------------------------------------

#[test]
fn build_update_sql_single_row_with_key_type() {
    let set_cols = vec!["name".to_string(), "value".to_string()];
    let sql = build_update_sql("items", "id", &set_cols, 1, Some("bigint"), None);
    assert_eq!(
        sql,
        "UPDATE \"items\" AS _t SET \"name\" = _d.\"name\", \"value\" = _d.\"value\" \
             FROM (VALUES ($1,$2,$3)) AS _d(_k, \"name\", \"value\") \
             WHERE _t.\"id\" = _d._k::bigint"
    );
}

#[test]
fn build_update_sql_multi_row_without_key_type() {
    let set_cols = vec!["score".to_string()];
    let sql = build_update_sql("players", "uid", &set_cols, 3, None, None);
    assert_eq!(
        sql,
        "UPDATE \"players\" AS _t SET \"score\" = _d.\"score\" \
             FROM (VALUES ($1,$2),($3,$4),($5,$6)) AS _d(_k, \"score\") \
             WHERE _t.\"uid\"::text = _d._k"
    );
}

#[test]
fn build_update_sql_quoted_identifiers() {
    let set_cols = vec!["col\"x".to_string()];
    let sql = build_update_sql("my\"table", "k\"ey", &set_cols, 1, None, None);
    assert!(sql.contains("\"my\"\"table\""));
    assert!(sql.contains("\"k\"\"ey\""));
    assert!(sql.contains("\"col\"\"x\""));
}

// -- build_insert_sql edge cases ------------------------------------------

#[test]
fn build_insert_sql_single_column_single_row() {
    let cols = vec!["x".to_string()];
    let sql = build_insert_sql("t", &cols, 1, None);
    assert_eq!(sql, "INSERT INTO \"t\" (\"x\") VALUES ($1)");
}

#[test]
fn build_insert_sql_many_rows() {
    let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let sql = build_insert_sql("t", &cols, 3, None);
    assert_eq!(
        sql,
        "INSERT INTO \"t\" (\"a\",\"b\",\"c\") VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9)"
    );
}

// -- parse_command_tag additional cases ------------------------------------

#[test]
fn command_tag_delete() {
    assert_eq!(
        parse_command_tag(b"DELETE 10\0"),
        ("DELETE".into(), Some(10))
    );
}

#[test]
fn command_tag_create_table() {
    assert_eq!(
        parse_command_tag(b"CREATE TABLE\0"),
        ("CREATE".into(), None)
    );
}

#[test]
fn command_tag_copy() {
    assert_eq!(parse_command_tag(b"COPY 100\0"), ("COPY".into(), Some(100)));
}

#[test]
fn command_tag_no_null_terminator() {
    let (cmd, rows) = parse_command_tag(b"UPDATE 5");
    assert_eq!(cmd, "UPDATE");
    assert_eq!(rows, Some(5));
}

// -- parse_error additional fields ----------------------------------------

#[test]
fn error_response_all_fields() {
    let body = b"SERROR\0C42P01\0Mtable not found\0P15\0Dmore info\0sfoo_schema\0tbar_table\0nmy_constraint\0\0";
    let err = parse_error(body);
    assert_eq!(err.severity.as_deref(), Some("ERROR"));
    assert_eq!(err.code.as_deref(), Some("42P01"));
    assert_eq!(err.message.as_deref(), Some("table not found"));
    assert_eq!(err.position.as_deref(), Some("15"));
    assert_eq!(err.detail.as_deref(), Some("more info"));
    assert_eq!(err.schema.as_deref(), Some("foo_schema"));
    assert_eq!(err.table.as_deref(), Some("bar_table"));
    assert_eq!(err.constraint.as_deref(), Some("my_constraint"));
}

#[test]
fn error_response_empty_body() {
    let err = parse_error(b"\0");
    assert!(err.severity.is_none());
    assert!(err.code.is_none());
    assert!(err.message.is_none());
}

#[test]
fn parse_error_string_missing_message_field() {
    let body = b"SERROR\0C12345\0\0"; // no M field
    assert_eq!(parse_error_string(body), "unknown database error");
}

// -- parse_notification edge cases ----------------------------------------

#[test]
fn notification_empty_payload() {
    let mut body = Vec::new();
    body.extend_from_slice(&1i32.to_be_bytes());
    body.extend_from_slice(b"test_channel\0\0");
    let n = parse_notification(&body).unwrap();
    assert_eq!(n.pid, 1);
    assert_eq!(n.channel, "test_channel");
    assert_eq!(n.payload, "");
}

#[test]
fn notification_too_short() {
    assert!(parse_notification(b"abc").is_none()); // < 5 bytes
}

#[test]
fn notification_with_unicode() {
    let mut body = Vec::new();
    body.extend_from_slice(&99i32.to_be_bytes());
    body.extend_from_slice("日本語\0メッセージ\0".as_bytes());
    let n = parse_notification(&body).unwrap();
    assert_eq!(n.pid, 99);
    assert_eq!(n.channel, "日本語");
    assert_eq!(n.payload, "メッセージ");
}

// -- parse_row_desc edge cases --------------------------------------------

#[test]
fn row_description_multiple_fields() {
    let mut body = Vec::new();
    body.extend_from_slice(&2u16.to_be_bytes()); // 2 fields
    // Field 1: "name", OID 25 (text)
    body.extend_from_slice(b"name\0");
    body.extend_from_slice(&0i32.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&25i32.to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    // Field 2: "age", OID 23 (int4)
    body.extend_from_slice(b"age\0");
    body.extend_from_slice(&0i32.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&23i32.to_be_bytes());
    body.extend_from_slice(&4i16.to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());

    let fields = parse_row_desc(&body).unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0], (Cow::Borrowed("name"), 25));
    assert_eq!(fields[1], (Cow::Borrowed("age"), 23));
}

#[test]
fn row_description_empty() {
    assert!(parse_row_desc(b"").is_err());
    assert!(parse_row_desc(b"\x00").is_err()); // < 2 bytes
}

#[test]
fn row_description_zero_fields() {
    let body = 0u16.to_be_bytes();
    let fields = parse_row_desc(&body).unwrap();
    assert!(fields.is_empty());
}

// -- parse_url edge cases -------------------------------------------------

#[test]
fn parse_url_ipv6_host() {
    let cfg = ConnectConfig::parse("postgres://user:pass@[::1]:5433/mydb?name=n").unwrap();
    assert_eq!(cfg.params.host, "[::1]");
    assert_eq!(cfg.params.port, 5433);
}

#[test]
fn parse_url_clamps_pool_and_queue() {
    let cfg = ConnectConfig::parse("postgres://u:p@h/d?name=n&max_connections=0&queue_capacity=0")
        .unwrap();
    assert_eq!(cfg.max_connections, 1);
    assert_eq!(cfg.queue_capacity, 1);
}

// -- start_message / end_message ------------------------------------------

#[test]
fn start_end_message_encodes_length_correctly() {
    let mut buf = Vec::new();
    let stub = start_message(&mut buf, b'Q');
    buf.extend_from_slice(b"SELECT 1\0");
    end_message(&mut buf, stub);
    // msg_type + 4 bytes length + payload
    assert_eq!(buf[0], b'Q');
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    // length includes itself (4 bytes) + "SELECT 1\0" (9 bytes) = 13
    assert_eq!(len, 13);
}

#[test]
fn append_sync_produces_5_bytes() {
    let mut buf = Vec::new();
    append_sync(&mut buf);
    assert_eq!(buf.len(), 5); // 'S' + 4-byte length(4)
    assert_eq!(buf[0], PQ_SYNC);
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    assert_eq!(len, 4);
}

#[test]
fn append_parse_unnamed_format() {
    let mut buf = Vec::new();
    append_parse_unnamed(&mut buf, b"SELECT $1");
    assert_eq!(buf[0], PQ_PARSE);
    // Body: length(4) + "" NUL (1) + "SELECT $1" NUL (10) + 0u16 (2) = 17
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    assert_eq!(len, 17);
    // stmt name = empty string + NUL
    assert_eq!(buf[5], 0); // empty statement name
    // query starts at 6
    assert_eq!(&buf[6..15], b"SELECT $1");
    assert_eq!(buf[15], 0); // query NUL terminator
}

// -- md5_hex additional ---------------------------------------------------

#[test]
fn md5_hex_hello_world() {
    assert_eq!(md5_hex(b"hello"), "5d41402abc4b2a76b9719d911017c592");
}

// -- conflict_clause edge cases -------------------------------------------

#[test]
fn conflict_clause_case_variations() {
    assert!(validate_conflict_clause("On Conflict DO NOTHING").is_ok());
    assert!(validate_conflict_clause("on conflict do nothing").is_ok());
}

#[test]
fn conflict_clause_rejects_block_comment_at_end() {
    assert!(validate_conflict_clause("ON CONFLICT DO NOTHING /**/").is_err());
}

// -- write_cstr -----------------------------------------------------------

#[test]
fn write_cstr_appends_nul() {
    let mut buf = Vec::new();
    write_cstr(&mut buf, b"hello");
    assert_eq!(buf, b"hello\0");
}

#[test]
fn write_cstr_empty() {
    let mut buf = Vec::new();
    write_cstr(&mut buf, b"");
    assert_eq!(buf, b"\0");
}

// -- read_i32 / read_u16 --------------------------------------------------

#[test]
fn read_i32_big_endian() {
    let buf = [0x00, 0x01, 0x00, 0x00];
    assert_eq!(read_i32(&buf, 0), 65536);
}

#[test]
fn read_i32_negative() {
    let buf = (-1i32).to_be_bytes();
    assert_eq!(read_i32(&buf, 0), -1);
}

#[test]
fn read_u16_big_endian() {
    let buf = [0x01, 0x00];
    assert_eq!(read_u16(&buf, 0), 256);
}
