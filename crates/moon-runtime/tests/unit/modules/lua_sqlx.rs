use super::*;
use moon_base::laux::LuaGlobalState;
use std::ptr::NonNull;

#[test]
fn decimal_integer_preserves_fractions_and_range() {
    for (text, expected) in [
        ("42.000", Some(42)),
        ("-42", Some(-42)),
        ("1e3", Some(1000)),
        ("0.001", None),
        ("-1.9", None),
        ("9223372036854775807", Some(i64::MAX)),
        ("-9223372036854775808", Some(i64::MIN)),
        ("9223372036854775808", None),
        ("9223372036854775807.1", None),
    ] {
        assert_eq!(decimal_integer(&text.parse().unwrap()), expected, "{text}");
    }
}

#[tokio::test]
async fn sqlite_rows_preserve_binary_and_timestamp_without_assuming_timezone() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(
        "CREATE TABLE sample (payload BLOB, created TIMESTAMP)",
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(
        "INSERT INTO sample VALUES (X'00FF41', '2026-10-17 12:34:56.123456')",
    ))
    .execute(&pool)
    .await
    .unwrap();
    let rows = sqlx::query(sqlx::AssertSqlSafe("SELECT * FROM sample"))
        .fetch_all(&pool)
        .await
        .unwrap();
    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).unwrap();
    let _owner = LuaGlobalState::new(state);
    assert_eq!(process_rows::<Sqlite>(state, &rows), 1);
    unsafe {
        ffi::lua_rawgeti(state.as_ptr(), -1, 1);
        ffi::lua_getfield(state.as_ptr(), -1, cstr!("payload"));
    }
    let lua = unsafe { LuaStack::from_raw(state) };
    assert_eq!(lua.value(-1).as_bytes().unwrap(), b"\x00\xffA");
    unsafe {
        ffi::lua_settop(state.as_ptr(), 2);
        ffi::lua_getfield(state.as_ptr(), -1, cstr!("created"));
    }
    assert_eq!(
        lua.value(-1).as_str().unwrap(),
        "2026-10-17T12:34:56.123456"
    );
}

fn lua_table(source: &str) -> (LuaState, LuaGlobalState) {
    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).unwrap();
    let guard = LuaGlobalState::new(state);
    unsafe { ffi::luaL_openlibs(state.as_ptr()) };
    let code = std::ffi::CString::new(format!("return {source}")).unwrap();
    assert_eq!(
        unsafe { ffi::luaL_dostring(state.as_ptr(), code.as_ptr()) },
        0
    );
    (state, guard)
}

#[test]
fn direct_table_params_match_text_encoding_semantics() {
    for source in [
        "{}",
        "{1, true, 'text', {a='hello\\nworld'}}",
        "{[1]='number', ['1']='string', other={}}",
        "setmetatable({}, {__object=false})",
        "setmetatable({[1]='one', [3]='three'}, {__array=false})",
        "setmetatable({hidden=function() end}, {__array=true,__object=true})",
        "{string.char(255), nested={1}}",
        "{[string.char(255)]='binary key'}",
        "{f=1.0, zero=-0.0, large=9223372036854775807, small=-9223372036854775807-1}",
        "{1.7976931348623157e308, 2.2250738585072014e-308, 5e-324, 1.2345678901234567}",
        "(function() local t={} for i=1,1000 do t[i]={name='value'..i,n=i/7} end return t end)()",
    ] {
        let (state, _guard) = lua_table(source);
        let mut lua = unsafe { LuaStack::from_raw(state) };
        let mut encoded = Vec::new();
        encode_table(&mut encoded, &mut lua, 1, 0, false, &JsonOptions::default()).unwrap();
        let parsed = serde_json::from_slice::<serde_json::Value>(&encoded);
        let actual = get_query_param(&mut lua, 1).unwrap();
        match (parsed, actual) {
            (Ok(expected), QueryParams::Json(actual)) => {
                assert_eq!(actual.to_string(), expected.to_string(), "{source}");
            }
            (Err(_), QueryParams::Bytes(actual)) => assert_eq!(actual, encoded, "{source}"),
            (_, actual) => panic!("unexpected parameter for {source}: {actual:?}"),
        }
        assert_eq!(laux::lua_top(state), 1, "{source}");
    }
}

#[test]
fn direct_table_param_errors_preserve_encoder_errors_and_stack() {
    for source in [
        "{x=function() end}",
        "{[true]=1}",
        "{math.huge}",
        "{0/0}",
        "(function() local t={} t.self=t return t end)()",
        "{bad=string.char(255), invalid=function() end}",
    ] {
        let (state, _guard) = lua_table(source);
        let mut lua = unsafe { LuaStack::from_raw(state) };
        let expected = encode_table(
            &mut Vec::new(),
            &mut lua,
            1,
            0,
            false,
            &JsonOptions::default(),
        )
        .unwrap_err();
        let actual = get_query_param(&mut lua, 1).unwrap_err();
        assert_eq!(actual, expected, "{source}");
        assert_eq!(laux::lua_top(state), 1, "{source}");
    }
}

#[tokio::test]
async fn borrowed_bindings_survive_source_drop_and_can_be_reused() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let binds = vec![
        QueryParams::Text("hello世界".into()),
        QueryParams::Json(serde_json::json!({"nested": [1, true, "value"]})),
        QueryParams::Bytes(vec![0, 255, 1]),
    ];
    let sql = Arc::new("SELECT ? AS text, ? AS document, ? AS bytes".to_string());
    let first = DatabasePool::make_query::<Sqlite>(sql.clone(), &binds).unwrap();
    let second = DatabasePool::make_query::<Sqlite>(sql, &binds).unwrap();
    drop(binds);
    for query in [first, second] {
        let row = query.fetch_one(&pool).await.unwrap();
        assert_eq!(row.get::<String, _>("text"), "hello世界");
        assert_eq!(
            row.get::<serde_json::Value, _>("document"),
            serde_json::json!({"nested": [1, true, "value"]})
        );
        assert_eq!(row.get::<Vec<u8>, _>("bytes"), [0, 255, 1]);
    }
}
