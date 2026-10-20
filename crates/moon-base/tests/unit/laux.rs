use super::*;

fn new_state() -> (LuaState, LuaGlobalState) {
    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).expect("Lua state allocation");
    let owner = LuaGlobalState::new(state);
    (state, owner)
}

fn push_one_entry(state: LuaState) {
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, 1);
        ffi::lua_pushliteral(state.as_ptr(), c"key");
        ffi::lua_pushinteger(state.as_ptr(), 42);
        ffi::lua_rawset(state.as_ptr(), -3);
    }
}

fn push_dense_array(state: LuaState) {
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 2, 0);
        ffi::lua_pushinteger(state.as_ptr(), 10);
        ffi::lua_rawseti(state.as_ptr(), -2, 1);
        ffi::lua_pushinteger(state.as_ptr(), 20);
        ffi::lua_rawseti(state.as_ptr(), -2, 2);
    }
}

#[test]
fn type_and_stack_checks_return_errors_without_raising() {
    let (state, _owner) = new_state();
    unsafe {
        ffi::lua_pushinteger(state.as_ptr(), 42);
        ffi::lua_pushnumber(state.as_ptr(), 42.0);
        ffi::lua_pushnumber(state.as_ptr(), 42.5);
    };

    assert!(lua_checktype(state, 1, ffi::LUA_TNUMBER).is_ok());
    let lua = unsafe { LuaStack::from_raw(state) };
    assert_eq!(lua.get::<i64>(1).unwrap(), 42);
    assert_eq!(lua.get::<i64>(2).unwrap(), 42);
    assert!(lua.get::<i64>(3).is_err());
    assert!(lua.get::<String>(1).is_err());
    assert_eq!(
        lua_checktype(state, 1, ffi::LUA_TTABLE).unwrap_err(),
        "table expected, got number"
    );
    assert!(lua_checkstack(state, 4, std::ptr::null()).is_ok());
    assert_eq!(
        lua_checkstack(state, i32::MAX, std::ptr::null()).unwrap_err(),
        "stack overflow"
    );
}

#[test]
fn lua_type_error_converts_to_string_for_callback_results() {
    let error = LuaTypeError {
        expected: "integer",
        actual: LuaType::String,
    };
    assert_eq!(String::from(error), "expected integer, got String");
}

#[test]
fn userdata_accessors_keep_full_and_light_userdata_distinct() {
    let (state, _owner) = new_state();
    let mut payload = 42i32;
    let light_ptr = (&mut payload as *mut i32).cast::<std::ffi::c_void>();
    unsafe {
        ffi::lua_pushlightuserdata(state.as_ptr(), light_ptr);
        ffi::lua_newuserdatauv(state.as_ptr(), std::mem::size_of::<i32>(), 0);
        ffi::lua_pushlightuserdata(state.as_ptr(), std::ptr::null_mut());
    }

    let lua = unsafe { LuaStack::from_raw(state) };
    assert!(lua.value(1).as_userdata::<i32>().is_none());
    assert_eq!(lua.value(1).as_light_userdata(), Some(light_ptr));

    assert!(lua.value(2).as_userdata::<i32>().is_some());
    assert!(lua.value(2).as_light_userdata().is_none());

    assert_eq!(lua.value(3).as_light_userdata(), Some(std::ptr::null_mut()));
}

#[test]
fn stack_value_append_only_borrow_reads_rooted_bytes() {
    let (state, _owner) = new_state();
    unsafe { ffi::lua_pushliteral(state.as_ptr(), c"rooted bytes") };

    let lua = unsafe { LuaStack::from_raw(state) };
    let value = lua.value(1);
    assert_eq!(value.kind(), LuaType::String);
    assert_eq!(value.as_bytes(), Some(b"rooted bytes".as_slice()));
    assert!(matches!(
        value.as_string_lossy(),
        Some(Cow::Borrowed("rooted bytes"))
    ));
    assert_eq!(
        unsafe { value.as_bytes_append_only() },
        Some(b"rooted bytes".as_slice())
    );
}

#[test]
fn owned_string_and_bytes_keep_utf8_semantics_distinct() {
    let (state, _owner) = new_state();
    let bytes = b"error:\xff";
    unsafe {
        ffi::lua_pushlstring(state.as_ptr(), bytes.as_ptr().cast::<c_char>(), bytes.len());
        ffi::lua_pushnil(state.as_ptr());
    }

    let lua = unsafe { LuaStack::from_raw(state) };
    assert_eq!(lua.get::<Vec<u8>>(1).unwrap(), bytes);
    assert!(lua.get::<String>(1).is_err());
    assert!(matches!(
        lua.value(1).as_string_lossy(),
        Some(Cow::Owned(_))
    ));
    assert_eq!(
        lua.value(1).as_string_lossy().as_deref(),
        Some(String::from_utf8_lossy(bytes).as_ref())
    );
    assert_eq!(lua.get::<Option<Vec<u8>>>(2).unwrap(), None);
    assert_eq!(lua.opt::<Vec<u8>>(1), Some(bytes.to_vec()));
    assert!(lua.get::<LuaNil>(1).is_err());
    assert!(lua.get::<LuaNil>(2).is_ok());
}

#[test]
fn optional_truthy_preserves_lua_boolean_conversion() {
    let (state, _owner) = new_state();
    unsafe {
        ffi::lua_pushnil(state.as_ptr());
        ffi::lua_pushboolean(state.as_ptr(), 0);
        ffi::lua_pushboolean(state.as_ptr(), 1);
        ffi::lua_pushinteger(state.as_ptr(), 0);
        ffi::lua_pushliteral(state.as_ptr(), c"");
        ffi::lua_createtable(state.as_ptr(), 0, 0);
    }

    let lua = unsafe { LuaStack::from_raw(state) };
    assert_eq!(lua.opt_truthy(99), None);
    assert_eq!(lua.opt_truthy(1), None);
    assert_eq!(lua.opt_truthy(2), Some(false));
    assert_eq!(lua.opt_truthy(3), Some(true));
    assert_eq!(lua.opt_truthy(4), Some(true));
    assert_eq!(lua.opt_truthy(5), Some(true));
    assert_eq!(lua.opt_truthy(6), Some(true));

    assert_eq!(lua.get::<Option<bool>>(1).unwrap(), None);
    assert_eq!(lua.get::<Option<bool>>(2).unwrap(), Some(false));
    assert!(lua.get::<Option<bool>>(4).is_err());
}

#[test]
fn strict_array_len_handles_dense_sparse_mixed_and_non_table_values() {
    let (state, _owner) = new_state();
    unsafe {
        push_dense_array(state); // 1: dense

        ffi::lua_createtable(state.as_ptr(), 1, 1); // 2: sparse
        ffi::lua_pushinteger(state.as_ptr(), 10);
        ffi::lua_rawseti(state.as_ptr(), -2, 1);
        ffi::lua_pushinteger(state.as_ptr(), 30);
        ffi::lua_rawseti(state.as_ptr(), -2, 3);

        ffi::lua_createtable(state.as_ptr(), 1, 1); // 3: mixed
        ffi::lua_pushinteger(state.as_ptr(), 10);
        ffi::lua_rawseti(state.as_ptr(), -2, 1);
        ffi::lua_pushliteral(state.as_ptr(), c"key");
        ffi::lua_pushinteger(state.as_ptr(), 20);
        ffi::lua_rawset(state.as_ptr(), -3);

        ffi::lua_createtable(state.as_ptr(), 0, 0); // 4: empty
        ffi::lua_pushliteral(state.as_ptr(), c"not a table"); // 5
    }

    let lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    assert_eq!(lua.array_len(1), 2);
    assert_eq!(lua.array_len(2), 0);
    assert_eq!(lua.array_len(3), 0);
    assert_eq!(lua.array_len(4), 0);
    assert_eq!(lua.array_len(5), 0);
    assert_eq!(lua.top(), top);
}

#[test]
fn field_accessors_and_cursor_restore_every_temporary_value() {
    let (state, _owner) = new_state();
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, 2);

        ffi::lua_pushliteral(state.as_ptr(), c"name");
        ffi::lua_pushliteral(state.as_ptr(), c"moon");
        ffi::lua_rawset(state.as_ptr(), -3);

        ffi::lua_pushliteral(state.as_ptr(), c"headers");
        ffi::lua_createtable(state.as_ptr(), 0, 1);
        ffi::lua_pushliteral(state.as_ptr(), c"x-test");
        ffi::lua_pushinteger(state.as_ptr(), 42);
        ffi::lua_rawset(state.as_ptr(), -3);
        ffi::lua_rawset(state.as_ptr(), -3);

        ffi::lua_pushinteger(state.as_ptr(), 7);
    }

    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    assert_eq!(lua.opt_field::<String>(1, "name").as_deref(), Some("moon"));
    assert_eq!(lua.opt_field::<String>(1, "missing"), None);
    assert_eq!(lua.opt_field::<String>(2, "name"), None);
    assert_eq!(lua.top(), top);

    {
        let mut cursor = lua
            .table_field_cursor(1, "headers")
            .expect("table-valued field");
        let (key, value) = cursor.next().expect("one header");
        assert_eq!(key.as_str(), Some("x-test"));
        assert_eq!(value.as_integer(), Some(42));
    }
    assert_eq!(lua.top(), top);
    assert!(lua.table_field_cursor(1, "name").is_none());
    assert!(lua.table_field_cursor(2, "headers").is_none());
    assert_eq!(lua.top(), top);
}

#[test]
fn cursors_over_non_tables_are_empty_and_leave_the_stack_unchanged() {
    let (state, _owner) = new_state();
    unsafe { ffi::lua_pushliteral(state.as_ptr(), c"not a table") };
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();

    {
        let mut cursor = lua.table_cursor(1);
        assert!(cursor.next().is_none());
        assert!(cursor.next().is_none());
    }
    {
        let mut cursor = lua.array_cursor_len(1, 3);
        assert!(cursor.next().is_none());
        assert!(cursor.next().is_none());
    }
    assert_eq!(lua.top(), top);
}

#[test]
fn cursor_lua_mut_allows_stack_neutral_ffi_work() {
    let (state, _owner) = new_state();
    push_one_entry(state);
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    {
        let mut cursor = lua.table_cursor(1);
        assert!(cursor.next().is_some());
        let frame_top = unsafe { ffi::lua_gettop(state.as_ptr()) };
        unsafe {
            let lua = cursor.lua_mut();
            lua.push(99);
            lua.pop(1);
        }
        assert_eq!(unsafe { ffi::lua_gettop(state.as_ptr()) }, frame_top);
        assert!(cursor.next().is_none());
    }
    assert_eq!(lua.top(), top);
}

#[test]
fn partially_consumed_cursors_restore_the_stack() {
    let (state, _owner) = new_state();
    push_dense_array(state);
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();

    {
        let mut cursor = lua.array_cursor(1);
        assert_eq!(cursor.next().and_then(|value| value.as_integer()), Some(10));
    }
    assert_eq!(lua.top(), top);

    {
        let mut cursor = lua.table_cursor(1);
        assert!(cursor.next().is_some());
    }
    assert_eq!(lua.top(), top);
}

#[test]
fn table_cursor_is_fused_and_restores_stack() {
    let (state, _owner) = new_state();
    push_one_entry(state);
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    {
        let mut cursor = lua.table_cursor(-1);
        let (key, value) = cursor.next().expect("one table entry");
        assert_eq!(key.as_str(), Some("key"));
        assert_eq!(value.as_integer(), Some(42));
        assert!(cursor.next().is_none());
        assert!(cursor.next().is_none());
    }
    assert_eq!(lua.top(), top);
}

#[test]
fn table_cursor_supports_non_retaining_for_loops() {
    let (state, _owner) = new_state();
    push_one_entry(state);
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    let mut seen = 0;

    for entry in lua.table_cursor(1) {
        let key = entry.key();
        let value = entry.value();
        assert_eq!(key.as_str(), Some("key"));
        assert_eq!(value.as_integer(), Some(42));
        seen += 1;
    }

    assert_eq!(seen, 1);
    assert_eq!(lua.top(), top);
}

#[test]
fn array_cursor_supports_nested_tables_and_restores_stack() {
    let (state, _owner) = new_state();
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 1, 0); // outer
        ffi::lua_createtable(state.as_ptr(), 1, 0); // inner
        ffi::lua_pushinteger(state.as_ptr(), 7);
        ffi::lua_rawseti(state.as_ptr(), -2, 1);
        ffi::lua_rawseti(state.as_ptr(), -2, 1);
    }

    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    {
        let mut outer = lua.array_cursor(-1);
        let inner_index = {
            let inner = outer.next().expect("outer array entry");
            assert_eq!(inner.kind(), LuaType::Table);
            inner.index()
        };
        {
            let mut inner = outer.nested_array(inner_index);
            assert_eq!(inner.next().and_then(|v| v.as_integer()), Some(7));
            assert!(inner.next().is_none());
        }
        assert!(outer.next().is_none());
        assert!(outer.next().is_none());
    }
    assert_eq!(lua.top(), top);
}

#[test]
fn metafield_presence_preserves_stack_for_absolute_and_relative_indices() {
    let (state, _owner) = new_state();
    let mut lua = unsafe { LuaStack::from_raw(state) };
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, 0);
    }
    assert!(!lua.has_metafield(-1, cstr!("__array")));
    assert_eq!(lua.top(), 1);
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, 1);
        ffi::lua_pushboolean(state.as_ptr(), 0);
        ffi::lua_setfield(state.as_ptr(), -2, cstr!("__array"));
        ffi::lua_setmetatable(state.as_ptr(), 1);
    }
    for index in [1, -1] {
        assert!(
            lua.has_metafield(index, cstr!("__array")),
            "false still means present"
        );
        assert!(!lua.has_metafield(index, cstr!("__object")));
        assert_eq!(lua.top(), 1);
    }
    lua_push(state, 42i64);
    assert!(lua.has_metafield(-2, cstr!("__array")));
    assert!(!lua.has_metafield(-1, cstr!("__array")));
    assert_eq!(lua.top(), 2);
    assert_eq!(lua.get::<i64>(2).unwrap(), 42);
}
