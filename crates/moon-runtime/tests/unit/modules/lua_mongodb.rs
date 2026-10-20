use super::*;
use moon_base::laux::LuaGlobalState;
use std::ptr::{NonNull, null_mut};

fn new_state() -> (LuaState, LuaGlobalState) {
    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).expect("Lua state allocation");
    let owner = LuaGlobalState::new(state);
    (state, owner)
}

unsafe fn rawset(state: LuaState, key: &str, push_value: impl FnOnce()) {
    laux::lua_push(state, key);
    push_value();
    unsafe { ffi::lua_rawset(state.as_ptr(), -3) };
}

#[test]
fn nested_bson_conversion_restores_the_lua_stack() {
    let (state, _owner) = new_state();
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, 4);
        rawset(state, "_id", || {
            laux::lua_push(state, "507f1f77bcf86cd799439011")
        });
        rawset(state, "name", || laux::lua_push(state, "moon"));
        rawset(state, "nested", || {
            ffi::lua_createtable(state.as_ptr(), 0, 3);
            rawset(state, "enabled", || laux::lua_push(state, true));
            rawset(state, "values", || {
                ffi::lua_createtable(state.as_ptr(), 3, 0);
                for value in 1_i64..=3 {
                    laux::lua_push(state, value);
                    ffi::lua_rawseti(state.as_ptr(), -2, value);
                }
            });
            rawset(state, "empty", || {
                ffi::lua_createtable(state.as_ptr(), 0, 0)
            });
        });
    }

    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    let document = table_to_doc(&mut lua, 1).expect("nested document conversion");

    assert_eq!(lua.top(), top);
    assert_eq!(
        document,
        doc! {
            "_id": oid::ObjectId::from_str("507f1f77bcf86cd799439011").unwrap(),
            "name": "moon",
            "nested": {
                "enabled": true,
                "values": [1_i64, 2_i64, 3_i64],
                "empty": {},
            },
        }
    );
}

#[test]
fn nested_bson_error_restores_the_lua_stack() {
    let (state, _owner) = new_state();
    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, 1);
        rawset(state, "nested", || {
            ffi::lua_createtable(state.as_ptr(), 0, 1);
            rawset(state, "bad", || {
                ffi::lua_pushlightuserdata(state.as_ptr(), null_mut())
            });
        });
    }

    let mut lua = unsafe { LuaStack::from_raw(state) };
    let top = lua.top();
    let error = table_to_doc(&mut lua, 1).expect_err("unsupported nested Lua value");

    assert_eq!(error, "Invalid type: lightuserdata");
    assert_eq!(lua.top(), top);
}
