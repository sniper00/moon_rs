use super::*;

#[test]
fn stack_error_message_lossily_decodes_invalid_utf8() {
    unsafe {
        let raw = ffi::luaL_newstate();
        let state = LuaState::new(raw).expect("Lua state allocation failed");
        let bytes = b"error:\xff";
        ffi::lua_pushlstring(raw, bytes.as_ptr().cast(), bytes.len());

        let message = {
            let lua = LuaStack::from_raw(state);
            stack_error_message(&lua)
        };

        assert_eq!(message, "error:\u{fffd}");
        ffi::lua_close(raw);
    }
}
