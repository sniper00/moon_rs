use buffer::Buffer;
use lib_lua::{
    cstr, ffi,
    laux::{lua_get, lua_type, LuaStateRaw},
};

pub mod actor;
pub mod buffer;
pub mod context;
pub mod error;
pub mod log;

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_buffer(state: LuaStateRaw, index: i32) -> Option<Box<Buffer>> {
    match lua_type(state, index) {
        ffi::LUA_TSTRING => Some(Box::new(lua_get::<&[u8]>(state, index).into())),
        ffi::LUA_TLIGHTUSERDATA => unsafe {
            let ptr = ffi::lua_touserdata(state, index) as *mut Buffer;
            Some(Box::from_raw(ptr))
        },
        ffi::LUA_TNIL => None,
        _ => {
            unsafe {
                ffi::luaL_argerror(
                    state,
                    index,
                    cstr!("nil, lightuserdata(buffer*) or string expected"),
                )
            };
            None
        }
    }
}
