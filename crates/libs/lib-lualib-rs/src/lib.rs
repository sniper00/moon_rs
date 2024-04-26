use lib_lua::{cstr, ffi};

mod lua_buffer;
mod lua_excel;
mod lua_fs;
mod lua_http;
mod lua_json;
mod lua_seri;
mod lua_socket;
mod lua_utils;

pub mod lua_actor;

macro_rules! lua_require {
    ($state:expr, $name:expr, $fn:expr) => {
        unsafe {
            ffi::luaL_requiref($state, cstr!($name), $fn, 0);
            ffi::lua_pop($state, 1);
        }
    };
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn luaopen_custom_libs(state: *mut ffi::lua_State) {
    lua_require!(state, "http.core", lua_http::luaopen_http);
    lua_require!(state, "net.core", lua_socket::luaopen_socket);
    lua_require!(state, "excel", lua_excel::luaopen_excel);
    lua_require!(state, "fs", lua_fs::luaopen_fs);
    lua_require!(state, "json", lua_json::luaopen_json);
    lua_require!(state, "buffer", lua_buffer::luaopen_buffer);
    lua_require!(state, "seri", lua_seri::luaopen_seri);
}
