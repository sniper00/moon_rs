use lib_lua::{cstr, ffi, laux::LuaState};

mod lua_buffer;
mod lua_excel;
mod lua_fs;
mod lua_http;
mod lua_seri;
mod lua_socket;
mod lua_utils;
mod lua_sqlx;
mod lua_mongodb;
pub mod lua_json;
pub mod lua_actor;

#[macro_export]
macro_rules! not_null_wrapper {
    ($fn:expr) => {
        {
            unsafe extern "C-unwind" fn func_wrapper(state: *mut ffi::lua_State) -> i32 {
                #[allow(unused_unsafe)]
                #[allow(clippy::macro_metavars_in_unsafe)]
                unsafe { $fn(LuaState::new(state).unwrap()) }
            }
            func_wrapper
        }
    };
}

#[macro_export]
macro_rules! lua_require {
    ($state:expr, $name:expr, $fn:expr) => {
        #[allow(unused_unsafe)]
        #[allow(clippy::macro_metavars_in_unsafe)]
        unsafe {
            ffi::luaL_requiref($state.as_ptr(), cstr!($name), not_null_wrapper!($fn), 0);
            ffi::lua_pop($state.as_ptr(), 1);
        }
    };
}

pub fn luaopen_custom_libs(state: LuaState) {
    lua_require!(state, "http.core", lua_http::luaopen_http);
    lua_require!(state, "net.core", lua_socket::luaopen_socket);
    lua_require!(state, "excel", lua_excel::luaopen_excel);
    lua_require!(state, "fs", lua_fs::luaopen_fs);
    lua_require!(state, "json", lua_json::luaopen_json);
    lua_require!(state, "buffer", lua_buffer::luaopen_buffer);
    lua_require!(state, "seri", lua_seri::luaopen_seri);
    lua_require!(state, "sqlx.core", lua_sqlx::luaopen_sqlx);
    lua_require!(state, "mongodb.core", lua_mongodb::luaopen_mongodb);
    lua_require!(state, "utils", lua_utils::luaopen_utils);
}
