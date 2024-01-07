//! Low level bindings to Lua 5.4.

use std::os::raw::c_int;

pub use lauxlib::*;
pub use lua::*;
pub use lualib::*;

pub mod lauxlib;
pub mod lua;
pub mod lualib;

pub type Callback = extern "C-unwind" fn(i32);

extern "C-unwind" {
    pub fn lua_json_decode(L: *mut lua_State) -> c_int;
    pub fn luaopen_serialize(L: *mut lua_State) -> c_int;
}
