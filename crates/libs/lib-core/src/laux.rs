pub use lib_lua as ffi;
use std::ffi::{c_char, c_int};

use crate::c_str;

use super::buffer::Buffer;

pub extern "C-unwind" fn lua_null_function(_: *mut ffi::lua_State) -> c_int {
    0
}

pub struct LuaScopePop {
    state: *mut ffi::lua_State,
}

impl LuaScopePop {
    pub fn new(state: *mut ffi::lua_State) -> Self {
        LuaScopePop { state }
    }
}

impl Drop for LuaScopePop {
    fn drop(&mut self) {
        unsafe {
            ffi::lua_pop(self.state, 1);
        }
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn lua_traceback(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        let msg = ffi::lua_tostring(state, 1);
        if !msg.is_null() {
            ffi::luaL_traceback(state, state, msg, 1);
        } else {
            ffi::lua_pushliteral(state, "(no error message)");
        }
        1
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_vec(state: *mut ffi::lua_State, index: c_int) -> Vec<u8> {
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_checklstring(state, index, &mut len);
        let slice = std::slice::from_raw_parts(ptr as *const u8, len);
        slice.to_vec()
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_slice(state: *mut ffi::lua_State, index: c_int) -> &'static [u8] {
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_checklstring(state, index, &mut len);
        std::slice::from_raw_parts(ptr as *const u8, len)
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_str(state: *mut ffi::lua_State, index: i32) -> &'static str {
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_checklstring(state, index, &mut len);
        let slice = std::slice::from_raw_parts(ptr as *const u8, len);
        std::str::from_utf8_unchecked(slice)
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn push_str(state: *mut ffi::lua_State, v: &str) {
    unsafe {
        ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn push_string(state: *mut ffi::lua_State, v: &String) {
    unsafe {
        ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn opt_str(state: *mut ffi::lua_State, index: i32, def: &'static str) -> &'static str {
    unsafe {
        if ffi::lua_isnil(state, index) != 0 {
            def
        } else {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice)
        }
    }
}

pub trait LuaValue {
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> Self;

    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: Self) -> Self;

    fn push_lua(state: *mut ffi::lua_State, v: Self);
}

impl LuaValue for &str {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> &'static str {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: &str) -> &str {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                def
            } else {
                let mut len = 0;
                let ptr = ffi::luaL_checklstring(state, index, &mut len);
                let slice = std::slice::from_raw_parts(ptr as *const u8, len);
                std::str::from_utf8_unchecked(slice)
            }
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: &str) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaValue for String {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> String {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice).to_string()
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: String) -> String {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                def.to_owned()
            } else {
                let mut len = 0;
                let ptr = ffi::luaL_checklstring(state, index, &mut len);
                let slice = std::slice::from_raw_parts(ptr as *const u8, len);
                std::str::from_utf8_unchecked(slice).to_string()
            }
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: String) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaValue for bool {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> bool {
        unsafe {
            ffi::luaL_checktype(state, index, ffi::LUA_TBOOLEAN);
            ffi::lua_toboolean(state, index) != 0
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: bool) -> bool {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                def
            } else {
                ffi::lua_toboolean(state, index) != 0
            }
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: bool) {
        unsafe {
            ffi::lua_pushboolean(state, v as c_int);
        }
    }
}

impl LuaValue for i32 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> i32 {
        unsafe { ffi::luaL_checkinteger(state, index) as i32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: i32) -> i32 {
        unsafe { ffi::luaL_optinteger(state, index, def as ffi::lua_Integer) as i32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: i32) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for u32 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> u32 {
        unsafe { ffi::luaL_checkinteger(state, index) as u32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: u32) -> u32 {
        unsafe { ffi::luaL_optinteger(state, index, def as ffi::lua_Integer) as u32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: u32) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for usize {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> usize {
        unsafe { ffi::luaL_checkinteger(state, index) as usize }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: usize) -> usize {
        unsafe { ffi::luaL_optinteger(state, index, def as ffi::lua_Integer) as usize }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: usize) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for i64 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> i64 {
        unsafe { ffi::luaL_checkinteger(state, index) as i64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: i64) -> i64 {
        unsafe { ffi::luaL_optinteger(state, index, def) as i64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: i64) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for u64 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> u64 {
        unsafe { ffi::luaL_checkinteger(state, index) as u64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: u64) -> u64 {
        unsafe { ffi::luaL_optinteger(state, index, def as ffi::lua_Integer) as u64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: u64) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for f64 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> f64 {
        unsafe { ffi::luaL_checknumber(state, index) as f64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: f64) -> f64 {
        unsafe { ffi::luaL_optnumber(state, index, def) as f64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: f64) {
        unsafe {
            ffi::lua_pushnumber(state, v as ffi::lua_Number);
        }
    }
}

impl LuaValue for &[u8] {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32, def: &[u8]) -> &[u8] {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                def
            } else {
                let mut len = 0;
                let ptr = ffi::luaL_checklstring(state, index, &mut len);
                std::slice::from_raw_parts(ptr as *const u8, len)
            }
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: &[u8]) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn opt_field<T>(state: *mut ffi::lua_State, mut index: i32, field: &str, def: T) -> T
where
    T: LuaValue,
{
    if index < 0 {
        unsafe {
            index = ffi::lua_gettop(state) + index + 1;
        }
    }

    let bytes = field.as_bytes();
    let _scope = LuaScopePop::new(state);
    unsafe {
        ffi::lua_pushlstring(state, bytes.as_ptr() as *const c_char, bytes.len());
        if ffi::lua_rawget(state, index) <= ffi::LUA_TNIL {
            return def;
        }
    }

    LuaValue::from_lua_opt(state, -1, def)
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_type(state: *mut ffi::lua_State, index: i32) -> i32 {
    unsafe { ffi::lua_type(state, index) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_error(state: *mut ffi::lua_State, message: &String) -> ! {
    unsafe {
        ffi::lua_pushlstring(state, message.as_ptr() as *const c_char, message.len());
        ffi::lua_error(state)
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn type_name(state: *mut ffi::lua_State, ltype: i32) -> &'static str {
    unsafe {
        std::ffi::CStr::from_ptr(ffi::lua_typename(state, ltype))
            .to_str()
            .unwrap_or_default()
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_buffer(state: *mut ffi::lua_State, index: i32) -> Option<Box<Buffer>> {
    match lua_type(state, index) {
        ffi::LUA_TSTRING => Some(Box::new(check_vec(state, index).into())),
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
                    c_str!("nil, lightuserdata(buffer*) or string expected"),
                )
            };
            None
        }
    }
}
