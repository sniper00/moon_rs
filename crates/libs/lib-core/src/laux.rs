pub use lib_lua as ffi;
use std::ffi::{c_char, c_int};

use crate::c_str;

use super::buffer::Buffer;

#[derive(PartialEq)]
pub struct LuaState(pub *mut ffi::lua_State);

unsafe impl Send for LuaState {}

impl LuaState {
    pub fn new(l: *mut ffi::lua_State) -> Self {
        LuaState(l)
    }
}

impl Drop for LuaState {
    fn drop(&mut self) {
        unsafe {
            if !self.0.is_null() {
                ffi::lua_close(self.0);
            }
        }
    }
}

pub struct LuaStateRef(pub *mut ffi::lua_State);

impl LuaStateRef {

    #[inline]
    pub fn new(state: *mut ffi::lua_State) -> Self {
        LuaStateRef(state)
    }

    #[inline]
    pub fn get<T>(&self, index: i32) -> T where T: LuaValue {
        LuaValue::from_lua_check(self.0, index)
    }

    #[inline]
    pub fn opt<T>(&self, index: i32) -> Option<T> where T: LuaValue {
        LuaValue::from_lua_opt(self.0, index)
    }

    #[inline]
    pub fn push<T>(&self, v: T) where T: LuaValue {
        LuaValue::push_lua(self.0, v);
    }

    #[inline]
    pub fn push_nil(&self) {
        unsafe {
            ffi::lua_pushnil(self.0);
        }
    }

    #[inline]
    pub fn ltype(&self, index: i32) -> i32 {
        unsafe {
            ffi::lua_type(self.0, index)
        }
    }

    #[inline]
    pub fn check_slice(&self, index: i32) -> &'static [u8] {
        check_slice(self.0, index)
    }

    #[inline]
    pub fn error(&self, message: &str) -> ! {
        lua_error(self.0, message);
    }

    #[inline]
    pub fn is_integer(&self, index:i32)->bool {
        unsafe{
            ffi::lua_isinteger(self.0, index)!=0
        }
    }

    #[inline]
    pub fn checktype(&self, index: i32, ltype: i32) {
        unsafe {
            ffi::luaL_checktype(self.0, index, ltype);
        }
    }

    #[inline]
    pub fn top(&self) -> i32 {
        unsafe {
            ffi::lua_gettop(self.0)
        }
    }

    #[inline]
    pub fn pop(&self, n: i32) {
        unsafe {
            ffi::lua_pop(self.0, n);
        }
    }

    #[inline]
    pub fn to_slice(&self, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_tolstring(self.0, index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    #[inline]
    pub fn to_str(&self, index: i32) -> &'static str {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_tolstring(self.0, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice)
        }
    }
}

#[derive(PartialEq)]
pub struct LuaThread(pub *mut ffi::lua_State);

unsafe impl Send for LuaThread {}

impl LuaThread {
    pub fn new(l: *mut ffi::lua_State) -> Self {
        LuaThread(l)
    }
}

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

    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<Self> where Self: Sized;

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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<&'static str> {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                None
            } else {
                let mut len = 0;
                let ptr = ffi::luaL_checklstring(state, index, &mut len);
                let slice = std::slice::from_raw_parts(ptr as *const u8, len);
                Some(std::str::from_utf8_unchecked(slice))
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<String> {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                None
            } else {
                let mut len = 0;
                let ptr = ffi::luaL_checklstring(state, index, &mut len);
                let slice = std::slice::from_raw_parts(ptr as *const u8, len);
                Some(std::str::from_utf8_unchecked(slice).to_string())
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<bool> {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                None
            } else {
                Some(ffi::lua_toboolean(state, index) != 0)
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

impl LuaValue for i8 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> i8 {
        unsafe { ffi::luaL_checkinteger(state, index) as i8 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<i8> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as i8 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: i8) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for u8 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: *mut ffi::lua_State, index: i32) -> u8 {
        unsafe { ffi::luaL_checkinteger(state, index) as u8 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<u8> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as u8 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: *mut ffi::lua_State, v: u8) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<i32> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as i32 })
        }
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<u32> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as u32 })
        }
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<usize> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as usize })
        }
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<i64> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as i64 })
        }
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<u64> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as u64 })
        }
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<f64> {
        if unsafe { ffi::lua_isnumber(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tonumber(state, index) as f64 })
        }
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
    fn from_lua_opt(state: *mut ffi::lua_State, index: i32) -> Option<&'static [u8]> {
        unsafe {
            if ffi::lua_isnil(state, index) != 0 {
                None
            } else {
                let mut len = 0;
                let ptr = ffi::luaL_checklstring(state, index, &mut len);
                Some(std::slice::from_raw_parts(ptr as *const u8, len))
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
pub fn opt_field<T>(state: *mut ffi::lua_State, mut index: i32, field: &str) -> Option<T>
where
    T: LuaValue,
{
    if index < 0 {
        unsafe {
            index = ffi::lua_gettop(state) + index + 1;
        }
    }

    let _scope = LuaScopePop::new(state);
    unsafe {
        ffi::lua_pushlstring(state, field.as_ptr() as *const c_char, field.len());
        if ffi::lua_rawget(state, index) <= ffi::LUA_TNIL {
            return None;
        }
    }

    LuaValue::from_lua_opt(state, -1)
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_type(state: *mut ffi::lua_State, index: i32) -> i32 {
    unsafe { ffi::lua_type(state, index) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_error(state: *mut ffi::lua_State, message: &str) -> ! {
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
