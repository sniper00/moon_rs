use crate::ffi;
use std::ffi::{c_char, c_int};

pub type LuaStateRaw = *mut ffi::lua_State;

#[derive(PartialEq)]
pub struct LuaThread(pub LuaStateRaw);

unsafe impl Send for LuaThread {}

impl LuaThread {
    pub fn new(l: LuaStateRaw) -> Self {
        LuaThread(l)
    }
}

#[derive(PartialEq)]
pub struct LuaState(pub LuaStateRaw);

unsafe impl Send for LuaState {}

impl LuaState {
    pub fn new(l: LuaStateRaw) -> Self {
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

pub extern "C-unwind" fn lua_null_function(_: LuaStateRaw) -> c_int {
    0
}

pub struct LuaScopePop {
    state: LuaStateRaw,
}

impl LuaScopePop {
    pub fn new(state: LuaStateRaw) -> Self {
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
pub extern "C-unwind" fn lua_traceback(state: LuaStateRaw) -> c_int {
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

pub trait LuaValue {
    fn from_lua_check(state: LuaStateRaw, index: i32) -> Self;

    fn from_lua(state: LuaStateRaw, index: i32) -> Self;

    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<Self>
    where
        Self: Sized;

    fn push_lua(state: LuaStateRaw, v: Self);
}

impl LuaValue for bool {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> bool {
        unsafe {
            ffi::luaL_checktype(state, index, ffi::LUA_TBOOLEAN);
            ffi::lua_toboolean(state, index) != 0
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> bool {
        unsafe { ffi::lua_toboolean(state, index) != 0 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<bool> {
        unsafe {
            if ffi::lua_isnoneornil(state, index) != 0 {
                None
            } else {
                Some(ffi::lua_toboolean(state, index) != 0)
            }
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: bool) {
        unsafe {
            ffi::lua_pushboolean(state, v as c_int);
        }
    }
}

impl LuaValue for i8 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> i8 {
        unsafe { ffi::luaL_checkinteger(state, index) as i8 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> i8 {
        unsafe { ffi::lua_tointeger(state, index) as i8 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<i8> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as i8 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: i8) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for u8 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> u8 {
        unsafe { ffi::luaL_checkinteger(state, index) as u8 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> u8 {
        unsafe { ffi::lua_tointeger(state, index) as u8 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<u8> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as u8 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: u8) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for i32 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> i32 {
        unsafe { ffi::luaL_checkinteger(state, index) as i32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> i32 {
        unsafe { ffi::lua_tointeger(state, index) as i32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<i32> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as i32 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: i32) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for u32 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> u32 {
        unsafe { ffi::luaL_checkinteger(state, index) as u32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> u32 {
        unsafe { ffi::lua_tointeger(state, index) as u32 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<u32> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as u32 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: u32) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for usize {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> usize {
        unsafe { ffi::luaL_checkinteger(state, index) as usize }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> usize {
        unsafe { ffi::lua_tointeger(state, index) as usize }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<usize> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as usize })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: usize) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for isize {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> isize {
        unsafe { ffi::luaL_checkinteger(state, index) as isize }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> isize {
        unsafe { ffi::lua_tointeger(state, index) as isize }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<isize> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as isize })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: isize) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for i64 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> i64 {
        unsafe { ffi::luaL_checkinteger(state, index) as i64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> i64 {
        unsafe { ffi::lua_tointeger(state, index) as i64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<i64> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as i64 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: i64) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for u64 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> u64 {
        unsafe { ffi::luaL_checkinteger(state, index) as u64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> u64 {
        unsafe { ffi::lua_tointeger(state, index) as u64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<u64> {
        if unsafe { ffi::lua_isinteger(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tointeger(state, index) as u64 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: u64) {
        unsafe {
            ffi::lua_pushinteger(state, v as ffi::lua_Integer);
        }
    }
}

impl LuaValue for f64 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> f64 {
        unsafe { ffi::luaL_checknumber(state, index) as f64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> f64 {
        unsafe { ffi::lua_tonumber(state, index) as f64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<f64> {
        if unsafe { ffi::lua_isnumber(state, index) } == 0 {
            None
        } else {
            Some(unsafe { ffi::lua_tonumber(state, index) as f64 })
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_lua(state: LuaStateRaw, v: f64) {
        unsafe {
            ffi::lua_pushnumber(state, v as ffi::lua_Number);
        }
    }
}

impl LuaValue for &[u8] {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::lua_tolstring(state, index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<&'static [u8]> {
        unsafe {
            if ffi::lua_isnoneornil(state, index) != 0 {
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
    fn push_lua(state: LuaStateRaw, v: &[u8]) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaValue for &str {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> &'static str {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> &'static str {
        unsafe {
            let mut len = 0;
            let ptr = ffi::lua_tolstring(state, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<&'static str> {
        unsafe {
            if ffi::lua_isnoneornil(state, index) != 0 {
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
    fn push_lua(state: LuaStateRaw, v: &str) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaValue for String {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_check(state: LuaStateRaw, index: i32) -> String {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice).to_string()
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua(state: LuaStateRaw, index: i32) -> String {
        unsafe {
            let mut len = 0;
            let ptr = ffi::lua_tolstring(state, index, &mut len);
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            std::str::from_utf8_unchecked(slice).to_string()
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_lua_opt(state: LuaStateRaw, index: i32) -> Option<String> {
        unsafe {
            if ffi::lua_isnoneornil(state, index) != 0 {
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
    fn push_lua(state: LuaStateRaw, v: String) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn opt_field<T>(state: LuaStateRaw, mut index: i32, field: &str) -> Option<T>
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
pub fn lua_get<T>(state: LuaStateRaw, index: i32) -> T
where
    T: LuaValue,
{
    LuaValue::from_lua_check(state, index)
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_to<T>(state: LuaStateRaw, index: i32) -> T
where
    T: LuaValue,
{
    LuaValue::from_lua(state, index)
}

#[inline]
pub fn lua_opt<T>(state: LuaStateRaw, index: i32) -> Option<T>
where
    T: LuaValue,
{
    LuaValue::from_lua_opt(state, index)
}

#[inline]
pub fn lua_push<T>(state: LuaStateRaw, v: T)
where
    T: LuaValue,
{
    LuaValue::push_lua(state, v);
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_type(state: LuaStateRaw, index: i32) -> i32 {
    unsafe { ffi::lua_type(state, index) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_error(state: LuaStateRaw, message: &str) -> ! {
    unsafe {
        ffi::lua_pushlstring(state, message.as_ptr() as *const c_char, message.len());
        ffi::lua_error(state)
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn throw_error(state: LuaStateRaw) -> ! {
    unsafe { ffi::lua_error(state) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn type_name(state: LuaStateRaw, ltype: i32) -> &'static str {
    unsafe {
        std::ffi::CStr::from_ptr(ffi::lua_typename(state, ltype))
            .to_str()
            .unwrap_or_default()
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_pushnil(state: LuaStateRaw) {
    unsafe {
        ffi::lua_pushnil(state);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn is_integer(state: LuaStateRaw, index: i32) -> bool {
    unsafe { ffi::lua_isinteger(state, index) != 0 }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_top(state: LuaStateRaw) -> i32 {
    unsafe { ffi::lua_gettop(state) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_settop(state: LuaStateRaw, idx: i32) {
    unsafe {
        ffi::lua_settop(state, idx);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_pop(state: LuaStateRaw, n: i32) {
    unsafe {
        ffi::lua_pop(state, n);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_rawget(state: LuaStateRaw, index: i32) -> i32 {
    unsafe { ffi::lua_rawget(state, index) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_rawlen(state: LuaStateRaw, index: i32) -> usize {
    unsafe { ffi::lua_rawlen(state, index) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_rawgeti(state: LuaStateRaw, index: i32, n: usize) {
    unsafe {
        ffi::lua_rawgeti(state, index, n as ffi::lua_Integer);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_next(state: LuaStateRaw, index: i32) -> bool {
    unsafe { ffi::lua_next(state, index) != 0 }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_checktype(state: LuaStateRaw, index: i32, ltype: i32) {
    unsafe {
        ffi::luaL_checktype(state, index, ltype);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn push_c_string(state: LuaStateRaw, s: *const i8) {
    unsafe {
        ffi::lua_pushstring(state, s);
    }
}

///stack +1
#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_as_str(state: LuaStateRaw, index: i32) -> &'static str {
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_tolstring(state, index, &mut len);
        let slice = std::slice::from_raw_parts(ptr as *const u8, len);
        std::str::from_utf8_unchecked(slice)
    }
}

///stack +1
#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_as_slice(state: LuaStateRaw, index: i32) -> &'static [u8] {
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_tolstring(state, index, &mut len);
        std::slice::from_raw_parts(ptr as *const u8, len)
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_pushlightuserdata(state: LuaStateRaw, p: *mut std::ffi::c_void) {
    unsafe {
        ffi::lua_pushlightuserdata(state, p);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn to_string_unchecked(state: *mut ffi::lua_State, index: i32)-> String{
    match lua_type(state, index) {
        ffi::LUA_TNIL => {
            String::from("nil")
        }
        ffi::LUA_TSTRING => {
            lua_get::<String>(state, index)
        }
        ffi::LUA_TNUMBER => {
            if is_integer(state, index) {
                lua_to::<i64>(state, index).to_string()
            } else {
                lua_to::<f64>(state, index).to_string()
            }
        }
        ffi::LUA_TBOOLEAN => {
            if lua_to::<bool>(state, index) { 
                String::from("true") 
            } else { 
                String::from("false") 
            }
        }
        _ => {
            String::from("string type expected")
        }
    }
}