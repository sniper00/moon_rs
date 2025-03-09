use crate::ffi;
use std::{
    ffi::{c_char, c_int},
    fmt::{Display, Formatter},
    marker::PhantomData,
};

pub type LuaStateRef = *mut ffi::lua_State;

pub trait LuaStack {
    fn from_checked(state: LuaStateRef, index: i32) -> Self;

    fn from_unchecked(state: LuaStateRef, index: i32) -> Self;

    fn from_opt(state: LuaStateRef, index: i32) -> Option<Self>
    where
        Self: Sized;

    fn push(state: LuaStateRef, v: Self);
}

macro_rules! impl_lua_stack_integer {
    ($($t:ty),*) => {
        $(
            impl LuaStack for $t {
                #[inline]
                #[allow(clippy::not_unsafe_ptr_arg_deref)]
                fn from_checked(state: LuaStateRef, index: i32) -> $t {
                    unsafe { ffi::luaL_checkinteger(state, index) as $t }
                }

                #[inline]
                #[allow(clippy::not_unsafe_ptr_arg_deref)]
                fn from_unchecked(state: LuaStateRef, index: i32) -> $t {
                    unsafe { ffi::lua_tointeger(state, index) as $t }
                }

                #[inline]
                #[allow(clippy::not_unsafe_ptr_arg_deref)]
                fn from_opt(state: LuaStateRef, index: i32) -> Option<$t> {
                    unsafe {
                        if ffi::lua_isinteger(state, index) == 0 {
                            None
                        } else {
                            Some(ffi::lua_tointeger(state, index) as $t)
                        }
                    }
                }

                #[inline]
                #[allow(clippy::not_unsafe_ptr_arg_deref)]
                fn push(state: LuaStateRef, v: $t) {
                    unsafe {
                        ffi::lua_pushinteger(state, v as ffi::lua_Integer);
                    }
                }
            }
        )*
    };
}

impl_lua_stack_integer!(i8, u8, i16, u16, i32, u32, usize, isize, i64, u64);

impl LuaStack for f64 {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_checked(state: LuaStateRef, index: i32) -> f64 {
        unsafe { ffi::luaL_checknumber(state, index) as f64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_unchecked(state: LuaStateRef, index: i32) -> f64 {
        unsafe { ffi::lua_tonumber(state, index) as f64 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_opt(state: LuaStateRef, index: i32) -> Option<f64> {
        unsafe {
            if ffi::lua_isnumber(state, index) == 0 {
                None
            } else {
                Some(ffi::lua_tonumber(state, index) as f64)
            }
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push(state: LuaStateRef, v: f64) {
        unsafe {
            ffi::lua_pushnumber(state, v as ffi::lua_Number);
        }
    }
}

impl LuaStack for bool {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_checked(state: LuaStateRef, index: i32) -> bool {
        unsafe {
            ffi::luaL_checktype(state, index, ffi::LUA_TBOOLEAN);
            ffi::lua_toboolean(state, index) != 0
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_unchecked(state: LuaStateRef, index: i32) -> bool {
        unsafe { ffi::lua_toboolean(state, index) != 0 }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_opt(state: LuaStateRef, index: i32) -> Option<bool> {
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
    fn push(state: LuaStateRef, v: bool) {
        unsafe {
            ffi::lua_pushboolean(state, v as c_int);
        }
    }
}

impl LuaStack for &[u8] {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_checked(state: LuaStateRef, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state, index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_unchecked(state: LuaStateRef, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::lua_tolstring(state, index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_opt(state: LuaStateRef, index: i32) -> Option<&'static [u8]> {
        unsafe {
            if ffi::lua_type(state, index) != ffi::LUA_TSTRING {
                None
            } else {
                let mut len = 0;
                let ptr = ffi::lua_tolstring(state, index, &mut len);
                if ptr.is_null() {
                    None
                } else {
                    Some(std::slice::from_raw_parts(ptr as *const u8, len))
                }
            }
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push(state: LuaStateRef, v: &[u8]) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaStack for &str {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_checked(state: LuaStateRef, index: i32) -> &'static str {
        unsafe { std::str::from_utf8_unchecked(LuaStack::from_checked(state, index)) }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_unchecked(state: LuaStateRef, index: i32) -> &'static str {
        unsafe { std::str::from_utf8_unchecked(LuaStack::from_unchecked(state, index)) }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_opt(state: LuaStateRef, index: i32) -> Option<&'static str> {
        LuaStack::from_opt(state, index).map(|s| unsafe { std::str::from_utf8_unchecked(s) })
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push(state: LuaStateRef, v: &str) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaStack for String {
    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_checked(state: LuaStateRef, index: i32) -> String {
        String::from_utf8_lossy(LuaStack::from_checked(state, index)).into_owned()
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_unchecked(state: LuaStateRef, index: i32) -> String {
        String::from_utf8_lossy(LuaStack::from_unchecked(state, index)).into_owned()
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from_opt(state: LuaStateRef, index: i32) -> Option<String> {
        LuaStack::from_opt(state, index).map(|s| String::from_utf8_lossy(s).into_owned())
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push(state: LuaStateRef, v: String) {
        unsafe {
            ffi::lua_pushlstring(state, v.as_ptr() as *const c_char, v.len());
        }
    }
}

#[derive(PartialEq)]
pub struct LuaThread(pub LuaStateRef);

unsafe impl Send for LuaThread {}

impl LuaThread {
    pub fn new(l: LuaStateRef) -> Self {
        LuaThread(l)
    }
}

#[derive(PartialEq)]
pub struct LuaState(pub LuaStateRef);

unsafe impl Send for LuaState {}

impl LuaState {
    pub fn new(l: LuaStateRef) -> Self {
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

pub extern "C-unwind" fn lua_null_function(_: LuaStateRef) -> c_int {
    0
}

pub struct LuaScopePop {
    state: LuaStateRef,
}

impl LuaScopePop {
    pub fn new(state: LuaStateRef) -> Self {
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
pub extern "C-unwind" fn lua_traceback(state: LuaStateRef) -> c_int {
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
pub fn opt_field<T>(state: LuaStateRef, mut index: i32, field: &str) -> Option<T>
where
    T: LuaStack,
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

    LuaStack::from_opt(state, -1)
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_get<T>(state: LuaStateRef, index: i32) -> T
where
    T: LuaStack,
{
    LuaStack::from_checked(state, index)
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_to<T>(state: LuaStateRef, index: i32) -> T
where
    T: LuaStack,
{
    LuaStack::from_unchecked(state, index)
}

#[inline]
pub fn lua_opt<T>(state: LuaStateRef, index: i32) -> Option<T>
where
    T: LuaStack,
{
    LuaStack::from_opt(state, index)
}

#[inline]
pub fn lua_push<T>(state: LuaStateRef, v: T)
where
    T: LuaStack,
{
    LuaStack::push(state, v);
}

#[derive(PartialEq, Eq)]
pub enum LuaType {
    None,
    Nil,
    Boolean,
    LightUserData,
    Number,
    String,
    Table,
    Function,
    UserData,
    Thread,
    Integer,
}

impl From<LuaType> for i32 {
    fn from(lua_type: LuaType) -> Self {
        match lua_type {
            LuaType::None => ffi::LUA_TNONE,
            LuaType::Nil => ffi::LUA_TNIL,
            LuaType::Boolean => ffi::LUA_TBOOLEAN,
            LuaType::LightUserData => ffi::LUA_TLIGHTUSERDATA,
            LuaType::Number => ffi::LUA_TNUMBER,
            LuaType::Integer => ffi::LUA_TNUMBER,
            LuaType::String => ffi::LUA_TSTRING,
            LuaType::Table => ffi::LUA_TTABLE,
            LuaType::Function => ffi::LUA_TFUNCTION,
            LuaType::UserData => ffi::LUA_TUSERDATA,
            LuaType::Thread => ffi::LUA_TTHREAD,
        }
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_type(state: LuaStateRef, index: i32) -> LuaType {
    let ltype = unsafe { ffi::lua_type(state, index) };
    match ltype {
        ffi::LUA_TNONE => LuaType::None,
        ffi::LUA_TNIL => LuaType::Nil,
        ffi::LUA_TBOOLEAN => LuaType::Boolean,
        ffi::LUA_TLIGHTUSERDATA => LuaType::LightUserData,
        ffi::LUA_TNUMBER => {
            if unsafe { ffi::lua_isinteger(state, index) != 0 } {
                LuaType::Integer
            } else {
                LuaType::Number
            }
        }
        ffi::LUA_TSTRING => LuaType::String,
        ffi::LUA_TTABLE => LuaType::Table,
        ffi::LUA_TFUNCTION => LuaType::Function,
        ffi::LUA_TUSERDATA => LuaType::UserData,
        ffi::LUA_TTHREAD => LuaType::Thread,
        _ => unreachable!(),
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_error(state: LuaStateRef, message: &str) -> ! {
    unsafe {
        ffi::lua_pushlstring(state, message.as_ptr() as *const c_char, message.len());
        ffi::lua_error(state)
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn throw_error(state: LuaStateRef) -> ! {
    unsafe { ffi::lua_error(state) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn type_name(state: LuaStateRef, idx: i32) -> &'static str {
    unsafe {
        std::ffi::CStr::from_ptr(ffi::lua_typename(state, idx))
            .to_str()
            .unwrap_or_default()
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_pushnil(state: LuaStateRef) {
    unsafe {
        ffi::lua_pushnil(state);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn is_integer(state: LuaStateRef, index: i32) -> bool {
    unsafe { ffi::lua_isinteger(state, index) != 0 }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_top(state: LuaStateRef) -> i32 {
    unsafe { ffi::lua_gettop(state) }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_settop(state: LuaStateRef, idx: i32) {
    unsafe {
        ffi::lua_settop(state, idx);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_pop(state: LuaStateRef, n: i32) {
    unsafe {
        ffi::lua_pop(state, n);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_checktype(state: LuaStateRef, index: i32, ltype: i32) {
    unsafe {
        ffi::luaL_checktype(state, index, ltype);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn luaL_checkstack(state: LuaStateRef, sz: i32, msg: *const c_char) {
    unsafe {
        ffi::luaL_checkstack(state, sz, msg);
    }
}

///stack +1
#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_as_slice(state: LuaStateRef, index: i32) -> &'static [u8] {
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_tolstring(state, index, &mut len);
        std::slice::from_raw_parts(ptr as *const u8, len)
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_pushlightuserdata(state: LuaStateRef, p: *mut std::ffi::c_void) {
    unsafe {
        ffi::lua_pushlightuserdata(state, p);
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_newuserdata<T>(
    state: *mut ffi::lua_State,
    val: T,
    metaname: *const c_char,
    lib: &[ffi::luaL_Reg],
) -> Option<&T> {
    extern "C-unwind" fn lua_dropuserdata<T>(state: *mut ffi::lua_State) -> c_int {
        unsafe {
            let p = ffi::lua_touserdata(state, 1);
            if p.is_null() {
                return 0;
            }
            let p = p as *mut T;
            std::ptr::drop_in_place(p);
        }
        0
    }

    unsafe {
        let ptr = ffi::lua_newuserdatauv(state, std::mem::size_of::<T>(), 0) as *mut T;
        let ptr = std::ptr::NonNull::new(ptr)?;

        ptr.as_ptr().write(val);

        if ffi::luaL_newmetatable(state, metaname) != 0 {
            ffi::lua_createtable(state, 0, lib.len() as c_int);
            ffi::luaL_setfuncs(state, lib.as_ptr(), 0);
            ffi::lua_setfield(state, -2, cstr!("__index"));
            ffi::lua_pushcfunction(state, lua_dropuserdata::<T>);
            ffi::lua_setfield(state, -2, cstr!("__gc"));
        }

        ffi::lua_setmetatable(state, -2);
        Some(&*ptr.as_ptr())
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_touserdata<T>(state: *mut ffi::lua_State, index: i32) -> Option<&'static mut T> {
    unsafe {
        let ptr = ffi::lua_touserdata(state, index);
        let ptr = std::ptr::NonNull::new(ptr)?;
        let ptr = ptr.as_ptr() as *mut T;
        Some(&mut *ptr)
    }
}

/// Converts an `isize` value from Lua state at the given index into a Rust `T` object.
///
/// # Arguments
///
/// * `state` - The Lua state.
/// * `index` - The index in the Lua stack.
///
/// # Safety
///
/// This function is unsafe because it dereferences a raw pointer.
///
/// # Returns
///
/// A `Box<T>` containing the Rust object.
pub fn lua_into_userdata<T>(state: LuaStateRef, index: i32) -> Box<T> {
    let p_as_isize: isize = lua_get(state, index);
    unsafe { Box::from_raw(p_as_isize as *mut T) }
}

pub struct LuaTable {
    state: LuaStateRef,
    index: i32,
}

impl LuaTable {
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn new(state: LuaStateRef, narr: usize, nrec: usize) -> Self {
        unsafe {
            ffi::lua_createtable(state, narr as i32, nrec as i32);
            LuaTable {
                state,
                index: ffi::lua_gettop(state),
            }
        }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn from_stack(state: LuaStateRef, mut index: i32) -> Self {
        if index < 0 {
            index = unsafe { ffi::lua_gettop(state) + index + 1 };
        }
        LuaTable { state, index }
    }

    pub fn len(&self) -> usize {
        unsafe { ffi::lua_rawlen(self.state, self.index) }
    }

    pub fn array_len(&self) -> usize {
        lua_array_size(self.state, self.index)
    }

    pub fn lua_state(&self) -> LuaStateRef {
        self.state
    }

    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn seti(&self, n: usize) {
        unsafe {
            ffi::lua_rawseti(self.state, self.index, n as ffi::lua_Integer);
        }
    }

    pub fn rawset<K, V>(&self, key: K, val: V) -> &Self
    where
        K: LuaStack,
        V: LuaStack,
    {
        unsafe {
            K::push(self.state, key);
            V::push(self.state, val);
            ffi::lua_rawset(self.state, self.index);
        }
        self
    }

    pub fn rawset_x<K, F>(&self, key: K, f: F) -> &Self
    where
        K: LuaStack,
        F: FnOnce(),
    {
        unsafe {
            K::push(self.state, key);
            f();
            ffi::lua_rawset(self.state, self.index);
        }
        self
    }

    pub fn rawget<K>(&self, key: K) -> LuaScopeValue
    where
        K: LuaStack,
    {
        unsafe {
            K::push(self.state, key);
            ffi::lua_rawget(self.state, self.index);
            LuaScopeValue {
                state: self.state,
                value: LuaValue::from_stack(self.state, -1),
                _marker: PhantomData,
            }
        }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn getmetafield(&self, e: *const c_char) -> Option<LuaScopeValue> {
        unsafe {
            if ffi::luaL_getmetafield(self.state, self.index, e) == ffi::LUA_TNIL {
                None
            } else {
                Some(LuaScopeValue {
                    state: self.state,
                    value: LuaValue::from_stack(self.state, -1),
                    _marker: PhantomData,
                })
            }
        }
    }

    pub fn iter(&self) -> LuaTableIterator {
        unsafe {
            ffi::lua_pushnil(self.state);
        }
        LuaTableIterator {
            table: self,
            has_value: false,
            _marker: PhantomData,
        }
    }

    pub fn array_iter(&self, len: usize) -> LuaArrayIterator {
        LuaArrayIterator {
            table: self,
            pos: 0,
            len,
            has_value: false,
            _marker: PhantomData,
        }
    }
}

pub struct LuaTableIterator<'a> {
    table: &'a LuaTable,
    has_value: bool,
    _marker: PhantomData<&'a mut LuaTable>,
}

impl<'a> Iterator for LuaTableIterator<'a> {
    type Item = (LuaValue<'a>, LuaValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.has_value {
                ffi::lua_pop(self.table.state, 1);
                self.has_value = false;
            }

            if ffi::lua_next(self.table.state, self.table.index) == 0 {
                return None;
            }

            self.has_value = true;

            Some((
                LuaValue::from_stack(self.table.state, -2),
                LuaValue::from_stack(self.table.state, -1),
            ))
        }
    }
}

impl Drop for LuaTableIterator<'_> {
    fn drop(&mut self) {
        unsafe {
            if self.has_value {
                ffi::lua_pop(self.table.state, 1);
            }
        }
    }
}

pub struct LuaArrayIterator<'a> {
    table: &'a LuaTable,
    pos: usize,
    len: usize,
    has_value: bool,
    _marker: PhantomData<&'a mut LuaTable>,
}

impl<'a> Iterator for LuaArrayIterator<'a> {
    type Item = LuaValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.has_value {
                self.has_value = false;
                ffi::lua_pop(self.table.state, 1);
            }

            if self.pos >= self.len {
                return None;
            }

            self.pos += 1;
            ffi::lua_rawgeti(
                self.table.state,
                self.table.index,
                self.pos as ffi::lua_Integer,
            );
            self.has_value = true;
            Some(LuaValue::from_stack(self.table.state, -1))
        }
    }
}

impl Drop for LuaArrayIterator<'_> {
    fn drop(&mut self) {
        unsafe {
            // Clean up any remaining items on stack
            if self.has_value {
                ffi::lua_pop(self.table.state, 1);
            }
        }
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_array_size(state: *mut ffi::lua_State, idx: i32) -> usize {
    unsafe {
        ffi::lua_pushnil(state);
        if ffi::lua_next(state, idx) == 0 {
            return 0;
        }

        let first_key = if ffi::lua_isinteger(state, -2) != 0 {
            ffi::lua_tointeger(state, -2)
        } else {
            0
        };

        ffi::lua_pop(state, 2);

        if first_key <= 0 {
            return 0;
        } else if first_key == 1 {
            /*
             * https://www.lua.org/manual/5.4/manual.html#3.4.7
             * The length operator applied on a table returns a border in that table.
             * A border in a table t is any natural number that satisfies the following condition :
             * (border == 0 or t[border] ~= nil) and t[border + 1] == nil
             */
            let len = ffi::lua_rawlen(state, idx) as ffi::lua_Integer;
            ffi::lua_pushinteger(state, len);
            if ffi::lua_next(state, idx) != 0 {
                ffi::lua_pop(state, 2);
                return 0;
            }
            return len as usize;
        }

        let len = ffi::lua_rawlen(state, idx) as ffi::lua_Integer;
        if first_key > len {
            return 0;
        }

        ffi::lua_pushnil(state);
        while ffi::lua_next(state, idx) != 0 {
            if ffi::lua_isinteger(state, -2) != 0 {
                let x = ffi::lua_tointeger(state, -2);
                if x > 0 && x <= len {
                    ffi::lua_pop(state, 1);
                    continue;
                }
            }
            ffi::lua_pop(state, 2);
            return 0;
        }

        len as usize
    }
}

pub enum LuaValue<'a> {
    None,
    Nil,
    Boolean(bool),
    LightUserData(*mut std::ffi::c_void),
    Number(f64),
    Integer(i64),
    String(&'a [u8]),
    Table(LuaTable),
    Function(*const std::ffi::c_void),
    UserData(*mut std::ffi::c_void),
    Thread(LuaStateRef),
}

impl LuaValue<'_> {
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn from_stack(state: LuaStateRef, index: i32) -> Self {
        match lua_type(state, index) {
            LuaType::None => LuaValue::None,
            LuaType::Nil => LuaValue::Nil,
            LuaType::Boolean => LuaValue::Boolean(lua_to(state, index)),
            LuaType::LightUserData => {
                LuaValue::LightUserData(unsafe { ffi::lua_touserdata(state, index) })
            }
            LuaType::Number => LuaValue::Number(lua_to(state, index)),
            LuaType::Integer => LuaValue::Integer(lua_to(state, index)),
            LuaType::String => LuaValue::String(lua_to(state, index)),
            LuaType::Table => LuaValue::Table(LuaTable::from_stack(state, index)),
            LuaType::Function => LuaValue::Function(unsafe { ffi::lua_topointer(state, index) }),
            LuaType::UserData => LuaValue::UserData(unsafe { ffi::lua_touserdata(state, index) }),
            LuaType::Thread => LuaValue::Thread(unsafe { ffi::lua_tothread(state, index) }),
        }
    }

    pub fn name(&self) -> String {
        match self {
            LuaValue::None => "none".to_string(),
            LuaValue::Nil => "nil".to_string(),
            LuaValue::Boolean(_) => "boolean".to_string(),
            LuaValue::LightUserData(_) => "lightuserdata".to_string(),
            LuaValue::Number(_) => "number".to_string(),
            LuaValue::Integer(_) => "number".to_string(),
            LuaValue::String(_) => "string".to_string(),
            LuaValue::Table(_) => "table".to_string(),
            LuaValue::Function(_) => "function".to_string(),
            LuaValue::UserData(_) => "userdata".to_string(),
            LuaValue::Thread(_) => "thread".to_string(),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            LuaValue::String(s) => s.to_vec(),
            LuaValue::Integer(n) => n.to_string().into_bytes(),
            LuaValue::Number(n) => n.to_string().into_bytes(),
            _ => Vec::new(),
        }
    }
}

impl Display for LuaValue<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LuaValue::None => write!(f, "none"),
            LuaValue::Nil => write!(f, "nil"),
            LuaValue::Boolean(b) => write!(f, "{}", b),
            LuaValue::LightUserData(p) => write!(f, "{:p}", p),
            LuaValue::Number(n) => write!(f, "{}", n),
            LuaValue::Integer(n) => write!(f, "{}", n),
            LuaValue::String(s) => write!(f, "{}", String::from_utf8_lossy(s)),
            LuaValue::Table(_) => write!(f, "table"),
            LuaValue::Function(p) => write!(f, "{:p}", p),
            LuaValue::UserData(p) => write!(f, "{:p}", p),
            LuaValue::Thread(p) => write!(f, "{:p}", p),
        }
    }
}

pub struct LuaScopeValue<'a> {
    state: LuaStateRef,
    pub value: LuaValue<'a>,
    _marker: PhantomData<&'a mut LuaTable>,
}

impl Drop for LuaScopeValue<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::lua_pop(self.state, 1);
        }
    }
}

pub struct LuaArgs {
    current: i32,
}

impl LuaArgs {
    pub fn new(start: i32) -> Self {
        LuaArgs { current: start }
    }

    pub fn iter_arg(&mut self) -> i32 {
        let result = self.current;
        self.current += 1;
        result
    }
}
