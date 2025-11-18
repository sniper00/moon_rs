use crate::{ffi, lua_State};
use std::{
    cell::Cell, ffi::{c_char, c_int}, fmt::{Display, Formatter}, marker::PhantomData, ptr::NonNull
};

pub type LuaState = NonNull<ffi::lua_State>;
pub type LuaCFunction = extern "C-unwind" fn(LuaState) -> i32;

#[repr(C)]
pub struct LuaReg {
    pub name: *const c_char,
    pub func: LuaCFunction,
}

pub struct LuaNil;

pub trait LuaStack {
    fn from_checked(state: LuaState, index: i32) -> Self;

    fn from_unchecked(state: LuaState, index: i32) -> Self;

    fn from_opt(state: LuaState, index: i32) -> Option<Self>
    where
        Self: Sized;

    fn push(state: LuaState, v: Self);
}

macro_rules! impl_lua_stack_integer {
    ($($t:ty),*) => {
        $(
            impl LuaStack for $t {
                fn from_checked(state: LuaState, index: i32) -> $t {
                    unsafe { ffi::luaL_checkinteger(state.as_ptr(), index) as $t }
                }

                fn from_unchecked(state: LuaState, index: i32) -> $t {
                    unsafe { ffi::lua_tointeger(state.as_ptr(), index) as $t }
                }

                fn from_opt(state: LuaState, index: i32) -> Option<$t> {
                    unsafe {
                        if ffi::lua_isinteger(state.as_ptr(), index) == 0 {
                            None
                        } else {
                            Some(ffi::lua_tointeger(state.as_ptr(), index) as $t)
                        }
                    }
                }

                fn push(state: LuaState, v: $t) {
                    unsafe {
                        ffi::lua_pushinteger(state.as_ptr(), v as ffi::lua_Integer);
                    }
                }
            }
        )*
    };
}

impl_lua_stack_integer!(i8, u8, i16, u16, i32, u32, usize, isize, i64, u64);

impl LuaStack for f64 {
    fn from_checked(state: LuaState, index: i32) -> f64 {
        unsafe { ffi::luaL_checknumber(state.as_ptr(), index) as f64 }
    }

    fn from_unchecked(state: LuaState, index: i32) -> f64 {
        unsafe { ffi::lua_tonumber(state.as_ptr(), index) as f64 }
    }

    fn from_opt(state: LuaState, index: i32) -> Option<f64> {
        unsafe {
            if ffi::lua_isnumber(state.as_ptr(), index) == 0 {
                None
            } else {
                Some(ffi::lua_tonumber(state.as_ptr(), index) as f64)
            }
        }
    }

    fn push(state: LuaState, v: f64) {
        unsafe {
            ffi::lua_pushnumber(state.as_ptr(), v as ffi::lua_Number);
        }
    }
}

impl LuaStack for bool {
    fn from_checked(state: LuaState, index: i32) -> bool {
        unsafe {
            ffi::luaL_checktype(state.as_ptr(), index, ffi::LUA_TBOOLEAN);
            ffi::lua_toboolean(state.as_ptr(), index) != 0
        }
    }

    fn from_unchecked(state: LuaState, index: i32) -> bool {
        unsafe { ffi::lua_toboolean(state.as_ptr(), index) != 0 }
    }

    fn from_opt(state: LuaState, index: i32) -> Option<bool> {
        unsafe {
            if ffi::lua_isnoneornil(state.as_ptr(), index) != 0 {
                None
            } else {
                Some(ffi::lua_toboolean(state.as_ptr(), index) != 0)
            }
        }
    }

    fn push(state: LuaState, v: bool) {
        unsafe {
            ffi::lua_pushboolean(state.as_ptr(), v as c_int);
        }
    }
}

impl LuaStack for &[u8] {
    fn from_checked(state: LuaState, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::luaL_checklstring(state.as_ptr(), index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    fn from_unchecked(state: LuaState, index: i32) -> &'static [u8] {
        unsafe {
            let mut len = 0;
            let ptr = ffi::lua_tolstring(state.as_ptr(), index, &mut len);
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    fn from_opt(state: LuaState, index: i32) -> Option<&'static [u8]> {
        unsafe {
            if ffi::lua_type(state.as_ptr(), index) != ffi::LUA_TSTRING {
                None
            } else {
                let mut len = 0;
                let ptr = ffi::lua_tolstring(state.as_ptr(), index, &mut len);
                if ptr.is_null() {
                    None
                } else {
                    Some(std::slice::from_raw_parts(ptr as *const u8, len))
                }
            }
        }
    }

    fn push(state: LuaState, v: &[u8]) {
        unsafe {
            ffi::lua_pushlstring(state.as_ptr(), v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaStack for &str {
    fn from_checked(state: LuaState, index: i32) -> &'static str {
        unsafe { std::str::from_utf8_unchecked(LuaStack::from_checked(state, index)) }
    }

    fn from_unchecked(state: LuaState, index: i32) -> &'static str {
        unsafe { std::str::from_utf8_unchecked(LuaStack::from_unchecked(state, index)) }
    }

    fn from_opt(state: LuaState, index: i32) -> Option<&'static str> {
        LuaStack::from_opt(state, index).map(|s| unsafe { std::str::from_utf8_unchecked(s) })
    }

    fn push(state: LuaState, v: &str) {
        unsafe {
            ffi::lua_pushlstring(state.as_ptr(), v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaStack for String {
    fn from_checked(state: LuaState, index: i32) -> String {
        String::from_utf8_lossy(LuaStack::from_checked(state, index)).into_owned()
    }

    fn from_unchecked(state: LuaState, index: i32) -> String {
        String::from_utf8_lossy(LuaStack::from_unchecked(state, index)).into_owned()
    }

    fn from_opt(state: LuaState, index: i32) -> Option<String> {
        LuaStack::from_opt(state, index).map(|s| String::from_utf8_lossy(s).into_owned())
    }

    fn push(state: LuaState, v: String) {
        unsafe {
            ffi::lua_pushlstring(state.as_ptr(), v.as_ptr() as *const c_char, v.len());
        }
    }
}

impl LuaStack for LuaNil {
    fn from_checked(state: LuaState, index: i32) -> LuaNil {
        unsafe {
            ffi::luaL_checktype(state.as_ptr(), index, ffi::LUA_TNIL);
        }
        LuaNil {}
    }

    fn from_unchecked(_state: LuaState, _index: i32) -> LuaNil {
        LuaNil {}
    }

    fn from_opt(state: LuaState, index: i32) -> Option<LuaNil> {
        if unsafe { ffi::lua_isnil(state.as_ptr(), index) == 1 } {
            Some(LuaNil {})
        } else {
            None
        }
    }

    fn push(state: LuaState, _v: LuaNil) {
        unsafe {
            ffi::lua_pushnil(state.as_ptr());
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

#[derive(PartialEq)]
pub struct LuaStateBox(pub LuaState);

unsafe impl Send for LuaStateBox {}

impl LuaStateBox {
    pub fn new(l: LuaState) -> Self {
        LuaStateBox(l)
    }
}

impl Drop for LuaStateBox {
    fn drop(&mut self) {
        unsafe {
            ffi::lua_close(self.0.as_ptr());
        }
    }
}

pub extern "C-unwind" fn lua_null_function(_: LuaState) -> i32 {
    0
}

pub struct LuaScopePop {
    state: LuaState,
}

impl LuaScopePop {
    pub fn new(state: LuaState) -> Self {
        LuaScopePop { state }
    }
}

impl Drop for LuaScopePop {
    fn drop(&mut self) {
        unsafe {
            ffi::lua_pop(self.state.as_ptr(), 1);
        }
    }
}

pub extern "C-unwind" fn lua_traceback(state: LuaState) -> i32 {
    unsafe {
        let msg = ffi::lua_tostring(state.as_ptr(), 1);
        if !msg.is_null() {
            ffi::luaL_traceback(state.as_ptr(), state.as_ptr(), msg, 1);
        } else {
            ffi::lua_pushliteral(state.as_ptr(), "(no error message)");
        }
        1
    }
}

pub fn opt_field<T>(state: LuaState, mut index: i32, field: &str) -> Option<T>
where
    T: LuaStack,
{
    if index < 0 {
        unsafe {
            index = ffi::lua_gettop(state.as_ptr()) + index + 1;
        }
    }

    let _scope = LuaScopePop::new(state);
    unsafe {
        ffi::lua_pushlstring(state.as_ptr(), field.as_ptr() as *const c_char, field.len());
        if ffi::lua_rawget(state.as_ptr(), index) <= ffi::LUA_TNIL {
            return None;
        }
    }

    LuaStack::from_opt(state, -1)
}

pub fn lua_get<T>(state: LuaState, index: i32) -> T
where
    T: LuaStack,
{
    LuaStack::from_checked(state, index)
}

pub fn lua_to<T>(state: LuaState, index: i32) -> T
where
    T: LuaStack,
{
    LuaStack::from_unchecked(state, index)
}

pub fn lua_opt<T>(state: LuaState, index: i32) -> Option<T>
where
    T: LuaStack,
{
    LuaStack::from_opt(state, index)
}

pub fn lua_push<T>(state: LuaState, v: T)
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

pub fn lua_type(state: LuaState, index: i32) -> LuaType {
    let ltype = unsafe { ffi::lua_type(state.as_ptr(), index) };
    match ltype {
        ffi::LUA_TNONE => LuaType::None,
        ffi::LUA_TNIL => LuaType::Nil,
        ffi::LUA_TBOOLEAN => LuaType::Boolean,
        ffi::LUA_TLIGHTUSERDATA => LuaType::LightUserData,
        ffi::LUA_TNUMBER => {
            if unsafe { ffi::lua_isinteger(state.as_ptr(), index) != 0 } {
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

pub fn lua_error(state: LuaState, message: String) -> ! {
    unsafe {
        ffi::lua_pushlstring(
            state.as_ptr(),
            message.as_ptr() as *const c_char,
            message.len(),
        );
        drop(message);
        ffi::lua_error(state.as_ptr())
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_arg_error(state: LuaState, index: i32, extra_msg: *const c_char) -> i32 {
    unsafe { ffi::luaL_argerror(state.as_ptr(), index, extra_msg) }
}

pub fn throw_error(state: LuaState) -> ! {
    unsafe { ffi::lua_error(state.as_ptr()) }
}

pub fn type_name(state: LuaState, t: i32) -> &'static str {
    unsafe {
        std::ffi::CStr::from_ptr(ffi::lua_typename(state.as_ptr(), t))
            .to_str()
            .unwrap_or_default()
    }
}

pub fn lua_pushnil(state: LuaState) {
    unsafe {
        ffi::lua_pushnil(state.as_ptr());
    }
}

pub fn is_integer(state: LuaState, index: i32) -> bool {
    unsafe { ffi::lua_isinteger(state.as_ptr(), index) != 0 }
}

pub fn lua_top(state: LuaState) -> i32 {
    unsafe { ffi::lua_gettop(state.as_ptr()) }
}

pub fn lua_settop(state: LuaState, idx: i32) {
    unsafe {
        ffi::lua_settop(state.as_ptr(), idx);
    }
}

pub fn lua_pop(state: LuaState, n: i32) {
    unsafe {
        ffi::lua_pop(state.as_ptr(), n);
    }
}

pub fn lua_checktype(state: LuaState, index: i32, ltype: i32) {
    unsafe {
        ffi::luaL_checktype(state.as_ptr(), index, ltype);
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_checkstack(state: LuaState, sz: i32, msg: *const c_char) {
    unsafe {
        ffi::luaL_checkstack(state.as_ptr(), sz, msg);
    }
}

///stack +1
pub fn lua_as_slice(state: LuaState, index: i32) -> &'static [u8] {
    unsafe {
        let mut len = 0;
        let ptr = ffi::luaL_tolstring(state.as_ptr(), index, &mut len);
        std::slice::from_raw_parts(ptr as *const u8, len)
    }
}

pub fn lua_absindex(state: LuaState, index: i32) -> i32 {
    unsafe { ffi::lua_absindex(state.as_ptr(), index) }
}

#[inline(always)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_pushlightuserdata(state: LuaState, p: *mut std::ffi::c_void) {
    unsafe {
        ffi::lua_pushlightuserdata(state.as_ptr(), p);
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_newuserdata<T>(
    state: LuaState,
    val: T,
    metaname: *const c_char,
    lib: &[LuaReg],
) -> Option<&T> {
    extern "C-unwind" fn lua_dropuserdata<T>(state: *mut lua_State) -> i32 {
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
        let ptr = ffi::lua_newuserdatauv(state.as_ptr(), std::mem::size_of::<T>(), 0) as *mut T;
        let ptr = std::ptr::NonNull::new(ptr)?;

        ptr.as_ptr().write(val);

        if ffi::luaL_newmetatable(state.as_ptr(), metaname) != 0 {
            ffi::lua_createtable(state.as_ptr(), 0, lib.len() as c_int);
            ffi::luaL_setfuncs(state.as_ptr(), lib.as_ptr() as *const ffi::luaL_Reg, 0);
            ffi::lua_setfield(state.as_ptr(), -2, cstr!("__index"));
            ffi::lua_pushcfunction(state.as_ptr(), lua_dropuserdata::<T>);
            ffi::lua_setfield(state.as_ptr(), -2, cstr!("__gc"));
        }

        ffi::lua_setmetatable(state.as_ptr(), -2);
        Some(&*ptr.as_ptr())
    }
}

pub fn lua_touserdata<T>(state: LuaState, index: i32) -> Option<&'static mut T> {
    unsafe {
        let ptr = ffi::lua_touserdata(state.as_ptr(), index);
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
pub fn lua_into_userdata<T>(state: LuaState, index: i32) -> Box<T> {
    let p_as_isize: isize = lua_get(state, index);
    unsafe { Box::from_raw(p_as_isize as *mut T) }
}

pub struct LuaTable {
    state: LuaState,
    index: i32,
    pos: Cell<u32>
}

impl LuaTable {
    pub fn new(state: LuaState, narr: usize, nrec: usize) -> Self {
        unsafe {
            ffi::lua_createtable(state.as_ptr(), narr as i32, nrec as i32);
            LuaTable {
                state,
                index: ffi::lua_gettop(state.as_ptr()),
                pos: Cell::new(0)
            }
        }
    }

    pub fn from_stack(state: LuaState, index: i32) -> Self {
        LuaTable { state, index: lua_absindex(state, index), pos: Cell::new(0) }
    }

    pub fn array_from_stack(state: LuaState, index: i32) -> Self {
        let len = unsafe { ffi::lua_rawlen(state.as_ptr(), index) };
        LuaTable { state, index: lua_absindex(state, index), pos: Cell::new(len as u32) }
    }

    pub fn len(&self) -> usize {
        unsafe { ffi::lua_rawlen(self.state.as_ptr(), self.index) }
    }

    pub fn array_len(&self) -> usize {
        lua_array_size(self.state, self.index)
    }

    pub fn lua_state(&self) -> LuaState {
        self.state
    }

    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert<K, V>(&self, key: K, val: V) -> &Self
    where
        K: LuaStack,
        V: LuaStack,
    {
        unsafe {
            K::push(self.state, key);
            V::push(self.state, val);
            ffi::lua_rawset(self.state.as_ptr(), self.index);
        }
        self
    }

    pub fn push<V>(&self, val: V) -> &Self
    where
        V: LuaStack,
    {
        unsafe {
            V::push(self.state, val);
            self.pos.set(self.pos.get() + 1);
            ffi::lua_rawseti(self.state.as_ptr(), self.index, self.pos.get() as ffi::lua_Integer);
        }
        self
    }

    pub fn push_table(&self, table: LuaTable) -> &Self
    {
        debug_assert!(table.index == lua_top(self.state));
        unsafe {
            ffi::lua_rawseti(self.state.as_ptr(), self.index, self.pos.get() as ffi::lua_Integer);
        }
        self
    }

    pub fn rawseti(&self, n: usize) {
        unsafe {
            ffi::lua_rawseti(self.state.as_ptr(), self.index, n as ffi::lua_Integer);
        }
    }

    /// Pops the value from the top of the stack and sets it in the table at the specified key.
    pub fn insert_from_stack(&self) -> &Self
    {
        unsafe {
            ffi::lua_rawset(self.state.as_ptr(), self.index);
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
            ffi::lua_rawset(self.state.as_ptr(), self.index);
        }
        self
    }

    pub fn rawget<K>(&self, key: K) -> LuaScopeValue<'_>
    where
        K: LuaStack,
    {
        unsafe {
            K::push(self.state, key);
            ffi::lua_rawget(self.state.as_ptr(), self.index);
            LuaScopeValue {
                state: self.state,
                value: LuaValue::from_stack(self.state, -1),
                _marker: PhantomData,
            }
        }
    }

    #[inline(always)]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn getmetafield(&self, e: *const c_char) -> Option<LuaScopeValue<'_>> {
        unsafe {
            if ffi::luaL_getmetafield(self.state.as_ptr(), self.index, e) == ffi::LUA_TNIL {
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

    pub fn iter(&self) -> LuaTableIterator<'_> {
        unsafe {
            ffi::lua_pushnil(self.state.as_ptr());
        }
        LuaTableIterator {
            table: self,
            has_value: false,
            _marker: PhantomData,
        }
    }

    pub fn array_iter(&self) -> LuaArrayIterator<'_> {
        LuaArrayIterator {
            table: self,
            pos: 0,
            len: self.array_len(),
            has_value: false,
            _marker: PhantomData,
        }
    }

    pub fn expected_array_iter(&self, len: usize) -> LuaArrayIterator<'_> {
        LuaArrayIterator {
            table: self,
            pos: 0,
            len,
            has_value: false,
            _marker: PhantomData,
        }
    }
}

impl LuaStack for LuaTable {
    fn from_checked(state: LuaState, index: i32) -> LuaTable {
        lua_checktype(state, index, ffi::LUA_TTABLE);
        LuaTable::from_stack(state, index)
    }

    fn from_unchecked(state: LuaState, index: i32) -> LuaTable {
        LuaTable::from_stack(state, index)
    }

    fn from_opt(state: LuaState, index: i32) -> Option<LuaTable> {
        if lua_type(state, index) != LuaType::Table {
            return None;
        }
        Some(LuaTable::from_stack(state, index))
    }

    fn push(_: LuaState, _: LuaTable) {

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
                ffi::lua_pop(self.table.state.as_ptr(), 1);
                self.has_value = false;
            }

            if ffi::lua_next(self.table.state.as_ptr(), self.table.index) == 0 {
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
                ffi::lua_pop(self.table.state.as_ptr(), 1);
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
                ffi::lua_pop(self.table.state.as_ptr(), 1);
            }

            if self.pos >= self.len {
                return None;
            }

            self.pos += 1;
            ffi::lua_rawgeti(
                self.table.state.as_ptr(),
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
                ffi::lua_pop(self.table.state.as_ptr(), 1);
            }
        }
    }
}

pub fn lua_array_size(state: LuaState, idx: i32) -> usize {
    unsafe {
        ffi::lua_pushnil(state.as_ptr());
        if ffi::lua_next(state.as_ptr(), idx) == 0 {
            return 0;
        }

        let first_key = if ffi::lua_isinteger(state.as_ptr(), -2) != 0 {
            ffi::lua_tointeger(state.as_ptr(), -2)
        } else {
            0
        };

        ffi::lua_pop(state.as_ptr(), 2);

        if first_key <= 0 {
            return 0;
        } else if first_key == 1 {
            /*
             * https://www.lua.org/manual/5.4/manual.html#3.4.7
             * The length operator applied on a table returns a border in that table.
             * A border in a table t is any natural number that satisfies the following condition :
             * (border == 0 or t[border] ~= nil) and t[border + 1] == nil
             */
            let len = ffi::lua_rawlen(state.as_ptr(), idx) as ffi::lua_Integer;
            ffi::lua_pushinteger(state.as_ptr(), len);
            if ffi::lua_next(state.as_ptr(), idx) != 0 {
                ffi::lua_pop(state.as_ptr(), 2);
                return 0;
            }
        }

        let len = ffi::lua_rawlen(state.as_ptr(), idx) as ffi::lua_Integer;
        if first_key > len {
            return 0;
        }

        ffi::lua_pushnil(state.as_ptr());
        while ffi::lua_next(state.as_ptr(), idx) != 0 {
            if ffi::lua_isinteger(state.as_ptr(), -2) != 0 {
                let x = ffi::lua_tointeger(state.as_ptr(), -2);
                if x > 0 && x <= len {
                    ffi::lua_pop(state.as_ptr(), 1);
                    continue;
                }
            }
            ffi::lua_pop(state.as_ptr(), 2);
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
    Thread(*mut lua_State),
}

impl LuaValue<'_> {
    pub fn from_stack(state: LuaState, index: i32) -> Self {
        match lua_type(state, index) {
            LuaType::None => LuaValue::None,
            LuaType::Nil => LuaValue::Nil,
            LuaType::Boolean => LuaValue::Boolean(lua_to(state, index)),
            LuaType::LightUserData => {
                LuaValue::LightUserData(unsafe { ffi::lua_touserdata(state.as_ptr(), index) })
            }
            LuaType::Number => LuaValue::Number(lua_to(state, index)),
            LuaType::Integer => LuaValue::Integer(lua_to(state, index)),
            LuaType::String => LuaValue::String(lua_to(state, index)),
            LuaType::Table => LuaValue::Table(LuaTable::from_stack(state, index)),
            LuaType::Function => {
                LuaValue::Function(unsafe { ffi::lua_topointer(state.as_ptr(), index) })
            }
            LuaType::UserData => {
                LuaValue::UserData(unsafe { ffi::lua_touserdata(state.as_ptr(), index) })
            }
            LuaType::Thread => {
                LuaValue::Thread(unsafe { ffi::lua_tothread(state.as_ptr(), index) })
            }
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
    state: LuaState,
    pub value: LuaValue<'a>,
    _marker: PhantomData<&'a mut LuaTable>,
}

impl Drop for LuaScopeValue<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::lua_pop(self.state.as_ptr(), 1);
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
