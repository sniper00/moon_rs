use crate::{ffi, lua_State};
use std::{
    borrow::Cow,
    cell::Cell,
    ffi::{c_char, c_int},
    fmt::{Display, Formatter},
    marker::PhantomData,
    ptr::NonNull,
    rc::Rc,
};

pub type LuaState = NonNull<ffi::lua_State>;
pub type LuaCFunction = extern "C-unwind" fn(LuaState) -> i32;

#[repr(C)]
pub struct LuaReg {
    pub name: *const c_char,
    pub func: LuaCFunction,
}

pub struct LuaNil;

#[derive(PartialEq)]
pub struct LuaThread(pub *mut ffi::lua_State);

unsafe impl Send for LuaThread {}

impl LuaThread {
    pub fn new(l: *mut ffi::lua_State) -> Self {
        LuaThread(l)
    }
}

#[derive(PartialEq)]
pub struct LuaGlobalState(pub LuaState);

unsafe impl Send for LuaGlobalState {}

impl LuaGlobalState {
    pub fn new(l: LuaState) -> Self {
        LuaGlobalState(l)
    }
}

impl Drop for LuaGlobalState {
    fn drop(&mut self) {
        unsafe {
            ffi::lua_close(self.0.as_ptr());
        }
    }
}

pub extern "C-unwind" fn lua_null_function(_: LuaState) -> i32 {
    0
}

pub extern "C-unwind" fn lua_traceback(state: LuaState) -> i32 {
    unsafe {
        let msg = ffi::lua_tostring(state.as_ptr(), 1);
        if !msg.is_null() {
            ffi::luaL_traceback(state.as_ptr(), state.as_ptr(), msg, 1);
        } else {
            ffi::lua_pushliteral(state.as_ptr(), c"(no error message)");
        }
        1
    }
}

pub fn lua_push<T>(state: LuaState, v: T)
where
    T: LuaPush,
{
    LuaPush::push(v, state);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
        // Match luaL_error/luaL_argerror: preserve the Lua caller's source
        // location before raising the Rust-produced error message. Do this
        // after releasing the Rust string so a location-allocation failure
        // cannot leak it.
        ffi::luaL_where(state.as_ptr(), 1);
        ffi::lua_insert(state.as_ptr(), -2);
        ffi::lua_concat(state.as_ptr(), 2);
        ffi::lua_error(state.as_ptr())
    }
}

pub fn type_name(state: LuaState, t: i32) -> &'static str {
    unsafe {
        std::ffi::CStr::from_ptr(ffi::lua_typename(state.as_ptr(), t))
            .to_str()
            .unwrap_or_default()
    }
}

/// Format type-mismatch error messages for `from_checked` and similar guards.
/// `actual` is the raw `ffi::lua_type()` integer — avoids a redundant FFI call
/// when the caller already has it.
fn type_mismatch(state: LuaState, actual: i32, expected: &str) -> String {
    let actual = type_name(state, actual);
    format!("{expected} expected, got {actual}")
}

pub fn lua_pushnil(state: LuaState) {
    unsafe {
        ffi::lua_pushnil(state.as_ptr());
    }
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

pub fn lua_checktype(state: LuaState, index: i32, ltype: i32) -> Result<(), String> {
    let actual = unsafe { ffi::lua_type(state.as_ptr(), index) };
    if actual == ltype {
        Ok(())
    } else {
        Err(type_mismatch(state, actual, type_name(state, ltype)))
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn lua_checkstack(state: LuaState, sz: i32, msg: *const c_char) -> Result<(), String> {
    if unsafe { ffi::lua_checkstack(state.as_ptr(), sz) } != 0 {
        return Ok(());
    }

    if msg.is_null() {
        Err("stack overflow".to_string())
    } else {
        let msg = unsafe { std::ffi::CStr::from_ptr(msg) }.to_string_lossy();
        Err(format!("stack overflow ({msg})"))
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
) -> Option<NonNull<T>> {
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
        Some(ptr)
    }
}

pub struct LuaTable {
    state: LuaState,
    index: i32,
    pos: Cell<usize>,
}

impl LuaTable {
    pub fn new(state: LuaState, narr: usize, nrec: usize) -> Self {
        unsafe {
            ffi::lua_createtable(state.as_ptr(), narr as i32, nrec as i32);
            LuaTable {
                state,
                index: ffi::lua_gettop(state.as_ptr()),
                pos: Cell::new(0),
            }
        }
    }

    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn insert<K, V>(&self, key: K, val: V) -> &Self
    where
        K: LuaPush,
        V: LuaPush,
    {
        unsafe {
            K::push(key, self.state);
            V::push(val, self.state);
            ffi::lua_rawset(self.state.as_ptr(), self.index);
        }
        self
    }

    pub fn push<V>(&self, val: V)
    where
        V: LuaPush,
    {
        unsafe {
            V::push(val, self.state);
            self.pos.set(self.pos.get() + 1);
            ffi::lua_rawseti(
                self.state.as_ptr(),
                self.index,
                self.pos.get() as ffi::lua_Integer,
            );
        }
    }

    pub fn push_table(&self, table: LuaTable) {
        debug_assert!(table.index == lua_top(self.state));
        unsafe {
            self.pos.set(self.pos.get() + 1);
            ffi::lua_rawseti(
                self.state.as_ptr(),
                self.index,
                self.pos.get() as ffi::lua_Integer,
            );
        }
    }

    pub fn rawseti(&self, n: usize) {
        unsafe {
            ffi::lua_rawseti(self.state.as_ptr(), self.index, n as ffi::lua_Integer);
        }
    }

    /// Pops the value from the top of the stack and sets it in the table at the specified key.
    pub fn insert_from_stack(&self) {
        unsafe {
            ffi::lua_rawset(self.state.as_ptr(), self.index);
        }
    }

    pub fn rawset_x<K, F>(&self, key: K, f: F) -> &Self
    where
        K: LuaPush,
        F: FnOnce(),
    {
        unsafe {
            K::push(key, self.state);
            f();
            ffi::lua_rawset(self.state.as_ptr(), self.index);
        }
        self
    }
}

fn lua_array_size(state: LuaState, idx: i32) -> usize {
    unsafe {
        let idx = ffi::lua_absindex(state.as_ptr(), idx);
        if ffi::lua_type(state.as_ptr(), idx) != ffi::LUA_TTABLE {
            return 0;
        }
        let len = ffi::lua_rawlen(state.as_ptr(), idx);
        if len == 0 {
            return 0;
        }

        // Lua table keys are unique. If every key is an integer in 1..=len and
        // there are exactly `len` keys, the table is necessarily contiguous and
        // has neither holes nor a hash part. This validates strict array
        // semantics in one traversal instead of raw-reading every slot first.
        let mut count = 0usize;
        ffi::lua_pushnil(state.as_ptr());
        while ffi::lua_next(state.as_ptr(), idx) != 0 {
            let valid = ffi::lua_isinteger(state.as_ptr(), -2) != 0 && {
                let key = ffi::lua_tointeger(state.as_ptr(), -2);
                key > 0 && (key as usize) <= len
            };
            if valid {
                count += 1;
                ffi::lua_pop(state.as_ptr(), 1);
            } else {
                ffi::lua_pop(state.as_ptr(), 2);
                return 0;
            }
        }

        if count == len { len } else { 0 }
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

// ---------------------------------------------------------------------------
// Lifetime-aware Lua context API
// ---------------------------------------------------------------------------

/// A borrow-scoped view of one Lua VM. It never owns or closes the VM.
pub struct LuaStack<'lua> {
    state: LuaState,
    _lua: PhantomData<&'lua mut ffi::lua_State>,
    _not_send: PhantomData<Rc<()>>,
}

/// Creates a context for the duration of a Lua callback.
pub unsafe fn with_context<R>(
    state: LuaState,
    f: impl for<'lua> FnOnce(&'lua mut LuaStack<'lua>) -> R,
) -> R {
    let mut context = unsafe { LuaStack::from_raw(state) };
    f(&mut context)
}

impl<'lua> LuaStack<'lua> {
    /// # Safety
    /// `state` must remain valid for the returned context's lifetime and must
    /// not be accessed concurrently from another thread.
    pub unsafe fn from_raw(state: LuaState) -> Self {
        Self {
            state,
            _lua: PhantomData,
            _not_send: PhantomData,
        }
    }

    pub fn state(&self) -> LuaState {
        self.state
    }
    pub fn as_ptr(&self) -> *mut ffi::lua_State {
        self.state.as_ptr()
    }
    pub fn top(&self) -> i32 {
        unsafe { ffi::lua_gettop(self.as_ptr()) }
    }
    pub fn abs_index(&self, index: i32) -> i32 {
        if index > 0 || index <= ffi::LUA_REGISTRYINDEX {
            index
        } else {
            self.top() + index + 1
        }
    }

    pub fn value<'ctx>(&'ctx self, index: i32) -> LuaStackValue<'ctx, 'lua> {
        let index = self.abs_index(index);
        LuaStackValue {
            context: self,
            index,
            kind: lua_type(self.state(), index),
        }
    }

    /// Returns the raw pointer stored in a full userdata slot without checking
    /// its Lua type or metatable.
    ///
    /// # Safety
    /// `index` must contain a non-null full userdata whose storage holds a
    /// valid `T` for every use of the returned pointer. The caller is also
    /// responsible for preventing aliased mutable access to that `T`.
    #[inline(always)]
    pub unsafe fn userdata_ptr_unchecked<T>(&self, index: i32) -> NonNull<T> {
        unsafe { NonNull::new_unchecked(ffi::lua_touserdata(self.as_ptr(), index).cast::<T>()) }
    }

    /// Returns the strict contiguous array length for a table at `index`.
    /// A sparse or mixed-key table returns zero, matching the legacy
    /// `LuaTable::array_len` behavior.
    pub fn array_len(&self, index: i32) -> usize {
        lua_array_size(self.state, index)
    }

    /// Returns Lua's raw length for a table or string without exposing the
    /// underlying state handle to callers.
    pub fn raw_len(&self, index: i32) -> usize {
        unsafe { ffi::lua_rawlen(self.as_ptr(), index) }
    }

    /// Borrows bytes from a rooted Lua string while a callback appends values
    /// above it on the Lua stack.
    ///
    /// # Safety
    /// The source slot must remain rooted and unchanged for the returned
    /// borrow's lifetime. Callers may only append values above that slot; they
    /// must not pop, replace, remove, reorder, or otherwise mutate the source
    /// value, and must not move the borrow across a different Lua state or an
    /// asynchronous boundary.
    pub unsafe fn value_bytes_append_only(&self, index: i32) -> Option<&[u8]> {
        unsafe { self.value(index).as_bytes_append_only() }
    }

    /// Converts the value at `index` using `T`'s strict [`FromLua`]
    /// implementation.
    ///
    /// Use `get::<Option<T>>` when absence is allowed but a present value must
    /// still have the expected type.
    pub fn get<T: FromLua>(&self, index: i32) -> Result<T, LuaTypeError> {
        T::from_lua(self, index)
    }

    /// Performs a lenient conversion, returning `None` for a missing value,
    /// nil, or a type mismatch.
    ///
    /// This preserves the legacy `lua_opt` behavior. New APIs should usually
    /// prefer `get::<Option<T>>` so malformed arguments remain visible.
    pub fn opt<T: FromLua>(&self, index: i32) -> Option<T> {
        self.get(index).ok()
    }

    /// Reads an optional value using Lua's truthiness rules.
    ///
    /// Missing and nil values return `None`. Every other Lua value returns
    /// `Some(lua_toboolean(value))`, so only `false` produces `Some(false)`.
    /// Use `get::<Option<bool>>` when a present value must be a Lua boolean.
    pub fn opt_truthy(&self, index: i32) -> Option<bool> {
        unsafe {
            if ffi::lua_isnoneornil(self.as_ptr(), index) != 0 {
                None
            } else {
                Some(ffi::lua_toboolean(self.as_ptr(), index) != 0)
            }
        }
    }

    /// Creates a lending cursor over a table's key/value pairs.
    pub fn table_cursor<'ctx>(&'ctx mut self, index: i32) -> LuaTableCursor<'ctx, 'lua> {
        LuaTableCursor::new(self, index)
    }

    /// Opens a table-valued raw field and returns a cursor whose drop restores
    /// the stack to its pre-field top. This keeps temporary field lookup and
    /// table iteration in one borrow-scoped operation.
    pub fn table_field_cursor<'ctx>(
        &'ctx mut self,
        index: i32,
        field: &str,
    ) -> Option<LuaTableCursor<'ctx, 'lua>> {
        let top = self.top();
        let index = self.abs_index(index);
        if self.value(index).kind() != LuaType::Table {
            return None;
        }
        unsafe {
            ffi::lua_pushlstring(self.as_ptr(), field.as_ptr() as *const c_char, field.len());
            ffi::lua_rawget(self.as_ptr(), index);
        }
        if self.value(-1).kind() != LuaType::Table {
            self.set_top(top);
            return None;
        }
        Some(LuaTableCursor::new_with_top(self, -1, top))
    }

    /// Checks whether a table value has a non-nil metafield without exposing
    /// the temporary value or changing the caller's stack top.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn has_metafield(&mut self, index: i32, field: *const c_char) -> bool {
        let present =
            unsafe { ffi::luaL_getmetafield(self.as_ptr(), index, field) != ffi::LUA_TNIL };
        if present {
            self.pop(1);
        }
        present
    }

    /// Creates a cursor over the contiguous array part of a table. A non-table,
    /// sparse, or mixed-key value produces an empty cursor; callers that need
    /// to distinguish an empty table from an invalid sequence should inspect
    /// the value type, `array_len`, and the table's keys separately.
    ///
    /// # Warning
    ///
    /// Values returned by `next()` must be consumed before advancing the
    /// cursor or mutating the Lua stack.
    pub fn array_cursor<'ctx>(&'ctx mut self, index: i32) -> LuaArrayCursor<'ctx, 'lua> {
        let len = self.array_len(index);
        LuaArrayCursor::new(self, index, len)
    }

    /// Creates an array cursor with a caller-provided length. This is used by
    /// encoders that already validated the sequence length and must preserve
    /// the legacy `expected_array_iter` behavior.
    pub fn array_cursor_len<'ctx>(
        &'ctx mut self,
        index: i32,
        len: usize,
    ) -> LuaArrayCursor<'ctx, 'lua> {
        LuaArrayCursor::new(self, index, len)
    }

    pub fn push<T: LuaPush>(&mut self, value: T) {
        value.push(self.state);
    }

    pub fn opt_field<T: FromLua>(&mut self, index: i32, field: &str) -> Option<T> {
        let top = self.top();
        let index = self.abs_index(index);
        if self.value(index).kind() != LuaType::Table {
            return None;
        }
        unsafe {
            ffi::lua_pushlstring(self.as_ptr(), field.as_ptr() as *const c_char, field.len());
            ffi::lua_rawget(self.as_ptr(), index);
        }
        let value = if unsafe { ffi::lua_isnil(self.as_ptr(), -1) != 0 } {
            None
        } else {
            T::from_lua(self, -1).ok()
        };
        self.set_top(top);
        value
    }

    pub fn pop(&mut self, count: i32) {
        unsafe { ffi::lua_pop(self.as_ptr(), count) }
    }
    pub fn set_top(&mut self, top: i32) {
        unsafe { ffi::lua_settop(self.as_ptr(), top) }
    }
}

/// A value borrowed from the Lua stack for the current context borrow.
#[derive(Clone, Copy)]
pub struct LuaStackValue<'ctx, 'lua> {
    context: &'ctx LuaStack<'lua>,
    index: i32,
    kind: LuaType,
}

impl<'ctx, 'lua> LuaStackValue<'ctx, 'lua> {
    pub fn state(&self) -> LuaState {
        self.context.state()
    }

    /// Returns the absolute stack slot represented by this value.
    ///
    /// The slot and cached type remain valid only while the surrounding
    /// cursor/context keeps the value rooted and unchanged; callers must not
    /// retain it across stack writes or reordering.
    pub fn index(&self) -> i32 {
        self.index
    }
    pub fn kind(&self) -> LuaType {
        self.kind
    }

    pub fn name(&self) -> &'static str {
        match self.kind() {
            LuaType::None => "none",
            LuaType::Nil => "nil",
            LuaType::Boolean => "boolean",
            LuaType::LightUserData => "lightuserdata",
            LuaType::Number | LuaType::Integer => "number",
            LuaType::String => "string",
            LuaType::Table => "table",
            LuaType::Function => "function",
            LuaType::UserData => "userdata",
            LuaType::Thread => "thread",
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        (self.kind() == LuaType::Boolean)
            .then(|| unsafe { ffi::lua_toboolean(self.context.as_ptr(), self.index) != 0 })
    }

    pub fn as_integer(&self) -> Option<i64> {
        (self.kind() == LuaType::Integer)
            .then(|| unsafe { ffi::lua_tointeger(self.context.as_ptr(), self.index) as i64 })
    }

    pub fn as_number(&self) -> Option<f64> {
        matches!(self.kind(), LuaType::Number | LuaType::Integer)
            .then(|| unsafe { ffi::lua_tonumber(self.context.as_ptr(), self.index) as f64 })
    }

    pub fn as_bytes(&self) -> Option<&'ctx [u8]> {
        if self.kind() != LuaType::String {
            return None;
        }
        unsafe {
            let mut len = 0;
            let ptr = NonNull::new(
                ffi::lua_tolstring(self.context.as_ptr(), self.index, &mut len) as *mut u8,
            )?;
            Some(std::slice::from_raw_parts(ptr.as_ptr(), len))
        }
    }

    /// Borrows string bytes while the caller only appends values above this
    /// stack slot and leaves the slot itself rooted and unchanged.
    ///
    /// # Safety
    /// The stack slot must not be popped, replaced, reordered, or otherwise
    /// mutated for the returned borrow's lifetime.
    pub unsafe fn as_bytes_append_only(&self) -> Option<&'ctx [u8]> {
        self.as_bytes()
    }

    pub fn as_str(&self) -> Option<&'ctx str> {
        self.as_bytes()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
    }

    /// Returns a string view that borrows valid UTF-8 directly from Lua and
    /// allocates only when invalid bytes must be replaced.
    pub fn as_string_lossy(&self) -> Option<Cow<'ctx, str>> {
        self.as_bytes().map(String::from_utf8_lossy)
    }

    pub fn as_userdata<T>(&self) -> Option<NonNull<T>> {
        (self.kind() == LuaType::UserData)
            .then(|| unsafe {
                NonNull::new(ffi::lua_touserdata(self.context.as_ptr(), self.index) as *mut T)
            })
            .flatten()
    }

    /// Returns the opaque pointer stored in a lightuserdata value.
    ///
    /// `Some(null)` represents a null lightuserdata; `None` means the stack
    /// value is not lightuserdata. Callers that know the pointee type must make
    /// that cast explicitly before dereferencing it.
    pub fn as_light_userdata(&self) -> Option<*mut std::ffi::c_void> {
        (self.kind() == LuaType::LightUserData)
            .then(|| unsafe { ffi::lua_touserdata(self.context.as_ptr(), self.index) })
    }
}

impl Display for LuaStackValue<'_, '_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            LuaType::None => f.write_str("none"),
            LuaType::Nil => f.write_str("nil"),
            LuaType::Boolean => write!(f, "{}", self.as_bool().unwrap_or(false)),
            LuaType::LightUserData => {
                let ptr = unsafe { ffi::lua_touserdata(self.context.as_ptr(), self.index) };
                write!(f, "{:p}", ptr)
            }
            LuaType::Number | LuaType::Integer => {
                write!(f, "{}", self.as_number().unwrap_or_default())
            }
            LuaType::String => {
                let value = self.as_string_lossy().unwrap_or_default();
                f.write_str(&value)
            }
            LuaType::Table => f.write_str("table"),
            LuaType::Function => {
                let ptr = unsafe { ffi::lua_topointer(self.context.as_ptr(), self.index) };
                write!(f, "{:p}", ptr)
            }
            LuaType::UserData => {
                let ptr = unsafe { ffi::lua_touserdata(self.context.as_ptr(), self.index) };
                write!(f, "{:p}", ptr)
            }
            LuaType::Thread => {
                let ptr = unsafe { ffi::lua_tothread(self.context.as_ptr(), self.index) };
                write!(f, "{:p}", ptr)
            }
        }
    }
}

pub trait LuaPush {
    fn push(self, state: LuaState);
}

macro_rules! impl_context_push_integer {
    ($($type:ty),* $(,)?) => { $(
        impl LuaPush for $type {
            fn push(self, state: LuaState) {
                unsafe { ffi::lua_pushinteger(state.as_ptr(), self as ffi::lua_Integer); }
            }
        }
    )* };
}

impl_context_push_integer!(i8, u8, i16, u16, i32, u32, i64, u64, isize, usize);

impl LuaPush for f64 {
    fn push(self, state: LuaState) {
        unsafe { ffi::lua_pushnumber(state.as_ptr(), self as ffi::lua_Number) }
    }
}
impl LuaPush for bool {
    fn push(self, state: LuaState) {
        unsafe { ffi::lua_pushboolean(state.as_ptr(), self as c_int) }
    }
}
impl LuaPush for &str {
    fn push(self, state: LuaState) {
        unsafe {
            ffi::lua_pushlstring(state.as_ptr(), self.as_ptr() as *const c_char, self.len());
        }
    }
}
impl LuaPush for String {
    fn push(self, state: LuaState) {
        self.as_str().push(state);
    }
}
impl LuaPush for &[u8] {
    fn push(self, state: LuaState) {
        unsafe {
            ffi::lua_pushlstring(state.as_ptr(), self.as_ptr() as *const c_char, self.len());
        }
    }
}
impl LuaPush for Vec<u8> {
    fn push(self, state: LuaState) {
        self.as_slice().push(state);
    }
}
impl LuaPush for LuaNil {
    fn push(self, state: LuaState) {
        unsafe { ffi::lua_pushnil(state.as_ptr()) }
    }
}

/// A lazily inspected key/value frame yielded by [`LuaTableCursor`].
///
/// Calling [`key`](Self::key) or [`value`](Self::value) performs the
/// corresponding Lua type query. An entry and every value borrowed from it
/// must be consumed before the parent iterator advances.
pub struct LuaTableEntry<'ctx, 'lua> {
    lua: NonNull<LuaStack<'lua>>,
    key_index: i32,
    value_index: i32,
    _ctx: PhantomData<&'ctx mut LuaStack<'lua>>,
}

impl<'lua> LuaTableEntry<'_, 'lua> {
    #[inline(always)]
    pub fn key(&self) -> LuaStackValue<'_, 'lua> {
        let context = unsafe { self.lua.as_ref() };
        LuaStackValue {
            context,
            index: self.key_index,
            kind: lua_type(context.state(), self.key_index),
        }
    }

    #[inline(always)]
    pub fn value(&self) -> LuaStackValue<'_, 'lua> {
        let context = unsafe { self.lua.as_ref() };
        LuaStackValue {
            context,
            index: self.value_index,
            kind: lua_type(context.state(), self.value_index),
        }
    }

    /// Directly reborrows the Lua context without stack guards.
    ///
    /// # Safety
    /// No value borrowed from this entry may still be used. The caller must
    /// preserve the entry's key/value slots and restore the current stack
    /// height before the iterator advances or is dropped.
    #[inline(always)]
    pub unsafe fn lua_mut(&mut self) -> &mut LuaStack<'lua> {
        debug_assert!(self.key_index > 0 && self.value_index > 0);
        unsafe { self.lua.as_mut() }
    }
}

/// A lending cursor over a Lua table's key/value pairs.
///
/// # Warning
///
/// A key/value returned by `next()` is valid only until the next cursor
/// operation or any stack mutation.
///
/// # Iterator safety contract
///
/// The [`Iterator`] implementation yields a lazy [`LuaTableEntry`] so the
/// cursor can be used in a `for` loop. The caller must consume every entry and
/// values borrowed from it within that loop iteration and must not retain or
/// return them after the iterator is advanced or dropped.
///
/// Do not use iterator operations that can retain items, including `collect`,
/// `peekable`, `zip`, `cycle`, `partition`, `unzip`, or collecting `map`/`filter`
/// results. Violating this contract can access a Lua stack slot after it has
/// been replaced or popped. Use the inherent lending [`LuaTableCursor::next`]
/// method whenever this contract cannot be guaranteed.
pub struct LuaTableCursor<'ctx, 'lua> {
    lua: &'ctx mut LuaStack<'lua>,
    index: i32,
    restore_top: i32,
    frame_top: i32,
    has_value: bool,
    finished: bool,
}

impl<'ctx, 'lua> LuaTableCursor<'ctx, 'lua> {
    fn new(lua: &'ctx mut LuaStack<'lua>, index: i32) -> Self {
        let frame_top = lua.top();
        Self::new_with_frame(lua, index, frame_top, frame_top)
    }

    fn new_with_top(lua: &'ctx mut LuaStack<'lua>, index: i32, restore_top: i32) -> Self {
        let frame_top = lua.top();
        Self::new_with_frame(lua, index, restore_top, frame_top)
    }

    fn new_with_frame(
        lua: &'ctx mut LuaStack<'lua>,
        index: i32,
        restore_top: i32,
        frame_top: i32,
    ) -> Self {
        let index = if index > 0 || index <= ffi::LUA_REGISTRYINDEX {
            index
        } else {
            frame_top + index + 1
        };
        let finished = lua.value(index).kind() != LuaType::Table;
        if !finished {
            unsafe {
                ffi::lua_pushnil(lua.as_ptr());
            }
        }
        Self {
            lua,
            index,
            restore_top,
            frame_top,
            has_value: false,
            finished,
        }
    }

    #[inline(always)]
    fn advance(&mut self) -> Option<(i32, i32)> {
        if self.finished {
            return None;
        }
        unsafe {
            if self.has_value {
                ffi::lua_pop(self.lua.as_ptr(), 1);
                self.has_value = false;
            }
            if ffi::lua_next(self.lua.as_ptr(), self.index) == 0 {
                self.finished = true;
                return None;
            }
            self.has_value = true;
            let key_index = self.frame_top + 1;
            let value_index = self.frame_top + 2;
            Some((key_index, value_index))
        }
    }

    pub fn next<'a>(&'a mut self) -> Option<(LuaStackValue<'a, 'lua>, LuaStackValue<'a, 'lua>)> {
        let (key_index, value_index) = self.advance()?;
        let context: &'a LuaStack<'lua> = &*self.lua;
        Some((
            LuaStackValue {
                context,
                index: key_index,
                kind: lua_type(self.lua.state(), key_index),
            },
            LuaStackValue {
                context,
                index: value_index,
                kind: lua_type(self.lua.state(), value_index),
            },
        ))
    }

    /// Directly reborrows the Lua context without stack guards or callbacks.
    ///
    /// # Safety
    /// The caller must preserve the cursor's key/value slots and restore the
    /// stack to its current height before advancing or dropping the cursor.
    #[inline(always)]
    pub unsafe fn lua_mut(&mut self) -> &mut LuaStack<'lua> {
        debug_assert!(self.has_value && !self.finished);
        &mut *self.lua
    }
}
impl Drop for LuaTableCursor<'_, '_> {
    fn drop(&mut self) {
        let frame_values = if self.finished {
            0
        } else if self.has_value {
            2
        } else {
            1
        };
        let owned_values = self.frame_top - self.restore_top + frame_values;
        if owned_values != 0 {
            unsafe { ffi::lua_pop(self.lua.as_ptr(), owned_values) }
        }
    }
}

/// Enables concise `for` loops over Lua table slots.
///
/// This implementation relies on the iterator safety contract documented on
/// [`LuaTableCursor`]. Standard iterator consumers that retain yielded items
/// must not be used.
impl<'ctx, 'lua> Iterator for LuaTableCursor<'ctx, 'lua> {
    type Item = LuaTableEntry<'ctx, 'lua>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let (key_index, value_index) = self.advance()?;
        Some(LuaTableEntry {
            lua: NonNull::from(&mut *self.lua),
            key_index,
            value_index,
            _ctx: PhantomData,
        })
    }
}

/// A lending cursor over a contiguous Lua table array.
///
/// # Warning
///
/// A value returned by `next()` is valid only until the next cursor operation
/// or any stack mutation.
pub struct LuaArrayCursor<'ctx, 'lua> {
    lua: &'ctx mut LuaStack<'lua>,
    index: i32,
    top: i32,
    pos: usize,
    len: usize,
    has_value: bool,
}

impl<'ctx, 'lua> LuaArrayCursor<'ctx, 'lua> {
    fn new(lua: &'ctx mut LuaStack<'lua>, index: i32, len: usize) -> Self {
        let top = lua.top();
        let index = if index > 0 || index <= ffi::LUA_REGISTRYINDEX {
            index
        } else {
            top + index + 1
        };
        let len = if lua.value(index).kind() == LuaType::Table {
            len
        } else {
            0
        };
        Self {
            lua,
            index,
            top,
            pos: 0,
            len,
            has_value: false,
        }
    }

    pub fn next<'a>(&'a mut self) -> Option<LuaStackValue<'a, 'lua>> {
        unsafe {
            if self.has_value {
                ffi::lua_pop(self.lua.as_ptr(), 1);
                self.has_value = false;
            }
            if self.pos >= self.len {
                return None;
            }
            self.pos += 1;
            ffi::lua_rawgeti(self.lua.as_ptr(), self.index, self.pos as ffi::lua_Integer);
            self.has_value = true;
            let context: &'a LuaStack<'lua> = &*self.lua;
            let value_index = self.top + 1;
            Some(LuaStackValue {
                context,
                index: value_index,
                kind: lua_type(self.lua.state(), value_index),
            })
        }
    }

    pub fn nested_array<'nested>(&'nested mut self, index: i32) -> LuaArrayCursor<'nested, 'lua> {
        let len = self.lua.array_len(index);
        LuaArrayCursor::new(&mut *self.lua, index, len)
    }

    /// Directly reborrows the Lua context without stack guards or callbacks.
    ///
    /// # Safety
    /// The caller must preserve the cursor's value slot and restore the stack
    /// to its current height before advancing or dropping the cursor.
    #[inline(always)]
    pub unsafe fn lua_mut(&mut self) -> &mut LuaStack<'lua> {
        debug_assert!(self.has_value);
        &mut *self.lua
    }
}

impl Drop for LuaArrayCursor<'_, '_> {
    fn drop(&mut self) {
        if self.has_value {
            unsafe { ffi::lua_pop(self.lua.as_ptr(), 1) }
        }
    }
}

#[derive(Debug)]
pub struct LuaTypeError {
    pub expected: &'static str,
    pub actual: LuaType,
}
impl Display for LuaTypeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "expected {}, got {:?}", self.expected, self.actual)
    }
}
impl std::error::Error for LuaTypeError {}

impl From<LuaTypeError> for String {
    fn from(error: LuaTypeError) -> Self {
        error.to_string()
    }
}

/// Converts a value rooted on a Lua stack into an owned Rust value.
///
/// Implementations are strict about the accepted Lua type. In particular,
/// `String` requires valid UTF-8; use `Vec<u8>` for arbitrary Lua strings.
pub trait FromLua: Sized {
    fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError>;
}

impl FromLua for bool {
    fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError> {
        let value = lua.value(index);
        value.as_bool().ok_or(LuaTypeError {
            expected: "boolean",
            actual: value.kind(),
        })
    }
}
impl FromLua for f64 {
    fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError> {
        let mut is_number = 0;
        let value = unsafe { ffi::lua_tonumberx(lua.as_ptr(), index, &mut is_number) };
        if is_number != 0 {
            Ok(value as f64)
        } else {
            Err(LuaTypeError {
                expected: "number",
                actual: lua_type(lua.state(), index),
            })
        }
    }
}
macro_rules! impl_context_from_lua_integer {
    ($($type:ty),* $(,)?) => { $(
        impl FromLua for $type {
            fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError> {
                let mut is_integer = 0;
                let value = unsafe {
                    ffi::lua_tointegerx(lua.as_ptr(), index, &mut is_integer)
                };
                if is_integer != 0 {
                    Ok(value as $type)
                } else {
                    Err(LuaTypeError {
                        expected: "integer",
                        actual: lua_type(lua.state(), index),
                    })
                }
            }
        }
    )* };
}
impl_context_from_lua_integer!(i8, u8, i16, u16, i32, u32, i64, u64, isize, usize);
impl FromLua for String {
    fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError> {
        let value = lua.value(index);
        value.as_str().map(str::to_owned).ok_or(LuaTypeError {
            expected: "UTF-8 string",
            actual: value.kind(),
        })
    }
}
impl FromLua for Vec<u8> {
    fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError> {
        let value = lua.value(index);
        value.as_bytes().map(<[u8]>::to_vec).ok_or(LuaTypeError {
            expected: "string",
            actual: value.kind(),
        })
    }
}
impl FromLua for LuaNil {
    fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError> {
        let value = lua.value(index);
        if value.kind() == LuaType::Nil {
            Ok(LuaNil)
        } else {
            Err(LuaTypeError {
                expected: "nil",
                actual: value.kind(),
            })
        }
    }
}
impl<T: FromLua> FromLua for Option<T> {
    fn from_lua(lua: &LuaStack<'_>, index: i32) -> Result<Self, LuaTypeError> {
        if matches!(lua.value(index).kind(), LuaType::None | LuaType::Nil) {
            Ok(None)
        } else {
            T::from_lua(lua, index).map(Some)
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/laux.rs"]
mod context_tests;
