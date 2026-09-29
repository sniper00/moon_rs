use std::{
    ffi::{c_int, c_void},
    fmt::{Display, Formatter},
};

use moon_base::{
    cstr,
    ffi::{self, LUA_TLIGHTUSERDATA},
    laux::{self, LuaNil, LuaStack, LuaState, LuaType},
    lreg_null, lreg_try, luaL_newlib,
};

use moon_runtime::buffer::Buffer;

const TYPE_NIL: u8 = 0;
const TYPE_BOOLEAN: u8 = 1;
// hibits 0 false 1 true
const TYPE_NUMBER: u8 = 2;
// hibits 0 : 0 , 1: byte, 2:word, 4: dword, 6: qword, 8 : double
const TYPE_NUMBER_ZERO: u8 = 0;
const TYPE_NUMBER_BYTE: u8 = 1;
const TYPE_NUMBER_WORD: u8 = 2;
const TYPE_NUMBER_DWORD: u8 = 4;
const TYPE_NUMBER_QWORD: u8 = 6;
const TYPE_NUMBER_REAL: u8 = 8;

const TYPE_USERDATA: u8 = 3;
const TYPE_SHORT_STRING: u8 = 4;
// hibits 0~31 : len
const TYPE_LONG_STRING: u8 = 5;
const TYPE_TABLE: u8 = 6;

const MAX_COOKIE: u8 = 32;

macro_rules! combine_type {
    ($t:expr, $v:expr) => {
        ($t) | ($v) << 3
    };
}

// const BLOCK_SIZE: usize = 128;
const MAX_DEPTH: usize = 32;

struct StackTopGuard {
    state: LuaState,
    top: i32,
}

impl StackTopGuard {
    fn new(lua: &LuaStack<'_>) -> Self {
        Self {
            state: lua.state(),
            top: lua.top(),
        }
    }
}

impl Drop for StackTopGuard {
    fn drop(&mut self) {
        unsafe { ffi::lua_settop(self.state.as_ptr(), self.top) }
    }
}

fn write_nil(buf: &mut Vec<u8>) {
    let n = TYPE_NIL;
    buf.push(n);
}

fn write_boolean(buf: &mut Vec<u8>, boolean: bool) {
    let n = combine_type!(TYPE_BOOLEAN, if boolean { 1 } else { 0 });
    buf.push(n);
}

fn write_integer(buf: &mut Vec<u8>, v: i64) {
    let type_ = TYPE_NUMBER;
    if v == 0 {
        let n = combine_type!(type_, TYPE_NUMBER_ZERO);
        buf.push(n);
    } else if v != v as i32 as i64 {
        let n = combine_type!(type_, TYPE_NUMBER_QWORD);
        buf.push(n);
        buf.extend_from_slice(&v.to_le_bytes());
    } else if v < 0 {
        let n = combine_type!(type_, TYPE_NUMBER_DWORD);
        buf.push(n);
        buf.extend_from_slice(&(v as i32).to_le_bytes());
    } else if v < 0x100 {
        let n = combine_type!(type_, TYPE_NUMBER_BYTE);
        buf.push(n);
        buf.push(v as u8);
    } else if v < 0x10000 {
        let n = combine_type!(type_, TYPE_NUMBER_WORD);
        buf.push(n);
        buf.extend_from_slice(&(v as u16).to_le_bytes());
    } else {
        let n = combine_type!(type_, TYPE_NUMBER_DWORD);
        buf.push(n);
        buf.extend_from_slice(&(v as u32).to_le_bytes());
    }
}

fn write_real(buf: &mut Vec<u8>, v: f64) {
    let n = combine_type!(TYPE_NUMBER, TYPE_NUMBER_REAL);
    buf.push(n);
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_pointer(buf: &mut Vec<u8>, v: *const std::ffi::c_void) {
    let n = TYPE_USERDATA;
    buf.push(n);
    buf.extend_from_slice(&(v as usize).to_le_bytes());
}

fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len < MAX_COOKIE as usize {
        let n = combine_type!(TYPE_SHORT_STRING, len as u8);
        buf.push(n);
        if len > 0 {
            buf.extend_from_slice(bytes);
        }
    } else {
        let n: u8;
        if len < 0x10000 {
            n = combine_type!(TYPE_LONG_STRING, 2);
            buf.push(n);
            buf.extend_from_slice(&(len as u16).to_le_bytes());
        } else {
            n = combine_type!(TYPE_LONG_STRING, 4);
            buf.push(n);
            buf.extend_from_slice(&(len as u32).to_le_bytes());
        }
        buf.extend_from_slice(bytes);
    }
}

fn write_table_array(
    lua: &mut LuaStack<'_>,
    index: i32,
    buf: &mut Vec<u8>,
    depth: i32,
) -> Result<usize, String> {
    let array_size = lua.raw_len(index);
    if array_size >= MAX_COOKIE as usize - 1 {
        let n = combine_type!(TYPE_TABLE, MAX_COOKIE - 1);
        buf.push(n);
        write_integer(buf, array_size as i64);
    } else {
        let n = combine_type!(TYPE_TABLE, array_size as u8);
        buf.push(n);
    }

    for i in 1..=array_size {
        unsafe { ffi::lua_rawgeti(lua.as_ptr(), index, i as ffi::lua_Integer) };
        let kind = laux::lua_type(lua.state(), -1);
        pack_value(lua, -1, kind, buf, depth)?;
        lua.pop(1);
    }

    Ok(array_size)
}

fn write_table_hash(
    lua: &mut LuaStack<'_>,
    index: i32,
    buf: &mut Vec<u8>,
    depth: i32,
    array_size: usize,
) -> Result<i32, String> {
    for mut entry in lua.table_cursor(index) {
        let (key_index, key_kind, skip) = {
            let key = entry.key();
            let key_kind = key.kind();
            let skip = key_kind == LuaType::Integer
                && key
                    .as_integer()
                    .is_some_and(|key| key > 0 && (key as usize) <= array_size);
            (key.index(), key_kind, skip)
        };
        if skip {
            continue;
        }

        let (value_index, value_kind) = {
            let value = entry.value();
            (value.index(), value.kind())
        };
        unsafe {
            let lua = entry.lua_mut();
            pack_value(lua, key_index, key_kind, buf, depth)?;
            pack_value(lua, value_index, value_kind, buf, depth)?;
        }
    }

    write_nil(buf);

    Ok(0)
}

#[cfg(test)]
fn write_table_hash_ffi(
    lua: &mut LuaStack<'_>,
    index: i32,
    buf: &mut Vec<u8>,
    depth: i32,
    array_size: usize,
) -> Result<i32, String> {
    unsafe {
        ffi::lua_pushnil(lua.as_ptr());
        while ffi::lua_next(lua.as_ptr(), index) != 0 {
            let key_kind = laux::lua_type(lua.state(), -2);
            let skip = key_kind == LuaType::Integer && {
                let key = ffi::lua_tointeger(lua.as_ptr(), -2);
                key > 0 && (key as usize) <= array_size
            };
            if !skip {
                let value_kind = laux::lua_type(lua.state(), -1);
                pack_value(lua, -2, key_kind, buf, depth)?;
                pack_value(lua, -1, value_kind, buf, depth)?;
            }
            ffi::lua_pop(lua.as_ptr(), 1);
        }
    }

    write_nil(buf);
    Ok(0)
}

fn write_table_metapairs(
    lua: &mut LuaStack<'_>,
    index: i32,
    buf: &mut Vec<u8>,
    depth: i32,
) -> Result<i32, String> {
    let n = combine_type!(TYPE_TABLE, 0);
    buf.push(n);

    let state = lua.state();
    unsafe {
        ffi::lua_pushvalue(state.as_ptr(), index);
        if ffi::lua_pcall(state.as_ptr(), 1, 3, 0) != ffi::LUA_OK {
            return Err(take_pcall_error(lua, "__pairs"));
        }
        loop {
            ffi::lua_pushvalue(state.as_ptr(), -2);
            ffi::lua_pushvalue(state.as_ptr(), -2);
            ffi::lua_copy(state.as_ptr(), -5, -3);
            if ffi::lua_pcall(state.as_ptr(), 2, 2, 0) != ffi::LUA_OK {
                return Err(take_pcall_error(lua, "__pairs iterator"));
            }

            if laux::lua_type(state, -2) == LuaType::Nil {
                laux::lua_pop(state, 4);
                break;
            }
            pack_one(lua, -2, buf, depth)?;
            pack_one(lua, -1, buf, depth)?;
            laux::lua_pop(state, 1);
        }
    }

    write_nil(buf);

    Ok(0)
}

/// Pop and format the error object left on top of the stack by a failed
/// `lua_pcall`, so the failure can be surfaced as an encoder `Err` instead of
/// being silently swallowed.
fn take_pcall_error(lua: &mut LuaStack<'_>, what: &str) -> String {
    let msg = lua
        .value(-1)
        .as_string_lossy()
        .map(|message| message.into_owned())
        .unwrap_or_else(|| "unknown error".to_string());
    lua.pop(1);
    format!("serialize {} error: {}", what, msg)
}

fn write_table(
    lua: &mut LuaStack<'_>,
    index: i32,
    buf: &mut Vec<u8>,
    depth: i32,
) -> Result<i32, String> {
    let state = lua.state();
    laux::lua_checkstack(state, ffi::LUA_MINSTACK, cstr!("serialize"))?;
    let index = lua.abs_index(index);
    let _stack = StackTopGuard::new(lua);
    unsafe {
        // `__pairs` is a call target, so unlike a presence check we must keep
        // the metafield function on the stack for `write_table_metapairs`.
        if ffi::luaL_getmetafield(state.as_ptr(), index, cstr!("__pairs")) != ffi::LUA_TNIL {
            write_table_metapairs(lua, index, buf, depth)?;
        } else {
            let array_size = write_table_array(lua, index, buf, depth)?;
            write_table_hash(lua, index, buf, depth, array_size)?;
        }
    }
    Ok(0)
}

fn pack_one(
    lua: &mut LuaStack<'_>,
    index: i32,
    buf: &mut Vec<u8>,
    depth: i32,
) -> Result<(), String> {
    let value = lua.value(index);
    pack_value(lua, value.index(), value.kind(), buf, depth)
}

fn pack_value(
    lua: &mut LuaStack<'_>,
    index: i32,
    kind: LuaType,
    buf: &mut Vec<u8>,
    depth: i32,
) -> Result<(), String> {
    if depth > MAX_DEPTH as i32 {
        return Err("serialize can't pack too depth table".to_string());
    }
    let state = lua.state();
    debug_assert_eq!(laux::lua_type(state, index), kind);
    match kind {
        LuaType::Nil => {
            write_nil(buf);
        }
        LuaType::Number => {
            let v = unsafe { ffi::lua_tonumber(state.as_ptr(), index) as f64 };
            if v.is_nan() {
                return Err("serialize can't pack 'nan' number value".to_string());
            }
            write_real(buf, v);
        }
        LuaType::Integer => {
            let v = unsafe { ffi::lua_tointeger(state.as_ptr(), index) as i64 };
            write_integer(buf, v);
        }
        LuaType::Boolean => {
            let v = unsafe { ffi::lua_toboolean(state.as_ptr(), index) != 0 };
            write_boolean(buf, v);
        }
        LuaType::String => {
            let mut len = 0;
            let ptr = unsafe { ffi::lua_tolstring(state.as_ptr(), index, &mut len) };
            debug_assert!(!ptr.is_null());
            let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
            write_bytes(buf, bytes);
        }
        LuaType::LightUserData => {
            let ptr = unsafe { ffi::lua_touserdata(state.as_ptr(), index) };
            write_pointer(buf, ptr);
        }
        LuaType::Table => {
            write_table(lua, index, buf, depth + 1)?;
        }
        _ => {
            return Err(format!(
                "Unsupport type `{}` to serialize",
                lua.value(index).name()
            ));
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum DecodeError {
    InvalidStream { remaining: usize, line: u32 },
    TooDeep,
    StackOverflow,
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStream { remaining, line } => {
                write!(f, "Invalid serialize stream {remaining} (line:{line})")
            }
            Self::TooDeep => f.write_str("serialize unpack: too deep"),
            Self::StackOverflow => f.write_str("stack overflow"),
        }
    }
}

macro_rules! invalid_stream {
    ($rb:expr) => {
        $rb.fail_invalid(line!());
        return None;
    };
}

struct ReadBlock<'a> {
    buf: &'a [u8],
    pos: usize,
    error: Option<DecodeError>,
}

impl ReadBlock<'_> {
    fn len(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn as_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.pos) }
    }

    fn fail_invalid(&mut self, line: u32) {
        self.fail(DecodeError::InvalidStream {
            remaining: self.len(),
            line,
        });
    }

    fn fail(&mut self, error: DecodeError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    fn error(&self) -> DecodeError {
        self.error.unwrap_or(DecodeError::InvalidStream {
            remaining: self.len(),
            line: 0,
        })
    }

    fn read_byte(&mut self) -> Option<u8> {
        if self.pos >= self.buf.len() {
            invalid_stream!(self);
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Some(b)
    }

    fn try_read_byte(&mut self) -> Option<u8> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Some(b)
    }

    fn read_u16(&mut self) -> Option<u16> {
        let mut n = [0u8; 2];
        if self.len() < n.len() {
            invalid_stream!(self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 2]);
        self.pos += 2;
        Some(u16::from_le_bytes(n))
    }

    fn read_u32(&mut self) -> Option<u32> {
        let mut n = [0u8; 4];
        if self.len() < n.len() {
            invalid_stream!(self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Some(u32::from_le_bytes(n))
    }

    fn read_i32(&mut self) -> Option<i32> {
        let mut n = [0u8; 4];
        if self.len() < n.len() {
            invalid_stream!(self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Some(i32::from_le_bytes(n))
    }

    fn read_i64(&mut self) -> Option<i64> {
        let mut n = [0u8; 8];
        if self.len() < n.len() {
            invalid_stream!(self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Some(i64::from_le_bytes(n))
    }

    fn read_real(&mut self) -> Option<f64> {
        let mut n = [0u8; 8];
        if self.len() < n.len() {
            invalid_stream!(self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Some(f64::from_le_bytes(n))
    }

    fn read_pointer(&mut self) -> Option<*mut std::ffi::c_void> {
        let mut n = [0u8; std::mem::size_of::<usize>()];
        if self.len() < n.len() {
            invalid_stream!(self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + std::mem::size_of::<usize>()]);
        self.pos += std::mem::size_of::<usize>();
        Some(usize::from_le_bytes(n) as *mut std::ffi::c_void)
    }

    fn consume(&mut self, len: usize) -> Option<&[u8]> {
        if self.len() < len {
            invalid_stream!(self);
        }
        let pos = self.pos;
        self.pos += len;
        Some(&self.buf[pos..pos + len])
    }

    fn offset(&self) -> usize {
        self.pos
    }
}

fn get_integer(br: &mut ReadBlock, cookie: u8) -> Option<i64> {
    match cookie {
        TYPE_NUMBER_ZERO => Some(0),
        TYPE_NUMBER_BYTE => Some(br.read_byte()? as i64),
        TYPE_NUMBER_WORD => Some(br.read_u16()? as i64),
        TYPE_NUMBER_DWORD => Some(br.read_i32()? as i64),
        TYPE_NUMBER_QWORD => br.read_i64(),
        _ => {
            invalid_stream!(br);
        }
    }
}

fn push_bytes(state: LuaState, br: &mut ReadBlock, len: usize) -> Option<()> {
    laux::lua_push(state, br.consume(len)?);
    Some(())
}

fn unpack_one(state: LuaState, br: &mut ReadBlock, depth: usize) -> Option<()> {
    let type_ = br.read_byte()?;
    push_value(state, br, type_ & 0x7, type_ >> 3, depth)
}

fn push_value(
    state: LuaState,
    br: &mut ReadBlock,
    type_: u8,
    cookie: u8,
    depth: usize,
) -> Option<()> {
    match type_ {
        TYPE_NIL => {
            laux::lua_push(state, LuaNil {});
        }
        TYPE_BOOLEAN => {
            laux::lua_push(state, cookie != 0);
        }
        TYPE_NUMBER => {
            if cookie == TYPE_NUMBER_REAL {
                laux::lua_push(state, br.read_real()?);
            } else {
                laux::lua_push(state, get_integer(br, cookie)?);
            }
        }
        TYPE_USERDATA => {
            laux::lua_pushlightuserdata(state, br.read_pointer()?);
        }
        TYPE_SHORT_STRING => {
            push_bytes(state, br, cookie as usize)?;
        }
        TYPE_LONG_STRING => {
            if cookie == 2 {
                let n = br.read_u16()?;
                push_bytes(state, br, n as usize)?;
            } else {
                if cookie != 4 {
                    invalid_stream!(br);
                }
                let n = br.read_u32()?;
                push_bytes(state, br, n as usize)?;
            }
        }
        TYPE_TABLE => {
            unpack_table(state, br, cookie as usize, depth)?;
        }
        _ => {
            invalid_stream!(br);
        }
    }
    Some(())
}

fn unpack_table(
    state: LuaState,
    br: &mut ReadBlock,
    mut array_size: usize,
    depth: usize,
) -> Option<()> {
    if depth > MAX_DEPTH {
        br.fail(DecodeError::TooDeep);
        return None;
    }
    if array_size == MAX_COOKIE as usize - 1 {
        let type_ = br.read_byte()?;
        let cookie = type_ >> 3;
        if (type_ & 7) != TYPE_NUMBER || cookie == TYPE_NUMBER_REAL {
            invalid_stream!(br);
        }
        array_size = get_integer(br, cookie)? as usize;
    }
    unsafe {
        if ffi::lua_checkstack(state.as_ptr(), ffi::LUA_MINSTACK) == 0 {
            br.fail(DecodeError::StackOverflow);
            return None;
        }
        ffi::lua_createtable(state.as_ptr(), array_size as i32, 0);
        for i in 1..=array_size {
            unpack_one(state, br, depth + 1)?;
            ffi::lua_rawseti(state.as_ptr(), -2, i as ffi::lua_Integer);
        }

        loop {
            unpack_one(state, br, depth + 1)?;
            if ffi::lua_isnil(state.as_ptr(), -1) != 0 {
                ffi::lua_pop(state.as_ptr(), 1);
                return Some(());
            }
            unpack_one(state, br, depth + 1)?;
            ffi::lua_rawset(state.as_ptr(), -3);
        }
    }
}

fn pack(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let n = laux::lua_top(state);
    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        pack_one(lua, i, buf.as_mut_vec(), 0)?;
    }

    laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);

    Ok(1)
}

fn pack_string(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let n = laux::lua_top(state);
    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        pack_one(lua, i, buf.as_mut_vec(), 0)?;
    }

    laux::lua_push(state, buf.as_slice());

    Ok(1)
}

fn unpack(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    unsafe {
        if ffi::lua_isnoneornil(state.as_ptr(), 1) == 1 {
            return Ok(0);
        }

        let mut len = 0;
        let data;
        if laux::lua_type(state, 1) == LuaType::String {
            data = ffi::lua_tolstring(state.as_ptr(), 1, &mut len) as *const u8;
        } else {
            data = ffi::lua_touserdata(state.as_ptr(), 1) as *const u8;
            // The pointer/length pair is a trusted contract with the C dispatch
            // layer (the runtime hands the protocol unpacker the real message
            // length). The one value we can still sanity-check cheaply is the
            // sign: a negative integer would wrap to a huge `usize` and turn the
            // `from_raw_parts` below into a wild out-of-bounds read.
            let raw_len = lua
                .get::<i64>(2)
                .map_err(|_| "deserialize length must be an integer".to_string())?;
            if raw_len < 0 {
                return Err("deserialize negative length".to_string());
            }
            len = raw_len as usize;
        }

        if len == 0 {
            return Ok(0);
        }

        if data.is_null() {
            return Err("deserialize null pointer".to_string());
        }

        decode_bytes(state, std::slice::from_raw_parts(data, len))
            .map_err(|error| error.to_string())
    }
}

/// Decode a serialized Lua payload from a runtime message (`PTYPE_LUA` / `PTYPE_DEBUG`).
///
/// The buffer is *borrowed* from the message, never taken. Malformed and
/// over-deep streams return `Err`; only Lua's own stack-allocation failure can
/// still leave through a VM-level longjmp. Leaving the buffer inside the
/// `Message` means the actor frame that owns it remains responsible for freeing
/// the storage on every normal success or error path.
pub unsafe extern "C-unwind" fn decode_buffer_message(
    state: LuaState,
    m: *mut moon_runtime::context::Message,
) -> c_int {
    match unsafe { crate::message_decode::borrow_buffer(m) } {
        Ok(buf) => match unsafe { decode_bytes(state, buf.as_slice()) } {
            Ok(count) => count,
            Err(e) => {
                laux::lua_settop(state, 0);
                crate::lua_push_error_tuple(state, &e.to_string())
            }
        },
        Err(e) => crate::lua_push_error_tuple(state, &e),
    }
}

unsafe fn decode_bytes(state: LuaState, data: &[u8]) -> Result<c_int, DecodeError> {
    if data.is_empty() {
        return Ok(0);
    }

    laux::lua_settop(state, 0);

    let br = &mut ReadBlock {
        buf: data,
        pos: 0,
        error: None,
    };

    let mut i = 0;
    loop {
        if i % 8 == 7 {
            if unsafe { ffi::lua_checkstack(state.as_ptr(), 8) } == 0 {
                return Err(DecodeError::StackOverflow);
            }
        }
        i += 1;

        if let Some(type_) = br.try_read_byte() {
            let cookie = type_ >> 3;
            if push_value(state, br, type_ & 0x7, cookie, 0).is_none() {
                return Err(br.error());
            }
        } else {
            break;
        }
    }

    Ok(laux::lua_top(state) as c_int)
}

fn peek_one(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    unsafe {
        if ffi::lua_isnoneornil(state.as_ptr(), 1) == 1 {
            return Ok(0);
        }

        if ffi::lua_type(state.as_ptr(), 1) != LUA_TLIGHTUSERDATA {
            return Err("peek_one need lightuserdata".to_string());
        }

        let seek = lua.opt_truthy(2).unwrap_or(false);

        let buf = ffi::lua_touserdata(state.as_ptr(), 1) as *mut Buffer;
        if buf.is_null() {
            return Err("null buffer pointer".to_string());
        }

        if (*buf).is_empty() {
            return Ok(0);
        }

        let br = &mut ReadBlock {
            buf: std::slice::from_raw_parts((*buf).as_ptr(), (*buf).len()),
            pos: 0,
            error: None,
        };

        let type_ = match br.read_byte() {
            Some(type_) => type_,
            None => return Err(br.error().to_string()),
        };

        if push_value(state, br, type_ & 0x7, type_ >> 3, 0).is_none() {
            return Err(br.error().to_string());
        }

        if seek {
            (*buf).consume(br.offset());
        }

        ffi::lua_pushlightuserdata(state.as_ptr(), br.as_ptr() as *mut c_void);
        ffi::lua_pushinteger(state.as_ptr(), br.len() as i64);

        Ok(3)
    }
}

pub unsafe extern "C-unwind" fn luaopen_seri(state: LuaState) -> c_int {
    let l = [
        lreg_try!("pack", pack),
        lreg_try!("packstring", pack_string),
        lreg_try!("unpack", unpack),
        lreg_try!("unpack_one", peek_one),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use moon_base::laux::LuaGlobalState;
    use std::{hint::black_box, ptr::NonNull, time::Instant};

    #[test]
    fn empty_pack_returns_a_buffer_and_preserves_zero_values() {
        let state = NonNull::new(unsafe { ffi::luaL_newstate() }).unwrap();
        let _owner = LuaGlobalState::new(state);
        let mut lua = unsafe { LuaStack::from_raw(state) };
        assert_eq!(pack(&mut lua).unwrap(), 1);
        let ptr = unsafe { ffi::lua_touserdata(state.as_ptr(), -1) } as *mut Buffer;
        let buf = unsafe { Box::from_raw(ptr) };
        assert!(buf.as_slice().is_empty());
        assert!(matches!(
            unsafe { decode_bytes(state, buf.as_slice()) },
            Ok(0)
        ));
        unsafe {
            ffi::lua_settop(state.as_ptr(), 0);
        }
        assert_eq!(pack_string(&mut lua).unwrap(), 1);
        assert_eq!(laux::lua_type(state, -1), LuaType::String);
        assert_eq!(unsafe { ffi::lua_rawlen(state.as_ptr(), -1) }, 0);
        unsafe {
            ffi::lua_settop(state.as_ptr(), 0);
            ffi::lua_pushnil(state.as_ptr());
        }
        assert_eq!(pack_string(&mut lua).unwrap(), 1);
        assert!(unsafe { ffi::lua_rawlen(state.as_ptr(), -1) } > 0);
    }

    fn push_benchmark_table(state: LuaState, array_size: usize, hash_size: usize) {
        unsafe {
            ffi::lua_createtable(state.as_ptr(), array_size as i32, hash_size as i32);
            for i in 1..=array_size {
                ffi::lua_pushinteger(state.as_ptr(), i as ffi::lua_Integer);
                ffi::lua_rawseti(state.as_ptr(), -2, i as ffi::lua_Integer);
            }
            for i in 1..=hash_size {
                let key = format!("field{i}");
                ffi::lua_pushlstring(state.as_ptr(), key.as_ptr().cast(), key.len());
                ffi::lua_pushinteger(state.as_ptr(), i as ffi::lua_Integer);
                ffi::lua_rawset(state.as_ptr(), -3);
            }
        }
    }

    fn run_hash_writer(
        lua: &mut LuaStack<'_>,
        array_size: usize,
        iterations: usize,
        lazy: bool,
        buf: &mut Vec<u8>,
    ) {
        for _ in 0..iterations {
            buf.clear();
            if lazy {
                write_table_hash(lua, 1, buf, 0, array_size).expect("lazy hash writer");
            } else {
                write_table_hash_ffi(lua, 1, buf, 0, array_size).expect("FFI hash writer");
            }
            black_box(buf.len());
        }
    }

    #[test]
    #[ignore = "manual release benchmark"]
    fn benchmark_lazy_hash_cursor_against_ffi() {
        const ITERATIONS: usize = 1_000_000;
        const ROUNDS: usize = 6;

        for (name, array_size, hash_size) in
            [("array32", 32, 0), ("hash16", 0, 16), ("mixed32_8", 32, 8)]
        {
            let state =
                NonNull::new(unsafe { ffi::luaL_newstate() }).expect("Lua state allocation");
            let _owner = LuaGlobalState::new(state);
            push_benchmark_table(state, array_size, hash_size);
            let mut lua = unsafe { LuaStack::from_raw(state) };
            let mut ffi_buf = Vec::with_capacity(512);
            let mut lazy_buf = Vec::with_capacity(512);

            run_hash_writer(&mut lua, array_size, 10_000, false, &mut ffi_buf);
            run_hash_writer(&mut lua, array_size, 10_000, true, &mut lazy_buf);
            assert_eq!(lazy_buf, ffi_buf);

            let mut ffi_elapsed = 0.0;
            let mut lazy_elapsed = 0.0;
            for round in 0..ROUNDS {
                if round % 2 == 0 {
                    let started = Instant::now();
                    run_hash_writer(&mut lua, array_size, ITERATIONS, false, &mut ffi_buf);
                    ffi_elapsed += started.elapsed().as_secs_f64();

                    let started = Instant::now();
                    run_hash_writer(&mut lua, array_size, ITERATIONS, true, &mut lazy_buf);
                    lazy_elapsed += started.elapsed().as_secs_f64();
                } else {
                    let started = Instant::now();
                    run_hash_writer(&mut lua, array_size, ITERATIONS, true, &mut lazy_buf);
                    lazy_elapsed += started.elapsed().as_secs_f64();

                    let started = Instant::now();
                    run_hash_writer(&mut lua, array_size, ITERATIONS, false, &mut ffi_buf);
                    ffi_elapsed += started.elapsed().as_secs_f64();
                }
            }

            assert_eq!(lazy_buf, ffi_buf);
            let operations = (ITERATIONS * ROUNDS) as f64;
            eprintln!(
                "{name}: ffi={:.2} ns lazy={:.2} ns delta={:+.2}%",
                ffi_elapsed / operations * 1e9,
                lazy_elapsed / operations * 1e9,
                (lazy_elapsed / ffi_elapsed - 1.0) * 100.0,
            );
        }
    }

    #[test]
    fn mixed_table_round_trip_preserves_all_array_and_hash_entries() {
        const ARRAY_SIZE: usize = 32;
        const HASH_SIZE: usize = 8;

        let state = NonNull::new(unsafe { ffi::luaL_newstate() }).expect("Lua state allocation");
        let _owner = LuaGlobalState::new(state);
        unsafe {
            ffi::lua_createtable(state.as_ptr(), ARRAY_SIZE as i32, HASH_SIZE as i32);
            for i in 1..=ARRAY_SIZE {
                ffi::lua_pushinteger(state.as_ptr(), (i as ffi::lua_Integer) * 10);
                ffi::lua_rawseti(state.as_ptr(), -2, i as ffi::lua_Integer);
            }
            for i in 1..=HASH_SIZE {
                let key = format!("field{i}");
                ffi::lua_pushlstring(state.as_ptr(), key.as_ptr().cast(), key.len());
                ffi::lua_pushinteger(state.as_ptr(), 1_000 + i as ffi::lua_Integer);
                ffi::lua_rawset(state.as_ptr(), -3);
            }
        }

        let encoded = {
            let mut lua = unsafe { LuaStack::from_raw(state) };
            let mut encoded = Vec::new();
            pack_one(&mut lua, 1, &mut encoded, 0).expect("pack mixed table");
            encoded
        };

        let count = match unsafe { decode_bytes(state, &encoded) } {
            Ok(count) => count,
            Err(error) => panic!("unpack mixed table: {error}"),
        };
        assert_eq!(count, 1);
        assert_eq!(unsafe { ffi::lua_gettop(state.as_ptr()) }, 1);
        assert_eq!(
            unsafe { ffi::lua_rawlen(state.as_ptr(), 1) },
            ARRAY_SIZE,
            "the encoded table header and array payload must agree"
        );

        for i in 1..=ARRAY_SIZE {
            unsafe {
                ffi::lua_rawgeti(state.as_ptr(), 1, i as ffi::lua_Integer);
                assert_eq!(
                    ffi::lua_tointeger(state.as_ptr(), -1),
                    (i as ffi::lua_Integer) * 10,
                    "array entry {i} was not preserved"
                );
                ffi::lua_pop(state.as_ptr(), 1);
            }
        }

        for i in 1..=HASH_SIZE {
            let key = format!("field{i}");
            unsafe {
                ffi::lua_pushlstring(state.as_ptr(), key.as_ptr().cast(), key.len());
                ffi::lua_rawget(state.as_ptr(), 1);
                assert_eq!(
                    ffi::lua_tointeger(state.as_ptr(), -1),
                    1_000 + i as ffi::lua_Integer,
                    "hash entry {key} was not preserved"
                );
                ffi::lua_pop(state.as_ptr(), 1);
            }
        }

        assert_eq!(unsafe { ffi::lua_gettop(state.as_ptr()) }, 1);
    }
}
