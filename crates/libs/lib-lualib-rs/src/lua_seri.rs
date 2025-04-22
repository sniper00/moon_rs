use std::ffi::{c_int, c_void};

use lib_lua::{
    self, cstr,
    ffi::{self, luaL_Reg, LUA_TLIGHTUSERDATA, LUA_TSTRING},
    laux::{self},
    lreg, lreg_null, luaL_newlib,
};

use lib_core::buffer::Buffer;

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
    state: *mut ffi::lua_State,
    buf: &mut Vec<u8>,
    index: i32,
    depth: i32,
) -> Result<usize, String> {
    let array_size = unsafe { ffi::lua_rawlen(state, index) };
    if array_size >= MAX_COOKIE as usize - 1 {
        let n = combine_type!(TYPE_TABLE, MAX_COOKIE - 1);
        buf.push(n);
        write_integer(buf, array_size as i64);
    } else {
        let n = combine_type!(TYPE_TABLE, array_size as u8);
        buf.push(n);
    }

    for i in 1..=array_size {
        unsafe {
            ffi::lua_rawgeti(state, index, i as ffi::lua_Integer);
            pack_one(state, buf, -1, depth)?;
            ffi::lua_pop(state, 1);
        }
    }

    Ok(array_size)
}

fn write_table_hash(
    state: *mut ffi::lua_State,
    buf: &mut Vec<u8>,
    index: i32,
    depth: i32,
    array_size: usize,
) -> Result<(), String> {
    unsafe {
        ffi::lua_pushnil(state);
        while ffi::lua_next(state, index) != 0 {
            if ffi::lua_type(state, -2) == ffi::LUA_TNUMBER && ffi::lua_isinteger(state, -2) != 0 {
                let x = ffi::lua_tointeger(state, -2);
                if x > 0 && x <= array_size as i64 {
                    ffi::lua_pop(state, 1);
                    continue;
                }
            }
            pack_one(state, buf, -2, depth)?;
            pack_one(state, buf, -1, depth)?;
            ffi::lua_pop(state, 1);
        }
    }

    write_nil(buf);

    Ok(())
}

fn write_table_metapairs(
    state: *mut ffi::lua_State,
    buf: &mut Vec<u8>,
    index: i32,
    depth: i32,
) -> Result<i32, String> {
    let n = combine_type!(TYPE_TABLE, 0);
    buf.push(n);

    unsafe {
        ffi::lua_pushvalue(state, index);
        if ffi::lua_pcall(state, 1, 3, 0) != ffi::LUA_OK {
            return Ok(1);
        }
        loop {
            ffi::lua_pushvalue(state, -2);
            ffi::lua_pushvalue(state, -2);
            ffi::lua_copy(state, -5, -3);
            if ffi::lua_pcall(state, 2, 2, 0) != ffi::LUA_OK {
                return Ok(1);
            }
            let type_ = ffi::lua_type(state, -2);
            if type_ == ffi::LUA_TNIL {
                ffi::lua_pop(state, 4);
                break;
            }
            pack_one(state, buf, -2, depth)?;
            pack_one(state, buf, -1, depth)?;
            ffi::lua_pop(state, 1);
        }
    }

    write_nil(buf);

    Ok(0)
}

fn write_table(
    state: *mut ffi::lua_State,
    buf: &mut Vec<u8>,
    mut index: i32,
    depth: i32,
) -> Result<i32, String> {
    unsafe {
        if ffi::lua_checkstack(state, ffi::LUA_MINSTACK) == 0 {
            ffi::lua_pushstring(state, cstr!("out of memory"));
            return Ok(1);
        }

        if index < 0 {
            index = ffi::lua_gettop(state) + index + 1;
        };

        if ffi::luaL_getmetafield(state, index, cstr!("__pairs")) != ffi::LUA_TNIL {
            write_table_metapairs(state, buf, index, depth)
        } else {
            let array_size = write_table_array(state, buf, index, depth)?;
            write_table_hash(state, buf, index, depth, array_size)?;
            Ok(0)
        }
    }
}

fn pack_one(
    state: *mut ffi::lua_State,
    buf: &mut Vec<u8>,
    mut index: i32,
    depth: i32,
) -> Result<(), String> {
    unsafe {
        if depth > MAX_DEPTH as i32 {
            return Err("serialize can't pack too depth table".to_string());
        }
        let type_ = ffi::lua_type(state, index);
        match type_ {
            ffi::LUA_TNIL => {
                write_nil(buf);
            }
            ffi::LUA_TNUMBER => {
                if ffi::lua_isinteger(state, index) != 0 {
                    let x = ffi::lua_tointeger(state, index);
                    write_integer(buf, x);
                } else {
                    let n = ffi::lua_tonumber(state, index);
                    if n.is_nan() {
                        return Err("serialize can't pack 'nan' number value".to_string());
                    }
                    write_real(buf, n);
                }
            }
            ffi::LUA_TBOOLEAN => {
                write_boolean(buf, ffi::lua_toboolean(state, index) != 0);
            }
            ffi::LUA_TSTRING => {
                let mut sz = 0;
                let str = ffi::lua_tolstring(state, index, &mut sz);
                write_bytes(buf, std::slice::from_raw_parts(str as *const u8, sz));
            }
            ffi::LUA_TLIGHTUSERDATA => {
                write_pointer(buf, ffi::lua_touserdata(state, index));
            }
            ffi::LUA_TTABLE => {
                if index < 0 {
                    index = ffi::lua_gettop(state) + index + 1
                }
                if write_table(state, buf, index, depth + 1)? != 0 {
                    return Err(laux::lua_opt::<String>(state, -1)
                        .unwrap_or("no error message".to_string()));
                }
            }
            _ => {
                let tname = std::ffi::CStr::from_ptr(ffi::lua_typename(state, type_))
                    .to_str()
                    .unwrap_or_default();
                return Err(format!("Unsupport type {} to serialize", tname));
            }
        }
    }

    Ok(())
}

fn invalid_stream_line(state: *mut ffi::lua_State, rb: &mut ReadBlock, line: i32) {
    let len = rb.len();
    laux::lua_error(
        state,
        format!("Invalid serialize stream {} (line:{})", len, line).as_str(),
    );
}

macro_rules! invalid_stream {
    ($state:expr, $rb:expr) => {
        invalid_stream_line($state, $rb, line!() as i32)
    };
}

struct ReadBlock<'a> {
    buf: &'a [u8],
    pos: usize,
    state: *mut ffi::lua_State,
}

impl ReadBlock<'_> {
    fn len(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn as_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.pos) }
    }

    fn read_byte(&mut self) -> u8 {
        if self.pos >= self.buf.len() {
            invalid_stream!(self.state, self);
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        b
    }

    fn try_read_byte(&mut self) -> Option<u8> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Some(b)
    }

    fn read_u16(&mut self) -> u16 {
        let mut n = [0u8; 2];
        if self.pos + 2 > self.buf.len() {
            invalid_stream!(self.state, self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 2]);
        self.pos += 2;
        u16::from_le_bytes(n)
    }

    fn read_u32(&mut self) -> u32 {
        let mut n = [0u8; 4];
        if self.pos + 4 > self.buf.len() {
            invalid_stream!(self.state, self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        u32::from_le_bytes(n)
    }

    fn read_i32(&mut self) -> i32 {
        let mut n = [0u8; 4];
        if self.pos + 4 > self.buf.len() {
            invalid_stream!(self.state, self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        i32::from_le_bytes(n)
    }

    fn read_i64(&mut self) -> i64 {
        let mut n = [0u8; 8];
        if self.pos + 8 > self.buf.len() {
            invalid_stream!(self.state, self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        i64::from_le_bytes(n)
    }

    fn read_real(&mut self) -> f64 {
        let mut n = [0u8; 8];
        if self.pos + 8 > self.buf.len() {
            invalid_stream!(self.state, self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        f64::from_le_bytes(n)
    }

    fn read_pointer(&mut self) -> *mut std::ffi::c_void {
        let mut n = [0u8; std::mem::size_of::<usize>()];
        if self.pos + std::mem::size_of::<usize>() > self.buf.len() {
            invalid_stream!(self.state, self);
        }
        n.copy_from_slice(&self.buf[self.pos..self.pos + std::mem::size_of::<usize>()]);
        self.pos += std::mem::size_of::<usize>();
        usize::from_le_bytes(n) as *mut std::ffi::c_void
    }

    fn consume(&mut self, len: usize) -> *const u8 {
        if self.pos + len > self.buf.len() {
            invalid_stream!(self.state, self);
        }
        let pos = self.pos;
        self.pos += len;
        self.buf[pos..pos + len].as_ptr()
    }

    fn offset(&self) -> usize {
        self.pos
    }
}

fn get_integer(state: *mut ffi::lua_State, br: &mut ReadBlock, cookie: u8) -> i64 {
    match cookie {
        TYPE_NUMBER_ZERO => 0,
        TYPE_NUMBER_BYTE => br.read_byte() as i64,
        TYPE_NUMBER_WORD => br.read_u16() as i64,
        TYPE_NUMBER_DWORD => br.read_i32() as i64,
        TYPE_NUMBER_QWORD => br.read_i64(),
        _ => {
            invalid_stream!(state, br);
            0
        }
    }
}

// fn get_pointer(state: *mut ffi::lua_State, buf: &[u8]) -> *mut std::ffi::c_void {
//     let mut n = [0u8; std::mem::size_of::<usize>()];
//     if buf.len() < std::mem::size_of::<usize>() {
//         invalid_stream!(state, buf);
//     }
//     n.copy_from_slice(&buf[..std::mem::size_of::<usize>()]);
//     usize::from_le_bytes(n) as *mut std::ffi::c_void
// }

fn get_buffer(state: *mut ffi::lua_State, br: &mut ReadBlock, len: usize) {
    unsafe {
        ffi::lua_pushlstring(state, br.consume(len) as *const std::os::raw::c_char, len);
    }
}

fn unpack_one(state: *mut ffi::lua_State, br: &mut ReadBlock) {
    let type_ = br.read_byte();
    push_value(state, br, type_ & 0x7, type_ >> 3);
}

fn push_value(state: *mut ffi::lua_State, br: &mut ReadBlock, type_: u8, cookie: u8) {
    match type_ {
        TYPE_NIL => unsafe {
            ffi::lua_pushnil(state);
        },
        TYPE_BOOLEAN => unsafe {
            ffi::lua_pushboolean(state, if cookie != 0 { 1 } else { 0 });
        },
        TYPE_NUMBER => {
            if cookie == TYPE_NUMBER_REAL {
                unsafe {
                    ffi::lua_pushnumber(state, br.read_real());
                }
            } else {
                unsafe {
                    ffi::lua_pushinteger(state, get_integer(state, br, cookie));
                }
            }
        }
        TYPE_USERDATA => unsafe {
            ffi::lua_pushlightuserdata(state, br.read_pointer());
        },
        TYPE_SHORT_STRING => {
            get_buffer(state, br, cookie as usize);
        }
        TYPE_LONG_STRING => {
            if cookie == 2 {
                let n = br.read_u16();
                get_buffer(state, br, n as usize);
            } else {
                if cookie != 4 {
                    invalid_stream!(state, br);
                }
                let n = br.read_u32();
                get_buffer(state, br, n as usize);
            }
        }
        TYPE_TABLE => {
            unpack_table(state, br, cookie as usize);
        }
        _ => {
            invalid_stream!(state, br);
        }
    }
}

fn unpack_table(state: *mut ffi::lua_State, br: &mut ReadBlock, mut array_size: usize) {
    if array_size == MAX_COOKIE as usize - 1 {
        let type_ = br.read_byte();
        let cookie = type_ >> 3;
        if (type_ & 7) != TYPE_NUMBER || cookie == TYPE_NUMBER_REAL {
            invalid_stream!(state, br);
        }
        array_size = get_integer(state, br, cookie) as usize;
    }
    unsafe {
        ffi::luaL_checkstack(state, ffi::LUA_MINSTACK, std::ptr::null());
        ffi::lua_createtable(state, array_size as i32, 0);
        for i in 1..=array_size {
            unpack_one(state, br);
            ffi::lua_rawseti(state, -2, i as ffi::lua_Integer);
        }

        loop {
            unpack_one(state, br);
            if ffi::lua_isnil(state, -1) != 0 {
                ffi::lua_pop(state, 1);
                return;
            }
            unpack_one(state, br);
            ffi::lua_rawset(state, -3);
        }
    }
}

extern "C-unwind" fn pack(state: *mut ffi::lua_State) -> c_int {
    let n = unsafe { ffi::lua_gettop(state) };
    if n == 0 {
        return 0;
    }

    let mut has_error = false;
    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        if let Err(err) = pack_one(state, buf.as_mut_vec(), i, 0) {
            has_error = true;
            laux::lua_push(state, err);
            break;
        }
    }

    if has_error {
        drop(buf);
        laux::throw_error(state);
    }

    unsafe {
        ffi::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
    }

    1
}

extern "C-unwind" fn pack_string(state: *mut ffi::lua_State) -> c_int {
    let n = unsafe { ffi::lua_gettop(state) };
    if n == 0 {
        return 0;
    }

    let mut has_error = false;
    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        if let Err(err) = pack_one(state, buf.as_mut_vec(), i, 0) {
            has_error = true;
            laux::lua_push(state, err);
            break;
        }
    }

    if has_error {
        drop(buf);
        laux::throw_error(state);
    }

    unsafe {
        ffi::lua_pushlstring(
            state,
            buf.as_ptr() as *const std::os::raw::c_char,
            buf.len(),
        );
    }
    1
}

extern "C-unwind" fn unpack(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        if ffi::lua_isnoneornil(state, 1) == 1 {
            return 0;
        }

        let mut len = 0;
        let data;
        if ffi::lua_type(state, 1) == LUA_TSTRING {
            data = ffi::lua_tolstring(state, 1, &mut len) as *const u8;
        } else {
            data = ffi::lua_touserdata(state, 1) as *const u8;
            len = ffi::luaL_checkinteger(state, 2) as usize;
        }

        if len == 0 {
            return 0;
        }

        if data.is_null() {
            ffi::luaL_error(state, cstr!("deserialize null pointer"));
        }

        ffi::lua_settop(state, 1);

        let br = &mut ReadBlock {
            buf: std::slice::from_raw_parts(data, len),
            pos: 0,
            state,
        };

        let mut i = 0;
        loop {
            if i % 8 == 7 {
                ffi::luaL_checkstack(state, 8, std::ptr::null());
            }
            i += 1;

            if let Some(type_) = br.try_read_byte() {
                let cookie = type_ >> 3;
                push_value(state, br, type_ & 0x7, cookie);
            } else {
                break;
            }
        }

        ffi::lua_gettop(state) - 1
    }
}

extern "C-unwind" fn peek_one(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        if ffi::lua_isnoneornil(state, 1) == 1 {
            return 0;
        }

        if ffi::lua_type(state, 1) != LUA_TLIGHTUSERDATA {
            ffi::luaL_argerror(state, 1, cstr!("peek_one need lightuserdata"));
        }

        let seek = laux::lua_opt(state, 2).unwrap_or(false);

        let buf = ffi::lua_touserdata(state, 1) as *mut Buffer;
        if buf.is_null() {
            ffi::luaL_argerror(state, 1, cstr!("null buffer pointer"));
        }

        if (*buf).is_empty() {
            return 0;
        }

        let br = &mut ReadBlock {
            buf: std::slice::from_raw_parts((*buf).as_ptr(), (*buf).len()),
            pos: 0,
            state,
        };

        let type_ = br.read_byte();

        push_value(state, br, type_ & 0x7, type_ >> 3);

        if seek {
            (*buf).consume(br.offset());
        }

        ffi::lua_pushlightuserdata(state, br.as_ptr() as *mut c_void);
        ffi::lua_pushinteger(state, br.len() as i64);

        3
    }
}

pub unsafe extern "C-unwind" fn luaopen_seri(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("pack", pack),
        lreg!("packstring", pack_string),
        lreg!("unpack", unpack),
        lreg!("unpack_one", peek_one),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
