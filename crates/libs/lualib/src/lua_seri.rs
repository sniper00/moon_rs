use std::ffi::c_void;

use luars::{LuaRawTable, LuaResult, LuaState, LuaTable, LuaValue};

use actor::buffer::{BUFFER_HEAD_RESERVE, Buffer};

use crate::{
    lua_check_lightuserdata_bytes,
    lua_check_typed_lightuserdata_mut,
    lua_opt_boolean,
    lua_opt_integer,
};

const TYPE_NIL: u8 = 0;
const TYPE_BOOLEAN: u8 = 1;
const TYPE_NUMBER: u8 = 2;
const TYPE_NUMBER_ZERO: u8 = 0;
const TYPE_NUMBER_BYTE: u8 = 1;
const TYPE_NUMBER_WORD: u8 = 2;
const TYPE_NUMBER_DWORD: u8 = 4;
const TYPE_NUMBER_QWORD: u8 = 6;
const TYPE_NUMBER_REAL: u8 = 8;

const TYPE_USERDATA: u8 = 3;
const TYPE_SHORT_STRING: u8 = 4;
const TYPE_LONG_STRING: u8 = 5;
const TYPE_TABLE: u8 = 6;

const MAX_COOKIE: u8 = 32;

macro_rules! combine_type {
    ($t:expr, $v:expr) => {
        ($t) | ($v) << 3
    };
}

const MAX_DEPTH: usize = 32;

fn write_nil(buf: &mut Vec<u8>) {
    buf.push(TYPE_NIL);
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

fn write_pointer(buf: &mut Vec<u8>, v: *mut c_void) {
    buf.push(TYPE_USERDATA);
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
    } else if len < 0x10000 {
        let n = combine_type!(TYPE_LONG_STRING, 2u8);
        buf.push(n);
        buf.extend_from_slice(&(len as u16).to_le_bytes());
        buf.extend_from_slice(bytes);
    } else {
        let n = combine_type!(TYPE_LONG_STRING, 4u8);
        buf.push(n);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }
}

fn pack_one(state: &mut LuaState, val: LuaValue, buf: &mut Vec<u8>, depth: i32) -> Result<(), String> {
    if depth > MAX_DEPTH as i32 {
        return Err("serialize can't pack too depth table".to_string());
    }

    if val.is_nil() {
        write_nil(buf);
    } else if let Some(v) = val.as_integer() {
        write_integer(buf, v);
    } else if let Some(v) = val.as_number() {
        if v.is_nan() {
            return Err("serialize can't pack 'nan' number value".to_string());
        }
        write_real(buf, v);
    } else if let Some(v) = val.as_boolean() {
        write_boolean(buf, v);
    } else if let Some(v) = val.as_bytes() {
        write_bytes(buf, v);
    } else if let Some(v) = val.as_lightuserdata() {
        write_pointer(buf, v);
    } else if val.is_table() {
        write_table(state, val, buf, depth + 1)?;
    } else {
        return Err(format!("Unsupport type `{}` to serialize", val.type_name()));
    }

    Ok(())
}

fn write_table(state: &mut LuaState, table: LuaValue, buf: &mut Vec<u8>, depth: i32) -> Result<(), String> {
    let t = table.as_table().ok_or("expected table")?;
    let has_metapairs = if t.has_metatable() {
        let mt = t.get_metatable().unwrap();
        let pairs_key = state.create_string("__pairs").map_err(|_| "oom".to_string())?;
        state.raw_get(&mt, &pairs_key).is_some()
    } else {
        false
    };

    if has_metapairs {
        write_table_metapairs(state, table, buf, depth)?;
    } else {
        let array_size = write_table_array(state, &t, buf, depth)?;
        write_table_hash(state, &t, buf, depth, array_size)?;
    }
    Ok(())
}

fn write_table_array(
    state: &mut LuaState,
    table: &LuaRawTable,
    buf: &mut Vec<u8>,
    depth: i32,
) -> Result<usize, String> {
    let array_size = table.len();

    if array_size >= MAX_COOKIE as usize - 1 {
        let n = combine_type!(TYPE_TABLE, MAX_COOKIE - 1);
        buf.push(n);
        write_integer(buf, array_size as i64);
    } else {
        let n = combine_type!(TYPE_TABLE, array_size as u8);
        buf.push(n);
    }

    for i in 1..=array_size as i64 {
        let val = table.raw_geti(i).unwrap_or(LuaValue::nil());
        pack_one(state, val, buf, depth)?;
    }

    Ok(array_size)
}

fn write_table_hash(
    state: &mut LuaState,
    table: &LuaRawTable,
    buf: &mut Vec<u8>,
    depth: i32,
    array_size: usize,
) -> Result<(), String> {
    let pairs: Vec<(LuaValue, LuaValue)> = table.iter_all();

    for (k, v) in pairs {
        if let Some(key) = k.as_integer()
            && key > 0 && (key as usize) <= array_size {
                continue;
            }
        pack_one(state, k, buf, depth)?;
        pack_one(state, v, buf, depth)?;
    }

    write_nil(buf);
    Ok(())
}

fn write_table_metapairs(
    state: &mut LuaState,
    table: LuaValue,
    buf: &mut Vec<u8>,
    depth: i32,
) -> Result<(), String> {
    let n = combine_type!(TYPE_TABLE, 0u8);
    buf.push(n);

    let pairs_key = state
        .create_string("__pairs")
        .map_err(|_| "seri: failed to create __pairs key".to_string())?;

    let t = table.as_table().ok_or("expected table")?;
    let metatable = t
        .get_metatable()
        .ok_or_else(|| "no metatable".to_string())?;

    let pairs_fn = state
        .raw_get(&metatable, &pairs_key)
        .ok_or_else(|| "no __pairs metamethod".to_string())?;

    let results = state
        .call(pairs_fn, vec![table])
        .map_err(|_| "seri: error calling __pairs".to_string())?;

    if results.len() < 3 {
        return Err("__pairs must return 3 values".to_string());
    }

    let iter_fn = results[0];
    let iter_state = results[1];
    let mut control = results[2];

    loop {
        let iter_results = state
            .call(iter_fn, vec![iter_state, control])
            .map_err(|_| "seri: error calling __pairs iterator".to_string())?;

        if iter_results.is_empty() || iter_results[0].is_nil() {
            break;
        }

        let key = iter_results[0];
        let value = if iter_results.len() > 1 {
            iter_results[1]
        } else {
            LuaValue::nil()
        };

        control = key;

        pack_one(state, key, buf, depth)?;
        pack_one(state, value, buf, depth)?;
    }

    write_nil(buf);
    Ok(())
}

struct ReadBlock<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl ReadBlock<'_> {
    fn len(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_byte(&mut self) -> Result<u8, String> {
        if self.pos >= self.buf.len() {
            return Err(format!("Invalid serialize stream {} (read_byte)", self.len()));
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn try_read_byte(&mut self) -> Option<u8> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Some(b)
    }

    fn read_u16(&mut self) -> Result<u16, String> {
        if self.pos + 2 > self.buf.len() {
            return Err(format!("Invalid serialize stream {} (read_u16)", self.len()));
        }
        let mut n = [0u8; 2];
        n.copy_from_slice(&self.buf[self.pos..self.pos + 2]);
        self.pos += 2;
        Ok(u16::from_le_bytes(n))
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        if self.pos + 4 > self.buf.len() {
            return Err(format!("Invalid serialize stream {} (read_u32)", self.len()));
        }
        let mut n = [0u8; 4];
        n.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(u32::from_le_bytes(n))
    }

    fn read_i32(&mut self) -> Result<i32, String> {
        if self.pos + 4 > self.buf.len() {
            return Err(format!("Invalid serialize stream {} (read_i32)", self.len()));
        }
        let mut n = [0u8; 4];
        n.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(i32::from_le_bytes(n))
    }

    fn read_i64(&mut self) -> Result<i64, String> {
        if self.pos + 8 > self.buf.len() {
            return Err(format!("Invalid serialize stream {} (read_i64)", self.len()));
        }
        let mut n = [0u8; 8];
        n.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(i64::from_le_bytes(n))
    }

    fn read_real(&mut self) -> Result<f64, String> {
        if self.pos + 8 > self.buf.len() {
            return Err(format!("Invalid serialize stream {} (read_real)", self.len()));
        }
        let mut n = [0u8; 8];
        n.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(f64::from_le_bytes(n))
    }

    fn read_pointer(&mut self) -> Result<*mut c_void, String> {
        let size = std::mem::size_of::<usize>();
        if self.pos + size > self.buf.len() {
            return Err(format!("Invalid serialize stream {} (read_pointer)", self.len()));
        }
        let mut n = [0u8; std::mem::size_of::<usize>()];
        n.copy_from_slice(&self.buf[self.pos..self.pos + size]);
        self.pos += size;
        Ok(usize::from_le_bytes(n) as *mut c_void)
    }

    fn consume(&mut self, len: usize) -> Result<&[u8], String> {
        if self.pos + len > self.buf.len() {
            return Err(format!("Invalid serialize stream {} (consume {})", self.len(), len));
        }
        let pos = self.pos;
        self.pos += len;
        Ok(&self.buf[pos..pos + len])
    }

    fn offset(&self) -> usize {
        self.pos
    }

    fn as_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.pos) }
    }
}

fn get_integer(br: &mut ReadBlock, cookie: u8) -> Result<i64, String> {
    match cookie {
        TYPE_NUMBER_ZERO => Ok(0),
        TYPE_NUMBER_BYTE => Ok(br.read_byte()? as i64),
        TYPE_NUMBER_WORD => Ok(br.read_u16()? as i64),
        TYPE_NUMBER_DWORD => Ok(br.read_i32()? as i64),
        TYPE_NUMBER_QWORD => br.read_i64(),
        _ => Err(format!("Invalid serialize stream (bad number cookie {})", cookie)),
    }
}

fn decode_one(state: &mut LuaState, br: &mut ReadBlock) -> LuaResult<LuaValue> {
    let type_ = br.read_byte().map_err(|e| state.error(e))?;
    decode_value(state, br, type_ & 0x7, type_ >> 3)
}

fn decode_value(state: &mut LuaState, br: &mut ReadBlock, type_: u8, cookie: u8) -> LuaResult<LuaValue> {
    match type_ {
        TYPE_NIL => Ok(LuaValue::nil()),
        TYPE_BOOLEAN => Ok(LuaValue::boolean(cookie != 0)),
        TYPE_NUMBER => {
            if cookie == TYPE_NUMBER_REAL {
                let v = br.read_real().map_err(|e| state.error(e))?;
                Ok(LuaValue::float(v))
            } else {
                let v = get_integer(br, cookie).map_err(|e| state.error(e))?;
                Ok(LuaValue::integer(v))
            }
        }
        TYPE_USERDATA => {
            let ptr = br.read_pointer().map_err(|e| state.error(e))?;
            Ok(LuaValue::lightuserdata(ptr))
        }
        TYPE_SHORT_STRING => {
            let bytes = br.consume(cookie as usize).map_err(|e| state.error(e))?;
            let val = state.create_bytes(bytes)?;
            Ok(val)
        }
        TYPE_LONG_STRING => {
            if cookie == 2 {
                let n = br.read_u16().map_err(|e| state.error(e))?;
                let bytes = br.consume(n as usize).map_err(|e| state.error(e))?;
                let val = state.create_bytes(bytes)?;
                Ok(val)
            } else if cookie == 4 {
                let n = br.read_u32().map_err(|e| state.error(e))?;
                let bytes = br.consume(n as usize).map_err(|e| state.error(e))?;
                let val = state.create_bytes(bytes)?;
                Ok(val)
            } else {
                Err(state.error(format!("seri: invalid stream (bad long string cookie {})", cookie)))
            }
        }
        TYPE_TABLE => {
            decode_table(state, br, cookie as usize)
        }
        _ => {
            Err(state.error(format!("seri: invalid stream (bad type {})", type_)))
        }
    }
}

fn decode_table(state: &mut LuaState, br: &mut ReadBlock, mut array_size: usize) -> LuaResult<LuaValue> {
    if array_size == MAX_COOKIE as usize - 1 {
        let type_ = br.read_byte().map_err(|e| state.error(e))?;
        let cookie = type_ >> 3;
        if (type_ & 7) != TYPE_NUMBER || cookie == TYPE_NUMBER_REAL {
            return Err(state.error("seri: invalid stream (bad table size)".to_string()));
        }
        array_size = get_integer(br, cookie).map_err(|e| state.error(e))? as usize;
    }

    let table = state.create_table(array_size, 0)?;

    for i in 1..=array_size as i64 {
        let val = decode_one(state, br)?;
        state.raw_seti(&table, i, val);
    }

    loop {
        let key = decode_one(state, br)?;
        if key.is_nil() {
            break;
        }
        let value = decode_one(state, br)?;
        state.raw_set(&table, key, value);
    }

    Ok(table)
}

fn pack(state: &mut LuaState) -> LuaResult<usize> {
    let n = state.arg_count();
    if n == 0 {
        return Ok(0);
    }

    let arg_count = state.arg_count();
    let mut buf = Box::new(Buffer::new());
    buf.commit(BUFFER_HEAD_RESERVE);
    for i in 1..=arg_count {
        if let Some(val) = state.get_arg(i) {
            if let Err(err) = pack_one(state, val, buf.as_mut_vec(), 0) {
                return Err(state.error(err));
            }
        }
    }
    buf.seek(BUFFER_HEAD_RESERVE as isize);
    state.push_value(LuaValue::lightuserdata(Box::into_raw(buf) as *mut c_void))?;
    Ok(1)
}

fn pack_string(state: &mut LuaState) -> LuaResult<usize> {
    let n = state.arg_count();
    if n == 0 {
        return Ok(0);
    }

    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        let val = state.get_arg(i).unwrap_or(LuaValue::nil());
        if let Err(err) = pack_one(state, val, buf.as_mut_vec(), 0) {
            return Err(state.error(err));
        }
    }

    let val = state.create_bytes(buf.as_slice())?;
    state.push_value(val)?;
    Ok(1)
}

fn unpack(state: &mut LuaState) -> LuaResult<usize> {
    let arg1 = match state.get_arg(1) {
        Some(v) if !v.is_nil() => v,
        _ => return Ok(0),
    };

    if arg1.is_string() {
        let bytes = arg1.as_bytes().unwrap_or(&[]);
        if bytes.is_empty() {
            return Ok(0);
        }
        let owned = bytes.to_vec();
        return unpack_from_slice(state, &owned);
    } else if arg1.is_lightuserdata() {
        let len: usize = lua_opt_integer(state, 2).unwrap_or(0);
        if len == 0 {
            return Ok(0);
        }
        let slice = lua_check_lightuserdata_bytes(state, 1, len)?;
        return unpack_from_slice(state, slice);
    }

    Ok(0)
}

fn unpack_from_slice(state: &mut LuaState, data: &[u8]) -> LuaResult<usize> {
    let br = &mut ReadBlock {
        buf: data,
        pos: 0,
    };

    let mut count = 0usize;
    while let Some(type_) = br.try_read_byte() {
        let cookie = type_ >> 3;
        let val = decode_value(state, br, type_ & 0x7, cookie)?;
        state.push_value(val)?;
        count += 1;
    }

    Ok(count)
}

fn peek_one(state: &mut LuaState) -> LuaResult<usize> {
    let arg1 = match state.get_arg(1) {
        Some(v) if !v.is_nil() => v,
        _ => return Ok(0),
    };

    if !arg1.is_lightuserdata() {
        return Err(state.error("bad argument #1 (lightuserdata expected)".to_string()));
    }

    let seek = lua_opt_boolean(state, 2).unwrap_or(false);

    let buf = lua_check_typed_lightuserdata_mut::<Buffer>(state, 1)?;
    if buf.is_empty() {
        return Ok(0);
    }

    let br = &mut ReadBlock {
        buf: unsafe { std::slice::from_raw_parts(buf.as_ptr(), buf.len()) },
        pos: 0,
    };

    let type_ = br.read_byte().map_err(|e| state.error(e))?;
    let val = decode_value(state, br, type_ & 0x7, type_ >> 3)?;
    state.push_value(val)?;

    if seek {
        buf.consume(br.offset());
    }

    state.push_value(LuaValue::lightuserdata(br.as_ptr() as *mut c_void))?;
    state.push_value(LuaValue::integer(br.len() as i64))?;

    Ok(3)
}

pub fn register_seri() -> luars::LibraryModule {
    luars::lua_module!("seri", {
        "pack" => pack,
        "packstring" => pack_string,
        "unpack" => unpack,
        "unpack_one" => peek_one,
    })
}
