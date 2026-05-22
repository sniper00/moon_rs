use luars::{LuaResult, LuaState, LuaValue};
use std::ffi::c_void;

use actor::buffer::{self, Buffer};

use crate::{lua_check_typed_lightuserdata_mut, lua_take_typed_lightuserdata, lua_opt_integer};

const MAX_DEPTH: i32 = 32;

fn concat_table(state: &mut LuaState, writer: &mut Buffer, table: &LuaValue, depth: i32) -> Result<(), String> {
    let t = table.as_table().ok_or("expected table")?;
    let len = t.len();
    for i in 1..=len as i64 {
        if let Some(val) = t.raw_geti(i) {
            concat_one(state, writer, &val, depth)?;
        }
    }
    Ok(())
}

fn concat_one(state: &mut LuaState, writer: &mut Buffer, val: &LuaValue, depth: i32) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("buffer.concat too depth table".into());
    }

    if val.is_nil() {
        // skip nil
    } else if let Some(n) = val.as_number() {
        if val.is_integer() {
            writer.write_chars(val.as_integer().unwrap());
        } else {
            writer.write_chars(n);
        }
    } else if let Some(b) = val.as_boolean() {
        let s = if b { "true" } else { "false" };
        writer.write_slice(s.as_bytes());
    } else if let Some(s) = val.as_bytes() {
        writer.write_slice(s);
    } else if val.is_table() {
        concat_table(state, writer, val, depth + 1)?;
    } else {
        return Err(format!("buffer.concat unsupport type :{}", val.type_name()));
    }

    Ok(())
}

fn concat(state: &mut LuaState) -> LuaResult<usize> {
    let n = state.arg_count();
    if n == 0 {
        return Ok(0);
    }

    let args: Vec<LuaValue> = (1..=n).filter_map(|i| state.get_arg(i)).collect();

    let mut buf = Box::new(Buffer::new());
    for val in &args {
        if let Err(err) = concat_one(state, buf.as_mut(), val, 0) {
            return Err(state.error(err));
        }
    }

    state.push_value(LuaValue::lightuserdata(Box::into_raw(buf) as *mut c_void))?;
    Ok(1)
}

fn concat_string(state: &mut LuaState) -> LuaResult<usize> {
    let n = state.arg_count();
    if n == 0 {
        return Ok(0);
    }

    let args: Vec<LuaValue> = (1..=n).filter_map(|i| state.get_arg(i)).collect();

    let mut buf = Buffer::new();
    for val in &args {
        if let Err(err) = concat_one(state, &mut buf, val, 0) {
            return Err(state.error(err));
        }
    }

    let val = state.create_bytes(buf.as_slice())?;
    state.push_value(val)?;
    Ok(1)
}

fn get_mut_buffer(state: &mut LuaState) -> LuaResult<&'static mut Buffer> {
    lua_check_typed_lightuserdata_mut::<Buffer>(state, 1)
}

fn unpack(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;

    let arg2 = state.get_arg(2);
    let is_format_string = arg2.as_ref().is_some_and(|v| v.is_string());

    let mut count = 0usize;

    if is_format_string {
        let opt = arg2.as_ref().and_then(|v| v.as_str()).unwrap_or_default();
        let mut pos: usize = lua_opt_integer(state, 3).unwrap_or(0);
        let len = buf.len();
        if pos > len {
            return Err(state.error("buffer: index out of range".to_string()));
        }
        let mut le = true;
        for c in opt.chars() {
            match c {
                '>' => le = false,
                '<' => le = true,
                'h' => {
                    if len - pos < 2 {
                        return Err(state.error("buffer: data out of range".to_string()));
                    }
                    state.push_value(LuaValue::integer(buf.read_i16(pos, le) as i64))?;
                    pos += 2;
                    count += 1;
                }
                'H' => {
                    if len - pos < 2 {
                        return Err(state.error("buffer: data out of range".to_string()));
                    }
                    state.push_value(LuaValue::integer(buf.read_u16(pos, le) as i64))?;
                    pos += 2;
                    count += 1;
                }
                'i' => {
                    if len - pos < 4 {
                        return Err(state.error("buffer: data out of range".to_string()));
                    }
                    state.push_value(LuaValue::integer(buf.read_i32(pos, le) as i64))?;
                    pos += 4;
                    count += 1;
                }
                'I' => {
                    if len - pos < 4 {
                        return Err(state.error("buffer: data out of range".to_string()));
                    }
                    state.push_value(LuaValue::integer(buf.read_u32(pos, le) as i64))?;
                    pos += 4;
                    count += 1;
                }
                'C' => {
                    state.push_value(LuaValue::lightuserdata(buf.as_ptr() as *mut c_void))?;
                    state.push_value(LuaValue::integer(buf.len() as i64))?;
                    count += 2;
                }
                'Z' => {
                    let val = state.create_bytes(buf.as_slice())?;
                    state.push_value(val)?;
                    count += 1;
                }
                _ => {
                    return Err(state.error(format!("buffer: bad format option '{}'", c)));
                }
            }
        }
    } else {
        let pos: usize = lua_opt_integer(state, 2).unwrap_or(0);
        let len = buf.len();
        if pos > len {
            return Err(state.error("buffer: index out of range".to_string()));
        }
        let count_arg: i64 = lua_opt_integer(state, 3).unwrap_or(-1);
        let cnt = if count_arg < 0 {
            len - pos
        } else {
            std::cmp::min(len - pos, count_arg as usize)
        };
        let val = state.create_bytes(&buf.as_slice()[pos..pos + cnt])?;
        state.push_value(val)?;
        count = 1;
    }

    Ok(count)
}

fn buffer_new(state: &mut LuaState) -> LuaResult<usize> {
    let capacity: usize = lua_opt_integer(state, 1).unwrap_or(buffer::DEFAULT_RESERVE);

    if capacity >= usize::MAX / 2 {
        return Err(state.error("bad argument #1 (invalid capacity)".to_string()));
    }

    let buf = Box::new(Buffer::with_capacity(capacity));
    state.push_value(LuaValue::lightuserdata(Box::into_raw(buf) as *mut c_void))?;
    Ok(1)
}

fn buffer_drop(state: &mut LuaState) -> LuaResult<usize> {
    let _ = lua_take_typed_lightuserdata::<Buffer>(state, 1)?;
    Ok(0)
}

fn read(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    let len: usize = lua_opt_integer(state, 2).unwrap_or(0);

    if len > buf.len() {
        return Err(state.error("buffer: read length out of range".to_string()));
    }

    let val = state.create_bytes(&buf.as_slice()[..len])?;
    state.push_value(val)?;
    buf.consume(len);
    Ok(1)
}

fn write_front(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    let top = state.arg_count();
    for i in (2..=top).rev() {
        let s = state
            .get_arg(i)
            .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
            .unwrap_or_default();
        if !buf.write_front(&s) {
            return Err(state.error("buffer: no more front space".to_string()));
        }
    }
    Ok(0)
}

fn write_string(state: &mut LuaState, buf: &mut Buffer, index: usize) -> Result<(), String> {
    let val = state.get_arg(index).unwrap_or(LuaValue::nil());
    if val.is_nil() {
        // skip nil
    } else if let Some(s) = val.as_bytes() {
        buf.write_slice(s);
    } else if val.is_integer() {
        buf.write_chars(val.as_integer().unwrap());
    } else if let Some(n) = val.as_number() {
        buf.write_chars(n);
    } else if let Some(b) = val.as_boolean() {
        let s = if b { "true" } else { "false" };
        buf.write_slice(s.as_bytes());
    } else {
        return Err(format!("unsupport type :{}", val.type_name()));
    }
    Ok(())
}

fn write(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    let top = state.arg_count();
    for i in 2..=top {
        if let Err(err) = write_string(state, buf, i) {
            return Err(state.error(err));
        }
    }
    Ok(0)
}

fn seek(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    let pos: isize = lua_opt_integer::<i64>(state, 2).unwrap_or(0) as isize;
    state.push_value(LuaValue::boolean(buf.seek(pos)))?;
    Ok(1)
}

fn commit(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    let len: usize = lua_opt_integer(state, 2).unwrap_or(0);
    state.push_value(LuaValue::boolean(buf.commit(len)))?;
    Ok(1)
}

fn prepare(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    let len: usize = lua_opt_integer(state, 2).unwrap_or(0);
    let space: *mut u8 = buf.prepare(len).as_mut_ptr();
    state.push_value(LuaValue::lightuserdata(space as *mut c_void))?;
    Ok(1)
}

fn size(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    state.push_value(LuaValue::integer(buf.len() as i64))?;
    Ok(1)
}

fn clear(state: &mut LuaState) -> LuaResult<usize> {
    let buf = get_mut_buffer(state)?;
    buf.clear();
    Ok(0)
}

pub fn register_buffer() -> luars::LibraryModule {
    luars::lua_module!("buffer", {
        "new" => buffer_new,
        "drop" => buffer_drop,
        "concat" => concat,
        "concat_string" => concat_string,
        "unpack" => unpack,
        "read" => read,
        "write" => write,
        "write_front" => write_front,
        "seek" => seek,
        "commit" => commit,
        "prepare" => prepare,
        "size" => size,
        "clear" => clear,
    })
}
