use moon_lua_sys::{
    self, cstr,
    ffi::{self, lua_Integer},
    laux::{self, FromLua, LuaArrayCursor, LuaStack, LuaState, LuaType},
    lreg_null, lreg_try, luaL_newlib,
};
use std::ffi::{c_int, c_void};

use moon_runtime::buffer::{self, Buffer};

use crate::check_arc_buffer;

const MAX_DEPTH: i32 = 32;

fn concat_array(
    writer: &mut Buffer,
    cursor: &mut LuaArrayCursor<'_, '_>,
    depth: i32,
) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("buffer.concat too depth table".into());
    }

    while let Some(value) = cursor.next() {
        let index = value.index();
        match value.kind() {
            LuaType::Nil => {}
            LuaType::Number => writer
                .write_chars(value.as_number().unwrap_or_default())
                .map_err(|err| err.to_string())?,
            LuaType::Integer => writer
                .write_chars(value.as_integer().unwrap_or_default())
                .map_err(|err| err.to_string())?,
            LuaType::Boolean => {
                let s = if value.as_bool().unwrap_or(false) {
                    "true"
                } else {
                    "false"
                };
                writer.write_slice(s.as_bytes());
            }
            LuaType::String => writer.write_slice(value.as_bytes().unwrap_or_default()),
            LuaType::Table => {
                let mut nested = cursor.nested_array(index);
                concat_array(writer, &mut nested, depth + 1)?;
            }
            _ => {
                return Err(format!("buffer.concat unsupport type :{}", value.name()));
            }
        }
    }

    Ok(())
}

fn concat_one(
    lua: &mut LuaStack<'_>,
    writer: &mut Buffer,
    index: i32,
    depth: i32,
) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("buffer.concat too depth table".into());
    }

    let value = lua.value(index);
    match value.kind() {
        LuaType::Nil => {}
        LuaType::Number => writer
            .write_chars(value.as_number().unwrap_or_default())
            .map_err(|err| err.to_string())?,
        LuaType::Integer => writer
            .write_chars(value.as_integer().unwrap_or_default())
            .map_err(|err| err.to_string())?,
        LuaType::Boolean => {
            let s = if value.as_bool().unwrap_or(false) {
                "true"
            } else {
                "false"
            };
            writer.write_slice(s.as_bytes());
        }
        LuaType::String => writer.write_slice(value.as_bytes().unwrap_or_default()),
        LuaType::Table => {
            let mut cursor = lua.array_cursor(index);
            concat_array(writer, &mut cursor, depth + 1)?;
        }
        _ => {
            return Err(format!("buffer.concat unsupport type :{}", value.name()));
        }
    }

    Ok(())
}

fn concat(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let n = laux::lua_top(state);
    if n == 0 {
        return Ok(0);
    }

    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        concat_one(lua, buf.as_mut(), i, 0)?;
    }

    laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);

    Ok(1)
}

fn concat_string(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let n = laux::lua_top(state);
    if n == 0 {
        return Ok(0);
    }

    let mut buf = Buffer::new();
    for i in 1..=n {
        concat_one(lua, &mut buf, i, 0)?;
    }

    laux::lua_push(state, buf.as_slice());
    Ok(1)
}

fn get_buffer_ptr(lua: &LuaStack<'_>) -> Result<std::ptr::NonNull<Buffer>, String> {
    lua.value(1)
        .as_light_userdata()
        .and_then(|ptr| std::ptr::NonNull::new(ptr.cast::<Buffer>()))
        .ok_or_else(|| "bad argument #1 (invalid `Buffer` pointer)".to_string())
}

fn unpack(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: buffer pointers are created by `buffer.new`/`buffer.concat` and
    // this callback keeps the only mutable Buffer reference for its duration.
    let buf = unsafe { buf_ptr.as_mut() };
    let top = laux::lua_top(state);

    if laux::lua_type(state, 2) == LuaType::String {
        // SAFETY: argument #2 remains rooted and is never modified; this loop
        // only appends decoded values above the existing stack top.
        let opt = unsafe {
            lua.value_bytes_append_only(2)
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .unwrap_or_default()
        };
        let mut pos = FromLua::from_lua(lua, 3).ok().unwrap_or_default();
        let len = buf.len();
        if pos > len {
            return Err("bad argument #3 (out of range)".to_string());
        }
        let mut le = true;
        for c in opt.chars() {
            match c {
                '>' => le = false,
                '<' => le = true,
                'h' => {
                    if len - pos < 2 {
                        return Err("bad argument #2 (data out of range)".to_string());
                    }
                    laux::lua_push(state, buf.read_i16(pos, le));
                    pos += 2;
                }
                'H' => {
                    if len - pos < 2 {
                        return Err("bad argument #2 (data out of range)".to_string());
                    }
                    laux::lua_push(state, buf.read_u16(pos, le));
                    pos += 2;
                }
                'i' => {
                    if len - pos < 4 {
                        return Err("bad argument #2 (data out of range)".to_string());
                    }
                    laux::lua_push(state, buf.read_i32(pos, le));
                    pos += 4;
                }
                'I' => {
                    if len - pos < 4 {
                        return Err("bad argument #2 (data out of range)".to_string());
                    }
                    laux::lua_push(state, buf.read_u32(pos, le));
                    pos += 4;
                }
                'C' => {
                    laux::lua_pushlightuserdata(state, unsafe { buf.as_ptr().add(pos) }
                        as *mut c_void);
                    laux::lua_push(state, (len - pos) as lua_Integer);
                }
                'Z' => {
                    laux::lua_push(state, buf.as_slice());
                }
                _ => {
                    return Err(format!("invalid format option '{0}'", c));
                }
            }
        }
    } else {
        let pos = lua.opt::<usize>(2).unwrap_or(0);
        let len = buf.len();
        if pos > len {
            return Err("bad argument #2 (out of range)".to_string());
        }
        let count_arg = lua.opt::<isize>(3).unwrap_or(-1);
        let count = if count_arg < 0 {
            (len - pos) as usize
        } else {
            std::cmp::min(len - pos, count_arg as usize)
        };

        laux::lua_push(state, &buf.as_slice()[pos..pos + count]);
    }

    Ok(laux::lua_top(state) - top)
}

fn buffer_new(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let capacity = lua.opt::<usize>(1).unwrap_or(buffer::DEFAULT_RESERVE);

    if capacity >= (usize::MAX / 2) {
        return Err("bad argument #1 (invalid capacity)".to_string());
    }

    let buf = Box::new(Buffer::with_capacity(capacity));
    laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);

    Ok(1)
}

fn buffer_drop(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let buf = get_buffer_ptr(lua)?;
    unsafe {
        drop(Box::from_raw(buf.as_ptr()));
    }
    Ok(0)
}

fn read(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    let len = lua.get(2)?;
    if len > buf.len() {
        return Err("bad argument #2 (out of range)".to_string());
    }

    laux::lua_push(state, &buf.as_slice()[..len]);
    buf.consume(len);

    Ok(1)
}

fn write_front(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    let top = laux::lua_top(state);
    for i in (2..=top).rev() {
        let s = match lua.value(i).as_bytes() {
            Some(s) => s,
            None => return Err(format!("bad argument #{i} (string expected)")),
        };
        if !buf.write_front(s) {
            return Err("no more front space".to_string());
        }
    }
    Ok(0)
}

fn write_string(lua: &LuaStack<'_>, buf: &mut Buffer, index: i32) -> Result<(), String> {
    let value = lua.value(index);
    match value.kind() {
        LuaType::Nil => {}
        LuaType::String => {
            buf.write_slice(value.as_bytes().unwrap_or_default());
        }
        LuaType::Number => buf
            .write_chars(value.as_number().unwrap_or_default())
            .map_err(|err| err.to_string())?,
        LuaType::Integer => buf
            .write_chars(value.as_integer().unwrap_or_default())
            .map_err(|err| err.to_string())?,
        LuaType::Boolean => {
            let val = value.as_bool().unwrap_or(false);
            let s = if val { "true" } else { "false" };
            buf.write_slice(s.as_bytes());
        }
        _ => {
            return Err(format!("unsupport type :{}", value.name()));
        }
    }
    Ok(())
}

fn write(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    let top = laux::lua_top(state);
    for i in 2..=top {
        write_string(lua, buf, i)?;
    }

    Ok(0)
}

fn seek(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    let pos = lua.get(2)?;
    laux::lua_push(state, buf.seek(pos));
    Ok(1)
}

fn commit(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    let len = lua.get(2)?;
    // SAFETY: native callers initialize the region returned by prepare before
    // committing it. The Lua prepare wrapper also zero-initializes its region.
    laux::lua_push(state, unsafe { buf.commit(len) });
    Ok(1)
}

fn prepare(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    let len = lua.get(2)?;
    let space = buf.spare_capacity_mut(len);
    for byte in space.iter_mut() {
        byte.write(0);
    }
    let space = space.as_mut_ptr().cast::<u8>();
    laux::lua_pushlightuserdata(state, space as *mut c_void);
    Ok(1)
}

fn size(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    laux::lua_push(state, buf.len());
    Ok(1)
}

fn clear(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let mut buf_ptr = get_buffer_ptr(lua)?;
    // SAFETY: this callback holds the only mutable reference to the Buffer.
    let buf = unsafe { buf_ptr.as_mut() };
    buf.clear();
    Ok(0)
}

fn into_arc_buffer(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let state = lua.state();
    let buf = check_arc_buffer(lua, 1)?;
    laux::lua_newuserdata(state, buf, cstr!("shared_buffer"), &[lreg_null!()]);
    Ok(1)
}

pub extern "C-unwind" fn luaopen_buffer(state: LuaState) -> c_int {
    let l = [
        lreg_try!("new", buffer_new),
        lreg_try!("drop", buffer_drop),
        lreg_try!("concat", concat),
        lreg_try!("concat_string", concat_string),
        lreg_try!("unpack", unpack),
        lreg_try!("read", read),
        lreg_try!("write", write),
        lreg_try!("write_front", write_front),
        lreg_try!("seek", seek),
        lreg_try!("commit", commit),
        lreg_try!("prepare", prepare),
        lreg_try!("size", size),
        lreg_try!("clear", clear),
        lreg_try!("into_arc_buffer", into_arc_buffer),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
