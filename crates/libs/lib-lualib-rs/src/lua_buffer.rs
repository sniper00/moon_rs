use lib_lua::{
    self, cstr,
    ffi::{self, lua_Integer},
    laux::{self, LuaState, LuaTable, LuaType, LuaValue},
    lreg, lreg_null, luaL_newlib,
};
use std::ffi::{c_int, c_void};

use lib_core::buffer::{self, Buffer};

const MAX_DEPTH: i32 = 32;

fn concat_table(writer: &mut Buffer, table: LuaTable, depth: i32) -> Result<(), String> {
    laux::luaL_checkstack(table.lua_state(), ffi::LUA_MINSTACK, std::ptr::null());

    for val in table.array_iter() {
        concat_one(writer, val, depth)?;
    }

    Ok(())
}

fn concat_one(writer: &mut Buffer, val: LuaValue, depth: i32) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("buffer.concat too depth table".into());
    }

    match val {
        LuaValue::Nil => {}
        LuaValue::Number(val) => writer.write_chars(val),
        LuaValue::Integer(val) => writer.write_chars(val),
        LuaValue::Boolean(val) => {
            let s = if val { "true" } else { "false" };
            writer.write_slice(s.as_bytes());
        }
        LuaValue::String(val) => writer.write_slice(val),
        LuaValue::Table(table) => {
            concat_table(writer, table, depth + 1)?;
        }
        val => {
            return Err(format!("buffer.concat unsupport type :{}", val.name()));
        }
    }

    Ok(())
}

extern "C-unwind" fn concat(state: LuaState) -> c_int {
    let n = laux::lua_top(state);
    if n == 0 {
        return 0;
    }

    let mut has_error = false;
    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        if let Err(err) = concat_one(buf.as_mut(), LuaValue::from_stack(state, i), 0) {
            laux::lua_push(state, err);
            has_error = true;
            break;
        }
    }

    if has_error {
        drop(buf);
        laux::throw_error(state);
    }

    laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);

    1
}

extern "C-unwind" fn concat_string(state: LuaState) -> c_int {
    let n = laux::lua_top(state);
    if n == 0 {
        return 0;
    }

    let mut has_error = false;
    let mut buf = Buffer::new();
    for i in 1..=n {
        if let Err(err) = concat_one(&mut buf, LuaValue::from_stack(state, i), 0) {
            has_error = true;
            laux::lua_push(state, err);
            break;
        }
    }

    if has_error {
        drop(buf);
        laux::throw_error(state);
    }

    laux::lua_push(state, buf.as_slice());
    1
}

fn get_mut_buffer(state: LuaState) -> &'static mut Buffer {
    let buf = laux::lua_touserdata::<Buffer>(state, 1);
    if buf.is_none() {
        laux::lua_arg_error(state, 1, cstr!("Invalid `Buffer` pointer"));
    }
    buf.unwrap()
}

extern "C-unwind" fn unpack(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    let top = laux::lua_top(state);

    if laux::lua_type(state, 2) == LuaType::String {
        let opt: &str = laux::lua_opt(state, 2).unwrap_or_default();
        let mut pos = laux::lua_opt::<usize>(state, 3).unwrap_or_default();
        let len = buf.len();
        if pos > len {
            return laux::lua_arg_error(state, 3, cstr!("out of range"));
        }
        let mut le = true;
        for c in opt.chars() {
            match c {
                '>' => le = false,
                '<' => le = true,
                'h' => {
                    if len - pos < 2 {
                        laux::lua_arg_error(state, 2, cstr!("data out of range"));
                    }
                    laux::lua_push(state, buf.read_i16(pos, le));
                    pos += 2;
                }
                'H' => {
                    if len - pos < 2 {
                        laux::lua_arg_error(state, 2, cstr!("data out of range"));
                    }
                    laux::lua_push(state, buf.read_u16(pos, le));
                    pos += 2;
                }
                'i' => {
                    if len - pos < 4 {
                        laux::lua_arg_error(state, 2, cstr!("data out of range"));
                    }
                    laux::lua_push(state, buf.read_i32(pos, le));
                    pos += 4;
                }
                'I' => {
                    if len - pos < 4 {
                        laux::lua_arg_error(state, 2, cstr!("data out of range"));
                    }
                    laux::lua_push(state, buf.read_u32(pos, le));
                    pos += 4;
                }
                'C' => {
                    laux::lua_pushlightuserdata(state, buf.as_ptr() as *mut c_void);
                    laux::lua_push(state, buf.len() as lua_Integer);
                }
                'Z' => {
                    laux::lua_push(state, buf.as_slice());
                }
                _ => {
                    laux::lua_error(state, format!("invalid format option '{0}'", c).as_str());
                }
            }
        }
    } else {
        let pos = laux::lua_opt::<usize>(state, 2).unwrap_or(0);
        let len = buf.len();
        if pos > len {
            return laux::lua_arg_error(state, 2, cstr!("out of range"));
        }
        let count_arg = laux::lua_opt::<isize>(state, 3).unwrap_or(-1);
        let count = if count_arg < 0 {
            (len - pos) as usize
        } else {
            std::cmp::min(len - pos, count_arg as usize)
        };

        laux::lua_push(state, &buf.as_slice()[pos..pos + count]);
    }

    laux::lua_top(state) - top
}

extern "C-unwind" fn buffer_new(state: LuaState) -> c_int {
    let capacity = laux::lua_opt::<usize>(state, 1).unwrap_or(buffer::DEFAULT_RESERVE);

    if capacity >= (usize::MAX / 2) {
        laux::lua_arg_error(state, 1, cstr!("invalid capacity"));
    }

    let buf = Box::new(Buffer::with_capacity(capacity));
    laux::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);

    1
}

extern "C-unwind" fn buffer_drop(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    unsafe {
        let _ = Box::from_raw(buf);
    }
    0
}

extern "C-unwind" fn read(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    let len = laux::lua_get(state, 2);
    if len > buf.len() {
        laux::lua_arg_error(state, 2, cstr!("out of range"));
    }

    laux::lua_push(state, &buf.as_slice()[..len]);
    buf.consume(len);

    1
}

extern "C-unwind" fn write_front(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    let top = laux::lua_top(state);
    for i in (2..=top).rev() {
        let s = laux::lua_get::<&[u8]>(state, i);
        if !buf.write_front(s) {
            laux::lua_error(state, "no more front space");
        }
    }
    0
}

fn write_string(state: LuaState, buf: &mut Buffer, index: i32) -> Result<(), String> {
    match LuaValue::from_stack(state, index) {
        LuaValue::Nil => {}
        LuaValue::String(val) => {
            buf.write_slice(val);
        }
        LuaValue::Number(val) => buf.write_chars(val),
        LuaValue::Integer(val) => buf.write_chars(val),
        LuaValue::Boolean(val) => {
            let s = if val { "true" } else { "false" };
            buf.write_slice(s.as_bytes());
        }
        _ => {
            return Err(format!("unsupport type :{}", laux::type_name(state, index)));
        }
    }
    Ok(())
}

extern "C-unwind" fn write(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    let top = laux::lua_top(state);
    let mut has_error = false;
    for i in 2..=top {
        if let Err(err) = write_string(state, buf, i) {
            has_error = true;
            laux::lua_push(state, err);
            break;
        }
    }

    if has_error {
        laux::throw_error(state);
    }

    0
}

extern "C-unwind" fn seek(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    let pos = laux::lua_get(state, 2);
    laux::lua_push(state, buf.seek(pos));
    1
}

extern "C-unwind" fn commit(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    let len = laux::lua_get(state, 2);
    laux::lua_push(state, buf.commit(len));
    1
}

extern "C-unwind" fn prepare(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    let len = laux::lua_get(state, 2);
    let space: *mut u8 = buf.prepare(len).as_mut_ptr();
    laux::lua_pushlightuserdata(state, space as *mut c_void);
    1
}

extern "C-unwind" fn size(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    laux::lua_push(state, buf.len());
    1
}

extern "C-unwind" fn clear(state: LuaState) -> c_int {
    let buf = get_mut_buffer(state);
    buf.clear();
    0
}

pub extern "C-unwind" fn luaopen_buffer(state: LuaState) -> c_int {
    let l = [
        lreg!("new", buffer_new),
        lreg!("drop", buffer_drop),
        lreg!("concat", concat),
        lreg!("concat_string", concat_string),
        lreg!("unpack", unpack),
        lreg!("read", read),
        lreg!("write", write),
        lreg!("write_front", write_front),
        lreg!("seek", seek),
        lreg!("commit", commit),
        lreg!("prepare", prepare),
        lreg!("size", size),
        lreg!("clear", clear),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
