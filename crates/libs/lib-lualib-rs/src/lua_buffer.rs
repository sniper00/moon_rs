use lib_lua::{self, cstr, ffi::{self, luaL_Reg, lua_Integer}, laux::{self, LuaType}, lreg, lreg_null};
use std::ffi::{c_int, c_void};

use lib_core::buffer::{self, Buffer};

const MAX_DEPTH: i32 = 32;

fn concat_table(
    state: *mut ffi::lua_State,
    writer: &mut Buffer,
    index: i32,
    depth: i32,
) -> Result<(), String> {
    unsafe {
        ffi::luaL_checkstack(state, ffi::LUA_MINSTACK, std::ptr::null());
    }
    let mut index = index;
    if index < 0 {
        index = unsafe { ffi::lua_gettop(state) + index + 1 };
    }
    let array_size = unsafe { ffi::lua_rawlen(state, index) };
    for i in 1..=array_size {
        unsafe {
            ffi::lua_rawgeti(state, index, i as lua_Integer);
        }
        concat_one(state, writer, -1, depth)?;
        unsafe {
            ffi::lua_pop(state, 1);
        }
    }
    Ok(())
}

fn concat_one(
    state: *mut ffi::lua_State,
    writer: &mut Buffer,
    index: i32,
    depth: i32,
) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("buffer.concat too depth table".into());
    }

    match laux::lua_type(state, index) {
        LuaType::Nil => {}
        LuaType::Number => {
            if unsafe { ffi::lua_isinteger(state, index) } != 0 {
                writer.write_chars(unsafe { ffi::lua_tointeger(state, index) });
            } else {
                writer.write_chars(unsafe { ffi::lua_tonumber(state, index) });
            }
        }
        LuaType::Boolean => {
            let n = unsafe { ffi::lua_toboolean(state, index) };
            let s = if n != 0 { "true" } else { "false" };
            writer.write_slice(s.as_bytes());
        }
        LuaType::String => {
            let mut sz = 0;
            let str = unsafe { ffi::lua_tolstring(state, index, &mut sz) as *const u8 };
            unsafe {
                writer.write_slice(std::slice::from_raw_parts(str, sz).as_ref());
            }
        }
        LuaType::Table => {
            concat_table(state, writer, index, depth + 1)?;
        }
        t => {
            return Err(format!(
                "buffer.concat unsupport type :{}",
                laux::type_name(state, t.into())
            ));
        }
    }

    Ok(())
}

extern "C-unwind" fn concat(state: *mut ffi::lua_State) -> c_int {
    let n = unsafe { ffi::lua_gettop(state) };
    if n == 0 {
        return 0;
    }

    let mut has_error = false;
    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        if let Err(err) = concat_one(state, buf.as_mut(), i, 0) {
            laux::lua_push(state, err);
            has_error = true;
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

extern "C-unwind" fn concat_string(state: *mut ffi::lua_State) -> c_int {
    let n = unsafe { ffi::lua_gettop(state) };
    if n == 0 {
        return 0;
    }

    let mut has_error = false;
    let mut buf = Buffer::new();
    for i in 1..=n {
        if let Err(err) = concat_one(state, &mut buf, i, 0) {
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
        ffi::lua_pushlstring(state, buf.as_ptr() as *const i8, buf.len());
    }
    1
}

fn get_mut_buffer(state: *mut ffi::lua_State) -> &'static mut Buffer {
    let buf = unsafe { ffi::lua_touserdata(state, 1) as *mut Buffer };
    if buf.is_null() {
        unsafe { ffi::luaL_argerror(state, 1, cstr!("null buffer pointer")) };
    }
    unsafe { &mut *buf }
}

extern "C-unwind" fn unpack(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    let top = unsafe { ffi::lua_gettop(state) };

    unsafe {
        if laux::lua_type(state, 2) == LuaType::String {
            let opt: &str = laux::lua_opt(state, 2).unwrap_or_default();
            let mut pos = ffi::luaL_optinteger(state, 3, 0) as usize;
            let len = buf.len();
            if pos > len {
                return ffi::luaL_argerror(state, 3, cstr!("out of range"));
            }
            let mut le = true;
            for c in opt.chars() {
                match c {
                    '>' => le = false,
                    '<' => le = true,
                    'h' => {
                        if len - pos < 2 {
                            ffi::luaL_error(state, cstr!("data out of range"));
                        }
                        ffi::lua_pushinteger(state, buf.read_i16(pos, le) as lua_Integer);
                        pos += 2;
                    }
                    'H' => {
                        if len - pos < 2 {
                            ffi::luaL_error(state, cstr!("data out of range"));
                        }
                        ffi::lua_pushinteger(state, buf.read_u16(pos, le) as lua_Integer);
                        pos += 2;
                    }
                    'i' => {
                        if len - pos < 4 {
                            ffi::luaL_error(state, cstr!("data out of range"));
                        }
                        ffi::lua_pushinteger(state, buf.read_i32(pos, le) as lua_Integer);
                        pos += 4;
                    }
                    'I' => {
                        if len - pos < 4 {
                            ffi::luaL_error(state, cstr!("data out of range"));
                        }
                        ffi::lua_pushinteger(state, buf.read_u32(pos, le) as lua_Integer);
                        pos += 4;
                    }
                    'C' => {
                        ffi::lua_pushlightuserdata(state, buf.as_ptr() as *mut c_void);
                        ffi::lua_pushinteger(state, buf.len() as lua_Integer);
                    }
                    'Z' => {
                        ffi::lua_pushlstring(state, buf.as_ptr() as *const i8, buf.len());
                    }
                    _ => {
                        ffi::luaL_error(state, cstr!("invalid format option '%c'"), c);
                    }
                }
            }
        } else {
            let pos = ffi::luaL_optinteger(state, 2, 0) as usize;
            let count = ffi::luaL_optinteger(state, 3, -1) as usize;
            let len = buf.len();
            if pos > len {
                return ffi::luaL_argerror(state, 2, cstr!("out of range"));
            }
            let count = std::cmp::min(len - pos, count);
            ffi::lua_pushlstring(state, buf.data().as_ptr() as *const i8, count);
        }
    }

    unsafe { ffi::lua_gettop(state) - top }
}

extern "C-unwind" fn buffer_new(state: *mut ffi::lua_State) -> c_int {
    let capacity = laux::lua_opt::<usize>(state, 1).unwrap_or(buffer::DEFAULT_RESERVE);
    unsafe {
        ffi::luaL_argcheck(
            state,
            if capacity < (usize::MAX / 2) { 1 } else { 0 },
            1,
            cstr!("invalid capacity"),
        )
    };
    let buf = Box::new(Buffer::with_capacity(capacity));
    unsafe {
        ffi::lua_pushlightuserdata(state, Box::into_raw(buf) as *mut c_void);
    }
    1
}

extern "C-unwind" fn buffer_drop(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    unsafe {
        let _ = Box::from_raw(buf);
    }
    0
}

extern "C-unwind" fn read(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    let len = laux::lua_get(state, 2);
    if len > buf.len() {
        unsafe {
            ffi::luaL_argerror(state, 2, cstr!("out of range"));
        }
    }

    unsafe {
        ffi::lua_pushlstring(state, buf.data().as_ptr() as *const i8, len);
        buf.consume(len)
    }
    1
}

extern "C-unwind" fn write_front(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    let top = unsafe { ffi::lua_gettop(state) };
    for i in (2..=top).rev() {
        let s = laux::lua_get::<&[u8]>(state, i);
        if !buf.write_front(s) {
            unsafe {
                ffi::luaL_error(state, cstr!("no more front space"));
            }
        }
    }
    0
}

fn write_string(state: *mut ffi::lua_State, buf: &mut Buffer, index: i32) -> Result<(), String> {
    match laux::lua_type(state, index) {
        LuaType::Nil => {}
        LuaType::String => {
            buf.write_slice(laux::lua_get::<&[u8]>(state, index));
        }
        LuaType::Number => {
            if unsafe { ffi::lua_isinteger(state, index) } != 0 {
                buf.write_chars(unsafe { ffi::lua_tointeger(state, index) });
            } else {
                buf.write_chars(unsafe { ffi::lua_tonumber(state, index) });
            }
        }
        LuaType::Boolean => {
            let n = unsafe { ffi::lua_toboolean(state, index) };
            let s = if n != 0 { "true" } else { "false" };
            buf.write_slice(s.as_bytes());
        }
        ltype => {
            return Err(format!("unsupport type :{}", laux::type_name(state, ltype.into())));
        }
    }
    Ok(())
}

extern "C-unwind" fn write(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    let top = unsafe { ffi::lua_gettop(state) };
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

extern "C-unwind" fn seek(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    let pos = laux::lua_get(state, 2);
    laux::lua_push(state, buf.seek(pos));
    1
}

extern "C-unwind" fn commit(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    let len = laux::lua_get(state, 2);
    laux::lua_push(state, buf.commit(len));
    1
}

extern "C-unwind" fn prepare(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    let len = laux::lua_get(state, 2);
    let space: *mut u8 = buf.prepare(len).as_mut_ptr();
    unsafe {
        ffi::lua_pushlightuserdata(state, space as *mut c_void);
    }
    1
}

extern "C-unwind" fn size(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    unsafe {
        ffi::lua_pushinteger(state, buf.len() as lua_Integer);
    }
    1
}

extern "C-unwind" fn clear(state: *mut ffi::lua_State) -> c_int {
    let buf = get_mut_buffer(state);
    buf.clear();
    0
}

pub unsafe extern "C-unwind" fn luaopen_buffer(state: *mut ffi::lua_State) -> c_int {
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

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    1
}
