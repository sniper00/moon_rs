use lib_lua::ffi::{self, luaL_Reg, lua_Integer};
use std::ffi::{c_int, c_void};

use lib_core::{
    buffer::Buffer,
    c_str,
    laux::{self, LuaValue},
    lreg, lreg_null,
};

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
    if depth > 32 {
        return Err("buffer.concat too depth table".into());
    }

    match laux::lua_type(state, index) {
        ffi::LUA_TNIL => {}
        ffi::LUA_TNUMBER => {
            if unsafe { ffi::lua_isinteger(state, index) } != 0 {
                writer.write_chars(unsafe { ffi::lua_tointeger(state, index) });
            } else {
                writer.write_chars(unsafe { ffi::lua_tonumber(state, index) });
            }
        }
        ffi::LUA_TBOOLEAN => {
            let n = unsafe { ffi::lua_toboolean(state, index) };
            let s = if n != 0 { "true" } else { "false" };
            writer.write_slice(s.as_bytes());
        }
        ffi::LUA_TSTRING => {
            let mut sz = 0;
            let str = unsafe { ffi::lua_tolstring(state, index, &mut sz) as *const u8 };
            unsafe {
                writer.write_slice(std::slice::from_raw_parts(str, sz).as_ref());
            }
        }
        ffi::LUA_TTABLE => {
            concat_table(state, writer, index, depth + 1)?;
        }
        t => {
            let tname = unsafe {
                std::ffi::CStr::from_ptr(ffi::lua_typename(state, t))
                    .to_str()
                    .unwrap_or_default()
            };
            return Err(format!("table.concat unsupport type :{}", tname));
        }
    }

    Ok(())
}

extern "C-unwind" fn concat(state: *mut ffi::lua_State) -> c_int {
    let n = unsafe { ffi::lua_gettop(state) };
    if n == 0 {
        return 0;
    }

    let mut buf = Box::new(Buffer::new());
    for i in 1..=n {
        if let Err(err) = concat_one(state, buf.as_mut(), i, 0) {
            drop(buf);
            laux::lua_error(state, &err);
        }
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

    let mut buf = Buffer::new();
    for i in 1..=n {
        if let Err(err) = concat_one(state, &mut buf, i, 0) {
            drop(buf);
            laux::lua_error(state, &err);
        }
    }

    unsafe {
        ffi::lua_pushlstring(state, buf.as_ptr() as *const i8, buf.len());
    }
    1
}

fn get_pointer(state: *mut ffi::lua_State) -> &'static mut Buffer {
    let buf = unsafe { ffi::lua_touserdata(state, 1) as *mut Buffer };
    if buf.is_null() {
        unsafe { ffi::luaL_argerror(state, 1, c_str!("null buffer pointer")) };
    }
    unsafe { &mut *buf }
}

extern "C-unwind" fn unpack(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    let top = unsafe { ffi::lua_gettop(state) };

    unsafe {
        if laux::lua_type(state, 2) == ffi::LUA_TSTRING {
            let opt = laux::opt_str(state, 2, "");
            let mut pos = ffi::luaL_optinteger(state, 3, 0) as usize;
            let len = buf.len();
            if pos > len {
                return ffi::luaL_argerror(state, 3, c_str!("out of range"));
            }
            let mut le = true;
            for c in opt.chars() {
                match c {
                    '>' => le = false,
                    '<' => le = true,
                    'h' => {
                        if len - pos < 2 {
                            ffi::luaL_error(state, c_str!("data out of range"));
                        }
                        ffi::lua_pushinteger(state, buf.read_i16(pos, le) as lua_Integer);
                        pos += 2;
                    }
                    'H' => {
                        if len - pos < 2 {
                            ffi::luaL_error(state, c_str!("data out of range"));
                        }
                        ffi::lua_pushinteger(state, buf.read_u16(pos, le) as lua_Integer);
                        pos += 2;
                    }
                    'i' => {
                        if len - pos < 4 {
                            ffi::luaL_error(state, c_str!("data out of range"));
                        }
                        ffi::lua_pushinteger(state, buf.read_i32(pos, le) as lua_Integer);
                        pos += 4;
                    }
                    'I' => {
                        if len - pos < 4 {
                            ffi::luaL_error(state, c_str!("data out of range"));
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
                        ffi::luaL_error(state, c_str!("invalid format option '%c'"), c);
                    }
                }
            }
        } else {
            let pos = ffi::luaL_optinteger(state, 2, 0) as usize;
            let count = ffi::luaL_optinteger(state, 3, -1) as usize;
            let len = buf.len();
            if pos > len {
                return ffi::luaL_argerror(state, 2, c_str!("out of range"));
            }
            let count = std::cmp::min(len - pos, count);
            ffi::lua_pushlstring(state, buf.data().as_ptr() as *const i8, count);
        }
    }

    unsafe { ffi::lua_gettop(state) - top }
}

extern "C-unwind" fn buffer_drop(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    unsafe {
        let _ = Box::from_raw(buf);
    }
    0
}

extern "C-unwind" fn read(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    let len = usize::from_lua_check(state, 2);
    if len > buf.len() {
        unsafe {
            ffi::luaL_argerror(state, 2, c_str!("out of range"));
        }
    }

    unsafe {
        ffi::lua_pushlstring(state, buf.data().as_ptr() as *const i8, len);
        buf.consume(len)
    }
    1
}

extern "C-unwind" fn write_front(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    let top = unsafe { ffi::lua_gettop(state) };
    for i in (2..=top).rev() {
        let s = laux::check_slice(state, i);
        if !buf.write_front(s) {
            unsafe {
                ffi::luaL_error(state, c_str!("no more front space"));
            }
        }
    }
    0
}

fn write_string(state: *mut ffi::lua_State, buf: &mut Buffer, index: i32) {
    match laux::lua_type(state, index) {
        ffi::LUA_TNIL => {}
        ffi::LUA_TSTRING => {
            buf.write_slice(laux::check_slice(state, index));
        }
        ffi::LUA_TNUMBER => {
            if unsafe { ffi::lua_isinteger(state, index) } != 0 {
                buf.write_chars(unsafe { ffi::lua_tointeger(state, index) });
            } else {
                buf.write_chars(unsafe { ffi::lua_tonumber(state, index) });
            }
        }
        ffi::LUA_TBOOLEAN => {
            let n = unsafe { ffi::lua_toboolean(state, index) };
            let s = if n != 0 { "true" } else { "false" };
            buf.write_slice(s.as_bytes());
        }
        ltype => laux::lua_error(
            state,
            &format!("unsupport type :{}", laux::type_name(state, ltype)),
        ),
    }
}

extern "C-unwind" fn write(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    let top = unsafe { ffi::lua_gettop(state) };
    for i in 2..=top {
        write_string(state, buf, i)
    }
    0
}

extern "C-unwind" fn seek(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    let pos = usize::from_lua_check(state, 2);
    if !buf.seek(pos) {
        unsafe {
            ffi::luaL_argerror(state, 2, c_str!("out of range"));
        }
    }
    0
}

extern "C-unwind" fn commit(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    let len = usize::from_lua_check(state, 2);
    buf.commit(len);
    0
}

extern "C-unwind" fn prepare(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    let len = usize::from_lua_check(state, 2);
    buf.prepare(len);
    0
}

extern "C-unwind" fn size(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    unsafe {
        ffi::lua_pushinteger(state, buf.len() as lua_Integer);
    }
    1
}

extern "C-unwind" fn clear(state: *mut ffi::lua_State) -> c_int {
    let buf = get_pointer(state);
    buf.clear();
    0
}

pub unsafe extern "C-unwind" fn luaopen_buffer(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("concat", concat),
        lreg!("concat_string", concat_string),
        lreg!("unpack", unpack),
        lreg!("drop", buffer_drop),
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