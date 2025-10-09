#[allow(unused_macros)]
#[macro_export]
macro_rules! cstr {
    ($s:expr) => {
        concat!($s, "\0") as *const str as *const [::std::os::raw::c_char]
            as *const ::std::os::raw::c_char
    };
}

#[macro_export]
macro_rules! lreg {
    ($name:expr, $func:expr) => {
        laux::LuaReg {
            name: cstr!($name),
            func: $func,
        }
    };
}

#[macro_export]
macro_rules! lreg_null {
    () => {
        laux::LuaReg {
            name: std::ptr::null(),
            func: laux::lua_null_function,
        }
    };
}

#[macro_export]
macro_rules! lua_rawsetfield {
    ($state:expr, $tbindex:expr, $kname:expr, $valueexp:expr) => {
        unsafe {
            ffi::lua_pushstring($state, cstr!($kname));
            $valueexp;
            ffi::lua_rawset($state, $tbindex-2);
        }
    };
}

#[macro_export]
macro_rules! push_lua_table {
    ($state:expr, $( $key:expr => $value:expr ),* ) => {
        unsafe {
            ffi::lua_createtable($state.as_ptr(), 0, 0);
            $(
                laux::lua_push($state, $key);
                laux::lua_push($state, $value);
                ffi::lua_settable($state.as_ptr(), -3);
            )*
        }
    };
}

#[macro_export]
macro_rules! luaL_newlib {
    ($state:expr, $l:expr) => {
        unsafe {
            ffi::lua_createtable($state.as_ptr(), 0, $l.len() as i32);
            ffi::luaL_setfuncs($state.as_ptr(), $l.as_ptr() as *const ffi::luaL_Reg, 0);
        }
    };
}