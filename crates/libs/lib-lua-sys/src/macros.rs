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
        luaL_Reg {
            name: cstr!($name),
            func: $func,
        }
    };
}

#[macro_export]
macro_rules! lreg_null {
    () => {
        luaL_Reg {
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
            ffi::lua_createtable($state, 0, 0);
            $(
                laux::lua_push($state, $key);
                laux::lua_push($state, $value);
                ffi::lua_settable($state, -3);
            )*
        }
    };
}

#[macro_export]
macro_rules! luaL_newlib {
    ($state:expr, $l:expr) => {
        unsafe {
            ffi::lua_createtable($state, 0, $l.len() as i32);
            ffi::luaL_setfuncs($state, $l.as_ptr(), 0);
        }
    };
}
