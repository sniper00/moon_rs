#[macro_export]
macro_rules! c_str {
    ($s:expr) => {
        concat!($s, "\0") as *const str as *const [::std::os::raw::c_char]
            as *const ::std::os::raw::c_char
    };
}

#[macro_export]
macro_rules! lreg {
    ($name:expr, $func:expr) => {
        luaL_Reg {
            name: c_str!($name),
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
            ffi::lua_pushstring($state, c_str!($kname));
            $valueexp;
            ffi::lua_rawset($state, $tbindex);
        }
    };
}
