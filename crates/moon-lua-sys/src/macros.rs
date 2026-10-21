#[allow(unused_macros)]
#[macro_export]
macro_rules! cstr {
    ($s:expr) => {
        concat!($s, "\0") as *const str as *const [::std::os::raw::c_char]
            as *const ::std::os::raw::c_char
    };
}

#[macro_export]
macro_rules! lreg_raw {
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

/// Registers a lifetime-aware callback without exposing a separate wrapper
/// function at the module level.
#[macro_export]
macro_rules! lreg {
    ($name:expr, $implementation:path) => {{
        extern "C-unwind" fn callback(state: $crate::laux::LuaState) -> ::std::ffi::c_int {
            let mut lua = unsafe { $crate::laux::LuaStack::from_raw(state) };
            $implementation(&mut lua)
        }
        laux::LuaReg {
            name: cstr!($name),
            func: callback,
        }
    }};
}

/// Registers a lifetime-aware callback whose implementation returns
/// `Result<c_int, String>`. Errors are converted to Lua errors at the ABI
/// boundary, after the context borrow has ended.
#[macro_export]
macro_rules! lreg_try {
    ($name:expr, $implementation:path) => {{
        extern "C-unwind" fn callback(state: $crate::laux::LuaState) -> ::std::ffi::c_int {
            let result = {
                let mut lua = unsafe { $crate::laux::LuaStack::from_raw(state) };
                $implementation(&mut lua)
            };
            match result {
                Ok(result) => result,
                Err(error) => $crate::laux::lua_error(state, error),
            }
        }
        laux::LuaReg {
            name: cstr!($name),
            func: callback,
        }
    }};
}

#[macro_export]
macro_rules! lua_rawsetfield {
    ($state:expr, $tbindex:expr, $kname:expr, $valueexp:expr) => {
        unsafe {
            ffi::lua_pushstring($state, cstr!($kname));
            $valueexp;
            ffi::lua_rawset($state, $tbindex - 2);
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
