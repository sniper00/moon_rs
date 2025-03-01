use buffer::Buffer;
use lib_lua::{
    cstr, ffi,
    laux::{lua_get, lua_type, LuaStateRaw, LuaType},
};

pub mod actor;
pub mod buffer;
pub mod context;
pub mod error;
pub mod log;

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_buffer(state: LuaStateRaw, index: i32) -> Option<Box<Buffer>> {
    match lua_type(state, index) {
        LuaType::String => Some(Box::new(lua_get::<&[u8]>(state, index).into())),
        LuaType::LightUserData => unsafe {
            let ptr = ffi::lua_touserdata(state, index) as *mut Buffer;
            Some(Box::from_raw(ptr))
        },
        LuaType::Nil => None,
        _ => {
            unsafe {
                ffi::luaL_argerror(
                    state,
                    index,
                    cstr!("nil, lightuserdata(buffer*) or string expected"),
                )
            };
            None
        }
    }
}

pub fn escape_print(input: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut result = String::with_capacity(input.len());
    
    for byte in input {
        if byte.is_ascii_graphic() || byte.is_ascii_whitespace() {
            result.push(*byte as char);
        } else {
            result.push('\\');
            result.push('x');
            result.push(HEX[(byte >> 4) as usize] as char);
            result.push(HEX[(byte & 0xf) as usize] as char);
        }
    }
    
    result
}
