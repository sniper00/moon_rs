use buffer::Buffer;
use moon_lua::{
    laux::{self, LuaState, LuaValue},
};

pub mod actor;
pub mod buffer;
pub mod context;
pub mod error;
pub mod log;

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_buffer(state: LuaState, index: i32) -> Box<Buffer> {
    match LuaValue::from_stack(state, index) {
        LuaValue::String(s) => Box::new(Buffer::from(s)),
        LuaValue::LightUserData(ptr) => unsafe {
            if ptr.is_null() {
                laux::lua_error(state, format!("bad argument #{} (buffer expected, got null pointer)", index));
            }
            Box::from_raw(ptr as *mut Buffer)
        },
        _ => {
            laux::lua_error(state, format!("bad argument #{} (buffer expected, got {})", index, laux::type_name(state, index)));
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
