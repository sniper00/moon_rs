#![allow(clippy::collapsible_if)]

use std::sync::Arc;

use buffer::Buffer;
use moon_lua::laux::{self, LuaState, LuaValue};

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
                laux::lua_error(
                    state,
                    format!(
                        "bad argument #{} (buffer expected, got null pointer)",
                        index
                    ),
                );
            }
            Box::from_raw(ptr as *mut Buffer)
        },
        _ => {
            laux::lua_error(
                state,
                format!(
                    "bad argument #{} (buffer expected, got {})",
                    index,
                    laux::type_name(state, index)
                ),
            );
        }
    }
}

#[inline]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_arc_buffer(state: LuaState, index: i32) -> Arc<Buffer> {
    match LuaValue::from_stack(state, index) {
        LuaValue::String(s) => Arc::new(Buffer::from(s)),
        LuaValue::LightUserData(ptr) => unsafe {
            if ptr.is_null() {
                laux::lua_error(
                    state,
                    format!(
                        "bad argument #{} (buffer expected, got null pointer)",
                        index
                    ),
                );
            }
            Arc::from(Box::from_raw(ptr as *mut Buffer))
        },
        LuaValue::UserData(ptr) => unsafe {
            if ptr.is_null() {
                laux::lua_error(
                    state,
                    format!(
                        "bad argument #{} (buffer expected, got null pointer)",
                        index
                    ),
                );
            }
            let arc = &*(ptr as *const Arc<Buffer>);
            arc.clone()
        },
        _ => {
            laux::lua_error(
                state,
                format!(
                    "bad argument #{} (buffer expected, got {})",
                    index,
                    laux::type_name(state, index)
                ),
            );
        }
    }
}

pub fn escape_print(input: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut result = String::with_capacity(input.len());

    for &byte in input {
        match byte {
            b'\\' => result.push_str("\\\\"),
            b'"' => result.push_str("\\\""),
            b if b.is_ascii_graphic() || b.is_ascii_whitespace() => result.push(b as char),
            _ => {
                result.push('\\');
                result.push('x');
                result.push(HEX[(byte >> 4) as usize] as char);
                result.push(HEX[(byte & 0xf) as usize] as char);
            }
        }
    }

    result
}
