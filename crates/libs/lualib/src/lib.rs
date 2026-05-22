use actor::buffer::Buffer;
use luars::{Lua, LuaResult, LuaState, LuaTable, LuaValue};

mod lua_buffer;
mod lua_excel;
mod lua_fs;
mod lua_http;
mod lua_seri;
mod lua_socket;
mod lua_utils;
mod lua_sqlx;
mod lua_mongodb;
pub mod lua_json;
pub mod lua_actor;

/// Extract a required integer argument at stack `index`, raising a Lua error on type mismatch.
pub fn lua_check_integer<T>(state: &mut LuaState, index: usize) -> LuaResult<T>
where
    T: TryFrom<i64> + std::fmt::Display,
    <T as TryFrom<i64>>::Error: std::fmt::Display,
{
    if let Some(value) = state.get_arg(index) {
        if let Some(v) = value.as_integer() {
            T::try_from(v).map_err(|e| {
                state.error(format!(
                    "bad argument #{} (integer overflow: {} cannot fit into {}: {})",
                    index, v, std::any::type_name::<T>(), e
                ))
            })
        } else {
            Err(state.error(format!("bad argument #{} (integer expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (integer expected, got none)", index)))
    }
}

/// Extract a required number argument at stack `index`, raising a Lua error on type mismatch.
pub fn lua_check_number(state: &mut LuaState, index: usize) -> LuaResult<f64> {
    if let Some(value) = state.get_arg(index) {
        if let Some(v) = value.as_number() {
            Ok(v)
        } else {
            Err(state.error(format!("bad argument #{} (number expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (number expected, got none)", index)))
    }
}

/// Extract a required boolean argument at stack `index`, raising a Lua error on type mismatch.
pub fn lua_check_boolean(state: &mut LuaState, index: usize) -> LuaResult<bool> {
    if let Some(value) = state.get_arg(index) {
        if let Some(v) = value.as_boolean() {
            Ok(v)
        } else {
            Err(state.error(format!("bad argument #{} (boolean expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (boolean expected, got none)", index)))
    }
}

/// Extract a required string argument at stack `index` as a `&str`.
///
/// # Safety
/// The returned reference is valid as long as the Lua string is not garbage-collected.
pub fn lua_check_str(state: &mut LuaState, index: usize) -> LuaResult<&'static str> {
    if let Some(value) = state.get_arg(index) {
        if let Some(s) = value.as_str() {
            Ok(unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(s.as_ptr(), s.len())) })
        } else {
            Err(state.error(format!("bad argument #{} (string expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (string expected, got none)", index)))
    }
}

/// Extract a required string argument at stack `index` as raw bytes.
pub fn lua_check_bytes(state: &mut LuaState, index: usize) -> LuaResult<&'static [u8]> {
    if let Some(value) = state.get_arg(index) {
        if let Some(b) = value.as_bytes() {
            Ok(unsafe { std::slice::from_raw_parts(b.as_ptr(), b.len()) })
        } else {
            Err(state.error(format!("bad argument #{} (string expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (string expected, got none)", index)))
    }
}

/// Extract a required lightuserdata argument at stack `index`.
pub fn lua_check_lightuserdata(state: &mut LuaState, index: usize) -> LuaResult<*mut std::ffi::c_void> {
    if let Some(value) = state.get_arg(index) {
        if let Some(p) = value.as_lightuserdata() {
            Ok(p)
        } else {
            Err(state.error(format!("bad argument #{} (lightuserdata expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (lightuserdata expected, got none)", index)))
    }
}

/// Extract a required lightuserdata argument at stack `index` and cast it to a typed shared reference.
pub fn lua_check_typed_lightuserdata_ref<T>(
    state: &mut LuaState,
    index: usize,
) -> LuaResult<&'static T> {
    let ptr = lua_check_lightuserdata(state, index)?;
    if ptr.is_null() {
        return Err(state.error(format!(
            "bad argument #{} (non-null lightuserdata expected)",
            index
        )));
    }
    Ok(unsafe { &*(ptr as *const T) })
}

/// Extract a required lightuserdata argument at stack `index` and cast it to a typed mutable reference.
pub fn lua_check_typed_lightuserdata_mut<T>(
    state: &mut LuaState,
    index: usize,
) -> LuaResult<&'static mut T> {
    let ptr = lua_check_lightuserdata(state, index)?;
    if ptr.is_null() {
        return Err(state.error(format!(
            "bad argument #{} (non-null lightuserdata expected)",
            index
        )));
    }
    Ok(unsafe { &mut *(ptr as *mut T) })
}

/// Extract a required lightuserdata argument at stack `index` and view it as a byte slice.
pub fn lua_check_lightuserdata_bytes(
    state: &mut LuaState,
    index: usize,
    len: usize,
) -> LuaResult<&'static [u8]> {
    let ptr = lua_check_lightuserdata(state, index)?;
    if ptr.is_null() {
        return Err(state.error(format!(
            "bad argument #{} (non-null lightuserdata expected)",
            index
        )));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr as *const u8, len) })
}

/// Extract a required lightuserdata argument at stack `index` and take ownership as `Box<T>`.
pub fn lua_take_typed_lightuserdata<T>(
    state: &mut LuaState,
    index: usize,
) -> LuaResult<Box<T>> {
    let ptr = lua_check_lightuserdata(state, index)?;
    if ptr.is_null() {
        return Err(state.error(format!(
            "bad argument #{} (non-null lightuserdata expected)",
            index
        )));
    }
    Ok(unsafe { Box::from_raw(ptr as *mut T) })
}

/// Extract a required argument of any type at stack `index`.
pub fn lua_check_value(state: &mut LuaState, index: usize) -> LuaResult<LuaValue> {
    if let Some(value) = state.get_arg(index) {
        Ok(value)
    } else {
        Err(state.error(format!("bad argument #{} (value expected, got none)", index)))
    }
}

/// Try to extract an optional integer argument. Returns `None` if absent or wrong type.
pub fn lua_opt_integer<T: TryFrom<i64>>(state: &mut LuaState, index: usize) -> Option<T> {
    state.get_arg(index)
        .and_then(|v| v.as_integer())
        .and_then(|v| T::try_from(v).ok())
}

/// Extract a buffer argument: accepts a Lua string (copied into `Buffer`),
/// a lightuserdata pointer (takes ownership), or nil (returns `None`).
#[inline]
pub fn lua_check_buffer(state: &mut LuaState, index: usize) -> LuaResult<Option<Box<Buffer>>> {
    if let Some(value) = state.get_arg(index) {
        if value.is_string() {
            Ok(Some(Box::new(Buffer::from(value.as_bytes().unwrap()))))
        } else if value.is_lightuserdata() {
            match value.as_lightuserdata() {
                Some(ptr) if !ptr.is_null() => Ok(Some(unsafe { Box::from_raw(ptr as *mut Buffer) })),
                _ => Ok(None),
            }
        } else if value.is_nil() {
            Ok(None)
        } else {
            Err(state.error(format!("bad argument #{} (buffer expected, got {})", index, value.type_name())))
        }
    } else {
        Err(state.error(format!("bad argument #{} (buffer expected, got none)", index)))
    }
}

pub fn lua_opt_boolean(state: &mut LuaState, index: usize) -> Option<bool> {
    state.get_arg(index).and_then(|v| v.as_boolean())
}

pub fn lua_opt_number(state: &mut LuaState, index: usize) -> Option<f64> {
    state.get_arg(index).and_then(|v| v.as_number())
}

pub fn lua_opt_str(state: &mut LuaState, index: usize) -> Option<&'static str> {
    state.get_arg(index).and_then(|v| {
        v.as_str().map(|s| unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(s.as_ptr(), s.len())) })
    })
}

/// Determine if a Lua table is a pure array (keys 1..n with no gaps or hash part).
/// Returns the array length, or 0 if the table has any non-sequential keys.
pub fn lua_array_size(t: &LuaTable) -> usize {
    let first = match t.next(&LuaValue::nil()) {
        Ok(Some((k, _))) => k,
        _ => return 0,
    };

    let first_key = first.as_integer().unwrap_or(0);
    if first_key <= 0 {
        return 0;
    }

    let len = t.len() as i64;

    if first_key == 1 {
        let len_key = LuaValue::integer(len);
        if t.next(&len_key).ok().flatten().is_some() {
            return 0;
        }
    }

    if first_key > len {
        return 0;
    }

    let mut key = LuaValue::nil();
    while let Ok(Some((k, _))) = t.next(&key) {
        if let Some(x) = k.as_integer()
            && x > 0 && x <= len
        {
            key = k;
            continue;
        }
        return 0;
    }

    len as usize
}

/// Unified Lua error return convention: pushes `(false, errmsg)` and returns `Ok(2)`.
/// Use this for recoverable errors in Lua C functions instead of `Err(state.error(...))`.
pub fn lua_push_error(state: &mut LuaState, msg: &str) -> LuaResult<usize> {
    state.push_value(LuaValue::boolean(false))?;
    let s = state.create_string(msg)?;
    state.push_value(s)?;
    Ok(2)
}

/// Read an optional string field from a Lua table.
pub fn opt_field_str(state: &mut LuaState, table: &LuaValue, field: &str) -> Option<String> {
    let key = state.create_string(field).ok()?;
    state.raw_get(table, &key).and_then(|v| v.as_str().map(|s| s.to_string()))
}

/// Read an optional raw-byte field from a Lua table.
pub fn opt_field_bytes(state: &mut LuaState, table: &LuaValue, field: &str) -> Option<Vec<u8>> {
    let key = state.create_string(field).ok()?;
    state.raw_get(table, &key).and_then(|v| v.as_bytes().map(|b| b.to_vec()))
}

/// Read an optional integer field from a Lua table.
pub fn opt_field_int(state: &mut LuaState, table: &LuaValue, field: &str) -> Option<i64> {
    let key = state.create_string(field).ok()?;
    state.raw_get(table, &key).and_then(|v| v.as_integer())
}

/// Read an optional boolean field from a Lua table.
pub fn opt_field_bool(state: &mut LuaState, table: &LuaValue, field: &str) -> Option<bool> {
    let key = state.create_string(field).ok()?;
    state.raw_get(table, &key).and_then(|v| v.as_boolean())
}

/// Push a structured error table `{ kind = ..., message = ... }` and return `Ok(1)`.
/// Used by database modules for typed error responses.
pub fn push_error_table(state: &mut LuaState, kind: &str, message: &str) -> LuaResult<usize> {
    let table = state.create_table(0, 2)?;
    let k = state.create_string("kind")?;
    let v = state.create_string(kind)?;
    state.raw_set(&table, k, v);
    let k = state.create_string("message")?;
    let v = state.create_string(message)?;
    state.raw_set(&table, k, v);
    state.push_value(table)?;
    Ok(1)
}

/// Push a single-field result table `{ key = val }` and return `Ok(1)`.
/// Used by database modules for success responses.
pub fn push_message_table(state: &mut LuaState, key: &str, val: &str) -> LuaResult<usize> {
    let table = state.create_table(0, 1)?;
    let k = state.create_string(key)?;
    let v = state.create_string(val)?;
    state.raw_set(&table, k, v);
    state.push_value(table)?;
    Ok(1)
}

/// Register all application-specific Lua libraries (http, socket, json, etc.)
/// into the given Lua VM.
pub fn luaopen_custom_libs(lua: &mut Lua) -> luars::LuaResult<()> {
    lua.install_library(lua_http::register_http())?;
    lua.install_library(lua_socket::register_socket())?;
    lua.install_library(lua_excel::register_excel())?;
    lua.install_library(lua_fs::register_fs())?;
    lua.install_library(lua_json::register_json())?;
    lua.install_library(lua_buffer::register_buffer())?;
    lua.install_library(lua_seri::register_seri())?;
    lua.install_library(lua_sqlx::register_sqlx())?;
    lua.install_library(lua_mongodb::register_mongodb())?;
    lua.install_library(lua_utils::register_utils())?;

    Ok(())
}
