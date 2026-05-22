use base64::{Engine, engine};
use luars::{LuaResult, LuaState, LuaValue};
use sha2::digest::DynDigest;
use std::time::Duration;

use crate::{lua_check_bytes, lua_check_integer, lua_check_str};

fn num_cpus(state: &mut LuaState) -> LuaResult<usize> {
    state.push_value(LuaValue::integer(num_cpus::get() as i64))?;
    Ok(1)
}

fn use_hasher(hasher: &mut dyn DynDigest, data: &[u8]) -> Box<[u8]> {
    hasher.update(data);
    hasher.finalize_reset()
}

fn select_hasher(s: &str) -> Option<Box<dyn DynDigest>> {
    match s {
        "md5" => Some(Box::<md5::Md5>::default()),
        "sha1" => Some(Box::<sha1::Sha1>::default()),
        "sha224" => Some(Box::<sha2::Sha224>::default()),
        "sha256" => Some(Box::<sha2::Sha256>::default()),
        "sha384" => Some(Box::<sha2::Sha384>::default()),
        "sha512" => Some(Box::<sha2::Sha512>::default()),
        _ => None,
    }
}

fn to_hex_string(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        use std::fmt::Write;
        write!(&mut s, "{:02x}", byte).expect("Writing to a String cannot fail");
    }
    s
}

fn hash(state: &mut LuaState) -> LuaResult<usize> {
    let hasher_type = lua_check_str(state, 1)?;
    let data = lua_check_bytes(state, 2)?;

    if let Some(mut hasher) = select_hasher(hasher_type) {
        let res = use_hasher(&mut *hasher, data);
        let hex = to_hex_string(res.as_ref());
        let val = state.create_string(&hex)?;
        state.push_value(val)?;
        return Ok(1);
    }

    Err(state.error(format!("utils: unsupported hasher '{}'", hasher_type)))
}

fn thread_sleep(state: &mut LuaState) -> LuaResult<usize> {
    let ms: u64 = lua_check_integer(state, 1)?;
    std::thread::sleep(Duration::from_millis(ms));
    Ok(0)
}

fn base64_encode(state: &mut LuaState) -> LuaResult<usize> {
    let data = lua_check_bytes(state, 1)?;
    let base64_string = engine::general_purpose::STANDARD.encode(data);
    let val = state.create_string(&base64_string)?;
    state.push_value(val)?;
    Ok(1)
}

fn base64_decode(state: &mut LuaState) -> LuaResult<usize> {
    let base64_string = lua_check_str(state, 1)?;
    match engine::general_purpose::STANDARD.decode(base64_string) {
        Ok(data) => {
            let val = state.create_bytes(&data)?;
            state.push_value(val)?;
            Ok(1)
        }
        Err(err) => Err(state.error(err.to_string())),
    }
}

pub fn register_utils() -> luars::LibraryModule {
    luars::lua_module!("utils", {
        "num_cpus" => num_cpus,
        "hash" => hash,
        "thread_sleep" => thread_sleep,
        "base64_encode" => base64_encode,
        "base64_decode" => base64_decode,
    })
}
