use base64::{Engine, engine};
use moon_lua_sys::{
    self, cstr, ffi,
    laux::{self, LuaStack, LuaState},
    lreg_null, lreg_try, luaL_newlib,
};
use sha2::digest::DynDigest;
use std::{ffi::c_int, time::Duration};

fn num_cpus(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    lua.push(num_cpus::get());
    Ok(1)
}

// Dynamic hash function
fn use_hasher(hasher: &mut dyn DynDigest, data: &[u8]) -> Box<[u8]> {
    hasher.update(data);
    hasher.finalize_reset()
}

// You can use something like this when parsing user input, CLI arguments, etc.
// DynDigest needs to be boxed here, since function return should be sized.
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

fn to_hex_string(bytes: &[u8]) -> Result<String, std::fmt::Error> {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        use std::fmt::Write;
        write!(&mut s, "{:02x}", byte)?;
    }
    Ok(s)
}

fn hash(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let hasher_type = crate::checked_str(lua, 1)?;
    let data = crate::checked_bytes(lua, 2)?;
    if let Some(mut hasher) = select_hasher(hasher_type) {
        let res = use_hasher(&mut *hasher, data);
        lua.push(to_hex_string(res.as_ref()).map_err(|err| err.to_string())?);
        return Ok(1);
    }

    Err(format!("unsupported hasher {}", hasher_type))
}

fn thread_sleep(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let ms: u64 = lua.get(1)?;
    std::thread::sleep(Duration::from_millis(ms));
    Ok(0)
}

fn base64_encode(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let data = crate::checked_bytes(lua, 1)?;
    let base64_string = engine::general_purpose::STANDARD.encode(data);
    lua.push(base64_string);
    Ok(1)
}

fn base64_decode(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let base64_string = crate::checked_str(lua, 1)?;
    let data = engine::general_purpose::STANDARD
        .decode(base64_string)
        .map_err(|error| error.to_string())?;
    lua.push(data);
    Ok(1)
}

pub extern "C-unwind" fn luaopen_utils(state: LuaState) -> c_int {
    let l = [
        lreg_try!("num_cpus", num_cpus),
        lreg_try!("hash", hash),
        lreg_try!("thread_sleep", thread_sleep),
        lreg_try!("base64_encode", base64_encode),
        lreg_try!("base64_decode", base64_decode),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);
    1
}
