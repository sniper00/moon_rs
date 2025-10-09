use base64::{engine, Engine};
use lib_lua::{
    self,
    laux::{self, LuaState},
};
use sha2::digest::DynDigest;
use std::{ffi::c_int, time::Duration};

pub extern "C-unwind" fn num_cpus(state: LuaState) -> c_int {
    laux::lua_push(state, num_cpus::get());
    1
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

fn to_hex_string(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        use std::fmt::Write;
        write!(&mut s, "{:02x}", byte).expect("Writing to a String cannot fail");
    }
    s
}

pub extern "C-unwind" fn hash(state: LuaState) -> c_int {
    let hasher_type = laux::lua_get(state, 1);
    let data = laux::lua_get::<&[u8]>(state, 2);
    if let Some(mut hasher) = select_hasher(hasher_type) {
        let res = use_hasher(&mut *hasher, data);
        laux::lua_push(state, to_hex_string(res.as_ref()).as_str());
        return 1;
    }

    laux::lua_error(
        state,
        format!("unsupported hasher {}", hasher_type).as_str(),
    );
}

pub extern "C-unwind" fn thread_sleep(state: LuaState) -> c_int {
    let ms: u64 = laux::lua_get(state, 1);
    std::thread::sleep(Duration::from_millis(ms as u64));
    0
}

pub extern "C-unwind" fn base64_encode(state: LuaState) -> c_int {
    let data = laux::lua_get::<&[u8]>(state, 1);
    let base64_string = engine::general_purpose::STANDARD.encode(data);
    laux::lua_push(state, base64_string);
    1
}

pub extern "C-unwind" fn base64_decode(state: LuaState) -> c_int {
    let base64_string = laux::lua_get::<&str>(state, 1);
    let data = engine::general_purpose::STANDARD.decode(base64_string);
    if data.is_err() {
        laux::lua_push(state, data.clone().unwrap_err().to_string());
        drop(data);
        laux::throw_error(state);
    }
    laux::lua_push(state, data.unwrap().as_slice());
    1
}
