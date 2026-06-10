// extern crate cc;
// use std::env;
// use std::path::PathBuf;

fn main() {
    // let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    let mut builder = cc::Build::new();
    builder.file("./lua55/onelua.c");
    builder.define("MAKE_LIB", None);
    builder.include("./lua55");
    builder.std("c11");

    #[cfg(unix)]
    builder.define("LUA_USE_POSIX", None);

    #[cfg(target_os = "windows")]
    builder.flag("/experimental:c11atomics");
    builder.compile("lua55");

    cc::Build::new()
        .file("lualib-src/yyjson/yyjson.c")
        .file("lualib-src/lua_json_decode.c")
        .include("./lua55")
        .include("lualib-src")
        .std("c11")
        .compile("lua_json_decode");

    cc::Build::new()
        .file("lualib-src/lua_sharetable.c")
        .include("./lua55")
        .std("c11")
        .compile("lua_sharetable");

    println!("cargo:rerun-if-changed=lualib-src");
}
