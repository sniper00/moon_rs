// extern crate cc;
// use std::env;
// use std::path::PathBuf;

fn main() {
    // let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    let mut builder = cc::Build::new();
    builder.file("./lua54/onelua.c");
    builder.define("MAKE_LIB", None);
    builder.static_flag(true);
    builder.include("./lua54");
    builder.std("c11");

    #[cfg(target_os = "windows")]
    builder.flag("/experimental:c11atomics");
    builder.compile("lua54");

    cc::Build::new()
        .file("lualib-src/yyjson/yyjson.c")
        .file("lualib-src/lua_json_decode.c")
        .static_flag(true)
        .include("./lua54")
        .include("lualib-src")
        .std("c11")
        .compile("lua_json_decode");

    println!("cargo:rerun-if-changed=lualib-src");
}
