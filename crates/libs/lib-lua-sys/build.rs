// extern crate cc;
// use std::env;
// use std::path::PathBuf;

fn main() {
    // let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    cc::Build::new()
        .file("./lua54/onelua.c")
        .define("MAKE_LIB", None)
        .static_flag(true)
        .include("./lua54")
        .std("c11")
        .flag_if_supported("/experimental:c11atomics")
        // .flag("/MD")
        .compile("lua54");

    cc::Build::new()
        .file("lualib-src/yyjson/yyjson.c")
        .file("lualib-src/lua_json_decode.c")
        .static_flag(true)
        .include("./lua54")
        .include("lualib-src")
        .std("c11")
        .compile("lua_json_decode");

    // cc::Build::new()
    //     .cpp(true)
    //     .std("c++17")
    //     .flag_if_supported("-std=c++17")
    //     .include("D:/GitProject/moon/third/lua/")
    //     .include("D:/GitProject/moon/third/")
    //     .include("D:/GitProject/moon/")
    //     .include("D:/GitProject/moon/moon-src/core/")
    //     .file("lualib-src/lua_serialize.cpp")
    //     // .cargo_metadata(true)
    //     .compile("lualib");

    println!("cargo:rerun-if-changed=lualib-src");
}
