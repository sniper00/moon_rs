// extern crate cc;
// use std::env;
// use std::path::PathBuf;

fn main() {
    // let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());


    cc::Build::new()
        .file("common/common.c")
        .static_flag(true)
        .compile("common");

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

    println!("cargo:rerun-if-changed=common");
}
