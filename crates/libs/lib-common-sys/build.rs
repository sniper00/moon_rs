// extern crate cc;
// use std::env;
// use std::path::PathBuf;

fn main() {
    // let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    cc::Build::new()
        .file("common/common.c")
        .static_flag(true)
        .compile("common");

    println!("cargo:rerun-if-changed=common");
}
