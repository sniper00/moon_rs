// extern crate cc;
// use std::env;
// use std::path::PathBuf;

fn main() {
    // The Lua C sources (and the small C helpers) are only compiled and linked
    // when the `bundled` feature is enabled. A Lua C extension built as a cdylib
    // disables this feature so it does NOT carry its own copy of Lua; the Lua C
    // API symbols are instead resolved from the host executable at load time.
    #[cfg(feature = "bundled")]
    {
        let mut builder = cc::Build::new();
        builder.file("./lua55/onelua.c");
        builder.define("MAKE_LIB", None);
        builder.include("./lua55");
        builder.std("c11");

        // Enable loading of Lua C dynamic extension libraries (require of
        // .so/.dylib/.dll). On unix this turns on POSIX features plus
        // dlopen-based dynamic linking in loadlib.c. On Windows luaconf.h
        // already enables LUA_DL_DLL via LUA_USE_WINDOWS.
        #[cfg(unix)]
        {
            builder.define("LUA_USE_POSIX", None);
            builder.define("LUA_USE_DLOPEN", None);
        }

        #[cfg(target_os = "windows")]
        builder.flag("/experimental:c11atomics");
        builder.compile("lua55");

        // dlopen lives in libdl on Linux; macOS provides it in libc.
        // Propagates to the final binary that links this crate.
        #[cfg(target_os = "linux")]
        println!("cargo:rustc-link-lib=dylib=dl");

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
    }

    println!("cargo:rerun-if-changed=lualib-src");
}
