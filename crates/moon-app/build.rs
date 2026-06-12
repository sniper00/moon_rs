fn main() {
    // Lua C extension libraries (loaded at runtime via `require`) are linked with
    // unresolved Lua symbols and expect to find the Lua C API (`lua_*`, `luaL_*`)
    // exported by the host executable. Export the dynamic symbol table so dlopen'd
    // modules can resolve those symbols against this binary.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "linux" | "android" => {
            println!("cargo:rustc-link-arg-bins=-rdynamic");
        }
        "macos" | "ios" => {
            println!("cargo:rustc-link-arg-bins=-Wl,-export_dynamic");
        }
        _ => {}
    }
}
