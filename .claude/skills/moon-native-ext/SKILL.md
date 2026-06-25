---
name: moon-native-ext
description: >-
  Build, load, and distribute native Lua C extension modules (shared libraries)
  for moon_rs: dlopen-based require, symbol export from the host executable,
  Rust cdylib extensions, C++ extensions, the cargo xtask remote-extension
  workflow, and Windows PE import-library generation. Use when creating a new
  .so/.dylib/.dll extension, debugging a require failure, setting up a
  remote-extension repo, or touching crates/moon-thrift / crates/moon-app/build.rs.
---

# moon_rs: Native Lua C Extension Libraries

moon_rs can load native Lua C extension modules at runtime through Lua's
standard `require`, mirroring the `clib/` convention from the original
[Moon](https://github.com/sniper00/moon) `premake5.lua`.

An extension is an ordinary Lua/C module (a shared library exposing a
`luaopen_<module>` entry point). It is **not** linked against a Lua library;
instead it resolves the Lua C API (`lua_*`, `luaL_*`) against the symbols
exported by the host `moon_rs` executable.

## What makes this work

Three pieces are required, all already wired up in the build:

1. **dlopen-based dynamic loading is enabled in Lua.**
   `crates/moon-base/build.rs` defines `LUA_USE_DLOPEN` (plus `LUA_USE_POSIX`) on
   unix and links `libdl` on Linux, so `loadlib.c` uses `dlopen`. On Windows
   `luaconf.h` enables `LUA_DL_DLL` automatically.

2. **The host executable exports the Lua C API.**
   `crates/moon-app/build.rs` passes `-rdynamic` on Linux and
   `-Wl,-export_dynamic` on macOS, so the statically linked Lua symbols land in
   the binary's dynamic symbol table. (Note: `strip = true` in the release
   profile removes the *static* symbol table but keeps the dynamic export trie,
   which is what the loader uses.)

3. **`package.cpath` points at `clib/`.**
   On startup `crates/moon-app/src/main.rs` prepends
   `<root>/clib/?.<ext>` to `package.cpath` (`<ext>` is `so` / `dylib` / `dll`).
   `<root>` is the directory that contains `lualib` (the working dir, or the
   executable's directory). This value propagates to every actor.

So `require "foo"` will load `clib/foo.so` (or `.dylib` / `.dll`) and call
`luaopen_foo`.

## Writing and building an extension

See the example under `assets/example/native_ext/`:

- `example_clib.c` — a minimal module exposing `add(a, b)` and a `greeting` field.
- `build.sh` — compiles it into `<repo>/clib/` with the correct flags.

```bash
# Build the example into ./clib
bash assets/example/native_ext/build.sh

# Run the integration test
cargo run --release assets/test/test_clib.lua
```

The relevant compile flags per platform:

- **macOS:** `cc -O2 -fPIC -I<lua-headers> -bundle -undefined dynamic_lookup mod.c -o clib/mod.dylib`
- **Linux:** `cc -O2 -fPIC -I<lua-headers> -shared mod.c -o clib/mod.so`

Lua headers live at `crates/moon-base/lua55`.

## Rust cdylib extensions (thrift example)

`crates/moon-thrift` is a full extension written in Rust and built as a
`cdylib`. It is the thrift compact-protocol encoder/decoder, previously a module
inside `moon-runtime`, now shipped as a loadable `thrift.so` / `thrift.dylib`.

Key points of how it stays decoupled from the host's Lua:

- It depends on `moon-base` with `default-features = false, features = ["lua55"]`,
  so the `bundled` feature is **off** — only the Rust FFI declarations are used
  and the Lua C API symbols are left unresolved (bound to `moon_rs` at load).
- `moon-base/build.rs` only compiles/links the Lua C sources when the `bundled`
  feature is enabled (the default for the main workspace).
- Its `build.rs` handles each platform's linker:
  - **macOS:** `-Wl,-undefined,dynamic_lookup` — allows undefined symbols, resolved from the host at runtime
  - **Linux:** no extra flags needed, ELF allows it by default
  - **Windows:** links `moon_rs.lib` (import library) + `/EXPORT:luaopen_thrift`
- It exports the C entry point `luaopen_thrift` via `#[unsafe(no_mangle)]`.
- It uses the shared `Buffer` from `moon_base::buffer`. `moon-base` is the
  dependency-light foundation crate (Lua FFI bindings + helper macros + Buffer),
  so the extension pulls in no heavyweight runtime dependencies.

To author another Rust extension, mirror this crate: a `cdylib` depending on
`moon-base` with `default-features = false`, excluded from the workspace, with a
`#[unsafe(no_mangle)] luaopen_<name>` entry point.

## C++ extensions (gamecore example)

`gamecore` includes C++ code (sol2 library). Differences from pure Rust extensions:

- C++ is compiled via the `cc` crate; Lua header paths are provided by `DEP_LUA55_INCLUDE`
- The `luaopen_*` C++ entry points are not referenced by Rust code, requiring:
  - **Windows:** `/WHOLEARCHIVE:gamecore_cpp.lib` to force-link the entire C++ archive
  - **macOS:** `-Wl,-force_load,libgamecore_cpp.a`
  - **Linux:** `-Wl,--whole-archive -lgamecore_cpp -Wl,--no-whole-archive`
- C++ entry points are unreferenced symbols that would be stripped; they need explicit export:
  - **Windows:** `/EXPORT:luaopen_gamecore_aoi` etc.
  - **macOS:** `-Wl,-exported_symbol,_luaopen_gamecore_aoi` etc.
  - **Linux:** `-Wl,--export-dynamic-symbol=luaopen_gamecore_aoi` etc.

```rust
// Typical gamecore build.rs structure
let lua_include = PathBuf::from(std::env::var("DEP_LUA55_INCLUDE").unwrap());
let lua_entry_points = &["luaopen_gamecore_aoi", "luaopen_gamecore_uuid", /*...*/];

// 1. Platform linker flags
if macos {
    println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
    println!("cargo:rustc-cdylib-link-arg=-Wl,-force_load,...");
    for sym in lua_entry_points {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-exported_symbol,_{sym}");
    }
} else if windows {
    println!("cargo:rustc-cdylib-link-arg=/WHOLEARCHIVE:...");
    println!("cargo:rustc-link-arg=moon_rs.lib");
    for sym in lua_entry_points {
        println!("cargo:rustc-cdylib-link-arg=/EXPORT:{sym}");
    }
} else { // linux
    // --whole-archive + --export-dynamic-symbol
}

// 2. Compile C++ modules
cc::Build::new()
    .cpp(true)
    .include(&lua_include)
    .file("lua_aoi.cpp").file("lua_uuid.cpp") // ...
    .compile("gamecore_cpp");
```

## Remote extensions via `cargo xtask`

Extensions are intended to live in their **own GitHub repos**, not vendored here.
The `xtask` task runner fetches, builds, and installs them:

```bash
cargo xtask update        # resolve each ref -> commit, writing extensions.lock
cargo xtask build         # clone the locked commit, build, install into clib/
cargo xtask list          # show the registry + lock status
cargo xtask clean         # remove cached checkouts under .extensions/
# build a single one:   cargo xtask build thrift
# flags: --offline (cache only), --force (re-fetch)
```

The registry lives in `extensions.toml` (URLs there are placeholders — fill in the
real repos), and resolved commits are pinned in `extensions.lock` for
reproducibility. Cloned sources are cached under `.extensions/` (gitignored).

How an external extension gets `moon-base`: it declares `moon-base` as a git
dependency on this repo (`moon_base_git` in `extensions.toml`), and `xtask`
injects a Cargo `[patch]` at build time redirecting it to the **local**
`crates/moon-base`. This guarantees every extension is compiled against the host's
exact Lua 5.5 FFI / `Buffer` ABI (a cross-allocator/ABI mismatch would otherwise
corrupt memory across the `.so`/`.dylib` boundary).

Then run and use from Lua:

```bash
cargo run --release assets/test/test_thrift.lua
```

From Lua:

```lua
local thrift = require "thrift"
thrift.load({ enums = {}, structs = { --[[ ... ]] } })
local bytes = thrift.encode("Person", { name = "alice", age = 30 })
local obj   = thrift.decode("Person", bytes)
```

## Windows

Windows PE DLLs must resolve all symbols at link time — there is no macOS/Linux
"deferred runtime resolution" mechanism. Therefore the host EXE must export Lua
symbols, and an import library must be generated for extensions to link against.

### How it works

Three parts cooperate:

**1. Host EXE exports Lua symbols**

`moon-base/build.rs` defines `LUA_BUILD_AS_DLL` when compiling `onelua.c` on Windows:

```rust
#[cfg(target_os = "windows")]
{
    builder.flag("/experimental:c11atomics");
    builder.define("LUA_BUILD_AS_DLL", None);
}
```

In `luaconf.h`:
```c
#if defined(LUA_BUILD_AS_DLL)
  #if defined(LUA_CORE) || defined(LUA_LIB)
    #define LUA_API __declspec(dllexport)  // export
  #endif
#endif
```

`onelua.c` internally defines both `LUA_CORE` and `LUA_LIB`; combined with
`LUA_BUILD_AS_DLL`, all `lua_*` functions are marked `__declspec(dllexport)`, and
the MSVC linker places them in the EXE's export table.

**2. Generate import library moon_rs.lib**

`moon-app/build.rs` creates a temporary `.def` file and invokes `lib.exe` to
generate the import library, deleting the `.def` afterwards:

```rust
"windows" => {
    let mut def = String::from("LIBRARY moon_rs.exe\nEXPORTS\n");
    for sym in lua_symbols { def.push_str(sym); def.push('\n'); }
    // write temp .def ...
    // locate lib.exe via the cc crate
    let lib_exe = compiler.path().parent().unwrap().join("lib.exe");
    Command::new(&lib_exe)
        .arg(format!("/def:{}", def_path.display()))
        .arg("/machine:x64")
        .arg(format!("/out:{}", target_dir.join("moon_rs.lib").display()))
        .status();
    // delete temp .def
    std::fs::remove_file(&def_path);
}
```

The `.def` file is **not** passed to the linker — EXE exports are handled by
`__declspec(dllexport)`. The `.def` is only input to `lib.exe`. The
`LIBRARY moon_rs.exe` line ensures the import library references the correct
module name.

**3. Extensions link against the import library**

Extension `build.rs` uses `moon_rs.lib` to resolve all Lua symbols:

```rust
"windows" => {
    let host_target = std::env::var("MOON_HOST_TARGET_DIR").unwrap();
    println!("cargo:rustc-link-search=native={}", host_target);
    println!("cargo:rustc-link-arg=moon_rs.lib");
    // MSVC requires explicit export of entry points
    println!("cargo:rustc-cdylib-link-arg=/EXPORT:luaopen_thrift");
}
```

`xtask` passes the `MOON_HOST_TARGET_DIR` environment variable when invoking
`cargo build`, pointing to the host `target/release/`.

### Verification

```powershell
# Check EXE exports
dumpbin /EXPORTS target\release\moon_rs.exe | findstr lua_

# Check DLL imports
dumpbin /IMPORTS clib\thrift.dll | findstr moon_rs
# Should output "moon_rs.exe"
```

### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| LNK2019 unresolved symbols | `moon_rs.lib` doesn't exist | Build host first (`cargo build --release`), then `cargo xtask build` |
| DLL load failure | Import library module name mismatch | Check that the `LIBRARY` name in `.def` matches the EXE name |
| ACCESS_VIOLATION | Leftover `/FORCE:UNRESOLVED`, symbol pointers are null | Remove all `/FORCE:UNRESOLVED`, use the import-library approach |
