---
name: moon-native-ext
description: >-
  Build, load, and distribute native Lua C/C++/Rust extension modules (shared
  libraries) for moon_rs: dlopen-based require, resolving the Lua C API from the
  host executable, Rust cdylib extensions (thrift), C/C++ extensions via the cc
  crate (crypt, gamecore), the cargo xtask workflow (git + local path mode), and
  Windows PE import-library generation. Use when creating a new .so/.dylib/.dll
  extension, debugging a require/symbol-export failure, registering an extension
  in extensions.toml, or touching xtask / crates/moon-app/build.rs.
---

# moon_rs: Native Lua Extension Libraries

moon_rs loads native extension modules at runtime via Lua's standard `require`,
mirroring the `clib/` convention from the original
[Moon](https://github.com/sniper00/moon).

An extension is a shared library exposing a `luaopen_<module>` entry point. It is
**not** linked against a Lua library; instead it resolves the Lua C API (`lua_*`,
`luaL_*`) against the symbols exported by the host `moon_rs` executable.

`require "foo"` loads `clib/foo.{so,dylib,dll}` and calls `luaopen_foo`.

---

## 1. How `require` loads an extension (host-side plumbing)

Three pieces make this work — all already wired up; you normally don't touch them:

1. **Lua's dlopen loader is enabled.** `crates/moon-base/build.rs` defines
   `LUA_USE_DLOPEN` + `LUA_USE_POSIX` on unix (and links `libdl` on Linux) so
   `loadlib.c` uses `dlopen`; Windows `luaconf.h` enables `LUA_DL_DLL`.

2. **The host exports the Lua C API.** `crates/moon-app/build.rs` passes
   `-rdynamic` (Linux) / `-Wl,-export_dynamic` (macOS) so the statically-linked
   Lua symbols land in the binary's dynamic symbol table. (`strip = true` in the
   release profile drops the *static* table but keeps the dynamic export trie,
   which is what the loader uses.) Windows needs an import library — see §6.

3. **`package.cpath` includes `clib/`.** On startup `crates/moon-app/src/main.rs`
   prepends `<root>/clib/?.<ext>` to `package.cpath` (`<root>` = the dir
   containing `lualib`). This propagates to every actor.

---

## 2. The two rules every extension must satisfy

Whatever the language, the produced library must:

**Rule A — leave `lua_*` / `luaL_*` undefined** so they bind to the host at load.
**Rule B — keep and export exactly your `luaopen_<name>`** so `dlsym` finds it.

The catch for Rule B: in a Rust `cdylib`, only Rust `#[unsafe(no_mangle)] pub`
symbols are exported automatically. A `luaopen_*` written in **C/C++** is (a) an
unreferenced member of the `cc` static archive, so the linker drops it, and (b)
demoted to a *local* symbol even if kept (`nm` shows lowercase `t`, not `T`) —
`dlsym` then fails. Force-loading the whole archive + an explicit export fixes
both.

Per-platform linker flags (emit from `build.rs` as `cargo:rustc-cdylib-link-arg`
unless noted). `<a>` = the `cc` archive name, `<n>` = your module name:

| Goal | macOS | Linux | Windows (MSVC) |
|------|-------|-------|----------------|
| **A.** resolve `lua_*` from host | `-Wl,-undefined,dynamic_lookup` | _(default; needs host `-rdynamic`)_ | link `moon_rs.lib` (see §6) |
| **B.** keep + export a **C/C++** entry | `-Wl,-force_load,$OUT_DIR/lib<a>.a` and `-Wl,-exported_symbol,_luaopen_<n>` | `-Wl,--whole-archive -l<a> -Wl,--no-whole-archive` and `-Wl,--export-dynamic-symbol=luaopen_<n>` | `/WHOLEARCHIVE:$OUT_DIR\<a>.lib` and `/EXPORT:luaopen_<n>` |
| **B.** export a **Rust** entry (`#[no_mangle] pub`) | _(automatic)_ | _(automatic)_ | `/EXPORT:luaopen_<n>` |

Note: on macOS `-exported_symbol` is an allowlist — it also *hides* the module's
internal symbols, which is what you want.

**Always verify** the result:

```bash
nm -gU clib/<n>.dylib | grep luaopen   # must show "T _luaopen_<n>" (defined+exported)
nm -u  clib/<n>.dylib | grep -E 'lua_|luaL_'   # lua_* must be undefined (host-bound)
```

---

## 3. Extension types

All three depend on `moon-base` with `default-features = false, features =
["lua55"]` (the `bundled` feature **off**, so no second copy of Lua is linked —
only the FFI declarations + the shared `Buffer`), and are **excluded from the
workspace** (so workspace feature-unification can't turn `bundled` back on).
`moon-base`'s `links = "lua55"` exposes the header dir to your `build.rs` as
`DEP_LUA55_INCLUDE`.

### 3a. Rust cdylib — `crates/moon-thrift`

A full extension written in Rust (thrift compact-protocol codec). The entry point
is Rust, so just export it; no archive force-loading needed.

```rust
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn luaopen_thrift(state: *mut lua_State) -> c_int { /* ... */ }
```

`build.rs` only needs Rule A (+ `/EXPORT` on Windows). To author another, mirror
this crate.

### 3b. C / C++ via the `cc` crate — `.extensions/crypt` (C), `gamecore` (C++)

The module lives in `.c` / `.cpp` files; `build.rs` compiles them with `cc` and
applies Rules A + B (the C/C++ row of the table). **`src/lib.rs` stays empty**
(just a doc comment) — do not add a `#[no_mangle]` anchor; the entry is exported
from `build.rs`.

```rust
// build.rs — single C entry point `luaopen_crypt`, archive "moon_crypt_c"
let lua_include = PathBuf::from(std::env::var("DEP_LUA55_INCLUDE").unwrap());
let out_dir = std::env::var("OUT_DIR").unwrap();
match target_os.as_str() {
    "macos" | "ios" => {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
        println!("cargo:rustc-cdylib-link-arg=-Wl,-force_load,{out_dir}/libmoon_crypt_c.a");
        println!("cargo:rustc-cdylib-link-arg=-Wl,-exported_symbol,_luaopen_crypt");
    }
    "windows" => { /* /WHOLEARCHIVE + moon_rs.lib + /EXPORT (see §6) */ }
    _ => { /* --whole-archive + --export-dynamic-symbol */ }
}
cc::Build::new()
    .include(&lua_include)
    .file("csrc/lua-crypt.c").file("csrc/lsha1.c")
    .std("c11")           // .cpp(true).std("c++23") for C++ (gamecore)
    .compile("moon_crypt_c");
```

The `[lib] name` **must equal** the `require` name (`name = "crypt"` →
`clib/crypt.<ext>` → `luaopen_crypt`). Vendor upstream `.c` byte-for-byte
(upstream `-Wsign-compare` warnings are harmless). C++ extensions (gamecore) are
identical except `.cpp(true)` and one `force_load`/`/WHOLEARCHIVE`/`--whole-archive`
covering the whole C++ archive, with one `-exported_symbol`/`/EXPORT`/`--export-dynamic-symbol`
per `luaopen_*` entry.

### 3c. Quick manual build (no Rust crate)

For a one-off pure-C module you can skip Cargo entirely and compile straight into
`clib/` (headers at `crates/moon-base/lua55`):

- **macOS:** `cc -O2 -fPIC -I<lua-headers> -bundle -undefined dynamic_lookup mod.c -o clib/mod.dylib`
- **Linux:** `cc -O2 -fPIC -I<lua-headers> -shared mod.c -o clib/mod.so`

(A minimal module just does `luaL_newlib(L, funcs); return 1;` in `luaopen_mod`.)

---

## 4. Distribution with `cargo xtask`

`xtask` fetches/builds/installs extensions into `clib/`. The registry is
`extensions.toml`; cloned git sources are cached under `.extensions/` (gitignored).

```bash
cargo xtask list             # show registry + lock status
cargo xtask build [name...]  # build + install into clib/<lib>.<ext>
cargo xtask update [name...] # resolve each git ref -> commit, write extensions.lock
cargo xtask clean [name...]  # remove cached checkouts under .extensions/
# flags: --offline (cache only), --force (re-fetch)
```

### Git extensions (live in their own repos)

```toml
moon_base_git = "https://github.com/.../moon_rs.git"   # the [patch] key (below)

[extensions.thrift]
git = "https://github.com/.../moon-thrift.git"
ref = "v1.0.0"      # tag/branch; pinned to a commit in extensions.lock by `update`
lib = "thrift"
```

`build` clones the locked commit and compiles it. **ABI safety:** the extension
declares `moon-base` as a *git* dependency on `moon_base_git`; `xtask` injects a
Cargo `[patch]` redirecting it to this repo's **local** `crates/moon-base`, so the
extension is built against the host's exact Lua FFI / `Buffer` ABI (a mismatch
would corrupt memory across the `.so`/`.dylib` boundary). It also sets
`MOON_HOST_TARGET_DIR` (where `moon_rs.lib` lives) for Windows builds.

### Local (path) extensions — vendored / in-development

Use a `path =` entry for an extension developed in-tree (no clone, no lock,
built in place). `git`/`ref` and `path` are mutually exclusive.

```toml
[extensions.crypt]
path = ".extensions/crypt"   # relative to repo root
lib  = "crypt"
```

- `cargo xtask build crypt` builds it directly; `update` is a no-op
  ("nothing to lock") and `list` shows it as `[local]`.
- It uses a **path** dependency on `moon-base`
  (`{ path = "../../crates/moon-base", default-features = false, features = ["lua55"] }`),
  so `xtask` skips the `moon_base_git` `[patch]` — the path dep already points at
  the host's `moon-base`, preserving the ABI guarantee.
- Keep the source under `.extensions/<name>/` to match where cloned sources live.
  It is git-ignored — treat it as a local working copy / soon-to-be-own-repo.

Then test from Lua, e.g.:

```bash
cargo run --release assets/test/test_crypt.lua   # or test_thrift.lua, test_clib.lua
```

---

## 5. End-to-end checklist for a new C/C++ extension

1. Create `.extensions/<n>/` with `csrc/*.c`, `Cargo.toml` (`[lib] name = "<n>"`,
   `crate-type = ["cdylib"]`, `moon-base` path dep with `default-features = false`),
   `build.rs` (Rules A + B), and an empty `src/lib.rs`.
2. Register a `path` entry in `extensions.toml` and confirm `cargo xtask list`.
3. `cargo xtask build <n>` → installs `clib/<n>.<ext>`.
4. Verify exports with `nm -gU` / `nm -u` (§2).
5. `cargo build --release` (host), then run the Lua integration test.

---

## 6. Windows: the import library

Windows PE DLLs must resolve every symbol at link time — there is no deferred
runtime resolution. So the host EXE must export the Lua symbols, and an import
library (`moon_rs.lib`) is generated for extensions to link against.

**1. Host EXE exports Lua symbols.** `moon-base/build.rs` defines
`LUA_BUILD_AS_DLL` when compiling `onelua.c` on Windows:

```rust
#[cfg(target_os = "windows")]
{
    builder.flag("/experimental:c11atomics");
    builder.define("LUA_BUILD_AS_DLL", None);
}
```

`onelua.c` defines both `LUA_CORE` and `LUA_LIB`, so in `luaconf.h` all `lua_*`
become `__declspec(dllexport)` and the MSVC linker puts them in the EXE export
table:

```c
#if defined(LUA_BUILD_AS_DLL)
  #if defined(LUA_CORE) || defined(LUA_LIB)
    #define LUA_API __declspec(dllexport)
  #endif
#endif
```

**2. Generate `moon_rs.lib`.** `moon-app/build.rs` writes a temporary `.def`
(`LIBRARY moon_rs.exe` + the exported `lua_*` names) and runs `lib.exe` to
produce the import library, then deletes the `.def`:

```rust
let lib_exe = compiler.path().parent().unwrap().join("lib.exe");
Command::new(&lib_exe)
    .arg(format!("/def:{}", def_path.display()))
    .arg("/machine:x64")
    .arg(format!("/out:{}", target_dir.join("moon_rs.lib").display()))
    .status();
```

The `.def` is **not** passed to the EXE linker (exports come from
`__declspec(dllexport)`); it is only input to `lib.exe`. The `LIBRARY moon_rs.exe`
line makes the import library reference the correct module.

**3. Extensions link `moon_rs.lib`** (path comes from `MOON_HOST_TARGET_DIR`, set
by `xtask`) and `/EXPORT` their entry points:

```rust
let host_target = std::env::var("MOON_HOST_TARGET_DIR").unwrap();
println!("cargo:rustc-link-search=native={host_target}");
println!("cargo:rustc-link-arg=moon_rs.lib");
println!("cargo:rustc-cdylib-link-arg=/EXPORT:luaopen_thrift");
```

### Verification

```powershell
dumpbin /EXPORTS target\release\moon_rs.exe | findstr lua_   # host exports Lua
dumpbin /IMPORTS clib\thrift.dll | findstr moon_rs           # DLL imports from moon_rs.exe
```

### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| LNK2019 unresolved symbols | `moon_rs.lib` doesn't exist | Build host first (`cargo build --release`), then `cargo xtask build` |
| DLL load failure | Import library module-name mismatch | Ensure the `LIBRARY` name in `.def` matches the EXE name |
| ACCESS_VIOLATION | Leftover `/FORCE:UNRESOLVED`, null symbol pointers | Remove all `/FORCE:UNRESOLVED`; use the import-library approach |
