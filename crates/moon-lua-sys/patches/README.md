# Lua hotfix patch

`hotfix.patch` contains the additions for closure redirection in five Lua
core files and the `lhotfix.c` include in `onelua.c`. Binding analysis lives in
`lualib/hotfix.lua`; native helpers live in `lua/lhotfix.c`.
The native module is `hotfix.core`, initialized by `luaopen_hotfix_core`.
Moon's preexisting shared-code-cache changes are separate.

The patch is applied in the checked-in sources. From the repository root, verify
that they still contain it:

```sh
git apply --reverse --check crates/moon-lua-sys/patches/hotfix.patch
```

After updating the vendor sources, check and apply the patch if it is absent:

```sh
git apply --check crates/moon-lua-sys/patches/hotfix.patch
git apply crates/moon-lua-sys/patches/hotfix.patch
```

Keep `lua/lhotfix.c` when refreshing the vendor sources. Review it against the
new internal headers and `lualib/hotfix.lua` against the instruction layouts and
semantics. The build compiles `lua/onelua.c`, which includes `lhotfix.c` after
the core, allowing the helpers to use its static internal symbols. Keep
`hotfix.patch` in sync when changing the core integration.

Run the workspace build/tests, focused hotfix suite and local Lua regressions:

```sh
cargo build --workspace --all-features --locked
cargo test --workspace --all-features --locked
target/debug/moon_rs assets/test/test_hotfix.lua
python3 assets/test/run_all.py --skip-build --only local
```

Check automatic additions from official bytecode, normal/tail calls,
cached intermediate generations, coroutine
frames, rollback and GC retention. These cases are included in the focused suite.
