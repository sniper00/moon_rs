# laux Usage Guide

This guide describes the recommended use of the current moon_rs `laux` API. Its goal is to
preserve the performance model of the Lua C API while using Rust lifetimes to constrain stack
borrows, without adding unnecessary FFI calls, allocations, copies, or stack guards to hot paths.

`docs/laux-refactor.md` and `docs/laux-context-cleanup.md` record the migration history. When
writing or reviewing new code, treat this guide and the current implementation in
`crates/moon-base/src/laux.rs` as the source of truth.

## Core Principle

> Put safety and lifetime constraints at API boundaries and in documentation. Keep successful hot
> paths to one FFI conversion, with no allocation, copy, or dynamic guard. Leave type diagnostics,
> formatting, and error construction on cold paths.

In practice:

1. Create exactly one `LuaStack` per Lua callback. Do not call `from_raw` or `with_context` again
   inside that callback.
2. Use `LuaStackValue` for short-lived reads. Convert data to an owned value before stack mutation,
   an asynchronous boundary, or the end of the callback.
3. Consume every table cursor entry in its current loop iteration. Never retain or collect entries.
4. When unsafe code removes overhead, document the stack stability, value provenance, aliasing, and
   reentrancy requirements at the call site.
5. An optimization must pass semantic tests and benchmarks. Missing or invalid output is not a
   performance improvement.

## Choosing an API

| Requirement | Recommended API | Notes |
| --- | --- | --- |
| Fallible Lua function | `lreg_try!` | The implementation returns `Result<c_int, String>`; the wrapper raises the Lua error at the ABI boundary |
| Infallible Lua function | `lreg!` | The implementation returns `c_int` directly |
| Read an owned integer, float, or other scalar | `lua.get::<T>(index)?` | A successful numeric conversion uses one FFI call; `LuaTypeError` converts to callback `String` errors automatically |
| Read a typed optional argument | `lua.get::<Option<T>>(index)?` | `none` and `nil` become `None`; a present value of the wrong type is an error |
| Read a lenient optional/defaulted argument | `lua.opt::<T>(index)` | Returns `None` for absence and conversion failure; use only when the Lua API intentionally defaults invalid values |
| Read an optional Lua-truthy flag | `lua.opt_truthy(index)` | `none`/`nil` become `None`; `false` stays false and every other present value is true |
| Inspect a temporary stack value | `lua.value(index)` | Returns a borrowed `LuaStackValue` and caches its current type |
| Read an owned string or byte sequence | `lua.get::<String/Vec<u8>>(index)` | Copies the data; use it across stack mutations or async work |
| Read a rooted Lua string synchronously | `value.as_bytes()` / `as_str()` | The Lua stack must remain unchanged during the borrow |
| Borrow bytes while only appending above the source | `value_bytes_append_only` | Unsafe; requires the full append-only contract |
| Traverse a table | `lua.table_cursor(index)` | Yields lazy entries and inspects only the requested key/value |
| Traverse a strict contiguous array | `lua.array_cursor(index)` | Mixed, sparse, and non-table values produce an empty cursor |
| Traverse an array with a proven length | `lua.array_cursor_len(index, len)` | The caller is responsible for proving the length semantics |
| Read Lua's raw length boundary | `lua.raw_len(index)` | Equivalent to `lua_rawlen`; it does not prove that a table is a strict array |
| Access a trusted userdata receiver | `userdata_ptr_unchecked` | Unsafe; skips Lua type, metatable, and null checks |
| Build a result table | `LuaTable` | Convenient for result construction, but normal Lua stack ownership still applies |

## Writing Lua Callbacks

Prefer business functions that receive `&mut LuaStack<'_>`, and let the registration macro own the
raw ABI adaptation:

```rust
use moon_base::laux::{self, LuaStack};
use moon_base::{cstr, lreg_try, lreg_null};
use std::ffi::c_int;

fn add(lua: &mut LuaStack<'_>) -> Result<c_int, String> {
    let lhs = lua.get::<i64>(1)?;
    let rhs = lua.get::<i64>(2)?;
    lua.push(lhs + rhs);
    Ok(1)
}

static LIB: &[moon_base::laux::LuaReg] = &[
    lreg_try!("add", add),
    lreg_null!(),
];
```

`lreg_try!` calls `lua_error` only after the `LuaStack` borrow has ended. This prevents
an active Rust stack borrow from crossing Lua's error boundary. Business functions should return
`Err` instead of calling `ffi::lua_error` or a longjmp-capable `luaL_check*` function from deep inside
a Rust call stack.

Never wrap the same state in a second context inside a callback:

```rust
// Wrong: creates a second mutable LuaStack for the same state.
let nested = unsafe { LuaStack::from_raw(lua.state()) };
```

`LuaStack::from_raw` is for C ABI entry points only. Its caller must prove that the Lua state stays
valid for the entire borrow and that no concurrent or aliased access exists.

## Reading Arguments and Values

### Owned values

Use owned values when data must survive stack mutation, an async boundary, or the current callback:

```rust
let id = lua.get::<u64>(1)?;
let name = lua.get::<String>(2)?;
let payload = lua.get::<Vec<u8>>(3)?;
let timeout = lua.get::<Option<u64>>(4)?;
```

`LuaTypeError` implements `From<LuaTypeError> for String`, so callbacks returning
`Result<_, String>` can use `?` directly. This keeps strict conversion and the original type
diagnostic without repeating `map_err(|error| error.to_string())`.

Keep an explicit `map_err` when the error needs operation-specific context:

```rust
let url = lua
    .get::<String>(1)
    .map_err(|error| format!("redis.connect: {error}"))?;
```

Do not remove this context merely to shorten the expression. For errors that do not come from
`LuaTypeError`, use the conversion required by the underlying API as usual.

These optional forms have deliberately different contracts:

```rust
// Typed optional: nil is allowed, but "invalid" returns LuaTypeError.
let limit = lua
    .get::<Option<u64>>(1)?;

// Lenient default: nil and conversion failure both select the default.
let limit = lua.opt::<u64>(1).unwrap_or(DEFAULT_LIMIT);

// Lua flag compatibility: false is false; 0, "", tables, and userdata are true.
let enabled = lua.opt_truthy(2).unwrap_or(true);
```

Use `get::<Option<T>>` for new typed APIs. Reserve `opt<T>` and `opt_truthy` for APIs whose documented
or historical behavior intentionally uses a default or Lua truthiness. Do not change
`FromLua<bool>` to use truthiness: required booleans remain strict.

Successful integer and floating-point reads use one `lua_tointegerx` or `lua_tonumberx` call for
conversion and validation. Only the failure path queries the actual type and builds an error. Do not
manually call `lua_type` first when the conversion function already reports success or failure.

`String` requires valid UTF-8. `Vec<u8>` accepts arbitrary bytes from a Lua string. Lua strings may
contain `\0`, so protocol, serialization, and network code should generally operate on bytes.

### Borrowed values

`lua.value(index)` converts a relative index to an absolute index and caches the current type:

```rust
let value = lua.value(1);
match value.kind() {
    LuaType::String => consume(value.as_bytes().unwrap()),
    LuaType::Integer => consume_integer(value.as_integer().unwrap()),
    _ => return Err(format!("string or integer expected, got {}", value.name())),
}
```

The value is valid only while its stack slot remains rooted and unchanged. It becomes invalid after:

- popping, replacing, removing, or reordering its stack slot;
- advancing the cursor that owns the slot;
- overwriting the same index, which also makes the cached `kind` stale;
- leaving the current callback or entering asynchronous work.

Do not repeatedly call `lua.value(index)` for the same read. If both the type and content are needed,
keep one local `LuaStackValue` and consume it immediately.

## Table Traversal

### Read-only traversal

The `LuaTableCursor` iterator yields lazy `LuaTableEntry` values. Advancing the cursor runs
`lua_next`; the key or value type is queried only when `entry.key()` or `entry.value()` is called:

```rust
for entry in lua.table_cursor(1) {
    let key = entry.key();

    // Skip without querying the value type when this key is not relevant.
    if key.kind() != LuaType::String {
        continue;
    }

    let value = entry.value();
    consume_pair(key.as_bytes().unwrap(), value);
}
```

Call `key()` and `value()` at most once per entry. Extract an index, type, or scalar into a local
value, then finish processing it before the loop iteration ends.

### Mandatory iterator contract

To keep a `for` loop as compact and efficient as the Lua FFI loop, this iterator relies on a stack
slot lifetime contract that Rust's `Iterator` trait cannot fully express. Every entry and every value
borrowed from it must be consumed within the current loop iteration.

Do not use operations that can retain `LuaTableEntry` values, including:

- `collect`, `partition`, or `unzip`;
- `peekable`, `zip`, or `cycle`;
- collecting the result of `map` or `filter`;
- returning an entry, key, value, or borrowed byte slice from the loop body;
- storing an entry in a container, closure, future, or any deferred operation.

Incorrect:

```rust
// Forbidden: after the cursor advances or drops, these indices no longer identify the old entries.
let entries: Vec<_> = lua.table_cursor(1).collect();
```

When the surrounding code cannot clearly guarantee this contract, use the cursor's inherent lending
`next()` method:

```rust
let mut cursor = lua.table_cursor(1);
while let Some((key, value)) = cursor.next() {
    consume_pair(key, value);
}
```

This form binds each item's lifetime to the current `&mut cursor` borrow. The compiler prevents the
key or value from being used after the next `next()` call.

### Recursion or stack mutation during traversal

When a helper needs temporary Lua stack space, end all entry borrows first and then obtain the
context through `entry.lua_mut()`:

```rust
for mut entry in lua.table_cursor(index) {
    let (key_index, key_kind) = {
        let key = entry.key();
        (key.index(), key.kind())
    };
    let (value_index, value_kind) = {
        let value = entry.value();
        (value.index(), value.kind())
    };

    unsafe {
        let lua = entry.lua_mut();
        encode_value(lua, key_index, key_kind, out)?;
        encode_value(lua, value_index, value_kind, out)?;
    }
}
```

`lua_mut()` has no stack guard. The caller must prove all of the following:

1. No `LuaStackValue` borrowed from this entry will be used again.
2. The key and value slots are not popped, replaced, or moved.
3. Every helper restores the stack height it observed on entry.
4. Before cursor advancement or drop, the stack layout is still `[... table, key, value]`.

State this contract in a nearby `// SAFETY:` comment. General helpers should be naturally
stack-neutral so that a hot loop does not need a `lua_gettop`/`lua_settop` guard on every iteration.

### What a cursor cleans up

Dropping `LuaStack` does not restore the stack. `LuaTableCursor` removes only the key/value slots
owned by its `lua_next` frame and, for `table_field_cursor`, the temporary field table it opened. It
does not repair arbitrary stack changes left by its caller.

Likewise, `LuaArrayCursor` pops only the current value pushed by `lua_rawgeti`. Cursor drop is not a
general transaction or stack rollback mechanism.

## Arrays and Mixed Tables

These concepts are not interchangeable:

| API | Semantics |
| --- | --- |
| `raw_len(index)` | Lua's raw length boundary; the table may still contain hash keys |
| `array_len(index)` | Returns `len` only when the keys are exactly the strict contiguous array `1..=len` |
| `array_cursor_len(index, len)` | Executes `lua_rawgeti(1..=len)` using a length proven by the caller |

For example, `{1, 2, 3, name = "moon"}` may have a `raw_len` of 3, while `array_len` returns 0. This
is a semantic distinction, not a performance difference.

When encoding a mixed table, the array writer and the hash traversal's array-key skip logic must use
the same length:

```rust
let array_size = lua.raw_len(index);

for i in 1..=array_size {
    // lua_rawgeti(index, i), encode the value, then pop it.
}

for entry in lua.table_cursor(index) {
    // Skip only integer keys 1..=array_size that were already encoded above.
}
```

Do not use `raw_len` for the table header and hash-key skip logic while using the strict
`array_cursor()` to encode array values. A mixed table makes the strict cursor empty, so its array
values are omitted and then skipped again by the hash traversal.

This bug once made the old `seri-mixed` benchmark appear faster. The old implementation obtained
`rawlen = 32`, but its strict array iterator returned 0, so it wrote none of the 32 array values. The
hash traversal then skipped integer keys `1..=32`. Producing less output was faster, but the payload
could not be decoded correctly. The current
`mixed_table_round_trip_preserves_all_array_and_hash_entries` test uses the benchmark shape of 32
array entries and 8 hash entries in a complete round trip to prevent a recurrence.

## Userdata

`LuaStackValue::as_userdata<T>()` checks only that the Lua value is full userdata and that its pointer
is non-null. It does not check the metatable and cannot prove that the storage actually contains a
`T`. The caller must still establish the concrete type's provenance.

For externally controlled arguments, perform the metatable or module protocol checks. Use the
unchecked path only when the binding layer guarantees the receiver's origin and the hot path merits
removing repeated checks:

```rust
/// # Safety
/// `index` must be a ZSet receiver created by this module and bound to its target metatable.
#[inline(always)]
unsafe fn get_zset(lua: &LuaStack<'_>, index: i32) -> NonNull<ZSet> {
    unsafe { lua.userdata_ptr_unchecked(index) }
}
```

`userdata_ptr_unchecked` performs no Lua type, metatable, or null check. The caller must also avoid
creating overlapping mutable references to the same `T`. A value being "normally used as a Lua
method" is not enough proof: if callers can extract the function and pass a forged `self`, the
binding layer or function must enforce receiver provenance.

## Zero-Copy Strings and Bytes

Use a normal borrow when the stack does not change:

```rust
let bytes = lua
    .value(1)
    .as_bytes()
    .ok_or_else(|| "string expected".to_string())?;
consume(bytes);
```

If processing only appends results above the source slot, an append-only borrow can span those
pushes:

```rust
// SAFETY: argument 1 remains rooted and unchanged throughout parsing. Downstream code only appends
// results above it and never pops, replaces, removes, or reorders it. No async suspension or Lua
// state switch occurs.
let bytes = unsafe {
    lua.value_bytes_append_only(1)
        .ok_or_else(|| "string expected".to_string())?
};
parse_and_push_results(lua, bytes)?;
```

Before using this API, prove each condition:

1. The source index is stable and remains rooted for the entire borrow.
2. All downstream functions only append above the source slot.
3. No Lua operation can replace or collect the source value.
4. The borrow never crosses `async`/`await`, a thread, a coroutine switch, or another Lua callback.
5. The complete call chain does not quietly call `.to_vec()` or `String::from`; otherwise the path is
   not actually zero-copy.

When any condition cannot be proved, copy into a `Vec<u8>` or `String`. Asynchronous APIs must own
their data.

`Buffer::prepare` returns uninitialized spare capacity. Rust callers must
initialize bytes before calling the unsafe `commit`; asynchronous readers use
`spare_capacity_mut` with Tokio's `ReadBuf`. Initialize reserved headers with
`write_slice(&[0; N])`. `unsafe_write` requires spare capacity, and mutable access
to the underlying Vec must preserve the read position even if a write panics.
For textual output, Buffer implements `fmt::Write` so `write!` and `write_chars`
can format directly into its storage.
`write_chars` returns `fmt::Result`; propagate formatting errors to the callback's
`Result` rather than unwrapping them at the Lua boundary.

## Stack Discipline and Raw FFI

`LuaStack` is a borrowed view, not a stack guard. Every callback must still follow the Lua C API
return convention: leave its return values at the top and return the correct count.

When calling raw FFI:

- convert a relative index to `abs_index` before stack changes if it must be used later;
- make helpers stack-neutral, or document their exact net stack change;
- balance temporary pushes on every normal `Result` return path;
- do not unconditionally call `gettop/settop` in every hot-loop iteration unless the stack risk
  cannot be proved away;
- never let a Rust reference cross a Lua error/longjmp boundary;
- keep unsafe blocks small and describe the Lua stack facts, rather than writing only "FFI call".

A useful `// SAFETY:` comment answers these questions: which slots remain unchanged, which stack
operations are allowed, when the original stack height is restored, and where a userdata or pointer
gets its concrete type.

## Performance Experience

### Keep successful hot paths short

- Use one conversion function with a success flag for numeric reads. Query type names and allocate
  error strings only on failure.
- Table entries are lazy. Inspect the key and `continue` early; do not query a value that will be
  skipped.
- Call each entry's `key()` and `value()` at most once to avoid duplicate `lua_type` calls.
- Use `&[u8]` and byte keys for byte protocols. Avoid unnecessary UTF-8 validation and `String`
  allocation.
- Fixed overhead is amplified in small functions. Benchmark getters, userdata receivers, and short
  tables independently.
- Use guards for actual risks. When a clear stack contract proves correctness, do not add dynamic
  restoration work to every call.

The goal of the lazy table cursor is not to assume that an abstraction is automatically zero-cost.
Its expanded work should match direct FFI: one `lua_next`, type queries only when requested, no
allocation, no copy, and no retained entry. In the measured workloads it can match or outperform
direct FFI, but that conclusion must be verified for each workload rather than inferred from the API
shape.

### Performance review questions

Review a `laux` hot path in this order:

1. How many Lua C API calls run on each successful operation?
2. Does it allocate a `String`, `Vec`, `Cow::Owned`, or temporary collection?
3. Does it repeat a type query, absolute-index calculation, or metatable check?
4. Does a cursor inspect a value that will ultimately be skipped?
5. Does a guard query and restore the stack top on every iteration?
6. Is zero-copy behavior preserved across the entire call chain?
7. Do the old and new implementations encode identical bytes, return identical Lua values, and
   handle identical errors?

## Tests and Benchmarks

Verify semantics before measuring time:

```bash
# A mixed table must preserve all 32 array entries and 8 hash entries.
cargo test -p moon-runtime mixed_table_round_trip_preserves_all_array_and_hash_entries

# laux and all callers.
cargo test --workspace

# Public API documentation and links.
cargo doc --workspace --no-deps

# In-process microbenchmark comparing the lazy cursor with direct FFI.
cargo test -p moon-runtime --release \
  benchmark_lazy_hash_cursor_against_ffi -- --ignored --nocapture

# End-to-end benchmark for small Lua API operations.
cargo run --release -- assets/benchmark/benchmark_lua_api.lua
```

A valid benchmark comparison requires:

- comparable input, iteration count, release configuration, and machine load;
- identical semantics, especially serialized bytes and round-trip output;
- both small and representative inputs, so fixed overhead can be separated from throughput;
- repeated runs with observed variance instead of a conclusion from one sample;
- counting FFI calls, allocations, and copies before attempting more complex unsafe optimization.

## Pre-Commit Checklist

- [ ] The callback uses `lreg!` or `lreg_try!` and does not create a second
      `LuaStack`.
- [ ] Every stack borrow ends before its stack slot changes.
- [ ] Optional values intentionally choose typed `Option<T>`, lenient `opt<T>`, or `opt_truthy`.
- [ ] No table entry is collected, retained, returned, or moved into asynchronous code.
- [ ] Helpers called through `entry.lua_mut()` or `cursor.lua_mut()` are strictly stack-neutral.
- [ ] `raw_len`, `array_len`, and caller-provided length semantics are not mixed.
- [ ] The concrete userdata type, provenance, and aliasing rules are auditable.
- [ ] Every append-only byte borrow documents the no-pop, no-replace, no-reorder, and no-async
      conditions.
- [ ] Errors reach the ABI boundary through `Result`; no Rust borrow crosses a Lua error.
- [ ] Return values, encoded bytes, or round-trip semantics are verified before performance is
      measured.
- [ ] `cargo test --workspace` and `cargo doc --workspace --no-deps` pass.
