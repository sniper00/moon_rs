use super::*;
use moon_base::laux::LuaGlobalState;
use std::{hint::black_box, ptr::NonNull, time::Instant};

#[test]
fn empty_pack_returns_a_buffer_and_preserves_zero_values() {
    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).unwrap();
    let _owner = LuaGlobalState::new(state);
    let mut lua = unsafe { LuaStack::from_raw(state) };
    assert_eq!(pack(&mut lua).unwrap(), 1);
    let ptr = unsafe { ffi::lua_touserdata(state.as_ptr(), -1) } as *mut Buffer;
    let buf = unsafe { Box::from_raw(ptr) };
    assert!(buf.as_slice().is_empty());
    assert!(matches!(
        unsafe { decode_bytes(state, buf.as_slice(), 1) },
        Ok(0)
    ));
    unsafe {
        ffi::lua_settop(state.as_ptr(), 0);
    }
    assert_eq!(pack_string(&mut lua).unwrap(), 1);
    assert_eq!(laux::lua_type(state, -1), LuaType::String);
    assert_eq!(unsafe { ffi::lua_rawlen(state.as_ptr(), -1) }, 0);
    unsafe {
        ffi::lua_settop(state.as_ptr(), 0);
        ffi::lua_pushnil(state.as_ptr());
    }
    assert_eq!(pack_string(&mut lua).unwrap(), 1);
    assert!(unsafe { ffi::lua_rawlen(state.as_ptr(), -1) } > 0);
}

fn push_benchmark_table(state: LuaState, array_size: usize, hash_size: usize) {
    unsafe {
        ffi::lua_createtable(state.as_ptr(), array_size as i32, hash_size as i32);
        for i in 1..=array_size {
            ffi::lua_pushinteger(state.as_ptr(), i as ffi::lua_Integer);
            ffi::lua_rawseti(state.as_ptr(), -2, i as ffi::lua_Integer);
        }
        for i in 1..=hash_size {
            let key = format!("field{i}");
            ffi::lua_pushlstring(state.as_ptr(), key.as_ptr().cast(), key.len());
            ffi::lua_pushinteger(state.as_ptr(), i as ffi::lua_Integer);
            ffi::lua_rawset(state.as_ptr(), -3);
        }
    }
}

fn run_hash_writer(
    lua: &mut LuaStack<'_>,
    array_size: usize,
    iterations: usize,
    lazy: bool,
    buf: &mut Vec<u8>,
) {
    for _ in 0..iterations {
        buf.clear();
        if lazy {
            write_table_hash(lua, 1, buf, 0, array_size).expect("lazy hash writer");
        } else {
            write_table_hash_ffi(lua, 1, buf, 0, array_size).expect("FFI hash writer");
        }
        black_box(buf.len());
    }
}

#[test]
#[ignore = "manual release benchmark"]
fn benchmark_lazy_hash_cursor_against_ffi() {
    const ITERATIONS: usize = 1_000_000;
    const ROUNDS: usize = 6;

    for (name, array_size, hash_size) in
        [("array32", 32, 0), ("hash16", 0, 16), ("mixed32_8", 32, 8)]
    {
        let state = NonNull::new(unsafe { ffi::luaL_newstate() }).expect("Lua state allocation");
        let _owner = LuaGlobalState::new(state);
        push_benchmark_table(state, array_size, hash_size);
        let mut lua = unsafe { LuaStack::from_raw(state) };
        let mut ffi_buf = Vec::with_capacity(512);
        let mut lazy_buf = Vec::with_capacity(512);

        run_hash_writer(&mut lua, array_size, 10_000, false, &mut ffi_buf);
        run_hash_writer(&mut lua, array_size, 10_000, true, &mut lazy_buf);
        assert_eq!(lazy_buf, ffi_buf);

        let mut ffi_elapsed = 0.0;
        let mut lazy_elapsed = 0.0;
        for round in 0..ROUNDS {
            if round % 2 == 0 {
                let started = Instant::now();
                run_hash_writer(&mut lua, array_size, ITERATIONS, false, &mut ffi_buf);
                ffi_elapsed += started.elapsed().as_secs_f64();

                let started = Instant::now();
                run_hash_writer(&mut lua, array_size, ITERATIONS, true, &mut lazy_buf);
                lazy_elapsed += started.elapsed().as_secs_f64();
            } else {
                let started = Instant::now();
                run_hash_writer(&mut lua, array_size, ITERATIONS, true, &mut lazy_buf);
                lazy_elapsed += started.elapsed().as_secs_f64();

                let started = Instant::now();
                run_hash_writer(&mut lua, array_size, ITERATIONS, false, &mut ffi_buf);
                ffi_elapsed += started.elapsed().as_secs_f64();
            }
        }

        assert_eq!(lazy_buf, ffi_buf);
        let operations = (ITERATIONS * ROUNDS) as f64;
        eprintln!(
            "{name}: ffi={:.2} ns lazy={:.2} ns delta={:+.2}%",
            ffi_elapsed / operations * 1e9,
            lazy_elapsed / operations * 1e9,
            (lazy_elapsed / ffi_elapsed - 1.0) * 100.0,
        );
    }
}

#[test]
fn mixed_table_round_trip_preserves_all_array_and_hash_entries() {
    const ARRAY_SIZE: usize = 32;
    const HASH_SIZE: usize = 8;

    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).expect("Lua state allocation");
    let _owner = LuaGlobalState::new(state);
    unsafe {
        ffi::lua_createtable(state.as_ptr(), ARRAY_SIZE as i32, HASH_SIZE as i32);
        for i in 1..=ARRAY_SIZE {
            ffi::lua_pushinteger(state.as_ptr(), (i as ffi::lua_Integer) * 10);
            ffi::lua_rawseti(state.as_ptr(), -2, i as ffi::lua_Integer);
        }
        for i in 1..=HASH_SIZE {
            let key = format!("field{i}");
            ffi::lua_pushlstring(state.as_ptr(), key.as_ptr().cast(), key.len());
            ffi::lua_pushinteger(state.as_ptr(), 1_000 + i as ffi::lua_Integer);
            ffi::lua_rawset(state.as_ptr(), -3);
        }
    }

    let encoded = {
        let mut lua = unsafe { LuaStack::from_raw(state) };
        let mut encoded = Vec::new();
        pack_one(&mut lua, 1, &mut encoded, 0).expect("pack mixed table");
        encoded
    };

    laux::lua_settop(state, 0);
    let count = match unsafe { decode_bytes(state, &encoded, 0) } {
        Ok(count) => count,
        Err(error) => panic!("unpack mixed table: {error}"),
    };
    assert_eq!(count, 1);
    assert_eq!(unsafe { ffi::lua_gettop(state.as_ptr()) }, 1);
    assert_eq!(
        unsafe { ffi::lua_rawlen(state.as_ptr(), 1) },
        ARRAY_SIZE,
        "the encoded table header and array payload must agree"
    );

    for i in 1..=ARRAY_SIZE {
        unsafe {
            ffi::lua_rawgeti(state.as_ptr(), 1, i as ffi::lua_Integer);
            assert_eq!(
                ffi::lua_tointeger(state.as_ptr(), -1),
                (i as ffi::lua_Integer) * 10,
                "array entry {i} was not preserved"
            );
            ffi::lua_pop(state.as_ptr(), 1);
        }
    }

    for i in 1..=HASH_SIZE {
        let key = format!("field{i}");
        unsafe {
            ffi::lua_pushlstring(state.as_ptr(), key.as_ptr().cast(), key.len());
            ffi::lua_rawget(state.as_ptr(), 1);
            assert_eq!(
                ffi::lua_tointeger(state.as_ptr(), -1),
                1_000 + i as ffi::lua_Integer,
                "hash entry {key} was not preserved"
            );
            ffi::lua_pop(state.as_ptr(), 1);
        }
    }

    assert_eq!(unsafe { ffi::lua_gettop(state.as_ptr()) }, 1);
}

fn write_table_hash_ffi(
    lua: &mut LuaStack<'_>,
    index: i32,
    buf: &mut Vec<u8>,
    depth: i32,
    array_size: usize,
) -> Result<(), String> {
    unsafe {
        ffi::lua_pushnil(lua.as_ptr());
        while ffi::lua_next(lua.as_ptr(), index) != 0 {
            let key_kind = laux::lua_type(lua.state(), -2);
            let skip = key_kind == LuaType::Integer && {
                let key = ffi::lua_tointeger(lua.as_ptr(), -2);
                key > 0 && (key as usize) <= array_size
            };
            if !skip {
                let value_kind = laux::lua_type(lua.state(), -1);
                pack_value(lua, -2, key_kind, buf, depth)?;
                pack_value(lua, -1, value_kind, buf, depth)?;
            }
            ffi::lua_pop(lua.as_ptr(), 1);
        }
    }

    write_nil(buf);
    Ok(())
}

#[test]
fn unpack_keeps_input_rooted_and_counts_only_decoded_values() {
    let state = NonNull::new(unsafe { ffi::luaL_newstate() }).unwrap();
    let _owner = LuaGlobalState::new(state);
    let mut lua = unsafe { LuaStack::from_raw(state) };
    let payload = vec![b'x'; 4096];
    let mut encoded = Vec::new();
    write_nil(&mut encoded);
    write_integer(&mut encoded, 42);
    write_bytes(&mut encoded, &payload);
    write_nil(&mut encoded);
    laux::lua_push(state, encoded.as_slice());
    laux::lua_push(state, "ignored extra argument");

    assert_eq!(unpack(&mut lua).unwrap(), 4);
    assert_eq!(lua.top(), 5);
    assert_eq!(lua.value(1).as_bytes().unwrap(), encoded);
    assert_eq!(lua.value(2).kind(), LuaType::Nil);
    assert_eq!(lua.get::<i64>(3).unwrap(), 42);
    assert_eq!(lua.value(4).as_bytes().unwrap(), payload);
    assert_eq!(lua.value(5).kind(), LuaType::Nil);
    unsafe {
        ffi::lua_gc(state.as_ptr(), ffi::LUA_GCCOLLECT);
    }
    assert_eq!(lua.value(1).as_bytes().unwrap(), encoded);
    assert_eq!(lua.value(4).as_bytes().unwrap(), payload);
}
