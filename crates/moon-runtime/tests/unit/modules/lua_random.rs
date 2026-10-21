use super::*;
use moon_lua_sys::laux::LuaGlobalState;
use serial_test::serial;
use std::ffi::CString;

fn new_vm() -> (LuaState, LuaGlobalState) {
    unsafe {
        let raw = ffi::luaL_newstate();
        assert!(!raw.is_null());
        let state = LuaState::new(raw).unwrap();
        let guard = LuaGlobalState::new(state);
        ffi::luaL_openlibs(raw);
        ffi::luaL_requiref(
            raw,
            cstr!("random"),
            crate::not_null_wrapper!(luaopen_random),
            1,
        );
        ffi::lua_pop(raw, 1);
        (state, guard)
    }
}

fn run(state: LuaState, code: &str) -> Result<(), String> {
    unsafe {
        let c = CString::new(code).unwrap();
        if ffi::luaL_dostring(state.as_ptr(), c.as_ptr()) != ffi::LUA_OK {
            let err = ffi::lua_tostring(state.as_ptr(), -1);
            let msg = if err.is_null() {
                "unknown error".to_string()
            } else {
                std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
            };
            ffi::lua_pop(state.as_ptr(), 1);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

/// The frequency and subset checks are the ones that catch a sampling bug
/// still yielding distinct in-range values; distinctness alone would not.
#[test]
#[serial]
fn rand_range_some_is_uniform_distinct_and_in_range() {
    let (state, _guard) = new_vm();
    let code = r#"
            local random = require("random")

            -- (1) Distinctness, range, and per-value frequency. 600 draws x 3
            -- values = 1800 picks over 6 values => ~300 each, sigma ~16.
            local counts = {}
            for _ = 1, 600 do
                local r = random.rand_range_some(1, 6, 3)
                assert(#r == 3, "expected 3 values, got " .. #r)
                local seen = {}
                for _, v in ipairs(r) do
                    assert(v >= 1 and v <= 6, "out of range: " .. v)
                    assert(not seen[v], "duplicate: " .. v)
                    seen[v] = true
                    counts[v] = (counts[v] or 0) + 1
                end
            end
            for v = 1, 6 do
                local c = counts[v] or 0
                assert(c > 200 and c < 400, "value " .. v .. " appeared " .. c .. " times")
            end

            -- (2) count == range_len must yield the whole range, each once.
            local full = random.rand_range_some(1, 5, 5)
            assert(#full == 5, "expected 5 values, got " .. #full)
            local got = {}
            for _, v in ipairs(full) do
                assert(not got[v], "duplicate in full range: " .. v)
                got[v] = true
            end
            for v = 1, 5 do assert(got[v], "missing " .. v) end

            -- (3) Every 2-subset of 1..3 must occur about equally often.
            local sub = {}
            for _ = 1, 300 do
                local r = random.rand_range_some(1, 3, 2)
                local a, b = r[1], r[2]
                if a > b then a, b = b, a end
                local key = a .. "," .. b
                sub[key] = (sub[key] or 0) + 1
            end
            for _, key in ipairs({"1,2", "1,3", "2,3"}) do
                local c = sub[key] or 0
                assert(c > 50 and c < 150, "subset " .. key .. " occurred " .. c .. " times")
            end

            -- (4) count == 1, and a wide sparse range still costs O(count).
            local one = random.rand_range_some(1, 1, 1)
            assert(#one == 1 and one[1] == 1, "single-element range")
            local wide = random.rand_range_some(0, 1000000000, 5)
            local wseen = {}
            for _, v in ipairs(wide) do
                assert(v >= 0 and v <= 1000000000, "out of wide range: " .. v)
                assert(not wseen[v], "duplicate in wide range: " .. v)
                wseen[v] = true
            end
        "#;
    run(state, code).expect("sampling must be uniform, distinct and in range");
}
