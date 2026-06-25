use moon_base::{cstr, ffi, laux::LuaState};
use moon_runtime::{
    actor::LuaActor,
    context::{CONTEXT, Watchdog},
};
use std::ffi::c_int;
use std::sync::atomic::Ordering;

/// Lua count-hook callback installed by the monitor (via `check_watchdogs`) or
/// by `switch_l` when a trap is pending. Clears the hook, resets the trap flag,
/// and raises a Lua error to unwind the stuck coroutine.
/// Traceback is captured HERE (before lua_error unwinds the stack).
///
/// # Safety
/// `l` must be a valid `lua_State` pointer whose actor was created by this
/// runtime (so `LuaActor::from_lua_state` yields a live actor). Intended to be
/// invoked only by the Lua VM as a debug hook; `_ar` is unused. May raise a Lua
/// error via `longjmp`/unwind, so the caller must be inside the Lua VM.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn moon_signal_hook(l: *mut ffi::lua_State, _ar: *mut ffi::lua_Debug) {
    unsafe {
        ffi::lua_sethook(l, None, 0, 0);
        let state = LuaState::new(l).unwrap();
        let actor = LuaActor::from_lua_state(state);
        let wd = (*actor).watchdog;
        if !wd.is_null() {
            let trap = (*wd).trap.load(Ordering::Acquire);
            if trap != 0 {
                (*wd).trap.store(0, Ordering::Release);
                ffi::luaL_traceback(
                    l,
                    l,
                    cstr!("interrupted: possible infinite loop detected"),
                    0,
                );
                ffi::lua_error(l);
            }
        }
    }
}

/// Update `active_l` to `l` and, if a trap is pending, install the count-hook
/// on this state so it fires on the very next VM instruction.
#[inline]
unsafe fn switch_l(l: *mut ffi::lua_State, wd: *const Watchdog) {
    unsafe {
        (*wd)
            .active_l
            .store(l as *mut std::ffi::c_void, Ordering::Release);
        if (*wd).trap.load(Ordering::Acquire) != 0 {
            ffi::lua_sethook(l, Some(moon_signal_hook), ffi::LUA_MASKCOUNT, 1);
        }
    }
}

/// Wraps `lua_resume` with the switchL protocol: arms the hook on the target
/// coroutine before resume, and restores tracking to the caller after.
unsafe fn lua_resume_x(
    co: *mut ffi::lua_State,
    from: *mut ffi::lua_State,
    nargs: c_int,
    nresults: &mut c_int,
    wd: *const Watchdog,
) -> c_int {
    unsafe {
        switch_l(co, wd);
        let status = ffi::lua_resume(co, from, nargs, nresults as *mut c_int);
        if (*wd).trap.load(Ordering::Acquire) > 0 {
            while (*wd).trap.load(Ordering::Acquire) >= 0 {
                std::hint::spin_loop();
            }
        }
        switch_l(from, wd);
        status
    }
}

/// Resolve the watchdog for the actor that owns `l`. Returns a null pointer for
/// VMs created without an actor (e.g. unit-test VMs), in which case callers fall
/// back to a plain `lua_resume` without interrupt support.
#[inline]
unsafe fn actor_watchdog(l: *mut ffi::lua_State) -> *const Watchdog {
    unsafe {
        let state = LuaState::new(l).unwrap();
        let actor = LuaActor::from_lua_state(state);
        (*actor).watchdog
    }
}

/// Core resume helper. Moves `narg` arguments from `l` to `co`, resumes `co`
/// (using the switchL-aware path when `wd` is non-null), then moves the results
/// (or error message) back onto `l`. Returns the number of results on success,
/// or -1 on error (error message left on top of `l`'s stack).
unsafe fn core_resume(
    l: *mut ffi::lua_State,
    co: *mut ffi::lua_State,
    narg: c_int,
    wd: *const Watchdog,
) -> c_int {
    unsafe {
        if ffi::lua_checkstack(co, narg) == 0 {
            ffi::lua_pushstring(l, cstr!("too many arguments to resume"));
            return -1;
        }
        ffi::lua_xmove(l, co, narg);
        let mut nres: c_int = 0;
        let status = if wd.is_null() {
            ffi::lua_resume(co, l, narg, &mut nres)
        } else {
            lua_resume_x(co, l, narg, &mut nres, wd)
        };
        if status == ffi::LUA_OK || status == ffi::LUA_YIELD {
            if ffi::lua_checkstack(l, nres + 1) == 0 {
                ffi::lua_pop(co, nres);
                ffi::lua_pushstring(l, cstr!("too many results to resume"));
                return -1;
            }
            ffi::lua_xmove(co, l, nres);
            nres
        } else {
            ffi::lua_xmove(co, l, 1);
            -1
        }
    }
}

/// Monotonic seconds, used as the profiling time source.
#[inline]
fn get_time() -> f64 {
    CONTEXT.clock()
}

/// Elapsed time since `start`. The clock is monotonic, so a negative delta can
/// only result from rounding and is clamped to zero.
#[inline]
fn diff_time(start: f64) -> f64 {
    (get_time() - start).max(0.0)
}

/// If the coroutine at `co_index` is currently being profiled, return its
/// recorded start time (the value stored in the start-time table at
/// `upvalueindex(1)`); otherwise return `None`.
unsafe fn timing_enable(l: *mut ffi::lua_State, co_index: c_int) -> Option<f64> {
    unsafe {
        ffi::lua_pushvalue(l, co_index);
        ffi::lua_rawget(l, ffi::lua_upvalueindex(1));
        if ffi::lua_isnil(l, -1) != 0 {
            ffi::lua_pop(l, 1);
            None
        } else {
            let start = ffi::lua_tonumber(l, -1);
            ffi::lua_pop(l, 1);
            Some(start)
        }
    }
}

/// Accumulated total time recorded for the coroutine at `co_index`
/// (the value stored in the total-time table at `upvalueindex(2)`).
unsafe fn timing_total(l: *mut ffi::lua_State, co_index: c_int) -> f64 {
    unsafe {
        ffi::lua_pushvalue(l, co_index);
        ffi::lua_rawget(l, ffi::lua_upvalueindex(2));
        let total = ffi::lua_tonumber(l, -1);
        ffi::lua_pop(l, 1);
        total
    }
}

/// Resume the coroutine at `co_index`, accumulating its run time into the
/// profiling tables when profiling is enabled for it.
unsafe fn timing_resume(l: *mut ffi::lua_State, co_index: c_int, narg: c_int) -> c_int {
    unsafe {
        let co = ffi::lua_tothread(l, co_index);
        let wd = actor_watchdog(l);

        if timing_enable(l, co_index).is_some() {
            // (re)set the start time just before resuming
            ffi::lua_pushvalue(l, co_index);
            ffi::lua_pushnumber(l, get_time());
            ffi::lua_rawset(l, ffi::lua_upvalueindex(1));
        }

        let r = core_resume(l, co, narg, wd);

        if let Some(start_time) = timing_enable(l, co_index) {
            let total_time = timing_total(l, co_index) + diff_time(start_time);
            ffi::lua_pushvalue(l, co_index);
            ffi::lua_pushnumber(l, total_time);
            ffi::lua_rawset(l, ffi::lua_upvalueindex(2));
        }

        r
    }
}

/// `coroutine.resume(co, ...)` — switchL-aware, profiling-aware version.
unsafe extern "C-unwind" fn lua_coroutine_resume(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        ffi::luaL_checktype(l, 1, ffi::LUA_TTHREAD);
        let r = timing_resume(l, 1, ffi::lua_gettop(l) - 1);
        if r < 0 {
            ffi::lua_pushboolean(l, 0);
            ffi::lua_insert(l, -2);
            2
        } else {
            ffi::lua_pushboolean(l, 1);
            ffi::lua_insert(l, -(r + 1));
            r + 1
        }
    }
}

/// The closure returned by `coroutine.wrap`. The wrapped coroutine is stored in
/// `upvalueindex(3)` (upvalues 1 and 2 are the shared profiling tables).
unsafe extern "C-unwind" fn lua_coroutine_aux_wrap(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        let co = ffi::lua_tothread(l, ffi::lua_upvalueindex(3));
        let r = timing_resume(l, ffi::lua_upvalueindex(3), ffi::lua_gettop(l));
        if r < 0 {
            let stat = ffi::lua_status(co);
            if stat != ffi::LUA_OK && stat != ffi::LUA_YIELD {
                ffi::lua_closethread(co, l);
            }
            if ffi::lua_type(l, -1) == ffi::LUA_TSTRING {
                // add source/line info to a string error, then propagate
                ffi::luaL_where(l, 1);
                ffi::lua_insert(l, -2);
                ffi::lua_concat(l, 2);
            }
            ffi::lua_error(l);
            #[allow(unreachable_code)]
            0
        } else {
            r
        }
    }
}

/// `coroutine.wrap(f)` — creates a coroutine and returns an iterator closure.
/// Upvalues 1 and 2 carry the shared profiling tables onto the closure.
unsafe extern "C-unwind" fn lua_coroutine_wrap(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        ffi::lua_pushvalue(l, ffi::lua_upvalueindex(1));
        ffi::lua_pushvalue(l, ffi::lua_upvalueindex(2));

        ffi::luaL_checktype(l, 1, ffi::LUA_TFUNCTION);
        let co = ffi::lua_newthread(l);
        ffi::lua_pushvalue(l, 1);
        ffi::lua_xmove(l, co, 1);

        ffi::lua_pushcclosure(l, lua_coroutine_aux_wrap, 3);
        1
    }
}

/// Push the target coroutine onto the stack at index 1: the explicit argument
/// if given, otherwise the running coroutine.
unsafe fn profile_target(l: *mut ffi::lua_State) {
    unsafe {
        if ffi::lua_gettop(l) != 0 {
            ffi::lua_settop(l, 1);
            ffi::luaL_checktype(l, 1, ffi::LUA_TTHREAD);
        } else {
            ffi::lua_pushthread(l);
        }
    }
}

/// `coroutine.profile.start([co])` — begin profiling a coroutine (defaults to
/// the running one), resetting its accumulated total time.
unsafe extern "C-unwind" fn lua_profile_start(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        profile_target(l);

        if timing_enable(l, 1).is_some() {
            return ffi::luaL_error(
                l,
                cstr!("coroutine.profile.start: thread %p is already being profiled"),
                ffi::lua_topointer(l, 1),
            );
        }

        // reset total time
        ffi::lua_pushvalue(l, 1);
        ffi::lua_pushnumber(l, 0.0);
        ffi::lua_rawset(l, ffi::lua_upvalueindex(2));

        // set start time
        ffi::lua_pushvalue(l, 1);
        ffi::lua_pushnumber(l, get_time());
        ffi::lua_rawset(l, ffi::lua_upvalueindex(1));

        0
    }
}

/// `coroutine.profile.stop([co])` — stop profiling a coroutine and return its
/// total accumulated run time in seconds.
unsafe extern "C-unwind" fn lua_profile_stop(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        profile_target(l);

        let start_time = match timing_enable(l, 1) {
            Some(t) => t,
            None => {
                return ffi::luaL_error(
                    l,
                    cstr!("coroutine.profile.stop: call profile.start() before profile.stop()"),
                );
            }
        };

        let total_time = timing_total(l, 1) + diff_time(start_time);

        // clear both profiling slots for this coroutine
        ffi::lua_pushvalue(l, 1);
        ffi::lua_pushnil(l);
        ffi::lua_rawset(l, ffi::lua_upvalueindex(1));

        ffi::lua_pushvalue(l, 1);
        ffi::lua_pushnil(l);
        ffi::lua_rawset(l, ffi::lua_upvalueindex(2));

        ffi::lua_pushnumber(l, total_time);
        1
    }
}

/// Opens the custom coroutine profiling module. Builds a `profile` table with
/// `start`/`stop`/`resume`/`wrap`, sharing two weak tables (start-time and
/// total-time, keyed by coroutine) as upvalues, and patches the global
/// `coroutine` table so `coroutine.resume`/`coroutine.wrap` use the
/// switchL-aware, profiling-aware versions. Returns the `profile` table.
pub extern "C-unwind" fn luaopen_coroutine_profile(state: LuaState) -> c_int {
    unsafe {
        let l = state.as_ptr();

        // The sentinel `func` is never invoked (luaL_setfuncs stops at the NULL
        // name); any valid pointer satisfies the non-nullable field type.
        let funcs = [
            ffi::luaL_Reg {
                name: cstr!("start"),
                func: lua_profile_start,
            },
            ffi::luaL_Reg {
                name: cstr!("stop"),
                func: lua_profile_stop,
            },
            ffi::luaL_Reg {
                name: cstr!("resume"),
                func: lua_coroutine_resume,
            },
            ffi::luaL_Reg {
                name: cstr!("wrap"),
                func: lua_coroutine_wrap,
            },
            ffi::luaL_Reg {
                name: std::ptr::null(),
                func: lua_profile_start,
            },
        ];

        // module table (index 2; the module name passed by `require` is at index 1)
        ffi::lua_createtable(l, 0, (funcs.len() - 1) as c_int);

        ffi::lua_newtable(l); // upvalue 1: coroutine -> start time
        ffi::lua_newtable(l); // upvalue 2: coroutine -> total time

        // shared weak metatable so finished coroutines can be collected
        ffi::lua_newtable(l);
        ffi::lua_pushstring(l, cstr!("kv"));
        ffi::lua_setfield(l, -2, cstr!("__mode"));
        ffi::lua_pushvalue(l, -1);
        ffi::lua_setmetatable(l, -3); // total-time table
        ffi::lua_setmetatable(l, -3); // start-time table

        ffi::luaL_setfuncs(l, funcs.as_ptr(), 2);

        // patch the global `coroutine` table to use our resume/wrap
        ffi::lua_getglobal(l, cstr!("coroutine"));
        ffi::lua_getfield(l, 2, cstr!("resume"));
        ffi::lua_setfield(l, -2, cstr!("resume"));
        ffi::lua_getfield(l, 2, cstr!("wrap"));
        ffi::lua_setfield(l, -2, cstr!("wrap"));
        ffi::lua_pop(l, 1); // pop the coroutine table, leaving the module table on top

        1
    }
}
