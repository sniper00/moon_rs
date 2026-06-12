use moon_base::{cstr, ffi, laux::LuaState};
use moon_runtime::{actor::LuaActor, context::Watchdog};
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

/// Internal resume helper. Returns nresults on success, -1 on error (error
/// message left on top of `L`'s stack). The error message from signal_hook
/// already includes traceback (captured before lua_error unwound the frames).
unsafe fn aux_resume(
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
        let status = lua_resume_x(co, l, narg, &mut nres, wd);
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

/// `coroutine.resume(co, ...)` — custom version with switchL interrupt support.
unsafe extern "C-unwind" fn lua_coroutine_resume(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        ffi::luaL_checktype(l, 1, ffi::LUA_TTHREAD);
        let co = ffi::lua_tothread(l, 1);
        let narg = ffi::lua_gettop(l) - 1;

        let state = LuaState::new(l).unwrap();
        let actor = LuaActor::from_lua_state(state);
        let wd = (*actor).watchdog;

        let r = if wd.is_null() {
            plain_resume(l, co, narg)
        } else {
            aux_resume(l, co, narg, wd)
        };

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

/// Plain resume without watchdog (fallback for test VMs without actor).
unsafe fn plain_resume(l: *mut ffi::lua_State, co: *mut ffi::lua_State, narg: c_int) -> c_int {
    unsafe {
        if ffi::lua_checkstack(co, narg) == 0 {
            ffi::lua_pushstring(l, cstr!("too many arguments to resume"));
            return -1;
        }
        ffi::lua_xmove(l, co, narg);
        let mut nres: c_int = 0;
        let status = ffi::lua_resume(co, l, narg, &mut nres);
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

/// `coroutine.wrap(f)` helper — the closure returned by wrap.
unsafe extern "C-unwind" fn lua_coroutine_aux_wrap(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        let co = ffi::lua_tothread(l, ffi::lua_upvalueindex(1));
        let narg = ffi::lua_gettop(l);

        let state = LuaState::new(l).unwrap();
        let actor = LuaActor::from_lua_state(state);
        let wd = (*actor).watchdog;

        let r = if wd.is_null() {
            plain_resume(l, co, narg)
        } else {
            aux_resume(l, co, narg, wd)
        };

        if r < 0 {
            let stat = ffi::lua_status(co);
            if stat != ffi::LUA_OK && stat != ffi::LUA_YIELD {
                ffi::lua_closethread(co, l);
            }
            ffi::lua_error(l);
            #[allow(unreachable_code)]
            0
        } else {
            r
        }
    }
}

/// `coroutine.wrap(f)` — creates a coroutine and returns an iterator function.
unsafe extern "C-unwind" fn lua_coroutine_wrap(l: *mut ffi::lua_State) -> c_int {
    unsafe {
        ffi::luaL_checktype(l, 1, ffi::LUA_TFUNCTION);
        let co = ffi::lua_newthread(l);
        ffi::lua_pushvalue(l, 1);
        ffi::lua_xmove(l, co, 1);
        ffi::lua_pushcclosure(l, lua_coroutine_aux_wrap, 1);
        1
    }
}

/// Opens the custom coroutine module and patches the global `coroutine` table
/// so that `coroutine.resume` and `coroutine.wrap` use our switchL-aware versions.
pub extern "C-unwind" fn luaopen_coroutine(state: LuaState) -> c_int {
    unsafe {
        let l = state.as_ptr();

        ffi::lua_getglobal(l, cstr!("coroutine"));

        ffi::lua_pushcfunction(l, lua_coroutine_resume);
        ffi::lua_setfield(l, -2, cstr!("resume"));

        ffi::lua_pushcfunction(l, lua_coroutine_wrap);
        ffi::lua_setfield(l, -2, cstr!("wrap"));

        1
    }
}
