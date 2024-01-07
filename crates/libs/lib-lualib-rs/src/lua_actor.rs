use base64::{engine, Engine};
use lib_lua::{ffi, ffi::luaL_Reg};
use std::alloc::{self, Layout};
// use regex::Regex;
use std::ffi::{c_char, c_int, c_void, CString};
use std::slice::{self};
use std::time::Duration;
use tokio::time::sleep;

use lib_core::{
    actor::LuaActor,
    c_str,
    context::{self, LuaActorParam, Message, CONTEXT},
    laux::{self, LuaValue},
    lreg, lreg_null,
};

use crate::luaopen_custom_libs;

unsafe extern "C-unwind" fn lua_actor_protect_init(state: *mut ffi::lua_State) -> c_int {
    let param = ffi::lua_touserdata(state, 1) as *mut LuaActorParam;
    ffi::luaL_openlibs(state);

    ffi::luaL_requiref(state, c_str!("moon.core"), luaopen_core, 0);
    ffi::lua_pop(state, 1);

    luaopen_custom_libs(state);

    let source = CString::new((*param).source.as_str()).unwrap();
    if ffi::luaL_loadfile(state, source.as_ptr() as *const c_char) != ffi::LUA_OK {
        return 1;
    }

    let params = CString::new((*param).params.as_str()).unwrap();
    if ffi::luaL_dostring(state, params.as_ptr() as *const c_char) != ffi::LUA_OK {
        return 1;
    }

    ffi::lua_call(state, 1, 0);

    0
}

unsafe extern "C-unwind" fn allocator(
    extra: *mut c_void,
    ptr: *mut c_void,
    osize: usize,
    nsize: usize,
) -> *mut c_void {
    let actor = &mut *(extra as *mut LuaActor);

    if nsize == 0 {
        if !ptr.is_null() {
            let layout = Layout::from_size_align_unchecked(osize, lib_lua::SYS_MIN_ALIGN);
            alloc::dealloc(ptr as *mut u8, layout);
            actor.mem -= osize;
        }
        return std::ptr::null_mut();
    }

    if nsize > isize::MAX as usize {
        return std::ptr::null_mut();
    }

    actor.mem += nsize;

    if actor.mem_limit > 0 && actor.mem > actor.mem_limit {
        log::error!(
            "Actor id:{:?} name:{:?} memory limit exceed: {}",
            actor.id,
            actor.name,
            actor.mem_limit
        );
        return std::ptr::null_mut();
    } else if actor.mem > actor.mem_warning {
        actor.mem_warning *= 2;
        log::warn!(
            "Actor id:{:?} name:{:?} memory warning: {:.2}MB",
            actor.id,
            actor.name,
            (actor.mem / (1024 * 1024)) as f32
        );
    }

    if ptr.is_null() {
        // Allocate new memory
        let new_layout = match Layout::from_size_align(nsize, lib_lua::SYS_MIN_ALIGN) {
            Ok(layout) => layout,
            Err(_) => return std::ptr::null_mut(),
        };
        let new_ptr = alloc::alloc(new_layout) as *mut c_void;
        if new_ptr.is_null() {
            alloc::handle_alloc_error(new_layout);
        }
        return new_ptr;
    }

    // Reallocate memory
    let old_layout = Layout::from_size_align_unchecked(osize, lib_lua::SYS_MIN_ALIGN);
    let new_ptr = alloc::realloc(ptr as *mut u8, old_layout, nsize) as *mut c_void;
    if new_ptr.is_null() {
        alloc::handle_alloc_error(old_layout);
    }
    new_ptr
}

pub fn new_actor(params: LuaActorParam) {
    tokio::spawn(async move {
        let mut actor = Box::<LuaActor>::new(LuaActor::new(params.name.clone(), params.unique));

        match init(&mut actor, &params) {
            Ok(_) => {
                log::info!(
                    "Actor id:{:?} name:{:?} started. ({}:{})",
                    actor.id,
                    actor.name,
                    file!(),
                    line!()
                );

                if params.creator != 0 {
                    CONTEXT.send(Message {
                        ptype: context::PTYPE_INTEGER,
                        from: 1,
                        to: params.creator,
                        session: params.session,
                        data: Some(Box::new(actor.id.to_string().into())),
                    });
                }

                loop {
                    let msg = actor.rx.recv().await;
                    match msg {
                        Some(msg) => {
                            handle(&mut actor, msg);
                        }
                        None => {
                            break;
                        }
                    }
                }

                log::info!(
                    "Actor id:{:?} name:{:?} stoped. ({}:{})",
                    actor.id,
                    actor.name,
                    file!(),
                    line!()
                );
            }
            Err(err) => {
                if params.creator != 0 {
                    CONTEXT.send(Message {
                        ptype: context::PTYPE_ERROR,
                        from: actor.id,
                        to: params.creator,
                        session: params.session,
                        data: Some(Box::new(0.to_string().into())),
                    });
                }
                log::error!("create actor failed: {}. ({}:{})", err, file!(), line!());
            }
        }

        CONTEXT.remove_actor(actor.id, actor.unique);
    });
}

pub fn init(actor: &mut LuaActor, params: &LuaActorParam) -> Result<(), String> {
    //log::info!("init actor id: {} name: {}", id, params.name);
    actor.name = params.name.clone();
    actor.unique = params.unique;

    CONTEXT.add_actor(actor)?;

    unsafe {
        let main_state = ffi::lua_newstate(allocator, actor as *const LuaActor as *mut c_void);
        if main_state.is_null() {
            return Err("create lua state failed".to_string());
        }

        actor.set_main_state(main_state);

        ffi::lua_gc(main_state, ffi::LUA_GCSTOP, 0);
        ffi::lua_gc(main_state, ffi::LUA_GCGEN, 0, 0);

        ffi::lua_pushcfunction(main_state, laux::lua_traceback);
        let trace_fn = ffi::lua_gettop(main_state);

        ffi::lua_pushcfunction(main_state, lua_actor_protect_init);

        let p = params as *const LuaActorParam as *mut c_void;
        ffi::lua_pushlightuserdata(main_state, p);

        if ffi::lua_pcall(main_state, 1, ffi::LUA_MULTRET, trace_fn) != ffi::LUA_OK
            || ffi::lua_gettop(main_state) != 1
        {
            return Err(format!(
                "init actor failed: {}",
                laux::opt_str(main_state, -1, "no error message")
            ));
        }

        ffi::lua_pop(main_state, 1);
        ffi::lua_gc(main_state, ffi::LUA_GCRESTART, 0);
        assert_eq!(ffi::lua_gettop(main_state), 0);
    }

    actor.ok = true;

    Ok(())
}

fn response_error(from: i64, to: i64, session: i64, err: String) {
    if session >= 0 {
        log::debug!("{}. ({}:{})", err, file!(), line!());
    } else {
        CONTEXT.send(Message {
            ptype: context::PTYPE_ERROR,
            from,
            to,
            session: -session,
            data: Some(Box::new(err.into_bytes().into())),
        });
    }
}

fn handle(actor: &mut LuaActor, mut m: Message) {
    if !actor.ok {
        return;
    }

    debug_assert!(!actor.callback_state.0.is_null(), "moon_rs not initialized");

    if m.ptype == context::PTYPE_QUIT {
        if actor.id == context::BOOTSTRAP_ACTOR_ADDR {
            CONTEXT.shutdown(0);
        }
        actor.ok = false;

        let err = "actor quited";

        while let Ok(m) = actor.rx.try_recv() {
            response_error(m.to, m.from, m.session, err.to_string());
        }
        actor.rx.close();
        return;
    }

    unsafe {
        let trace = 1;
        ffi::lua_pushvalue(actor.callback_state.0, 2);

        ffi::lua_pushinteger(actor.callback_state.0, m.ptype as ffi::lua_Integer);
        ffi::lua_pushinteger(actor.callback_state.0, m.from as ffi::lua_Integer);
        ffi::lua_pushinteger(actor.callback_state.0, m.session as ffi::lua_Integer);

        if let Some(data) = &m.data {
            ffi::lua_pushlightuserdata(actor.callback_state.0, data.as_ptr() as *mut c_void);
            ffi::lua_pushinteger(actor.callback_state.0, data.len() as ffi::lua_Integer);
        } else {
            ffi::lua_pushlightuserdata(actor.callback_state.0, std::ptr::null_mut() as *mut c_void);
            ffi::lua_pushinteger(actor.callback_state.0, 0 as ffi::lua_Integer);
        }

        ffi::lua_pushlightuserdata(
            actor.callback_state.0,
            &mut m as *mut Message as *mut c_void,
        );

        let r = ffi::lua_pcall(actor.callback_state.0, 6, 0, trace);
        if r == ffi::LUA_OK {
            return;
        }

        let err = match r {
            ffi::LUA_ERRRUN => {
                format!(
                    "actor '{}' dispatch message error:\n{}",
                    actor.name,
                    laux::opt_str(actor.callback_state.0, -1, "no error message")
                )
            }
            ffi::LUA_ERRMEM => {
                format!(
                    "actor '{}' dispatch message error:\n{}",
                    actor.name, "memory error"
                )
            }
            ffi::LUA_ERRERR => {
                format!(
                    "actor '{}' dispatch message error:\n{}",
                    actor.name, "error in error"
                )
            }
            _ => {
                format!(
                    "actor '{}' dispatch message error:\n{}",
                    actor.name, "unknown error"
                )
            }
        };

        ffi::lua_pop(actor.callback_state.0, 1);
        response_error(m.to, m.from, m.session, err.to_string());
    }
}

pub fn remove_actor(id: i64) -> Result<(), String> {
    if let Some(res) = CONTEXT.remove(id) {
        return match res.1.send(Message {
            ptype: context::PTYPE_QUIT,
            from: 0,
            to: id,
            session: 0,
            data: None,
        }) {
            Ok(_) => Ok(()),
            Err(err) => Err(format!("send error {}", err)),
        };
    }
    Err(format!("not found actor id= {}", id))
}

extern "C-unwind" fn lua_actor_query(state: *mut ffi::lua_State) -> c_int {
    let name = laux::check_str(state, 1);
    let addr = CONTEXT.query(name);
    match addr {
        Some(addr) => {
            i64::push_lua(state, *addr.value());
            1
        }
        None => {
            i64::push_lua(state, 0);
            1
        }
    }
}

extern "C-unwind" fn lua_actor_send(state: *mut ffi::lua_State) -> c_int {
    let ptype = i64::from_lua_check(state, 1) as i8;
    unsafe { ffi::luaL_argcheck(state, (ptype > 0) as i32, 1, c_str!("PTYPE must > 0")) }

    let to: i64 = i64::from_lua_check(state, 2);
    unsafe { ffi::luaL_argcheck(state, (to > 0) as i32, 2, c_str!("receiver must > 0")) }

    let session = i64::from_lua_check(state, 3);

    let data = laux::check_buffer(state, 4);

    let from = LuaActor::from_lua_state(state).id;

    if let Some(m) = CONTEXT.send(Message {
        ptype,
        from,
        to,
        session: -session,
        data,
    }) {
        response_error(
            m.to,
            m.from,
            m.session,
            format!(
                "actor not found: send message from {:0>8} to {:0>8} PTYPE {} session {} data {}",
                from,
                to,
                ptype,
                m.session,
                engine::general_purpose::STANDARD_NO_PAD
                    .encode(m.data.unwrap_or_default().as_slice())
            ),
        );
    }

    0
}

extern "C-unwind" fn lua_kill_actor(state: *mut ffi::lua_State) -> c_int {
    let who = i64::from_lua_check(state, 1);
    let res = remove_actor(who);
    match res {
        Ok(_) => {
            bool::push_lua(state, true);
            1
        }
        Err(err) => {
            bool::push_lua(state, false);
            laux::push_str(state, err.as_str());
            2
        }
    }
}

extern "C-unwind" fn lua_new_actor(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        ffi::luaL_checktype(state, 2, ffi::LUA_TTABLE);
    }

    let session = i64::from_lua_check(state, 1);

    let creator = LuaActor::from_lua_state(state).id;
    let name: String = laux::opt_field(state, 2, "name", "").to_string();
    let source = laux::opt_field(state, 2, "source", "").to_string();
    let memlimit: i64 = laux::opt_field(state, 2, "memlimit", 0);
    let unique: bool = laux::opt_field(state, 2, "unique", false);

    let mut params = String::from_lua_check(state, 3);
    if let Some(p) = CONTEXT.env.get("PATH") {
        params = p.value().clone() + params.as_str();
    }

    new_actor(LuaActorParam {
        unique,
        creator,
        session,
        memlimit,
        name,
        source,
        params,
    });

    0
}

unsafe extern "C-unwind" fn lua_actor_callback(state: *mut ffi::lua_State) -> c_int {
    ffi::luaL_checktype(state, 1, ffi::LUA_TFUNCTION);
    ffi::lua_settop(state, 1);
    let actor = LuaActor::from_lua_state(state);
    ffi::lua_newuserdatauv(state, 1, 1);
    actor.callback_state.0 = ffi::lua_newthread(state);
    ffi::lua_pushcfunction(actor.callback_state.0, laux::lua_traceback);
    ffi::lua_setuservalue(state, -2);
    ffi::lua_setfield(state, ffi::LUA_REGISTRYINDEX, c_str!("callback_context"));
    ffi::lua_xmove(state, actor.callback_state.0, 1);

    0
}

extern "C-unwind" fn lua_timeout(state: *mut ffi::lua_State) -> c_int {
    let interval = u64::from_lua_check(state, 1);
    let id = LuaActor::from_lua_state(state).id;
    let timer_id = CONTEXT.next_timer_id();
    tokio::spawn(async move {
        if interval > 0 {
            sleep(Duration::from_millis(interval)).await;
        }

        CONTEXT.send(Message {
            ptype: context::PTYPE_TIMER,
            from: timer_id,
            to: id,
            session: 0,
            data: None,
        });
    });

    unsafe {
        ffi::lua_pushinteger(state, timer_id);
    }

    1
}

unsafe extern "C-unwind" fn lua_actor_log(state: *mut ffi::lua_State) -> c_int {
    let log_level = i64::from_lua_check(state, 1);
    let stack_level = i64::from_lua_check(state, 2);

    let top = ffi::lua_gettop(state);
    let mut content = String::new();
    for i in 3..=top {
        let mut len: usize = 0;
        let s = ffi::luaL_tolstring(state, i, &mut len as *mut usize);
        if i > 3 {
            content.push_str("    ");
        }
        let sl: &[u8] = slice::from_raw_parts(s as *mut u8, len);
        content.push_str(std::str::from_utf8_unchecked(sl));
        ffi::lua_pop(state, 1);
    }

    let mut debug: ffi::lua_Debug = unsafe { std::mem::zeroed() };
    if ffi::lua_getstack(state, stack_level as c_int, &mut debug) != 0
        && ffi::lua_getinfo(state, c_str!("Sl"), &mut debug) != 0
    {
        content.push_str("    ");
        content.push('(');
        if debug.srclen > 1 {
            let sl = slice::from_raw_parts(debug.source as *mut u8, debug.srclen);
            content.push_str(std::str::from_utf8_unchecked(sl));
        }
        content.push(':');
        content.push_str(debug.currentline.to_string().as_str());
        content.push(')');
    }

    let actor = LuaActor::from_lua_state(state);

    match log_level {
        1 => {
            log::error!("{:0>8} | {}", actor.id, content);
        }
        2 => {
            log::warn!("{:0>8} | {}", actor.id, content);
        }
        3 => {
            log::info!("{:0>8} | {}", actor.id, content);
        }
        4 => {
            log::debug!("{:0>8} | {}", actor.id, content);
        }
        5 => {
            log::trace!("{:0>8}| {}", actor.id, content);
        }
        _ => {
            log::info!("{:0>8} | {}", actor.id, content);
        }
    }
    0
}

extern "C-unwind" fn lua_actor_exit(state: *mut ffi::lua_State) -> c_int {
    let exit_code = u32::from_lua_check(state, 1);
    CONTEXT.shutdown(exit_code);
    0
}

extern "C-unwind" fn num_cpus(state: *mut ffi::lua_State) -> c_int {
    usize::push_lua(state, num_cpus::get());
    1
}

extern "C-unwind" fn env(state: *mut ffi::lua_State) -> c_int {
    if unsafe { ffi::lua_gettop(state) == 2 } {
        let key = laux::check_str(state, 1);
        let value = laux::check_str(state, 2);
        CONTEXT.set_env(key, value);
        0
    } else {
        let key = laux::check_str(state, 1);
        match CONTEXT.get_env(key) {
            Some(value) => {
                laux::push_str(state, &value);
                1
            }
            None => 0,
        }
    }
}

extern "C-unwind" fn clock(state: *mut ffi::lua_State) -> c_int {
    f64::push_lua(state, CONTEXT.clock());
    1
}

unsafe extern "C-unwind" fn tostring(state: *mut ffi::lua_State) -> c_int {
    if laux::lua_type(state, 1) == ffi::LUA_TLIGHTUSERDATA {
        let data = ffi::lua_touserdata(state, 1) as *const u8;
        ffi::luaL_argcheck(
            state,
            if !data.is_null() { 1 } else { 0 },
            1,
            c_str!("lightuserdata(char*) expected"),
        );
        let len = ffi::luaL_checkinteger(state, 2) as usize;
        ffi::lua_pushlstring(state, data as *const c_char, len);
    }
    1
}

fn get_message_pointer(state: *mut ffi::lua_State) -> &'static mut Message {
    let m = unsafe { ffi::lua_touserdata(state, 1) as *mut Message };
    if m.is_null() {
        unsafe { ffi::luaL_argerror(state, 1, c_str!("null message pointer")) };
    }
    unsafe { &mut *m }
}

extern "C-unwind" fn lua_message_decode(state: *mut ffi::lua_State) -> c_int {
    let m = get_message_pointer(state);
    let opt = laux::check_str(state, 2);
    let top = unsafe { ffi::lua_gettop(state) };
    for c in opt.chars() {
        match c {
            'T' => {
                i64::push_lua(state, m.ptype as i64);
            }
            'S' => {
                i64::push_lua(state, m.from);
            }
            'R' => {
                i64::push_lua(state, m.to);
            }
            'E' => {
                i64::push_lua(state, m.session);
            }
            'Z' => {
                if let Some(data) = &m.data {
                    unsafe {
                        ffi::lua_pushlstring(state, data.as_ptr() as *const i8, data.len());
                    }
                } else {
                    unsafe {
                        ffi::lua_pushnil(state);
                    }
                }
            }
            'N' => {
                if let Some(data) = &m.data {
                    unsafe {
                        ffi::lua_pushinteger(state, data.len() as i64);
                    }
                } else {
                    unsafe {
                        ffi::lua_pushinteger(state, 0);
                    }
                }
            }
            'B' => {
                if let Some(data) = &mut m.data {
                    unsafe {
                        ffi::lua_pushlightuserdata(
                            state,
                            data.as_mut().as_pointer() as *mut std::ffi::c_void,
                        );
                    }
                } else {
                    unsafe {
                        ffi::lua_pushnil(state);
                    }
                }
            }
            'C' => {
                if let Some(data) = &m.data {
                    unsafe {
                        ffi::lua_pushlightuserdata(state, data.as_ptr() as *mut c_void);
                        ffi::lua_pushinteger(state, data.len() as ffi::lua_Integer);
                    }
                } else {
                    unsafe {
                        ffi::lua_pushlightuserdata(state, std::ptr::null_mut());
                        ffi::lua_pushinteger(state, 0 as ffi::lua_Integer);
                    }
                }
            }
            _ => unsafe {
                ffi::luaL_error(state, c_str!("invalid format option '%c'"), c);
            },
        }
    }
    unsafe { ffi::lua_gettop(state) - top }
}

unsafe extern "C-unwind" fn luaopen_core(state: *mut ffi::lua_State) -> c_int {
    let l = [
        lreg!("new_service", lua_new_actor),
        lreg!("query", lua_actor_query),
        lreg!("kill", lua_kill_actor),
        lreg!("send", lua_actor_send),
        lreg!("log", lua_actor_log),
        lreg!("callback", lua_actor_callback),
        lreg!("exit", lua_actor_exit),
        lreg!("timeout", lua_timeout),
        lreg!("decode", lua_message_decode),
        lreg!("num_cpus", num_cpus),
        // lreg!("match", lua_regex),
        lreg!("env", env),
        lreg!("clock", clock),
        lreg!("tostring", tostring),
        lreg_null!(),
    ];

    ffi::lua_createtable(state, 0, l.len() as c_int);
    ffi::luaL_setfuncs(state, l.as_ptr(), 0);

    let actor = LuaActor::from_lua_state(state);
    i64::push_lua(state, actor.id);
    ffi::lua_setfield(state, -2, c_str!("id"));

    laux::push_str(state, &actor.name);
    ffi::lua_setfield(state, -2, c_str!("name"));

    1
}
