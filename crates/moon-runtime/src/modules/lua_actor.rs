use crate::{lua_require, luaopen_custom_libs, not_null_wrapper};
use moon_base::{
    self, cstr,
    ffi::{self, luaL_Reg},
    laux::{self, LuaState, LuaThread, LuaType},
    lreg, lreg_null,
};
use moon_runtime::{
    actor::LuaActor,
    check_buffer,
    context::{self, CONTEXT, LOGGER, LuaActorParam, Message, MessageBody, Watchdog},
    log::Logger,
};
use tokio::sync::mpsc;

use std::{
    alloc::{self, Layout},
    ffi::{CString, c_int, c_void},
    ops::Deref,
    slice,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

fn global_seed() -> std::ffi::c_uint {
    static SEED: AtomicU32 = AtomicU32::new(0);
    let mut ret = SEED.load(Ordering::Acquire);
    while ret == 0 {
        let mut t = unsafe { ffi::luaL_makeseed(std::ptr::null_mut()) };
        if t == 0 {
            t = 1;
        }
        let _ = SEED.compare_exchange(ret, t, Ordering::AcqRel, Ordering::Acquire);
        ret = SEED.load(Ordering::Acquire);
    }
    ret
}

extern "C-unwind" fn lua_actor_protect_init(state: LuaState) -> c_int {
    unsafe {
        let param = ffi::lua_touserdata(state.as_ptr(), 1) as *mut LuaActorParam;
        if param.is_null() {
            laux::lua_error(state, "invalid param".to_string());
        }

        ffi::luaL_openlibs(state.as_ptr());

        lua_require!(state, "moon.core", luaopen_core);
        lua_require!(
            state,
            "coroutine.switchl",
            crate::lua_coroutine::luaopen_coroutine
        );

        luaopen_custom_libs(state);

        let source = CString::new((*param).source.as_str()).unwrap();
        if ffi::luaL_loadfile(state.as_ptr(), source.as_ptr()) != ffi::LUA_OK {
            return 1;
        }

        let params = CString::new((*param).params.as_str()).unwrap();
        if ffi::luaL_dostring(state.as_ptr(), params.as_ptr()) != ffi::LUA_OK {
            return 1;
        }

        ffi::lua_call(state.as_ptr(), 1, 0);

        0
    }
}

unsafe extern "C" fn allocator(
    extra: *mut c_void,
    ptr: *mut c_void,
    osize: usize,
    nsize: usize,
) -> *mut c_void {
    unsafe {
        let actor = &mut *(extra as *mut LuaActor);

        if nsize == 0 {
            if !ptr.is_null() {
                let layout = Layout::from_size_align_unchecked(osize, moon_base::SYS_MIN_ALIGN);
                alloc::dealloc(ptr as *mut u8, layout);
                actor.mem -= osize as isize;
            }
            return std::ptr::null_mut();
        }

        // Do not allocate more than isize::MAX
        if nsize > isize::MAX as usize {
            return std::ptr::null_mut();
        }

        // Are we fit to the memory limits?
        let mut mem_diff = nsize as isize;
        if !ptr.is_null() {
            mem_diff -= osize as isize;
        }

        let mem_limit = actor.mem_limit;
        let new_used_memory = actor.mem + mem_diff;

        if mem_limit > 0 && new_used_memory > mem_limit {
            log::error!(
                "Actor id:{:?} name:{:?} memory limit exceed: {}",
                actor.id,
                actor.name,
                actor.mem_limit
            );
            return std::ptr::null_mut();
        }

        actor.mem += mem_diff;

        if actor.mem > actor.mem_warning {
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
            let new_layout = match Layout::from_size_align(nsize, moon_base::SYS_MIN_ALIGN) {
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
        let old_layout = Layout::from_size_align_unchecked(osize, moon_base::SYS_MIN_ALIGN);
        let new_ptr = alloc::realloc(ptr as *mut u8, old_layout, nsize) as *mut c_void;
        if new_ptr.is_null() {
            alloc::handle_alloc_error(old_layout);
        }
        new_ptr
    }
}

fn handle_message(
    actor: &mut LuaActor,
    m: &mut Message,
    rx: &mut mpsc::UnboundedReceiver<Message>,
    watchdog: &Watchdog,
) -> bool {
    if m.ptype() == context::PTYPE_QUIT {
        if actor.id == context::BOOTSTRAP_ACTOR_ADDR {
            CONTEXT.shutdown(0);
        }
        actor.ok = false;

        let err = "actor quited";
        while let Ok(m) = rx.try_recv() {
            // Only fail messages that carry a pending request session; fire-and-forget
            // notifications (session == 0, e.g. the PTYPE_SHUTDOWN this actor enqueues
            // to itself while quitting) have no caller waiting and must not be logged.
            if m.session != 0 {
                CONTEXT.response_error(m.to, m.from, m.session, err.to_string());
            }
        }

        log::info!(
            "{0:08X}| Actor id:0x{:08X} name:{1:?} stoped. ({2}:{3})",
            actor.id,
            actor.name,
            file!(),
            line!()
        );
        return false;
    }

    watchdog.begin(CONTEXT.clock_ms(), m.ptype(), m.from, m.to, m.session);
    handle(actor, m);
    watchdog.end();
    true
}

fn actor_started(actor: &LuaActor, params: &LuaActorParam) {
    log::info!(
        "{0:08X}| Actor id:0x{:08X} name:{1:?} started. ({2}:{3})",
        actor.id,
        actor.name,
        file!(),
        line!()
    );

    if params.creator != 0 {
        let _ = CONTEXT.send(Message {
            from: actor.id,
            to: params.creator,
            session: params.session,
            data: MessageBody::ISize(context::PTYPE_INTEGER, actor.id as isize),
        });
    }
}

fn actor_init_failed(params: &LuaActorParam, err: String) {
    if params.creator != 0 {
        let _ = CONTEXT.send(Message {
            from: params.id,
            to: params.creator,
            session: params.session,
            data: MessageBody::ISize(context::PTYPE_INTEGER, 0),
        });
    }
    log::error!("Create actor failed: {}. ({}:{})", err, file!(), line!());
}

fn run_actor_blocking(
    params: LuaActorParam,
    tx: mpsc::UnboundedSender<Message>,
    mut rx: mpsc::UnboundedReceiver<Message>,
) {
    match init(&params, tx) {
        Ok((mut actor, watchdog)) => {
            watchdog.set_active_l(actor.callback_state.0 as *mut std::ffi::c_void);
            actor_started(&actor, &params);
            let mut buffer = Vec::new();
            loop {
                buffer.clear();
                let n = rx.blocking_recv_many(&mut buffer, 16);
                if n == 0 {
                    break; // channel closed
                }
                if !dispatch_batch(&mut actor, &mut buffer, &mut rx, &watchdog) {
                    break;
                }
            }
        }
        Err(err) => actor_init_failed(&params, err),
    }
    CONTEXT.remove_actor(params.id, &params.name);
}

async fn run_actor_async(
    params: LuaActorParam,
    tx: mpsc::UnboundedSender<Message>,
    mut rx: mpsc::UnboundedReceiver<Message>,
) {
    match init(&params, tx) {
        Ok((mut actor, watchdog)) => {
            watchdog.set_active_l(actor.callback_state.0 as *mut std::ffi::c_void);
            actor_started(&actor, &params);
            while let Some(mut m) = rx.recv().await {
                if !handle_message(&mut actor, &mut m, &mut rx, &watchdog) {
                    break;
                }
            }
        }
        Err(err) => actor_init_failed(&params, err),
    }
    CONTEXT.remove_actor(params.id, &params.name);
}

fn dispatch_batch(
    actor: &mut LuaActor,
    buffer: &mut [Message],
    rx: &mut mpsc::UnboundedReceiver<Message>,
    watchdog: &Watchdog,
) -> bool {
    for m in buffer.iter_mut() {
        if !handle_message(actor, m, rx, watchdog) {
            return false;
        }
    }
    true
}

pub fn new_actor(params: LuaActorParam) {
    let (tx, rx) = mpsc::unbounded_channel();

    if params.unique {
        std::thread::Builder::new()
            .name(format!("actor-{}", params.name))
            .spawn(move || {
                run_actor_blocking(params, tx, rx);
            })
            .expect("failed to spawn exclusive thread");
    } else {
        CONTEXT.main_handle().spawn(async move {
            run_actor_async(params, tx, rx).await;
        });
    }
}

pub fn init(
    params: &LuaActorParam,
    tx: mpsc::UnboundedSender<Message>,
) -> Result<(Box<LuaActor>, Arc<Watchdog>), String> {
    let mut actor = Box::new(LuaActor::new(params));
    let watchdog = CONTEXT.add_actor(&mut actor, tx)?;
    actor.watchdog = Arc::as_ptr(&watchdog);

    //log::info!("init actor id: {} name: {}", id, params.name);
    unsafe {
        let main_state = ffi::lua_newstate(
            allocator,
            actor.deref() as *const LuaActor as *mut c_void,
            global_seed(),
        );
        if main_state.is_null() {
            return Err("lua_newstate failed".to_string());
        }

        let state = LuaState::new(main_state).unwrap();

        actor.set_main_state(state);

        ffi::lua_gc(main_state, ffi::LUA_GCSTOP, 0);
        ffi::lua_gc(main_state, ffi::LUA_GCGEN, 0, 0);

        ffi::lua_pushcfunction(main_state, not_null_wrapper!(laux::lua_traceback));
        let trace_fn = ffi::lua_gettop(main_state);

        ffi::lua_pushcfunction(main_state, not_null_wrapper!(lua_actor_protect_init));

        let p = params as *const LuaActorParam as *mut c_void;
        ffi::lua_pushlightuserdata(main_state, p);

        if ffi::lua_pcall(main_state, 1, ffi::LUA_MULTRET, trace_fn) != ffi::LUA_OK
            || ffi::lua_gettop(main_state) != 1
        {
            return Err(format!(
                "init actor failed: {}",
                laux::lua_opt_str(state, -1).unwrap_or("no error message")
            ));
        }

        ffi::lua_pop(main_state, 1);
        ffi::lua_gc(main_state, ffi::LUA_GCRESTART, 0);
        assert_eq!(ffi::lua_gettop(main_state), 0);
    }

    actor.ok = true;

    Ok((actor, watchdog))
}

fn handle(actor: &mut LuaActor, m: &mut Message) {
    if !actor.ok {
        return;
    }

    debug_assert!(!actor.callback_state.0.is_null(), "moon_rs not initialized");
    let callback_state = actor.callback_state.0;

    let (msg_to, msg_from, msg_session) = (m.to, m.from, m.session);

    unsafe {
        let trace = 1;
        ffi::luaL_checkstack(callback_state, 8, cstr!("message dispatch"));
        ffi::lua_pushvalue(callback_state, 2);

        ffi::lua_pushinteger(callback_state, m.ptype() as ffi::lua_Integer);
        ffi::lua_pushinteger(callback_state, m.from as ffi::lua_Integer);
        ffi::lua_pushinteger(callback_state, m.session as ffi::lua_Integer);
        ffi::lua_pushlightuserdata(callback_state, m as *mut Message as *mut c_void);

        let r = ffi::lua_pcall(callback_state, 4, 0, trace);
        if r == ffi::LUA_OK {
            return;
        }

        let err = match r {
            ffi::LUA_ERRRUN => {
                format!(
                    "actor '{}' dispatch message error:\n{}",
                    actor.name,
                    laux::lua_opt_str(LuaState::new(callback_state).unwrap(), -1)
                        .unwrap_or("no error message")
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

        laux::lua_pop(LuaState::new(callback_state).unwrap(), 1);

        // If watchdog interrupt just fired (trap was reset to 0 by signal_hook),
        // send the error with traceback to bootstrap as a system notification.
        let wd = actor.watchdog;
        if !wd.is_null() && (*wd).timeout_count.load(Ordering::Relaxed) >= 3 {
            let notify = format!("timeout_kill,From 0x{:08X} {}", actor.id, err);
            let _ = CONTEXT.send(Message {
                from: actor.id,
                to: context::BOOTSTRAP_ACTOR_ADDR,
                session: 0,
                data: MessageBody::Buffer(
                    context::PTYPE_SYSTEM,
                    Box::new(notify.into_bytes().into()),
                ),
            });
        }

        CONTEXT.response_error(msg_to, msg_from, msg_session, err.to_string());
    }
}

pub fn remove_actor(id: context::ActorId) -> Result<(), String> {
    // Only deliver PTYPE_QUIT; do NOT remove the registry entry here. The actor's
    // run loop processes the quit message, exits, and then calls
    // `CONTEXT.remove_actor`, which is the single owner of teardown (map removal,
    // `actor_counter` decrement, `unique_actors` cleanup, and exit notification).
    // Removing the entry here would race with that teardown: the method would see
    // the entry already gone and skip the counter decrement, leaving `stopped()`
    // permanently false and hanging shutdown.
    match CONTEXT.send(Message {
        from: 0,
        to: id,
        session: 0,
        data: MessageBody::None(context::PTYPE_QUIT),
    }) {
        None => Ok(()),
        Some(_) => Err(format!("not found actor id= {}", id)),
    }
}

extern "C-unwind" fn lua_actor_query(state: LuaState) -> c_int {
    if laux::lua_type(state, 1) == LuaType::Integer {
        return 1;
    }

    let name = unsafe { laux::lua_check_str(state, 1) };

    if let Some(addr) = CONTEXT.query(name) {
        laux::lua_push(state, *addr.value());
    } else {
        laux::lua_push(state, 0);
    }
    1
}

extern "C-unwind" fn lua_actor_send(state: LuaState) -> c_int {
    let ptype = laux::lua_get(state, 1);

    if ptype == 0 {
        laux::lua_arg_error(state, 1, cstr!("PTYPE must > 0"));
    }

    let to: context::ActorId = laux::lua_get(state, 2);
    if to == 0 {
        laux::lua_arg_error(state, 2, cstr!("receiver must > 0"));
    }

    let data = check_buffer(state, 3);

    let actor = LuaActor::from_lua_state(state);

    let session = laux::lua_opt(state, 4).unwrap_or(unsafe { (*actor).next_session() });

    let from: context::ActorId = laux::lua_opt(state, 5).unwrap_or(unsafe { (*actor).id });

    if let Some(m) = CONTEXT.send(Message {
        from,
        to,
        session: -session,
        data: MessageBody::Buffer(ptype, data),
    }) {
        CONTEXT.response_error(
            m.to,
            m.from,
            m.session,
            format!(
                "Dead service 0x{:08x} recv message from 0x{:08x}: {}.",
                to, from, m.data
            ),
        );
    }

    laux::lua_push(state, session);
    laux::lua_push(state, to);

    2
}

extern "C-unwind" fn lua_kill_actor(state: LuaState) -> c_int {
    let who: context::ActorId = laux::lua_get(state, 1);
    let res = remove_actor(who);
    match res {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(err) => crate::lua_push_error(state, &err),
    }
}

extern "C-unwind" fn lua_new_actor(state: LuaState) -> c_int {
    laux::lua_checktype(state, 1, ffi::LUA_TTABLE);

    let actor = LuaActor::from_lua_state(state);

    let creator = unsafe { (*actor).id };
    let session = unsafe { (*actor).next_session() };
    let name: String = laux::opt_field(state, 1, "name").unwrap_or_default();
    let source = laux::opt_field(state, 1, "source").unwrap_or_default();
    let memlimit: i64 = laux::opt_field(state, 1, "memlimit").unwrap_or_default();
    let unique: bool = laux::opt_field(state, 1, "unique").unwrap_or_default();

    let mut params: String = laux::lua_get(state, 2);
    if let Some(p) = CONTEXT.get_env("PATH") {
        params = (*p).clone() + params.as_str();
    }

    new_actor(LuaActorParam {
        id: CONTEXT.next_actor_id(),
        unique,
        creator,
        session,
        memlimit,
        name,
        source,
        params,
        block: false,
    });

    laux::lua_push(state, session);

    1
}

extern "C-unwind" fn lua_actor_callback(state: LuaState) -> c_int {
    unsafe {
        ffi::luaL_checktype(state.as_ptr(), 1, ffi::LUA_TFUNCTION);
        ffi::lua_settop(state.as_ptr(), 1);
        let actor = LuaActor::from_lua_state(state);
        ffi::lua_newuserdatauv(state.as_ptr(), 1, 1);
        let callback_state = ffi::lua_newthread(state.as_ptr());
        ffi::lua_pushcfunction(callback_state, not_null_wrapper!(laux::lua_traceback));
        ffi::lua_setuservalue(state.as_ptr(), -2);
        ffi::lua_setfield(
            state.as_ptr(),
            ffi::LUA_REGISTRYINDEX,
            cstr!("callback_context"),
        );
        ffi::lua_xmove(state.as_ptr(), callback_state, 1);
        (*actor).callback_state = LuaThread::new(callback_state);
        0
    }
}

extern "C-unwind" fn lua_timeout(state: LuaState) -> c_int {
    let interval: i64 = laux::lua_get(state, 1);
    let actor = LuaActor::from_lua_state(state);
    let owner = unsafe { (*actor).id };
    let timer_id = unsafe { (*actor).next_session() };

    if interval <= 0 {
        let _ = CONTEXT.send(Message {
            from: 0,
            to: owner,
            session: 0,
            data: MessageBody::ISize(context::PTYPE_TIMER, timer_id as isize),
        });
    } else {
        context::insert_timer(owner, timer_id, interval as u64);
    }

    laux::lua_push(state, timer_id);
    1
}

extern "C-unwind" fn lua_loglevel(state: LuaState) -> c_int {
    if laux::lua_top(state) == 0 {
        laux::lua_push(state, LOGGER.get_log_level() as u8);
        return 1;
    }

    let level = laux::lua_get(state, 1);
    LOGGER.set_log_level(Logger::string_to_level(level));
    0
}

extern "C-unwind" fn lua_actor_log(state: LuaState) -> c_int {
    let log_level = laux::lua_get(state, 1);
    let stack_level: i32 = laux::lua_get(state, 2);
    let actor = LuaActor::from_lua_state(state);

    let mut content = LOGGER.make_line(true, Logger::u8_to_level(log_level), 256);
    content.write_str(format!("{:08X}| ", unsafe { (*actor).id }).as_str());

    let top = laux::lua_top(state);
    for i in 3..=top {
        if i > 3 {
            content.write_str("    ");
        }

        content.write_slice(unsafe { laux::lua_as_slice(state, i) });
        laux::lua_pop(state, 1);
    }

    let mut debug: ffi::lua_Debug = unsafe { std::mem::zeroed() };
    if unsafe {
        ffi::lua_getstack(state.as_ptr(), stack_level as c_int, &mut debug) != 0
            && ffi::lua_getinfo(state.as_ptr(), cstr!("Sl"), &mut debug) != 0
    } {
        content.write_str("    ");
        content.write(b'(');
        if debug.srclen > 1 {
            let file_name = unsafe { slice::from_raw_parts(debug.source as *mut u8, debug.srclen) };
            if file_name[0] == b'@' {
                content.write_slice(&file_name[1..]);
            } else {
                content.write_slice(file_name);
            }
        }
        content.write(b':');
        content.write_str(debug.currentline.to_string().as_str());
        content.write(b')');
    }

    LOGGER.write(content);

    0
}

extern "C-unwind" fn lua_actor_exit(state: LuaState) -> c_int {
    let exit_code = laux::lua_get(state, 1);
    CONTEXT.shutdown(exit_code);
    0
}

extern "C-unwind" fn env(state: LuaState) -> c_int {
    if laux::lua_top(state) == 2 {
        let key = unsafe { laux::lua_check_str(state, 1) };
        let value = unsafe { laux::lua_check_str(state, 2) };
        CONTEXT.set_env(key, value);
        0
    } else {
        let key = unsafe { laux::lua_check_str(state, 1) };
        if let Some(value) = CONTEXT.get_env(key) {
            laux::lua_push(state, value.as_str());
            1
        } else {
            0
        }
    }
}

extern "C-unwind" fn clock(state: LuaState) -> c_int {
    laux::lua_push(state, CONTEXT.clock());
    1
}

extern "C-unwind" fn now(state: LuaState) -> c_int {
    let unit: i64 = laux::lua_opt(state, 1).unwrap_or(1);
    let unit = unit.max(1);
    laux::lua_push(state, CONTEXT.now().timestamp_millis() / unit);
    1
}

fn get_message_pointer(state: LuaState) -> *mut Message {
    let m = unsafe { ffi::lua_touserdata(state.as_ptr(), 1) as *mut Message };
    if m.is_null() {
        unsafe { ffi::luaL_argerror(state.as_ptr(), 1, cstr!("null message pointer")) };
    }
    m
}

extern "C-unwind" fn lua_message_decode(state: LuaState) -> c_int {
    let m = get_message_pointer(state);
    let opt = unsafe { laux::lua_check_str(state, 2) };
    let top = unsafe { ffi::lua_gettop(state.as_ptr()) };
    // Access the Message only through the raw pointer with transient,
    // narrowly-scoped (re)borrows; never mint a long-lived `&mut Message`.
    for c in opt.chars() {
        match c {
            'T' => {
                laux::lua_push(state, unsafe { (*m).ptype() });
            }
            'S' => {
                laux::lua_push(state, unsafe { (*m).from });
            }
            'R' => {
                laux::lua_push(state, unsafe { (*m).to });
            }
            'E' => {
                laux::lua_push(state, unsafe { (*m).session });
            }
            'Z' => {
                if let MessageBody::Buffer(_, data) = unsafe { &(*m).data } {
                    laux::lua_push(state, data.as_slice());
                } else {
                    laux::lua_pushnil(state);
                }
            }
            'N' => {
                if let MessageBody::Buffer(_, data) = unsafe { &(*m).data } {
                    laux::lua_push(state, data.len());
                } else {
                    laux::lua_push(state, 0);
                }
            }
            'B' => {
                if let MessageBody::Buffer(_, data) = unsafe { &mut (*m).data } {
                    laux::lua_pushlightuserdata(state, data.as_mut().as_pointer() as *mut c_void);
                } else {
                    laux::lua_pushnil(state);
                }
            }
            'C' => {
                if let MessageBody::Buffer(_, data) = unsafe { &(*m).data } {
                    laux::lua_pushlightuserdata(state, data.as_ptr() as *mut c_void);
                    laux::lua_push(state, data.len());
                } else {
                    laux::lua_pushlightuserdata(state, std::ptr::null_mut());
                    laux::lua_push(state, 0);
                }
            }
            _ => {
                laux::lua_error(state, format!("invalid format option '{0}'", c));
            }
        }
    }
    laux::lua_top(state) - top
}

extern "C-unwind" fn lua_decode_message_payload(state: LuaState) -> c_int {
    laux::lua_checkstack(state, 16, std::ptr::null());
    let m = get_message_pointer(state);
    unsafe {
        let ptype = (*m).ptype();
        let decode = crate::DECODERS[ptype as usize];
        decode(state, m)
    }
}

extern "C-unwind" fn next_session(state: LuaState) -> c_int {
    laux::lua_push(state, unsafe {
        (*LuaActor::from_lua_state(state)).next_session()
    });
    1
}

unsafe extern "C-unwind" fn luaopen_core(state: LuaState) -> c_int {
    let l = [
        lreg!("new_service", lua_new_actor),
        lreg!("query", lua_actor_query),
        lreg!("kill", lua_kill_actor),
        lreg!("send", lua_actor_send),
        lreg!("log", lua_actor_log),
        lreg!("loglevel", lua_loglevel),
        lreg!("callback", lua_actor_callback),
        lreg!("exit", lua_actor_exit),
        lreg!("timeout", lua_timeout),
        lreg!("decode", lua_message_decode),
        lreg!("decode_message", lua_decode_message_payload),
        lreg!("env", env),
        lreg!("clock", clock),
        lreg!("now", now),
        lreg!("next_session", next_session),
        lreg_null!(),
    ];

    unsafe {
        ffi::lua_createtable(state.as_ptr(), 0, l.len() as c_int);
        ffi::luaL_setfuncs(state.as_ptr(), l.as_ptr() as *const luaL_Reg, 0);

        let actor = LuaActor::from_lua_state(state);
        laux::lua_push(state, (*actor).id);
        ffi::lua_setfield(state.as_ptr(), -2, cstr!("id"));

        laux::lua_push(state, (*actor).name.as_str());
        ffi::lua_setfield(state.as_ptr(), -2, cstr!("name"));

        1
    }
}
