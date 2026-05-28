use crate::{
    lua_check_buffer, lua_check_integer, lua_check_lightuserdata_bytes, lua_check_str,
    lua_check_typed_lightuserdata_mut, lua_opt_integer, lua_opt_str, lua_push_error,
    luaopen_custom_libs, opt_field_bool, opt_field_int, opt_field_str,
};
use actor::{
    actor::LuaActor,
    context::{self, CONTEXT, LOGGER, LuaActorParam, Message, MessageBody},
    log::Logger,
};
use luars::{CFunction, LuaFunction, Lua, LuaApi, LuaResult, LuaState, LuaValue};
use std::ffi::c_void;
use std::ptr::NonNull;
use tokio::sync::mpsc;

/// Thin wrapper around a raw pointer to `LuaActor`.
///
/// Unlike `&'static mut LuaActor`, this does NOT create a Rust mutable reference,
/// so it does not trigger the aliasing rules that would make concurrent access UB.
/// Safety is guaranteed by the actor model: each actor runs on a single Tokio task,
/// and messages are processed serially.
#[derive(Clone, Copy)]
pub struct ActorRef(NonNull<LuaActor>);

impl ActorRef {
    pub fn from_state(state: &mut LuaState) -> Self {
        let ptr = state.extra_space() as *mut LuaActor;
        if ptr.is_null() {
            panic!("actor pointer not set");
        }
        Self(NonNull::new(ptr).expect("null actor pointer"))
    }

    #[inline]
    pub fn id(self) -> i64 {
        // Safety: single-threaded access guaranteed by actor model
        unsafe { (*self.0.as_ptr()).id }
    }

    #[inline]
    pub fn next_session(self) -> i64 {
        // Safety: single-threaded access guaranteed by actor model
        unsafe { (*self.0.as_ptr()).next_session() }
    }

    #[inline]
    pub fn set_callback(self, f: Option<LuaFunction>) {
        // Safety: single-threaded access guaranteed by actor model
        unsafe { (*self.0.as_ptr()).callback_fn = f }
    }
}

fn register_core_module(lua: &mut Lua, actor: &LuaActor) -> LuaResult<()> {
    let funcs: &[(&str, CFunction)] = &[
        ("new_service", lua_new_actor),
        ("query", lua_actor_query),
        ("kill", lua_kill_actor),
        ("send", lua_actor_send),
        ("log", lua_actor_log),
        ("loglevel", lua_loglevel),
        ("callback", lua_actor_callback),
        ("exit", lua_actor_exit),
        ("timeout", lua_timeout),
        ("decode", lua_message_decode),
        ("env", env),
        ("clock", clock),
        ("tostring", tostring_fn),
        ("next_session", next_session),
    ];

    let state = lua.global_state_mut();

    let table = state.create_table(0, funcs.len() + 2)?;
    for (name, func) in funcs {
        let key = state.create_string(name)?;
        state.raw_set(&table, key, LuaValue::cfunction(*func));
    }

    let id_key = state.create_string("id")?;
    state.raw_set(&table, id_key, LuaValue::integer(actor.id));

    let name_key = state.create_string("name")?;
    let name_val = state.create_string(&actor.name)?;
    state.raw_set(&table, name_key, name_val);

    let loaded_key = state.create_string("loaded")?;
    if let Ok(Some(package)) = state.get_global("package")
        && let Some(loaded) = state.raw_get(&package, &loaded_key)
    {
        let mod_key = state.create_string("moon.core")?;
        state.raw_set(&loaded, mod_key, table);
    }
    Ok(())
}

pub fn new_actor(params: LuaActorParam) {
    tokio::spawn(async move {
        match init(&params) {
            Ok(mut actor) => {
                log::info!(
                    "{0:08X}| Actor id:{0:?} name:{1:?} started. ({2}:{3})",
                    actor.id,
                    actor.name,
                    file!(),
                    line!()
                );

                if params.creator != 0 {
                    CONTEXT.send(Message {
                        ptype: context::PTYPE_INTEGER,
                        from: actor.id,
                        to: params.creator,
                        session: params.session,
                        data: MessageBody::ISize(actor.id as isize),
                    });
                }

                while let Some(m) = actor.rx.recv().await {
                    CONTEXT.update_monitor(m.ptype, CONTEXT.clock(), m.from, actor.id);
                    handle(&mut actor, m);
                    CONTEXT.update_monitor(0, 0.0, 0, 0);
                }

                log::info!(
                    "{0:08X}| Actor id:{0:?} name:{1:?} stopped. ({2}:{3})",
                    actor.id,
                    actor.name,
                    file!(),
                    line!()
                );
            }
            Err(err) => {
                if params.creator != 0 {
                    CONTEXT.send(Message {
                        ptype: context::PTYPE_INTEGER,
                        from: params.id,
                        to: params.creator,
                        session: params.session,
                        data: MessageBody::ISize(0),
                    });
                }
                log::error!("actor: create failed: {}. ({}:{})", err, file!(), line!());
            }
        }
        let unique_name = if params.unique { params.name.as_str() } else { "" };
        CONTEXT.remove_actor(params.id, unique_name);
    });
}

pub fn init(params: &LuaActorParam) -> Result<Box<LuaActor>, String> {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut actor = Box::new(LuaActor::new(params, rx).map_err(|e| e.to_string())?);
    CONTEXT
        .add_actor(&mut actor, tx)
        .map_err(|e| e.to_string())?;

    let actor_ptr = actor.as_mut() as *mut LuaActor;

    // Stop GC during initialization for performance (matches official Lua behavior)
    actor.lua_mut().gc_stop();

    luaopen_custom_libs(actor.lua_mut()).map_err(|e| format!("register libs: {}", e))?;

    actor.lua_mut().set_extra_space(actor_ptr as *mut c_void);

    register_core_module(actor.lua_mut(), unsafe { &*actor_ptr })
        .map_err(|e| format!("register core: {}", e))?;

    let source = &params.source;

    let exec_results = match actor.lua_mut().eval::<LuaValue>(&params.params) {
        Ok(val) => val,
        Err(e) => {
            let msg = actor.lua_mut().get_error_message(e);
            return Err(format!("execute params: {}", msg));
        }
    };

    let file_content =
        std::fs::read_to_string(source).map_err(|e| format!("read file {}: {}", source, e))?;

    let chunk_name = format!("@{}", source);
    if let Err(e) = actor
        .lua_mut()
        .load(&file_content)
        .set_name(&chunk_name)
        .call::<_, LuaValue>(exec_results)
    {
        let msg = actor.lua_mut().get_error_message(e);
        return Err(format!("load {}: {}", source, msg));
    };

    // Restart GC after initialization is complete
    actor.lua_mut().gc_restart();

    actor.ok = true;
    Ok(actor)
}

fn handle(actor: &mut LuaActor, mut m: Message) {
    if !actor.ok {
        return;
    }

    if actor.callback_fn.is_none() {
        log::error!("actor '{}' callback not set", actor.name);
        return;
    }

    if m.ptype == context::PTYPE_QUIT {
        if actor.id == context::BOOTSTRAP_ACTOR_ADDR {
            CONTEXT.shutdown(0);
        }
        actor.ok = false;

        let err = "actor exited";
        while let Ok(m) = actor.rx.try_recv() {
            CONTEXT.response_error(m.to, m.from, m.session, err.to_string());
        }
        actor.rx.close();
        return;
    }

    let callback_fn = actor.callback_fn.as_ref().unwrap();

    let mut args = Vec::with_capacity(6);
    args.push(LuaValue::integer(m.ptype as i64));
    args.push(LuaValue::integer(m.from));
    args.push(LuaValue::integer(m.session));

    match &mut m.data {
        MessageBody::Buffer(data) => {
            args.push(LuaValue::lightuserdata(data.as_ptr() as *mut c_void));
            args.push(LuaValue::integer(data.len() as i64));
        }
        MessageBody::ISize(data) => {
            args.push(LuaValue::integer(*data as i64));
            args.push(LuaValue::integer(0));
        }
        MessageBody::Boxed(boxed) => {
            let ptr = boxed.into_raw();
            args.push(LuaValue::lightuserdata(ptr as *mut c_void));
            args.push(LuaValue::integer(0));
        }
        MessageBody::None => {
            args.push(LuaValue::lightuserdata(std::ptr::null_mut()));
            args.push(LuaValue::integer(0));
        }
    }

    args.push(LuaValue::lightuserdata(
        &mut m as *mut Message as *mut c_void,
    ));

    match callback_fn.call::<_, LuaValue>(args) {
        Ok(_) => {}
        Err(e) => {
            let msg = actor.lua_mut().get_error_message(e);
            let err = format!("actor '{}' dispatch error:\n{}", actor.name, msg);
            CONTEXT.response_error(m.to, m.from, m.session, err);
        }
    }
}

pub fn remove_actor(id: i64) -> Result<(), actor::error::MoonError> {
    if let Some(res) = CONTEXT.remove(id) {
        return match res.1.send(Message {
            ptype: context::PTYPE_QUIT,
            from: 0,
            to: id,
            session: 0,
            data: MessageBody::None,
        }) {
            Ok(_) => Ok(()),
            Err(err) => Err(format!("actor: send failed: {}", err).into()),
        };
    }
    Err(format!("actor: not found (id={})", id).into())
}

fn lua_actor_query(state: &mut LuaState) -> LuaResult<usize> {
    let arg = state.get_arg(1).unwrap_or(LuaValue::nil());
    if arg.is_integer() {
        state.push_value(arg)?;
        return Ok(1);
    }

    let name = arg.as_str().unwrap_or("");
    if let Some(addr) = CONTEXT.query(name) {
        state.push_value(LuaValue::integer(*addr.value()))?;
    } else {
        state.push_value(LuaValue::integer(0))?;
    }
    Ok(1)
}

fn lua_actor_send(state: &mut LuaState) -> LuaResult<usize> {
    let ptype: u8 = lua_check_integer(state, 1)?;

    if ptype == 0 {
        return Err(state.error("bad argument #1 (ptype must be > 0)".to_string()));
    }

    let to: i64 = lua_check_integer(state, 2)?;
    if to <= 0 {
        return Err(state.error("bad argument #2 (receiver must be > 0)".to_string()));
    }

    let data = lua_check_buffer(state, 3)?;

    let actor = ActorRef::from_state(state);
    let from = actor.id();
    let session: i64 = lua_opt_integer(state, 4).unwrap_or_else(|| actor.next_session());

    if let Some(m) = CONTEXT.send(Message {
        ptype,
        from,
        to,
        session: -session,
        data: MessageBody::Buffer(data),
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

    state.push_value(LuaValue::integer(session))?;
    state.push_value(LuaValue::integer(to))?;
    Ok(2)
}

fn lua_kill_actor(state: &mut LuaState) -> LuaResult<usize> {
    let who: i64 = lua_check_integer(state, 1)?;
    match remove_actor(who) {
        Ok(_) => {
            state.push_value(LuaValue::boolean(true))?;
            Ok(1)
        }
        Err(err) => lua_push_error(state, &err.to_string()),
    }
}

fn lua_new_actor(state: &mut LuaState) -> LuaResult<usize> {
    let arg1 = state.get_arg(1).unwrap_or(LuaValue::nil());
    if !arg1.is_table() {
        return Err(state.error("bad argument #1 (table expected)".to_string()));
    }

    let actor = ActorRef::from_state(state);
    let creator = actor.id();
    let session = actor.next_session();

    let name = opt_field_str(state, &arg1, "name").unwrap_or_default();
    let source = opt_field_str(state, &arg1, "source").unwrap_or_default();
    let memlimit = opt_field_int(state, &arg1, "memlimit").unwrap_or(0);
    let unique = opt_field_bool(state, &arg1, "unique").unwrap_or(false);

    let mut params: String = lua_opt_str(state, 2)
        .map(|s| s.to_string())
        .unwrap_or_default();

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

    state.push_value(LuaValue::integer(session))?;
    Ok(1)
}

fn lua_actor_callback(state: &mut LuaState) -> LuaResult<usize> {
    let callback_fn = state.get_arg_as::<LuaFunction>(1)?;
    if callback_fn.is_none() {
        return Err(state.error("bad argument #1 (function expected)".to_string()));
    }
    let actor = ActorRef::from_state(state);
    actor.set_callback(callback_fn);
    Ok(0)
}

fn lua_timeout(state: &mut LuaState) -> LuaResult<usize> {
    let interval: i64 = lua_check_integer(state, 1)?;
    let actor = ActorRef::from_state(state);
    let owner = actor.id();
    let timer_id = actor.next_session();

    if interval <= 0 {
        CONTEXT.send(Message {
            ptype: context::PTYPE_TIMER,
            from: timer_id,
            to: owner,
            session: 0,
            data: MessageBody::None,
        });
    } else {
        context::insert_timer(owner, timer_id, interval as u64);
    }

    state.push_value(LuaValue::integer(timer_id))?;
    Ok(1)
}

fn lua_loglevel(state: &mut LuaState) -> LuaResult<usize> {
    if state.arg_count() == 0 {
        state.push_value(LuaValue::integer(LOGGER.get_log_level() as i64))?;
        return Ok(1);
    }

    let level = lua_opt_str(state, 1).unwrap_or("info");
    LOGGER.set_log_level(Logger::string_to_level(level.to_string()));
    Ok(0)
}

fn lua_actor_log(state: &mut LuaState) -> LuaResult<usize> {
    let log_level: u8 = lua_check_integer(state, 1)?;
    let stack_level: usize = lua_opt_integer(state, 2).unwrap_or(0);
    let actor = ActorRef::from_state(state);

    let mut content = LOGGER.make_line(true, Logger::u8_to_level(log_level), 256);
    content.write_str(format!("{:08X}| ", actor.id()).as_str());

    let top = state.arg_count();

    for i in 3..=top {
        if i > 3 {
            content.write_str("    ");
        }
        if let Some(val) = state.get_arg(i) {
            if let Some(s) = val.as_str() {
                content.write_slice(s.as_bytes());
            } else {
                let s = state.to_string(&val)?;
                content.write_str(&s);
            }
        }
    }

    if let Some(debug) = state.get_info_by_level(stack_level, "Sl") {
        content.write_str("    (");
        if let Some(src) = debug.source {
            if let Some(s) = src.strip_prefix('@') {
                content.write_str(s);
            } else {
                content.write_str(&src);
            }
        }
        content.write(b':');
        content.write_str(debug.currentline.unwrap_or(-1).to_string().as_str());
        content.write(b')');
    }

    LOGGER.write(content);
    Ok(0)
}

fn lua_actor_exit(state: &mut LuaState) -> LuaResult<usize> {
    let exit_code: i32 = lua_opt_integer(state, 1).unwrap_or(0);
    CONTEXT.shutdown(exit_code);
    Ok(0)
}

fn env(state: &mut LuaState) -> LuaResult<usize> {
    if state.arg_count() == 2 {
        let key = lua_check_str(state, 1)?;
        let value = lua_check_str(state, 2)?;
        CONTEXT.set_env(key, value);
        Ok(0)
    } else {
        let key = lua_check_str(state, 1)?;
        if let Some(value) = CONTEXT.get_env(key) {
            let s = state.create_string(value.as_str())?;
            state.push_value(s)?;
            Ok(1)
        } else {
            Ok(0)
        }
    }
}

fn clock(state: &mut LuaState) -> LuaResult<usize> {
    state.push_value(LuaValue::float(CONTEXT.clock()))?;
    Ok(1)
}

fn tostring_fn(state: &mut LuaState) -> LuaResult<usize> {
    let arg = state.get_arg(1).unwrap_or(LuaValue::nil());
    if arg.is_lightuserdata() {
        let len: usize = lua_opt_integer(state, 2).unwrap_or(0);
        let data = lua_check_lightuserdata_bytes(state, 1, len)?;
        let s = state.create_bytes(data)?;
        state.push_value(s)?;
        Ok(1)
    } else {
        Err(state.error("bad argument #1 (lightuserdata expected)".to_string()))
    }
}

fn lua_message_decode(state: &mut LuaState) -> LuaResult<usize> {
    let m = lua_check_typed_lightuserdata_mut::<Message>(state, 1)?;

    let opt = state
        .get_arg(2)
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();

    let mut count = 0usize;
    for c in opt.chars() {
        match c {
            'T' => {
                state.push_value(LuaValue::integer(m.ptype as i64))?;
                count += 1;
            }
            'S' => {
                state.push_value(LuaValue::integer(m.from))?;
                count += 1;
            }
            'R' => {
                state.push_value(LuaValue::integer(m.to))?;
                count += 1;
            }
            'E' => {
                state.push_value(LuaValue::integer(m.session))?;
                count += 1;
            }
            'Z' => {
                if let MessageBody::Buffer(data) = &m.data {
                    let s = state.create_bytes(data.as_slice())?;
                    state.push_value(s)?;
                } else {
                    state.push_value(LuaValue::nil())?;
                }
                count += 1;
            }
            'N' => {
                if let MessageBody::Buffer(data) = &m.data {
                    state.push_value(LuaValue::integer(data.len() as i64))?;
                } else {
                    state.push_value(LuaValue::integer(0))?;
                }
                count += 1;
            }
            'B' => {
                if let MessageBody::Buffer(data) = &mut m.data {
                    state.push_value(LuaValue::lightuserdata(
                        data.as_mut().as_pointer() as *mut c_void
                    ))?;
                } else {
                    state.push_value(LuaValue::nil())?;
                }
                count += 1;
            }
            'C' => {
                if let MessageBody::Buffer(data) = &m.data {
                    state.push_value(LuaValue::lightuserdata(data.as_ptr() as *mut c_void))?;
                    state.push_value(LuaValue::integer(data.len() as i64))?;
                } else {
                    state.push_value(LuaValue::lightuserdata(std::ptr::null_mut()))?;
                    state.push_value(LuaValue::integer(0))?;
                }
                count += 2;
            }
            _ => {
                return Err(state.error(format!("bad format option '{}'", c)));
            }
        }
    }
    Ok(count)
}

fn next_session(state: &mut LuaState) -> LuaResult<usize> {
    let actor = ActorRef::from_state(state);
    state.push_value(LuaValue::integer(actor.next_session()))?;
    Ok(1)
}
