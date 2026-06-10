use crate::context::{ActorId, LuaActorParam, Watchdog};
use moon_lua::laux::{LuaGlobalState, LuaState, LuaThread};

pub use moon_lua as ffi;

pub struct LuaActor {
    pub ok: bool,
    pub unique: bool,
    pub id: ActorId,
    uuid: i64,
    pub name: String,
    pub main_state: Option<LuaGlobalState>,
    pub callback_state: LuaThread,
    pub mem: isize,
    pub mem_limit: isize,
    pub mem_warning: isize,
    /// Raw pointer to the per-actor Watchdog (kept alive by Arc<Watchdog> in
    /// ActorEntry). Used by lua_coroutine.rs switchL and signal_hook via
    /// extraspace chain.
    pub watchdog: *const Watchdog,
}

unsafe impl Send for LuaActor {}

impl LuaActor {
    pub fn new(params: &LuaActorParam) -> Self {
        LuaActor {
            ok: false,
            unique: params.unique,
            id: params.id,
            uuid: 0,
            name: params.name.clone(),
            main_state: None,
            callback_state: LuaThread::new(std::ptr::null_mut()),
            mem: 0,
            mem_limit: params.memlimit as isize,
            mem_warning: 8 * 1024 * 1024,
            watchdog: std::ptr::null(),
        }
    }

    pub fn set_main_state(&mut self, state: LuaState) {
        self.main_state = Some(LuaGlobalState::new(state));
        set_extra_object(state, self);
    }

    pub fn from_lua_state(state: LuaState) -> *mut Self {
        get_extra_object::<Self>(state)
    }

    pub fn next_session(&mut self) -> i64 {
        self.uuid += 1;
        self.uuid
    }
}

fn set_extra_object<T>(state: LuaState, obj: &T) {
    unsafe {
        let space = ffi::lua_getextraspace(state.as_ptr()) as *mut usize;
        std::ptr::write(space, obj as *const T as usize);
    }
}

fn get_extra_object<T>(state: LuaState) -> *mut T {
    unsafe {
        let space = ffi::lua_getextraspace(state.as_ptr()) as *mut usize;
        let v = std::ptr::read(space);
        debug_assert!(v != 0);
        v as *mut T
    }
}
