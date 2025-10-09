use crate::context::LuaActorParam;
use lib_lua::laux::{LuaState, LuaStateBox, LuaThread};

use super::context::Message;
pub use lib_lua as ffi;
use tokio::sync::mpsc;

pub struct LuaActor {
    pub ok: bool,
    pub unique: bool,
    pub id: i64,
    uuid: i64,
    pub name: String,
    pub rx: mpsc::UnboundedReceiver<Message>,
    pub main_state: Option<LuaStateBox>,
    pub callback_state: LuaThread,
    pub mem: isize,
    pub mem_limit: isize,
    pub mem_warning: isize,
}

impl LuaActor {
    pub fn new(params: &LuaActorParam, rx: mpsc::UnboundedReceiver<Message>) -> Self {
        LuaActor {
            ok: false,
            unique: params.unique,
            id: params.id,
            uuid: 0,
            name: params.name.clone(),
            rx,
            main_state: None,
            callback_state: LuaThread::new(std::ptr::null_mut()),
            mem: 0,
            mem_limit: 0,
            mem_warning: 8 * 1024 * 1024,
        }
    }

    pub fn set_main_state(&mut self, state: LuaState) {
        self.main_state = Some(LuaStateBox::new(state));
        set_extra_object(state, self);
    }

    pub fn from_lua_state(state: LuaState) -> &'static mut Self {
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

fn get_extra_object<T>(state: LuaState) -> &'static mut T {
    unsafe {
        let space = ffi::lua_getextraspace(state.as_ptr()) as *mut usize;
        let v = std::ptr::read(space);
        debug_assert!(v != 0);
        &mut *(v as *mut T)
    }
}
