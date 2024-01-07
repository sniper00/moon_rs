use super::context::Message;
pub use lib_lua as ffi;
use tokio::sync::mpsc;

#[derive(PartialEq)]
pub struct LuaState(pub *mut ffi::lua_State);

unsafe impl Send for LuaState {}

impl LuaState {
    fn new(l: *mut ffi::lua_State) -> Self {
        LuaState(l)
    }
}

impl Drop for LuaState {
    fn drop(&mut self) {
        unsafe {
            if !self.0.is_null() {
                ffi::lua_close(self.0);
            }
        }
    }
}

#[derive(PartialEq)]
pub struct LuaThread(pub *mut ffi::lua_State);

unsafe impl Send for LuaThread {}

impl LuaThread {
    fn new(l: *mut ffi::lua_State) -> Self {
        LuaThread(l)
    }
}

pub struct LuaActor {
    pub ok: bool,
    pub unique: bool,
    pub id: i64,
    pub name: String,
    pub tx: mpsc::UnboundedSender<Message>,
    pub rx: mpsc::UnboundedReceiver<Message>,
    pub main_state: LuaState,
    pub callback_state: LuaThread,
    pub mem: usize,
    pub mem_limit: usize,
    pub mem_warning: usize,
}

impl LuaActor {
    pub fn new(name: String, unique: bool) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        LuaActor {
            ok: false,
            unique,
            id: 0,
            name,
            tx,
            rx,
            main_state: LuaState::new(std::ptr::null_mut()),
            callback_state: LuaThread::new(std::ptr::null_mut()),
            mem: 0,
            mem_limit: 0,
            mem_warning: 8 * 1024 * 1024,
        }
    }

    pub fn set_main_state(&mut self, state: *mut ffi::lua_State) {
        self.main_state.0 = state;
        set_extra_object(state, self);
    }

    pub fn from_lua_state(state: *mut ffi::lua_State) -> &'static mut Self {
        get_extra_object::<Self>(state)
    }
}

fn set_extra_object<T>(state: *mut ffi::lua_State, obj: &T) {
    unsafe {
        let space = ffi::lua_getextraspace(state) as *mut usize;
        std::ptr::write_unaligned(space, obj as *const T as usize);
    }
}

fn get_extra_object<T>(state: *mut ffi::lua_State) -> &'static mut T {
    unsafe {
        let space = ffi::lua_getextraspace(state) as *mut usize;
        let v = std::ptr::read_unaligned(space);
        debug_assert!(v != 0);
        &mut *(v as *mut T)
    }
}
