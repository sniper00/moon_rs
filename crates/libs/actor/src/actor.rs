use crate::context::LuaActorParam;
use luars::{Function, GlobalState, Lua, LuaApi, SafeOption, Stdlib};

use super::context::Message;
use tokio::sync::mpsc;

/// A single actor instance owning a Lua VM and its message inbox.
///
/// Each `LuaActor` is moved to exactly one Tokio task after creation.
/// The Lua VM is accessed exclusively by that task in a serial message loop.
pub struct LuaActor {
    pub ok: bool,
    pub unique: bool,
    pub id: i64,
    uuid: i64,
    pub name: String,
    pub rx: mpsc::UnboundedReceiver<Message>,
    pub callback_fn: Option<Function>,
    pub lua: Lua,
    pub mem_limit: isize,
    pub mem_warning: isize,
}

impl LuaActor {
    /// Create a new actor with a fresh Lua VM configured from `params`.
    pub fn new(params: &LuaActorParam, rx: mpsc::UnboundedReceiver<Message>) -> Result<Self, crate::error::MoonError> {

        let option = SafeOption {
            max_memory_limit: if params.memlimit > 0 {
                params.memlimit as isize
            } else {
                isize::MAX
            },
            ..SafeOption::default()
        };

        let mut lua = Lua::new(option);

        lua.open_stdlib(Stdlib::All).map_err(|e| lua.get_error_message(e).to_string())?;

        Ok(LuaActor {
            ok: false,
            unique: params.unique,
            id: params.id,
            uuid: 0,
            name: params.name.clone(),
            rx,
            lua,
            callback_fn: None,
            mem_limit: if params.memlimit > 0 { params.memlimit as isize } else { 0 },
            mem_warning: 8 * 1024 * 1024,
        })
    }

    /// Allocate and return a monotonically increasing session id.
    pub fn next_session(&mut self) -> i64 {
        self.uuid += 1;
        self.uuid
    }

    pub fn state_mut(&mut self) -> &mut GlobalState {
        self.lua.global_state_mut()
    }

    pub fn lua_mut(&mut self) -> &mut Lua {
        &mut self.lua
    }
}

/// SAFETY: `LuaActor` is transferred (moved) to exactly one Tokio task after
/// creation. The Lua VM and all GC-managed objects are then accessed exclusively
/// by that single task in a serial message loop. No concurrent access occurs.
///
/// Invariants that must be upheld:
/// - `LuaActor` must never be shared across tasks (no `Arc<LuaActor>`)
/// - `LuaActor` must never be accessed after being moved into `tokio::spawn`
/// - All Lua C function callbacks run synchronously within `handle()`
unsafe impl Send for LuaActor {}
