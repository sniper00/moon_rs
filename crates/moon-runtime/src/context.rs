use chrono::{DateTime, Utc};
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::{
    collections::BTreeSet,
    ffi::c_void,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{
            AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8, AtomicU32, AtomicU64,
            AtomicUsize, Ordering,
        },
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{runtime::Builder, sync::mpsc, time::timeout};

use crate::escape_print;

use super::{actor::LuaActor, buffer::Buffer, log::Logger};

use moon_base::ffi as lua_ffi;

// Defined in `moon-runtime/src/modules/lua_coroutine.rs`. The monitor installs this
// hook on the active lua_State when a timeout is detected.
unsafe extern "C-unwind" {
    pub fn moon_signal_hook(l: *mut lua_ffi::lua_State, ar: *mut lua_ffi::lua_Debug);
}

pub const PTYPE_SYSTEM: u8 = 1;
pub const PTYPE_TEXT: u8 = 2;
pub const PTYPE_LUA: u8 = 3;
pub const PTYPE_ERROR: u8 = 4;
pub const PTYPE_DEBUG: u8 = 5;
pub const PTYPE_SHUTDOWN: u8 = 6;
pub const PTYPE_TIMER: u8 = 7;
pub const PTYPE_SOCKET_TCP: u8 = 8;
pub const PTYPE_SOCKET_UDP: u8 = 9;
pub const PTYPE_SOCKET_EVENT: u8 = 10;
pub const PTYPE_INTEGER: u8 = 12;
pub const PTYPE_HTTPC: u8 = 13;
pub const PTYPE_QUIT: u8 = 14;
pub const PTYPE_SQLX: u8 = 15;
pub const PTYPE_MONGODB: u8 = 16;
pub const PTYPE_WEBSOCKET: u8 = 17;
pub const PTYPE_HTTPD: u8 = 18;
pub const PTYPE_PG: u8 = 19;
pub const PTYPE_REDIS: u8 = 20;

pub type ActorId = u32;

pub const BOOTSTRAP_ACTOR_ADDR: ActorId = 1;
pub const CLUSTER_ACTOR_ADDR: ActorId = 2;
const ACTOR_ID_WRAP_START: u32 = 1000;

lazy_static! {
    pub static ref CONTEXT: LuaActorServer = {
        let io_runtime = Builder::new_multi_thread()
            .worker_threads(num_cpus::get().min(4))
            .enable_time()
            .enable_io()
            .build()
            .expect("Init IO tokio runtime failed");

        LuaActorServer {
            actor_uuid: AtomicU32::new(1),
            actor_counter: AtomicU32::new(0),
            exit_code: AtomicI32::new(i32::MAX),
            error_count: AtomicUsize::new(0),
            pending_timers: AtomicUsize::new(0),
            actors: DashMap::new(),
            unique_actors: DashMap::new(),
            clock: Instant::now(),
            env: DashMap::new(),
            timer_tx: OnceLock::new(),
            now: Utc::now(),
            time_offset: AtomicU64::new(0),
            io_runtime,
            main_handle: std::sync::OnceLock::new(),
            unique_threads: Mutex::new(Vec::new()),
        }
    };
    pub static ref LOGGER: Logger = Logger::new();
}

/// Type-erased heap value with automatic cleanup on drop.
///
/// When a `Message` carrying a `BoxedValue` is dropped without being decoded
/// (e.g. send failure to a dead actor), the destructor runs automatically,
/// preventing memory leaks.
pub struct BoxedValue {
    ptr: *mut (),
    drop_fn: unsafe fn(*mut ()),
}

unsafe impl Send for BoxedValue {}

unsafe fn typed_drop<T>(ptr: *mut ()) {
    unsafe {
        let _ = Box::from_raw(ptr as *mut T);
    }
}

impl BoxedValue {
    pub fn new<T: Send>(value: T) -> Self {
        let ptr = Box::into_raw(Box::new(value)) as *mut ();
        Self {
            ptr,
            drop_fn: typed_drop::<T>,
        }
    }

    /// Transfer ownership to the caller. After this call the destructor
    /// becomes a no-op — the caller must eventually `Box::from_raw` the pointer.
    pub fn into_raw(&mut self) -> *mut () {
        let ptr = self.ptr;
        self.ptr = std::ptr::null_mut();
        ptr
    }
}

impl Drop for BoxedValue {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { (self.drop_fn)(self.ptr) }
        }
    }
}

pub enum MessageBody {
    ISize(u8, isize),
    Buffer(u8, Box<Buffer>),
    Boxed(u8, Box<BoxedValue>),
    None(u8),
}

impl MessageBody {
    #[inline]
    pub fn ptype(&self) -> u8 {
        match self {
            MessageBody::ISize(p, _) => *p,
            MessageBody::Buffer(p, _) => *p,
            MessageBody::Boxed(p, _) => *p,
            MessageBody::None(p) => *p,
        }
    }
}

impl std::fmt::Display for MessageBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageBody::ISize(_, i) => write!(f, "ISize({})", i),
            MessageBody::Buffer(_, data) => {
                write!(f, "Buffer(\"{}\")", escape_print(data.as_slice()))
            }
            MessageBody::Boxed(_, b) => write!(f, "Boxed({:p})", b.ptr),
            MessageBody::None(_) => write!(f, "None"),
        }
    }
}

pub struct Message {
    pub from: ActorId,
    pub to: ActorId,
    pub session: i64,
    pub data: MessageBody,
}

impl Message {
    #[inline]
    pub fn ptype(&self) -> u8 {
        self.data.ptype()
    }

    /// Move the message payload out, leaving `MessageBody::None(0)`.
    pub fn take_body(&mut self) -> MessageBody {
        std::mem::replace(&mut self.data, MessageBody::None(0))
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Message {{ ptype: {}, from: 0x{:08x}, to: 0x{:08x}, session: {}, data: {} }}",
            self.ptype(),
            self.from,
            self.to,
            self.session,
            self.data
        )
    }
}

pub struct Watchdog {
    /// Dispatch start time in milliseconds (from CONTEXT.clock), 0 = idle.
    heartbeat_ms: AtomicU64,
    /// Diagnostic scalars copied out of the in-flight Message at `begin`, so the
    /// monitor thread can describe a blocked actor *without ever touching the
    /// actor's mutable Message memory* (no shared `&Message` / raw deref, hence
    /// no data race with the actor mutating it). Only meaningful while
    /// `heartbeat_ms != 0`.
    ptype: AtomicU8,
    from: AtomicU32,
    to: AtomicU32,
    session: AtomicI64,
    /// The currently executing lua_State pointer. Updated by the actor thread
    /// in handle_message (set to callback_state) and switchL (set to the
    /// coroutine being entered). The monitor reads this to install lua_sethook.
    pub active_l: AtomicPtr<c_void>,
    /// Interrupt protocol (3-state):
    ///   0 = idle
    ///   1 = monitor has set trap, sethook pending
    ///  -1 = monitor has completed sethook
    /// Actor thread's switchL re-installs hook when trap != 0.
    /// signal_hook resets to 0 after raising the Lua error.
    pub trap: AtomicI32,
    /// Number of consecutive timeout detections. Interrupt only fires after 3.
    pub timeout_count: AtomicU32,

    /// Per-actor statistics, published by the actor thread after each dispatch
    /// (`record_dispatch`) and read by `CONTEXT` (e.g. `server_stats`) through
    /// the shared `Arc<Watchdog>` in `ActorEntry`. All plain atomics, so reads
    /// from the monitor/stats path never touch the actor's live Lua memory.
    ///
    /// Total messages this actor has finished dispatching.
    message_total: AtomicU64,
    /// Cumulative time spent inside message dispatch, in milliseconds.
    cpu_ms_total: AtomicU64,
    /// Last observed Lua memory footprint of the actor, in bytes.
    memory: AtomicIsize,
}

impl Watchdog {
    pub fn new() -> Self {
        Watchdog {
            heartbeat_ms: AtomicU64::new(0),
            ptype: AtomicU8::new(0),
            from: AtomicU32::new(0),
            to: AtomicU32::new(0),
            session: AtomicI64::new(0),
            active_l: AtomicPtr::new(std::ptr::null_mut()),
            trap: AtomicI32::new(0),
            timeout_count: AtomicU32::new(0),
            message_total: AtomicU64::new(0),
            cpu_ms_total: AtomicU64::new(0),
            memory: AtomicIsize::new(0),
        }
    }

    /// Ordering contract (paired with `check_watchdogs`):
    ///
    /// `begin`: store the diagnostic scalars BEFORE heartbeat_ms.
    /// `end`:   store heartbeat_ms = 0.
    ///
    /// The monitor reads heartbeat_ms first (Acquire). If it sees a non-zero
    /// value written by `begin`, the Release/Acquire pair guarantees the
    /// preceding scalar stores are visible, so they describe this dispatch.
    /// If it sees 0 (written by `end`), it skips the entry and reads nothing.
    /// All published state is plain scalars, so even a benign race with the
    /// next `begin` only yields valid (if slightly newer) values — never UB.
    #[inline]
    pub fn begin(&self, clock_ms: u64, ptype: u8, from: ActorId, to: ActorId, session: i64) {
        self.ptype.store(ptype, Ordering::Relaxed);
        self.from.store(from, Ordering::Relaxed);
        self.to.store(to, Ordering::Relaxed);
        self.session.store(session, Ordering::Relaxed);
        self.timeout_count.store(0, Ordering::Relaxed);
        self.heartbeat_ms.store(clock_ms, Ordering::Release);
    }

    #[inline]
    pub fn end(&self) {
        self.heartbeat_ms.store(0, Ordering::Release);
    }

    #[inline]
    pub fn set_active_l(&self, l: *mut c_void) {
        self.active_l.store(l, Ordering::Release);
    }

    /// Publish per-dispatch statistics. Called by the actor thread right after a
    /// message has been handled: bumps the message counter, accumulates the
    /// elapsed dispatch time, and snapshots the actor's current memory usage.
    #[inline]
    pub fn record_dispatch(&self, begin_ms: u64, end_ms: u64, memory: isize) {
        self.message_total.fetch_add(1, Ordering::Relaxed);
        self.cpu_ms_total
            .fetch_add(end_ms.saturating_sub(begin_ms), Ordering::Relaxed);
        self.memory.store(memory, Ordering::Relaxed);
    }

    #[inline]
    pub fn message_total(&self) -> u64 {
        self.message_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn cpu_ms_total(&self) -> u64 {
        self.cpu_ms_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn memory(&self) -> isize {
        self.memory.load(Ordering::Relaxed)
    }
}

impl Default for Watchdog {
    fn default() -> Self {
        Self::new()
    }
}

struct ActorEntry {
    tx: mpsc::UnboundedSender<Message>,
    watchdog: Arc<Watchdog>,
}

pub struct LuaActorServer {
    actor_uuid: AtomicU32,
    actor_counter: AtomicU32,
    exit_code: AtomicI32,
    error_count: AtomicUsize,
    /// Timers that have been scheduled (`insert_timer`) but not yet fired by the
    /// `run_timer` task. The actual `BTreeSet` lives inside that task, so this
    /// atomic mirror is the only process-wide view of "alive" timers.
    pending_timers: AtomicUsize,
    actors: DashMap<ActorId, ActorEntry>,
    unique_actors: DashMap<String, ActorId>,
    clock: Instant,
    env: DashMap<String, Arc<Vec<u8>>>,
    /// The timer channel is created in `run_timer`, which keeps the receiver in
    /// its task and publishes the sender here. Only the sender lives in the
    /// global, and `send` needs just `&self`, so `OnceLock` fits exactly. The
    /// receiver is never stored globally (no `Mutex`/`&mut` juggling needed).
    timer_tx: OnceLock<mpsc::UnboundedSender<Timer>>,
    now: DateTime<Utc>,
    time_offset: AtomicU64,
    io_runtime: tokio::runtime::Runtime,
    main_handle: std::sync::OnceLock<tokio::runtime::Handle>,
    /// Join handles for unique actors, which each run on a dedicated OS thread.
    /// These threads must be joined before the process exits: a unique actor
    /// signals `stopped()` (via `remove_actor`) *before* its thread function
    /// returns, so without an explicit join the main thread can race ahead into
    /// libc's exit path (and mimalloc's process teardown) while an actor thread
    /// is still running its per-thread allocator cleanup (`_mi_thread_done`),
    /// corrupting mimalloc's global heap state and segfaulting.
    unique_threads: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl LuaActorServer {
    pub fn add_actor(
        &self,
        actor: &mut LuaActor,
        tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Arc<Watchdog>, String> {
        if actor.unique && self.unique_actors.contains_key(&actor.name) {
            return Err(format!("unique actor named {} already exists", actor.name));
        }

        self.actor_counter.fetch_add(1, Ordering::AcqRel);
        let watchdog = Arc::new(Watchdog::new());
        self.actors.insert(
            actor.id,
            ActorEntry {
                tx,
                watchdog: watchdog.clone(),
            },
        );
        if actor.unique {
            self.unique_actors.insert(actor.name.clone(), actor.id);
        }
        Ok(watchdog)
    }

    /// Register a non-Lua "actor" (e.g. the cluster endpoint) for message
    /// routing. Intentionally does NOT bump `actor_counter`: pseudo-actors must
    /// not keep `stopped()` from becoming true, and they are never torn down via
    /// `remove_actor` (whose counter decrement is guarded on real registration).
    pub fn register_pseudo_actor(&self, id: ActorId, tx: mpsc::UnboundedSender<Message>) {
        let watchdog = Arc::new(Watchdog::new());
        self.actors.insert(id, ActorEntry { tx, watchdog });
    }

    pub fn query(&self, name: &str) -> Option<dashmap::mapref::one::Ref<'_, String, ActorId>> {
        self.unique_actors.get(name)
    }

    pub fn remove_actor(&self, id: ActorId, name: &str) {
        // Only run registry-dependent cleanup when this actor was actually
        // registered. `add_actor` can fail *before* registering (e.g. a duplicate
        // unique name) yet the spawned task still calls `remove_actor` on exit. In
        // that case nothing was inserted and `actor_counter` was never bumped, so
        // we must NOT decrement the counter (it would underflow `AtomicU32` and
        // make `stopped()` never true, hanging shutdown) nor evict the *existing*
        // owner of `name` from `unique_actors`.
        if self.actors.remove(&id).is_none() {
            // A failed bootstrap must still bring the process down.
            if id == BOOTSTRAP_ACTOR_ADDR {
                self.shutdown(-1);
            }
            return;
        }

        if !name.is_empty() {
            self.unique_actors.remove(name);
        }
        self.actor_counter.fetch_sub(1, Ordering::AcqRel);

        if id == BOOTSTRAP_ACTOR_ADDR {
            self.shutdown(-1);
        }

        // Notify unique actors that this actor exited. This matches Moon's
        // `broadcast`: PTYPE_SYSTEM messages are delivered only to unique
        // services, whose Lua `_service_exit` handler releases watched calls.
        self.unique_actors.iter().for_each(|v| {
            let _ = self.send(Message {
                from: id,
                to: *v.value(),
                session: 0,
                data: MessageBody::Buffer(
                    PTYPE_SYSTEM,
                    Box::new(
                        format!("_service_exit,Actor id:{} quited", id)
                            .into_bytes()
                            .into(),
                    ),
                ),
            });
        });
    }

    /// Broadcast a PTYPE_SYSTEM message to all unique actors (same scope as
    /// `_service_exit`). Used by subsystems like cluster to deliver events.
    pub fn broadcast_system(&self, sender: ActorId, payload: &str) {
        self.unique_actors.iter().for_each(|v| {
            let _ = self.send(Message {
                from: sender,
                to: *v.value(),
                session: 0,
                data: MessageBody::Buffer(
                    PTYPE_SYSTEM,
                    Box::new(payload.to_string().into_bytes().into()),
                ),
            });
        });
    }

    pub fn set_env(&self, key: &str, value: &[u8]) {
        self.env
            .insert(key.to_string(), Arc::new(value.to_vec()));
    }

    pub fn get_env(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.env.get(key).map(|v| v.value().clone())
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code.load(Ordering::Acquire)
    }

    pub fn stopped(&self) -> bool {
        self.actor_counter.load(Ordering::Acquire) == 0
    }

    /// Returns a human-readable description of every actor still registered.
    /// Each entry is the actor id (`0x{:08X}`); unique actors additionally
    /// include their name. Used while waiting for shutdown to report which
    /// actors are still running.
    pub fn running_actors(&self) -> Vec<String> {
        // Collect actor IDs first (releasing the `actors` shard locks), then
        // look up unique names — avoids AB-BA deadlock with callers that hold
        // `unique_actors` → `actors` (e.g. broadcast_system / remove_actor).
        let ids: Vec<ActorId> = self.actors.iter().map(|e| *e.key()).collect();
        ids.into_iter()
            .map(|id| {
                let name = self
                    .unique_actors
                    .iter()
                    .find(|u| *u.value() == id)
                    .map(|u| u.key().clone());
                match name {
                    Some(name) => format!("0x{:08X} ({})", id, name),
                    None => format!("0x{:08X}", id),
                }
            })
            .collect()
    }

    pub fn shutdown(&self, exit_code: i32) {
        // Publish `exit_code` exactly once: the CAS success ordering is AcqRel
        // so the value is visible to other threads that observe the transition.
        if self
            .exit_code
            .compare_exchange(i32::MAX, exit_code, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        log::warn!("receive shutdown event, exit code: {}.", exit_code);
        self.actors.iter().for_each(|v| {
            let _ = v.value().tx.send(Message {
                from: 0,
                to: 0,
                session: 0,
                data: MessageBody::None(PTYPE_SHUTDOWN),
            });
        });
    }

    #[must_use]
    pub fn send(&self, msg: Message) -> Option<Message> {
        if let Some(entry) = self.actors.get(&msg.to) {
            if let Err(err) = entry.value().tx.send(msg) {
                return Some(err.0);
            } else {
                return None;
            }
        }
        Some(msg)
    }

    #[must_use]
    pub fn send_value<T: Send>(
        &self,
        protocol_type: u8,
        owner: ActorId,
        session: i64,
        res: T,
    ) -> Option<Message> {
        self.send(Message {
            from: 0,
            to: owner,
            session,
            data: MessageBody::Boxed(protocol_type, Box::new(BoxedValue::new(res))),
        })
    }

    pub fn next_actor_id(&self) -> ActorId {
        loop {
            let id = self.actor_uuid.fetch_add(1, Ordering::AcqRel);
            if id == 0 {
                // Wrapped around to 0. The fetch_add already advanced the
                // counter to 1, so try to CAS it forward to ACTOR_ID_WRAP_START
                // to skip IDs reserved for long-lived startup services.
                // If another thread already moved it past 1, the CAS is a no-op.
                let _ = self.actor_uuid.compare_exchange(
                    1,
                    ACTOR_ID_WRAP_START,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                continue;
            }
            if !self.actors.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn clock(&self) -> f64 {
        self.clock.elapsed().as_secs_f64()
    }

    pub fn now_clock(&self) -> Duration {
        self.clock.elapsed()
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.now + self.now_clock() + Duration::from_millis(self.time_offset.load(Ordering::Acquire))
    }

    /// Advance the simulated clock by `offset` milliseconds. The offset is
    /// cumulative (each call adds to the running total), hence `add_*`.
    pub fn add_time_offset(&self, offset: u64) {
        self.time_offset.fetch_add(offset, Ordering::Release);
    }

    pub fn response_error(&self, from: ActorId, to: ActorId, session: i64, err: String) {
        if session >= 0 {
            log::error!("{}.", err);
        } else {
            let _ = self.send(Message {
                from,
                to,
                session: -session,
                data: MessageBody::Buffer(PTYPE_ERROR, Box::new(err.into_bytes().into())),
            });
        }
    }

    pub fn clock_ms(&self) -> u64 {
        self.clock.elapsed().as_millis() as u64
    }

    pub fn check_watchdogs(&self) {
        let now_ms = self.clock_ms();
        self.actors.iter().for_each(|entry| {
            let id = *entry.key();
            let wd = &entry.value().watchdog;
            let hb = wd.heartbeat_ms.load(Ordering::Acquire);
            if hb > 0 && now_ms.saturating_sub(hb) >= 10_000 {
                let elapsed_s = (now_ms - hb) / 1000;
                // Read only the published scalars (paired Acquire above via `hb`);
                // never touch the actor's live Message.
                let msg_info = format!(
                    "Message {{ ptype: {}, from: 0x{:08x}, to: 0x{:08x}, session: {} }}",
                    wd.ptype.load(Ordering::Relaxed),
                    wd.from.load(Ordering::Relaxed),
                    wd.to.load(Ordering::Relaxed),
                    wd.session.load(Ordering::Relaxed),
                );
                let s = format!(
                    "slow_message,Actor 0x{:08X} blocked for {}s, msg: {}",
                    id, elapsed_s, msg_info
                );
                log::error!("{}", s);
                let _ = self.send(Message {
                    from: id,
                    to: BOOTSTRAP_ACTOR_ADDR,
                    session: 0,
                    data: MessageBody::Buffer(PTYPE_SYSTEM, Box::new(s.into())),
                });
                wd.heartbeat_ms.store(now_ms, Ordering::Release);

                // Only interrupt after 3 consecutive timeout detections (~30s).
                let count = wd.timeout_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= 3 {
                    // Interrupt: CAS(0→1), install hook on active_l, CAS(1→-1).
                    // If trap is already non-zero a previous interrupt is in flight.
                    if wd
                        .trap
                        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        let active = wd.active_l.load(Ordering::Acquire);
                        if !active.is_null() {
                            unsafe {
                                lua_ffi::lua_sethook(
                                    active as *mut lua_ffi::lua_State,
                                    Some(moon_signal_hook),
                                    lua_ffi::LUA_MASKCOUNT,
                                    1,
                                );
                            }
                        }
                        wd.trap.store(-1, Ordering::Release);
                    }
                }
            }
        });
    }

    pub fn io_runtime(&self) -> &tokio::runtime::Runtime {
        &self.io_runtime
    }

    pub fn set_main_handle(&self, handle: tokio::runtime::Handle) {
        self.main_handle.set(handle).ok();
    }

    /// Track a unique actor's dedicated OS thread so it can be joined at
    /// shutdown. Opportunistically reaps already-finished threads first so the
    /// vector does not grow without bound on servers that create and destroy
    /// many unique services over their lifetime. Joining a thread that already
    /// reports `is_finished()` returns immediately and guarantees its
    /// thread-local teardown has completed.
    pub fn register_unique_thread(&self, handle: thread::JoinHandle<()>) {
        let mut guard = self.unique_threads.lock().unwrap();
        let mut i = 0;
        while i < guard.len() {
            if guard[i].is_finished() {
                let h = guard.swap_remove(i);
                let _ = h.join();
            } else {
                i += 1;
            }
        }
        guard.push(handle);
    }

    /// Block until every registered unique-actor thread has fully terminated
    /// (including its per-thread allocator cleanup). Must be called from the
    /// main thread on the shutdown path, before the process exits.
    pub fn join_unique_threads(&self) {
        let handles: Vec<_> = {
            let mut guard = self.unique_threads.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for h in handles {
            let _ = h.join();
        }
    }

    pub fn main_handle(&self) -> &tokio::runtime::Handle {
        self.main_handle
            .get()
            .expect("main runtime not initialized")
    }

    pub fn actor_count(&self) -> u32 {
        self.actor_counter.load(Ordering::Acquire)
    }

    /// Number of routing entries currently registered, including pseudo-actors
    /// (e.g. the cluster endpoint) that do not bump `actor_counter`.
    pub fn registered_actor_count(&self) -> usize {
        self.actors.len()
    }

    /// Number of unique (named) services currently alive.
    pub fn unique_actor_count(&self) -> usize {
        self.unique_actors.len()
    }

    /// Total number of actors ever created since process start. `actor_uuid`
    /// is the next id to hand out and starts at 1, so subtract 1.
    pub fn total_actor_created(&self) -> u32 {
        self.actor_uuid.load(Ordering::Acquire).saturating_sub(1)
    }

    /// Number of timers scheduled but not yet fired.
    pub fn timer_count(&self) -> usize {
        self.pending_timers.load(Ordering::Acquire)
    }

    /// Number of environment variables stored in the registry.
    pub fn env_count(&self) -> usize {
        self.env.len()
    }

    /// Accumulated simulated-clock offset, in milliseconds.
    pub fn time_offset(&self) -> u64 {
        self.time_offset.load(Ordering::Acquire)
    }

    /// Process uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.clock.elapsed().as_secs()
    }

    /// Aggregate Lua memory across all actors, in bytes (last observed values).
    pub fn total_memory(&self) -> i64 {
        self.actors
            .iter()
            .map(|e| e.value().watchdog.memory() as i64)
            .sum()
    }

    /// Total messages dispatched across all actors.
    pub fn total_messages(&self) -> u64 {
        self.actors
            .iter()
            .map(|e| e.value().watchdog.message_total())
            .sum()
    }

    /// Aggregate dispatch time across all actors, in milliseconds.
    pub fn total_cpu_ms(&self) -> u64 {
        self.actors
            .iter()
            .map(|e| e.value().watchdog.cpu_ms_total())
            .sum()
    }

    /// Per-actor statistics snapshot, one entry per registered actor.
    pub fn actor_stats(&self) -> Vec<ActorStat> {
        // Collect watchdog data first (releasing the `actors` shard locks), then
        // look up unique names — avoids AB-BA deadlock with callers that hold
        // `unique_actors` → `actors` (e.g. broadcast_system / remove_actor).
        let entries: Vec<(ActorId, u64, u64, u64)> = self
            .actors
            .iter()
            .map(|e| {
                let wd = &e.value().watchdog;
                (*e.key(), wd.memory() as u64, wd.message_total(), wd.cpu_ms_total())
            })
            .collect();
        entries
            .into_iter()
            .map(|(id, memory, messages, cpu_ms)| {
                let name = self
                    .unique_actors
                    .iter()
                    .find(|u| *u.value() == id)
                    .map(|u| u.key().clone());
                ActorStat {
                    id,
                    name,
                    memory: memory as i64,
                    messages,
                    cpu_ms,
                }
            })
            .collect()
    }

    pub fn error_count(&self) -> usize {
        self.error_count.load(Ordering::Acquire)
    }

    pub fn increment_error_count(&self) {
        self.error_count.fetch_add(1, Ordering::Release);
    }
}

pub fn run_monitor() {
    thread::spawn(|| {
        loop {
            if CONTEXT.exit_code() != i32::MAX && CONTEXT.stopped() {
                break;
            }
            thread::sleep(Duration::from_secs(5));
            CONTEXT.check_watchdogs();
        }
    });
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
struct Timer {
    expiry_clock: i64,
    timer_id: i64,
    owner: ActorId,
}

pub fn insert_timer(owner: ActorId, timer_id: i64, interval: u64) {
    let expiry_clock = CONTEXT.now_clock() + Duration::from_millis(interval);
    let Some(timer_tx) = CONTEXT.timer_tx.get() else {
        log::error!("insert_timer called before run_timer started");
        return;
    };
    if timer_tx
        .send(Timer {
            timer_id,
            expiry_clock: expiry_clock.as_millis() as i64,
            owner,
        })
        .is_ok()
    {
        CONTEXT.pending_timers.fetch_add(1, Ordering::Release);
    }
}

pub fn run_timer() {
    // Create the channel here so the receiver can be moved straight into the
    // single timer task. Only the sender is published globally; this must be
    // called exactly once and before any `insert_timer`.
    let (timer_tx, mut rc) = mpsc::unbounded_channel();
    CONTEXT
        .timer_tx
        .set(timer_tx)
        .unwrap_or_else(|_| panic!("run_timer called more than once"));
    tokio::spawn(async move {
        let mut btree_map = BTreeSet::new();
        let mut wait_time = 1000;
        loop {
            // // Terminate cooperatively once shutdown has been requested. The
            // // global `timer_tx` sender lives in `CONTEXT` and never drops, so
            // // `rc.recv()` alone would never return `None` and this task would
            // // otherwise keep the IO runtime alive forever. The ≤1s `wait_time`
            // // bounds how long after shutdown we take to notice.
            // if CONTEXT.exit_code() != i32::MAX {
            //     break;
            // }
            match timeout(Duration::from_millis(wait_time), rc.recv()).await {
                Ok(Some(timer)) => {
                    //println!("insert timer: {:?} {:?}", timer, CONTEXT.now_clock());
                    btree_map.insert(timer);
                }
                Ok(None) => {
                    break;
                }
                Err(_) => {} //timeout
            }

            wait_time = 1000;

            while let Some(timer) = btree_map.first() {
                let diff = timer.expiry_clock - CONTEXT.now_clock().as_millis() as i64;
                if diff <= 0 {
                    //println!("timer timeout: {:?} {:?}", timer, CONTEXT.now_clock());
                    let _ = CONTEXT.send(Message {
                        from: 0,
                        to: timer.owner,
                        session: 0,
                        data: MessageBody::ISize(PTYPE_TIMER, timer.timer_id as isize),
                    });
                    btree_map.pop_first();
                    CONTEXT.pending_timers.fetch_sub(1, Ordering::Release);
                } else {
                    wait_time = diff as u64;
                    break;
                }
            }
        }
    });
}

/// Per-actor statistics snapshot produced by [`LuaActorServer::actor_stats`].
pub struct ActorStat {
    pub id: ActorId,
    /// Service name, only present for unique/named actors.
    pub name: Option<String>,
    /// Last observed Lua memory footprint, in bytes.
    pub memory: i64,
    /// Total messages dispatched.
    pub messages: u64,
    /// Cumulative dispatch time, in milliseconds.
    pub cpu_ms: u64,
}

pub struct LuaActorParam {
    pub id: ActorId,
    pub unique: bool,
    pub creator: ActorId,
    pub session: i64,
    pub memlimit: i64,
    pub name: String,
    pub source: String,
    pub params: String,
    pub block: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor_param(id: ActorId, name: &str, unique: bool) -> LuaActorParam {
        LuaActorParam {
            id,
            unique,
            creator: 0,
            session: 0,
            memlimit: 0,
            name: name.to_string(),
            source: String::new(),
            params: String::new(),
            block: false,
        }
    }

    #[test]
    fn remove_actor_sends_service_exit_only_to_unique_actors() {
        let target_id = 0x7100_0001;
        let unique_watcher_id = 0x7100_0002;
        let normal_watcher_id = 0x7100_0003;

        let (target_tx, _target_rx) = mpsc::unbounded_channel();
        let (unique_watcher_tx, mut unique_watcher_rx) = mpsc::unbounded_channel();
        let (normal_watcher_tx, mut normal_watcher_rx) = mpsc::unbounded_channel();

        let mut target = LuaActor::new(&actor_param(target_id, "exit-target", false));
        let mut unique_watcher =
            LuaActor::new(&actor_param(unique_watcher_id, "exit-unique-watcher", true));
        let mut normal_watcher = LuaActor::new(&actor_param(
            normal_watcher_id,
            "exit-normal-watcher",
            false,
        ));

        CONTEXT.add_actor(&mut target, target_tx).unwrap();
        CONTEXT
            .add_actor(&mut unique_watcher, unique_watcher_tx)
            .unwrap();
        CONTEXT
            .add_actor(&mut normal_watcher, normal_watcher_tx)
            .unwrap();

        CONTEXT.remove_actor(target_id, &target.name);

        // `CONTEXT` is a process-wide singleton shared by every test in this
        // crate. Other tests running in parallel (e.g. cluster's
        // `broadcast_system(CLUSTER_ACTOR_ID, ...)`) also fan PTYPE_SYSTEM
        // messages out to *all* unique actors, so foreign messages can land in
        // this watcher's mailbox. Scan for our own service-exit (identified by
        // `from == target_id`) instead of assuming it is the first message.
        let mut service_exit = None;
        while let Ok(msg) = unique_watcher_rx.try_recv() {
            if msg.from == target_id {
                service_exit = Some(msg);
                break;
            }
        }
        let msg = service_exit.expect("unique watcher should receive service-exit notice");
        assert_eq!(msg.to, unique_watcher_id);
        assert_eq!(msg.session, 0);
        assert_eq!(msg.ptype(), PTYPE_SYSTEM);

        match msg.data {
            MessageBody::Buffer(_, data) => {
                let text = std::str::from_utf8(data.as_slice()).unwrap();
                assert!(text.starts_with("_service_exit,"));
                assert!(text.contains(&format!("Actor id:{} quited", target_id)));
            }
            other => panic!("unexpected service-exit payload: {}", other),
        }

        // A non-unique actor must never receive the service-exit broadcast (this
        // matches Moon). It may still see unrelated foreign traffic, so assert
        // specifically that nothing came from `target_id`.
        while let Ok(msg) = normal_watcher_rx.try_recv() {
            assert_ne!(
                msg.from, target_id,
                "non-unique watcher should match Moon and skip PTYPE_SYSTEM broadcast"
            );
        }

        CONTEXT.remove_actor(unique_watcher_id, &unique_watcher.name);
        CONTEXT.remove_actor(normal_watcher_id, &normal_watcher.name);
    }
}
