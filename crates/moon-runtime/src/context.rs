use chrono::{DateTime, Utc};
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::{
    collections::BTreeSet,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicPtr, AtomicU32, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{
    runtime::Builder,
    sync::{Mutex, mpsc},
    time::timeout,
};

use crate::escape_print;

use super::{actor::LuaActor, buffer::Buffer, log::Logger};

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
const ACTOR_ID_WRAP_START: u32 = 1000;

lazy_static! {
    pub static ref CONTEXT: LuaActorServer = {
        let (timer_tx, timer_rx) = mpsc::unbounded_channel();

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
            actors: DashMap::new(),
            unique_actors: DashMap::new(),
            clock: Instant::now(),
            env: DashMap::new(),
            timer_tx,
            timer_rx: Mutex::new(timer_rx),
            now: Utc::now(),
            time_offset: AtomicU64::new(0),
            io_runtime,
            main_handle: std::sync::OnceLock::new(),
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
    /// Raw pointer to the Message currently being processed.
    /// Valid only when heartbeat_ms != 0 (actor is inside handle()).
    msg_ptr: AtomicPtr<Message>,
}

unsafe impl Send for Watchdog {}
unsafe impl Sync for Watchdog {}

impl Watchdog {
    pub fn new() -> Self {
        Watchdog {
            heartbeat_ms: AtomicU64::new(0),
            msg_ptr: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Ordering contract (paired with `check_watchdogs`):
    ///
    /// `begin`: store msg_ptr BEFORE heartbeat_ms.
    /// `end`:   store heartbeat_ms=0 BEFORE clearing msg_ptr.
    ///
    /// The monitor reads heartbeat_ms first (Acquire). If it sees a non-zero
    /// value written by `begin`, the Release/Acquire pair guarantees the
    /// preceding msg_ptr store is also visible, so the pointer is valid.
    /// If it sees 0 (written by `end`), it skips the entry entirely --
    /// msg_ptr may still be non-null momentarily, but is never read.
    #[inline]
    pub fn begin(&self, clock_ms: u64, m: *const Message) {
        self.msg_ptr.store(m as *mut Message, Ordering::Release);
        self.heartbeat_ms.store(clock_ms, Ordering::Release);
    }

    #[inline]
    pub fn end(&self) {
        self.heartbeat_ms.store(0, Ordering::Release);
        self.msg_ptr.store(ptr::null_mut(), Ordering::Release);
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
    actors: DashMap<ActorId, ActorEntry>,
    unique_actors: DashMap<String, ActorId>,
    clock: Instant,
    env: DashMap<String, Arc<String>>,
    timer_tx: mpsc::UnboundedSender<Timer>,
    timer_rx: Mutex<mpsc::UnboundedReceiver<Timer>>,
    now: DateTime<Utc>,
    time_offset: AtomicU64,
    io_runtime: tokio::runtime::Runtime,
    main_handle: std::sync::OnceLock<tokio::runtime::Handle>,
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

    pub fn register_pseudo_actor(&self, id: ActorId, tx: mpsc::UnboundedSender<Message>) {
        let watchdog = Arc::new(Watchdog::new());
        self.actors.insert(id, ActorEntry { tx, watchdog });
    }

    pub fn remove(&self, id: ActorId) -> Option<mpsc::UnboundedSender<Message>> {
        self.actors.remove(&id).map(|(_, entry)| entry.tx)
    }

    pub fn query(&self, name: &str) -> Option<dashmap::mapref::one::Ref<'_, String, ActorId>> {
        self.unique_actors.get(name)
    }

    pub fn remove_actor(&self, id: ActorId, name: &str) {
        self.actors.remove(&id);
        if !name.is_empty() {
            self.unique_actors.remove(name);
        }
        self.actor_counter.fetch_sub(1, Ordering::AcqRel);

        if id == BOOTSTRAP_ACTOR_ADDR {
            self.shutdown(-1);
        }

        //notify actor exit to unique actors
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

    pub fn set_env(&self, key: &str, value: &str) {
        self.env
            .insert(key.to_string(), Arc::new(value.to_string()));
    }

    pub fn get_env(&self, key: &str) -> Option<Arc<String>> {
        self.env.get(key).map(|v| v.value().clone())
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code.load(Ordering::Acquire)
    }

    pub fn stopped(&self) -> bool {
        self.actor_counter.load(Ordering::Acquire) == 0
    }

    pub fn shutdown(&self, exit_code: i32) {
        if self
            .exit_code
            .compare_exchange(i32::MAX, exit_code, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        log::warn!(
            "receive shutdown event, exit code: {}. ({}:{})",
            exit_code,
            file!(),
            line!()
        );
        self.exit_code.store(exit_code, Ordering::Release);
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
        self.clock.elapsed() + Duration::from_millis(self.time_offset.load(Ordering::Acquire))
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.now + self.now_clock()
    }

    pub fn set_time_offset(&self, offset: u64) {
        self.time_offset.fetch_add(offset, Ordering::Release);
    }

    pub fn response_error(&self, from: ActorId, to: ActorId, session: i64, err: String) {
        if session >= 0 {
            log::error!("{}. ({}:{})", err, file!(), line!());
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
                let msg_info = unsafe {
                    let p = wd.msg_ptr.load(Ordering::Acquire);
                    if !p.is_null() {
                        format!("{}", &*p)
                    } else {
                        "unknown".to_string()
                    }
                };
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
            }
        });
    }

    pub fn io_runtime(&self) -> &tokio::runtime::Runtime {
        &self.io_runtime
    }

    pub fn set_main_handle(&self, handle: tokio::runtime::Handle) {
        self.main_handle.set(handle).ok();
    }

    pub fn main_handle(&self) -> &tokio::runtime::Handle {
        self.main_handle
            .get()
            .expect("main runtime not initialized")
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
    let _ = CONTEXT.timer_tx.send(Timer {
        timer_id,
        expiry_clock: expiry_clock.as_millis() as i64,
        owner,
    });
}

pub fn run_timer() {
    tokio::spawn(async move {
        let mut btree_map = BTreeSet::new();
        let mut rc = CONTEXT.timer_rx.lock().await;
        let mut wait_time = 1000;
        loop {
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
                } else {
                    wait_time = diff as u64;
                    break;
                }
            }
        }
    });
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
