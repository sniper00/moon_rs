use chrono::{DateTime, Local, Utc};
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, Mutex},
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
pub const PTYPE_INTEGER: u8 = 12;
pub const PTYPE_HTTP: u8 = 13;
pub const PTYPE_QUIT: u8 = 14;
pub const PTYPE_SQLX: u8 = 15;
pub const PTYPE_MONGODB: u8 = 16;
pub const PTYPE_WEBSOCKET: u8 = 17;
pub const PTYPE_HTTP_SRV: u8 = 18;

pub const BOOTSTRAP_ACTOR_ADDR: i64 = 1;

static GLOBAL_THREAD_ID: AtomicU64 = AtomicU64::new(1);

lazy_static! {
    pub static ref CONTEXT: LuaActorServer = {
        let (timer_tx, timer_rx) = mpsc::unbounded_channel();

        LuaActorServer {
            actor_uuid: AtomicI64::new(1),
            actor_counter: AtomicU32::new(0),
            exit_code: AtomicI32::new(i32::MAX),
            actors: DashMap::new(),
            unique_actors: DashMap::new(),
            clock: Instant::now(),
            env: DashMap::new(),
            monitor: DashMap::new(),
            timer_tx,
            timer_rx: Mutex::new(timer_rx),
            now: Utc::now(),
            time_offset: AtomicU64::new(0),
        }
    };
    pub static ref LOGGER: Logger = Logger::new();
}

thread_local! {
    static THREAD_ID: u64 = GLOBAL_THREAD_ID.fetch_add(1, Ordering::SeqCst);
}

/// Type-erased heap value with automatic cleanup on drop.
///
/// When a `Message` carrying a `BoxedValue` is dropped without being decoded
/// (e.g. send failure, actor quit, channel closed), the destructor runs
/// automatically, preventing memory leaks.
pub struct BoxedValue {
    ptr: *mut (),
    drop_fn: unsafe fn(*mut ()),
}

// SAFETY: BoxedValue is only constructed from `T: Send` values via `new()`,
// and the pointed-to data is transferred (not shared) across thread boundaries.
unsafe impl Send for BoxedValue {}

unsafe fn typed_drop<T>(ptr: *mut ()) {
    let _ = Box::from_raw(ptr as *mut T);
}

impl BoxedValue {
    pub fn new<T: Send>(value: T) -> Self {
        let ptr = Box::into_raw(Box::new(value)) as *mut ();
        Self {
            ptr,
            drop_fn: typed_drop::<T>,
        }
    }

    /// Extract the raw pointer, transferring ownership to the caller.
    /// After this call, `Drop` will NOT free the memory — the caller
    /// is responsible for eventually calling `Box::from_raw`.
    /// Returns the raw pointer without transferring ownership.
    pub fn as_ptr(&self) -> *mut () {
        self.ptr
    }

    /// Extract the raw pointer, transferring ownership to the caller.
    /// After this call, `Drop` will NOT free the memory — the caller
    /// is responsible for eventually calling `Box::from_raw`.
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

/// Payload carried by an inter-actor [`Message`].
pub enum MessageBody {
    /// Small integer value (e.g. status codes).
    ISize(isize),
    /// Owned byte buffer.
    Buffer(Box<Buffer>),
    /// Type-erased heap value (see [`BoxedValue`]).
    Boxed(Box<BoxedValue>),
    /// No payload.
    None,
}

impl std::fmt::Display for MessageBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageBody::ISize(i) => write!(f, "ISize({})", i),
            MessageBody::Buffer(data) => {
                write!(f, "Buffer({:?})", escape_print(data.as_slice()))
            }
            MessageBody::Boxed(b) => write!(f, "Boxed({:p})", b.as_ptr()),
            MessageBody::None => write!(f, "None"),
        }
    }
}

/// An inter-actor message routed through [`LuaActorServer`].
pub struct Message {
    /// Protocol type tag (see `PTYPE_*` constants).
    pub ptype: u8,
    /// Sender actor id (0 for system-originated messages).
    pub from: i64,
    /// Destination actor id.
    pub to: i64,
    /// Request/response correlation id (0 for fire-and-forget).
    pub session: i64,
    /// Message payload.
    pub data: MessageBody,
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Message {{ ptype: {}, from: 0x{:08x}, to: 0x{:08x}, session: {}, data: {} }}",
            self.ptype, self.from, self.to, self.session, self.data
        )
    }
}

struct Monitor {
    ptype: u8,
    tm: f64,
    from: i64,
    to: i64,
}

/// Global actor registry and message router.
///
/// Manages actor lifecycles, message delivery, environment variables,
/// timers, and coordinated shutdown. Accessed via the [`CONTEXT`] singleton.
pub struct LuaActorServer {
    actor_uuid: AtomicI64,
    actor_counter: AtomicU32,
    exit_code: AtomicI32,
    actors: DashMap<i64, mpsc::UnboundedSender<Message>>,
    unique_actors: DashMap<String, i64>,
    clock: Instant,
    env: DashMap<String, Arc<String>>,
    monitor: DashMap<u64, Monitor>,
    timer_tx: mpsc::UnboundedSender<Timer>,
    timer_rx: Mutex<mpsc::UnboundedReceiver<Timer>>,
    now: DateTime<Utc>,
    time_offset: AtomicU64
}

impl LuaActorServer {
    /// Register an actor and its message sender. Returns an error if a
    /// unique actor with the same name already exists.
    pub fn add_actor(
        &self,
        actor: &mut LuaActor,
        tx: mpsc::UnboundedSender<Message>,
    ) -> Result<(), crate::error::MoonError> {
        if actor.unique {
            if let Some(v) = self.unique_actors.insert(actor.name.clone(), actor.id) {
                self.unique_actors.insert(actor.name.clone(), v);
                return Err(format!("unique actor named {} already exists", actor.name).into());
            }
        }
        self.actors.insert(actor.id, tx);
        self.actor_counter.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn remove(&self, id: i64) -> Option<(i64, mpsc::UnboundedSender<Message>)> {
        self.actors.remove(&id)
    }

    pub fn query(&self, name: &str) -> Option<dashmap::mapref::one::Ref<'_, String, i64>> {
        self.unique_actors.get(name)
    }

    pub fn remove_actor(&self, id: i64, name: &str) {
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
            self.send(Message {
                ptype: PTYPE_SYSTEM,
                from: id,
                to: *v.value(),
                session: 0,
                data: MessageBody::Buffer(Box::new(
                    format!("_service_exit,Actor id:{} exited", id)
                        .into_bytes()
                        .into(),
                )),
            });
        });
    }

    /// Set a runtime environment variable visible to all actors.
    pub fn set_env(&self, key: &str, value: &str) {
        self.env
            .insert(key.to_string(), Arc::new(value.to_string()));
    }

    /// Get a runtime environment variable, or `None` if not set.
    pub fn get_env(&self, key: &str) -> Option<Arc<String>> {
        self.env.get(key).map(|v| v.value().clone())
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code.load(Ordering::Acquire)
    }

    pub fn stopped(&self) -> bool {
        self.actor_counter.load(Ordering::Acquire) == 0
    }

    /// Initiate graceful shutdown: set the exit code and send `PTYPE_SHUTDOWN`
    /// to every registered actor. Only the first call takes effect.
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
            let _ = v.value().send(Message {
                ptype: PTYPE_SHUTDOWN,
                from: 0,
                to: 0,
                session: 0,
                data: MessageBody::None,
            });
        });
    }

    /// Route a message to its destination actor. Returns `Some(msg)` if
    /// the target actor is not registered or the channel is closed.
    pub fn send(&self, msg: Message) -> Option<Message> {
        //log::info!("send message: from {:?} to {} ptype {} session {}", msg.from, msg.to, msg.ptype, msg.session);
        if let Some(addr) = self.actors.get(&msg.to) {
            if let Err(err) = addr.value().send(msg) {
                return Some(err.0);
            } else {
                return None;
            }
        }
        Some(msg)
    }

    /// Wrap `res` in a [`BoxedValue`] and send it to actor `owner`.
    pub fn send_value<T: Send>(&self, protocol_type: u8, owner: i64, session: i64, res: T) -> Option<Message> {
        self.send(Message {
            ptype: protocol_type,
            from: 0,
            to: owner,
            session,
            data: MessageBody::Boxed(Box::new(BoxedValue::new(res))),
        })
    }

    /// Allocate a globally unique actor id.
    pub fn next_actor_id(&self) -> i64 {
        let id: i64 = self.actor_uuid.fetch_add(1, Ordering::AcqRel);
        if id == i64::MAX {
            panic!("actor id overflow");
        }
        id
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

    /// Send an error response back to the requesting actor as a `PTYPE_ERROR`
    /// message. Logs the error and is a no-op if `session < 0`.
    pub fn response_error(&self, from: i64, to: i64, session: i64, err: String) {
        if session >= 0 {
            log::error!("{}. ({}:{})", err, file!(), line!());
        } else {
            self.send(Message {
                ptype: PTYPE_ERROR,
                from,
                to,
                session: -session,
                data: MessageBody::Buffer(Box::new(err.into_bytes().into())),
            });
        }
    }

    pub fn update_monitor(&self, ptype: u8, tm: f64, from: i64, to: i64) {
        THREAD_ID.with(|id| {
            self.monitor.insert(
                *id,
                Monitor {
                    ptype,
                    tm,
                    from,
                    to,
                },
            );
        });
    }

    pub fn check_monitor(&self) {
        self.monitor.iter().for_each(|v| {
            let w = v.value();
            //log::info!("check_monitor thread id: {:?} tm: {:?} clock: {}. diff {}", v.key(), w.tm, self.clock(), self.clock() - w.tm);
            if w.tm > 0.0 && self.clock() - w.tm >= 1.0 {
                let s =  format!("endless_loop,A message PTYPE {} from {:08X} to {:08X} maybe in an endless loop (tm={})", v.ptype, v.from, v.to, (self.now + Duration::from_secs_f64(v.tm)).with_timezone(&Local));
                log::error!("{}", s);
                self.send(Message {
                    ptype: PTYPE_SYSTEM,
                    from: v.to,
                    to: BOOTSTRAP_ACTOR_ADDR,
                    session: 0,
                    data: MessageBody::Buffer(Box::new(s.into())),
                });
            }
        });
    }
}

pub fn run_monitor() {
    thread::spawn(|| loop {
        if CONTEXT.exit_code() != i32::MAX && CONTEXT.stopped() {
            break;
        }
        thread::sleep(Duration::from_secs(5));
        CONTEXT.check_monitor();
    });
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
struct Timer {
    expiry_clock: i64,
    timer_id: i64,
    owner: i64,
}

pub fn insert_timer(owner: i64, timer_id: i64, interval: u64) {
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
                    CONTEXT.send(Message {
                        ptype: PTYPE_TIMER,
                        from: timer.timer_id,
                        to: timer.owner,
                        session: 0,
                        data: MessageBody::None,
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

/// Parameters for creating a new [`LuaActor`](super::actor::LuaActor).
pub struct LuaActorParam {
    pub id: i64,
    pub unique: bool,
    pub creator: i64,
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
    use std::sync::atomic::AtomicI64;

    // --- BoxedValue tests ---

    #[test]
    fn boxed_value_drop_frees_memory() {
        static DROP_COUNT: AtomicI64 = AtomicI64::new(0);

        struct Tracked;
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);
        {
            let _bv = BoxedValue::new(Tracked);
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn boxed_value_into_raw_prevents_drop() {
        static DROP_COUNT: AtomicI64 = AtomicI64::new(0);

        struct Tracked(i32);
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);
        let ptr = {
            let mut bv = BoxedValue::new(Tracked(42));
            bv.into_raw()
        };
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 0, "into_raw should prevent drop");

        let recovered = unsafe { Box::from_raw(ptr as *mut Tracked) };
        assert_eq!(recovered.0, 42);
        drop(recovered);
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn boxed_value_as_ptr_preserves_value() {
        let bv = BoxedValue::new(999i64);
        let ptr = bv.as_ptr();
        let val = unsafe { *(ptr as *const i64) };
        assert_eq!(val, 999);
    }

    #[test]
    fn boxed_value_with_string() {
        let bv = BoxedValue::new("hello world".to_string());
        let ptr = bv.as_ptr();
        let val = unsafe { &*(ptr as *const String) };
        assert_eq!(val, "hello world");
    }

    #[test]
    fn boxed_value_with_vec() {
        let data = vec![1u8, 2, 3, 4, 5];
        let bv = BoxedValue::new(data.clone());
        let ptr = bv.as_ptr();
        let val = unsafe { &*(ptr as *const Vec<u8>) };
        assert_eq!(val, &data);
    }

    // --- MessageBody tests ---

    #[test]
    fn message_data_size_is_16_bytes() {
        assert_eq!(std::mem::size_of::<MessageBody>(), 16);
    }

    #[test]
    fn message_data_display_isize() {
        let md = MessageBody::ISize(42);
        assert_eq!(format!("{}", md), "ISize(42)");
    }

    #[test]
    fn message_data_display_none() {
        let md = MessageBody::None;
        assert_eq!(format!("{}", md), "None");
    }

    #[test]
    fn message_data_display_buffer() {
        let buf = Buffer::from("hello");
        let md = MessageBody::Buffer(Box::new(buf));
        let s = format!("{}", md);
        assert!(s.starts_with("Buffer("));
    }

    #[test]
    fn message_data_display_boxed() {
        let bv = BoxedValue::new(123i32);
        let md = MessageBody::Boxed(Box::new(bv));
        let s = format!("{}", md);
        assert!(s.starts_with("Boxed(0x"));
    }

    #[test]
    fn message_data_boxed_auto_drops() {
        static DROP_COUNT: AtomicI64 = AtomicI64::new(0);

        struct Tracked;
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);
        {
            let _md = MessageBody::Boxed(Box::new(BoxedValue::new(Tracked)));
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1, "BoxedValue in MessageBody should be dropped");
    }

    // --- Message Display ---

    #[test]
    fn message_display() {
        let msg = Message {
            ptype: PTYPE_LUA,
            from: 1,
            to: 2,
            session: 100,
            data: MessageBody::None,
        };
        let s = format!("{}", msg);
        assert!(s.contains("ptype: 3"));
        assert!(s.contains("session: 100"));
    }

    // --- LuaActorServer env tests ---

    #[test]
    fn context_env_set_get() {
        CONTEXT.set_env("TEST_KEY_1", "test_value");
        let val = CONTEXT.get_env("TEST_KEY_1");
        assert!(val.is_some());
        assert_eq!(val.unwrap().as_str(), "test_value");
    }

    #[test]
    fn context_env_missing_key() {
        let val = CONTEXT.get_env("NONEXISTENT_KEY_12345");
        assert!(val.is_none());
    }

    #[test]
    fn context_env_overwrite() {
        CONTEXT.set_env("TEST_KEY_2", "old");
        CONTEXT.set_env("TEST_KEY_2", "new");
        let val = CONTEXT.get_env("TEST_KEY_2").unwrap();
        assert_eq!(val.as_str(), "new");
    }

    // --- LuaActorServer send tests ---

    #[test]
    fn context_send_to_nonexistent_returns_message() {
        let msg = Message {
            ptype: PTYPE_LUA,
            from: 0,
            to: 999999,
            session: 1,
            data: MessageBody::ISize(42),
        };
        let result = CONTEXT.send(msg);
        assert!(result.is_some(), "send to nonexistent actor should return the message");
    }

    #[test]
    fn context_send_to_registered_actor() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let id = CONTEXT.next_actor_id();
        CONTEXT.actors.insert(id, tx);

        let msg = Message {
            ptype: PTYPE_TEXT,
            from: 0,
            to: id,
            session: 7,
            data: MessageBody::ISize(99),
        };
        let result = CONTEXT.send(msg);
        assert!(result.is_none(), "send to registered actor should succeed");

        let received = rx.try_recv().unwrap();
        assert_eq!(received.ptype, PTYPE_TEXT);
        assert_eq!(received.session, 7);
        if let MessageBody::ISize(v) = received.data {
            assert_eq!(v, 99);
        } else {
            panic!("expected ISize data");
        }

        CONTEXT.actors.remove(&id);
    }

    // --- next_actor_id ---

    #[test]
    fn context_next_actor_id_is_monotonic() {
        let a = CONTEXT.next_actor_id();
        let b = CONTEXT.next_actor_id();
        assert!(b > a);
    }

    // --- clock ---

    #[test]
    fn context_clock_is_positive() {
        let c = CONTEXT.clock();
        assert!(c >= 0.0);
    }
}
