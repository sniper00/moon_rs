use crate::context::{LuaActorParam, Message};
use lib_lua::laux::{LuaState, LuaStateBox, LuaThread};
use tokio::sync::mpsc;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

pub use lib_lua as ffi;

pub enum ActorReceiver {
    TokioReceiver(mpsc::UnboundedReceiver<Message>),
    ThreadReceiver(UnboundedReceiver<Message>),
}

impl ActorReceiver {
    pub fn recv(&mut self) -> Message {
        match self {
            ActorReceiver::ThreadReceiver(rx) => rx.recv(),
            _ => unreachable!(),
        }
    }

    pub fn try_recv(&mut self) -> Option<Message> {
        match self {
            ActorReceiver::ThreadReceiver(rx) => rx.try_recv(),
            ActorReceiver::TokioReceiver(rx) => rx.try_recv().ok(),
        }
    }

    pub async fn async_recv(&mut self) -> Option<Message> {
        match self {
            ActorReceiver::TokioReceiver(rx) => rx.recv().await,
            _ => unreachable!(),
        }
    }

    pub fn close(&mut self) {
        match self {
            ActorReceiver::TokioReceiver(rx) => rx.close(),
            ActorReceiver::ThreadReceiver(_) => {}
        }
    }
}

pub enum ActorSender {
    TokioSender(mpsc::UnboundedSender<Message>),
    ThreadSender(UnboundedSender<Message>),
}

impl ActorSender {
    pub fn send(&self, msg: Message) -> Result<(), Message> {
        match self {
            ActorSender::TokioSender(tx) => tx
                .send(msg)
                .map_err(|e| e.0),
            ActorSender::ThreadSender(tx) => tx
                .send(msg)
                .map_err(|e| e),
        }
    }
}

pub struct LuaActor {
    pub ok: bool,
    pub unique: bool,
    pub id: i64,
    uuid: i64,
    pub name: String,
    pub main_state: Option<LuaStateBox>,
    pub callback_state: LuaThread,
    pub mem: isize,
    pub mem_limit: isize,
    pub mem_warning: isize,
}

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

/// A small concurrent unbounded queue with separate write/read buffers to
/// minimize contention (swap-on-read idiom).
struct ConcurrentQueue<T> {
    queue: Mutex<VecDeque<T>>,
    producer: Condvar,
}

impl<T> ConcurrentQueue<T> {
    fn new() -> Self {
        ConcurrentQueue {
            queue: Mutex::new(VecDeque::new()),
            producer: Condvar::new(),
        }
    }

    /// Push an item to the queue.
    fn push_back(&self, item: T) -> usize {
        let len = {
            let mut queue = self.queue.lock().unwrap();
            queue.push_back(item);
            queue.len()
        };
        if len == 1 {
            self.producer.notify_one();
        }
        len
    }

    /// Blocking pop an item. Wait on condvar if both queues are empty.
    fn swap(&self, other: &mut VecDeque<T>) {
        let mut queue = self.queue.lock().unwrap();
        // wait_while takes ownership of the MutexGuard, so move `queue` into it and assign back
        queue = self.producer.wait_while(queue, |q| q.is_empty()).unwrap();
        std::mem::swap(&mut *queue, other);
    }

    fn try_swap(&self, other: &mut VecDeque<T>) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if queue.is_empty() {
            return false;
        }
        std::mem::swap(&mut *queue, other);
        true
    }

    fn len(&self) -> usize {
        let queue = self.queue.lock().unwrap();
        queue.len()
    }

    fn is_empty(&self) -> bool {
        let queue = self.queue.lock().unwrap();
        queue.is_empty()
    }
}

/// Sender side (cloneable).
#[derive(Clone)]
pub struct UnboundedSender<T> {
    q: Arc<ConcurrentQueue<T>>,
}

impl<T> UnboundedSender<T> {
    pub fn send(&self, item: T) -> Result<(), T> {
        // always succeeds for unbounded queue
        self.q.push_back(item);
        Ok(())
    }
}

/// Receiver side (non-cloneable, follows mpsc::Receiver style try_recv signature).
pub struct UnboundedReceiver<T> {
    q: Arc<ConcurrentQueue<T>>,
    reader: VecDeque<T>,
}

impl<T> UnboundedReceiver<T> {
    /// Blocking receive. Waits until an item is available.
    pub fn recv(&mut self) -> T {
        if let Some(v) = self.reader.pop_front() {
            return v;
        }
        self.q.swap(&mut self.reader);
        self.reader.pop_front().unwrap()
    }

    pub fn try_recv(&mut self) -> Option<T> {
        if let Some(v) = self.reader.pop_front() {
            return Some(v);
        }
        if self.q.try_swap(&mut self.reader){
            return self.reader.pop_front();
        }
        None
    }

    pub fn len(&self) -> usize {
        self.q.len()
    }

    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }
}

#[derive(Debug)]
pub enum TryRecvError {
    Empty,
}

/// Create a new unbounded channel returning (Sender, Receiver)
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let q = Arc::new(ConcurrentQueue::new());
    (
        UnboundedSender { q: q.clone() },
        UnboundedReceiver {
            q,
            reader: VecDeque::new(),
        },
    )
}
