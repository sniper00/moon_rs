use chrono::{DateTime, Local, Utc};
use dashmap::DashMap;
use lazy_static::lazy_static;
use reqwest::ClientBuilder;
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

use super::{actor::LuaActor, buffer::Buffer, log::Logger};

pub const PTYPE_SYSTEM: i8 = 1;
pub const PTYPE_TEXT: i8 = 2;
pub const PTYPE_LUA: i8 = 3;
pub const PTYPE_ERROR: i8 = 4;
pub const PTYPE_DEBUG: i8 = 5;
pub const PTYPE_SHUTDOWN: i8 = 6;
pub const PTYPE_TIMER: i8 = 7;
pub const PTYPE_SOCKET_TCP: i8 = 8;
pub const PTYPE_SOCKET_UDP: i8 = 9;
pub const PTYPE_INTEGER: i8 = 12;
pub const PTYPE_HTTP: i8 = 13;
pub const PTYPE_QUIT: i8 = 14;

pub const BOOTSTRAP_ACTOR_ADDR: i64 = 1;

static GLOBAL_THREAD_ID: AtomicU64 = AtomicU64::new(1);

lazy_static! {
    pub static ref CONTEXT: LuaActorServer = {

        let (timer_tx, timer_rx) = mpsc::unbounded_channel();

        LuaActorServer {
            actor_uuid: AtomicI64::new(1),
            timer_uuid: AtomicI64::new(1),
            net_uuid: AtomicI64::new(1),
            actor_counter: AtomicU32::new(0),
            exit_code: AtomicI32::new(i32::MAX),
            actors: DashMap::new(),
            unique_actors: DashMap::new(),
            clock: Instant::now(),
            http_clients: DashMap::new(),
            env: DashMap::new(),
            net: DashMap::new(),
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

#[derive(Debug)]
pub struct Message {
    pub ptype: i8,
    pub from: i64,
    pub to: i64,
    pub session: i64,
    pub data: Option<Box<Buffer>>,
}

#[derive(Debug)]
pub enum NetOp {
    Accept(i64, i64),                         //owner,session
    ReadUntil(i64, i64, usize, Vec<u8>, u64), //owner,session,max_size
    ReadBytes(i64, i64, usize, u64),          //owner,session,size
    Write(i64, Box<Buffer>, bool),            //owner,data
    Close(),
}

pub struct NetChannel(pub mpsc::Sender<NetOp>, pub mpsc::Sender<NetOp>);

struct Monitor {
    ptype: i8,
    tm: f64,
    from: i64,
    to: i64,
}

pub struct LuaActorServer {
    actor_uuid: AtomicI64,
    timer_uuid: AtomicI64,
    net_uuid: AtomicI64,
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
    time_offset: AtomicU64,
    pub net: DashMap<i64, NetChannel>,
    http_clients: DashMap<String, reqwest::Client>,
}

impl LuaActorServer {
    pub fn add_actor(&self, actor: &mut LuaActor) -> Result<(), String> {
        let id: i64 = self.actor_uuid.fetch_add(1, Ordering::AcqRel);
        self.actor_counter.fetch_add(1, Ordering::AcqRel);
        self.actors.insert(id, actor.tx.clone());
        actor.id = id;
        if actor.unique {
            if let Some(v) = self.unique_actors.insert(actor.name.clone(), actor.id) {
                self.unique_actors.insert(actor.name.clone(), v);
                return Err(format!("unique actor named {} already exists", actor.name));
            }
        }
        Ok(())
    }

    pub fn remove(&self, id: i64) -> Option<(i64, mpsc::UnboundedSender<Message>)> {
        self.actors.remove(&id)
    }

    pub fn query(&self, name: &str) -> Option<dashmap::mapref::one::Ref<'_, String, i64>> {
        self.unique_actors.get(name)
    }

    pub fn remove_actor(&self, id: i64, unique: bool) {
        self.actors.remove(&id);
        if unique {
            self.unique_actors.remove(&id.to_string());
        }
        self.actor_counter.fetch_sub(1, Ordering::AcqRel);

        //notify actor exit to unique actors
        self.unique_actors.iter().for_each(|v| {
            self.send(Message {
                ptype: PTYPE_SYSTEM,
                from: id,
                to: *v.value(),
                session: 0,
                data: Some(Box::new(
                    format!("_service_exit,Actor id:{} quited", id)
                        .into_bytes()
                        .into(),
                )),
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
            let _ = v.value().send(Message {
                ptype: PTYPE_SHUTDOWN,
                from: 0,
                to: 0,
                session: 0,
                data: None,
            });
        });
    }

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

    pub fn next_net_fd(&self) -> i64 {
        let fd = self.net_uuid.fetch_add(1, Ordering::AcqRel);
        if fd == i64::MAX {
            panic!("net fd overflow");
        }
        fd
    }

    pub fn next_timer_id(&self) -> i64 {
        let id = self.timer_uuid.fetch_add(1, Ordering::AcqRel);
        if id == 0 {
            panic!("timer fd overflow");
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

    pub fn response_error(&self, from: i64, to: i64, session: i64, err: String) {
        if session >= 0 {
            log::debug!("{}. ({}:{})", err, file!(), line!());
        } else {
            self.send(Message {
                ptype: PTYPE_ERROR,
                from,
                to,
                session: -session,
                data: Some(Box::new(err.into_bytes().into())),
            });
        }
    }

    pub fn update_monitor(&self, ptype: i8, tm: f64, from: i64, to: i64) {
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
                    data: Some(Box::new(s.into())),
                });
            }
        });
    }

    pub fn get_http_client(&self, timeout: u64, proxy: &String) -> reqwest::Client {
        let name = format!("{}_{}", timeout, proxy);
        if let Some(client) = self.http_clients.get(&name) {
            return client.clone();
        }

        if timeout > 100 {
            log::warn!("http client timeout {} is too long", timeout);
        }

        let builder = ClientBuilder::new()
            .timeout(Duration::from_secs(timeout))
            .use_rustls_tls()
            .tcp_nodelay(true);

        let client = if proxy.is_empty() {
            builder.build().unwrap_or_default()
        } else {
            builder.proxy(reqwest::Proxy::all(proxy).unwrap()).build().unwrap_or_default()
        };

        self.http_clients.insert(name.to_string(), client.clone());
        client
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
                        data: None,
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
    pub unique: bool,
    pub creator: i64,
    pub session: i64,
    pub memlimit: i64,
    pub name: String,
    pub source: String,
    pub params: String,
    pub block: bool,
}
