use dashmap::DashMap;
use lazy_static::lazy_static;
use reqwest::ClientBuilder;
use std::{
    sync::{
        atomic::{AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

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
        let client_builder = ClientBuilder::new()
            .timeout(Duration::from_secs(5))
            .use_rustls_tls()
            .tcp_nodelay(true);

        LuaActorServer {
            actor_uuid: AtomicI64::new(1),
            timer_uuid: AtomicI64::new(1),
            net_uuid: AtomicI64::new(1),
            actor_counter: AtomicU32::new(0),
            exit_code: AtomicI32::new(i32::MAX),
            actors: DashMap::new(),
            unique_actors: DashMap::new(),
            clock: Instant::now(),
            http_client: client_builder.build().unwrap_or_default(),
            env: DashMap::new(),
            net: DashMap::new(),
            monitor: DashMap::new(),
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
    pub http_client: reqwest::Client,
    env: DashMap<String, Arc<String>>,
    pub net: DashMap<i64, NetChannel>,
    monitor: DashMap<u64, Monitor>,
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

    pub fn get(
        &self,
        id: i64,
    ) -> Option<dashmap::mapref::one::Ref<'_, i64, mpsc::UnboundedSender<Message>>> {
        self.actors.get(&id)
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
        if fd == 0 {
            self.net_uuid.fetch_add(1, Ordering::AcqRel);
        }
        fd
    }

    pub fn next_timer_id(&self) -> i64 {
        let id = self.timer_uuid.fetch_add(1, Ordering::AcqRel);
        if id == 0 {
            self.timer_uuid.fetch_add(1, Ordering::AcqRel);
        }
        id
    }

    pub fn clock(&self) -> f64 {
        self.clock.elapsed().as_secs_f64()
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
            // log::info!("check_monitor thread id: {:?}", v.key());
            let w = v.value();
            if w.tm > 0.0 && self.clock() >= 5.0 {
                let s =  format!("endless_loop,A message PTYPE {} from {:08X} to {:08X} maybe in an endless loop (tm={})", v.ptype, v.from, v.to, v.tm);
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
