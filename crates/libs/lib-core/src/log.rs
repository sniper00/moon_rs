use chrono::Local;
use colored::*;
use log::{Level, Metadata, Record};
use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::prelude::*,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        mpsc::{self, Sender},
        Arc,
    },
    thread
};

use crate::{buffer::Buffer, context::CONTEXT};

const STATE_INIT: u8 = 0;
const STATE_RUNNING: u8 = 1;
const STATE_STOPPING: u8 = 2;
const STATE_STOPPED: u8 = 3;

struct LoggerConext {
    error_count: AtomicUsize,
    enable_stdout: AtomicBool,
    state: Arc<AtomicU8>,
    level: AtomicU8,
}

pub struct Logger {
    sender: Sender<LogMessage>,
    state: Arc<LoggerConext>,
}

enum LogMessage {
    Line(Buffer),
    File(File),
    Stop,
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Logger::u8_to_level(self.state.level.load(Ordering::Acquire))
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if let Some(str_record) = record.args().as_str() {
                let mut line = self.make_line(true, record.level(), str_record.bytes().len());
                line.write_str(str_record);
                self.write(line);
            } else {
                let str_record = record.args().to_string();
                let mut line = self.make_line(true, record.level(), str_record.bytes().len());
                line.write_str(str_record.as_str());
                self.write(line);
            }
        }
    }

    fn flush(&self) {}
}

impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger {
    pub fn new() -> Logger {
        let (sender, receiver) = mpsc::channel::<LogMessage>();

        let state = Arc::new(LoggerConext {
            error_count: AtomicUsize::new(0),
            enable_stdout: AtomicBool::new(true),
            state: Arc::new(AtomicU8::new(STATE_INIT)),
            level: AtomicU8::new(Logger::level_to_u8(Level::Debug)),
        });

        let clone_state = state.clone();

        thread::spawn(move || {
            let mut file: Option<File> = None;

            clone_state.state.store(STATE_RUNNING, Ordering::Release);

            loop {
                match receiver.recv() {
                    Ok(message) => match message {
                        LogMessage::Line(mut line) => {
                            let enable_stdout = line.read_u8(0) != 0;
                            let level = line.read_u8(1);
                            line.consume(2);
                            if enable_stdout {
                                let console_str: ColoredString = match Logger::u8_to_level(level) {
                                    Level::Error => line.to_string().red(),
                                    Level::Warn => line.to_string().yellow(),
                                    Level::Info => line.to_string().normal(),
                                    Level::Debug => line.to_string().green(),
                                    Level::Trace => line.to_string().normal(),
                                };

                                println!("{}", console_str);
                            }

                            if let Some(ref mut file) = file {
                                file.write_all(line.as_slice()).unwrap();
                                file.write_all(b"\n").unwrap();
                            }
                        }
                        LogMessage::File(new_file) => {
                            file = Some(new_file);
                        }
                        LogMessage::Stop => {
                            break;
                        }
                    },
                    Err(_) => {
                        if STATE_STOPPING == clone_state.state.load(Ordering::Acquire) {
                            break;
                        }
                    }
                }
            }
            clone_state.state.store(STATE_STOPPED, Ordering::Release);
        });

        Logger { sender, state }
    }

    pub fn level_to_u8(level: Level) -> u8 {
        match level {
            Level::Error => 1,
            Level::Warn => 2,
            Level::Info => 3,
            Level::Debug => 4,
            Level::Trace => 5,
        }
    }

    pub fn u8_to_level(lv: u8) -> Level {
        match lv {
            1 => Level::Error,
            2 => Level::Warn,
            3 => Level::Info,
            4 => Level::Debug,
            5 => Level::Trace,
            _ => Level::Trace,
        }
    }

    pub fn string_to_level(lv: String) -> Level {
        match lv.to_uppercase().as_str() {
            "EROR" => Level::Error,
            "WARN" => Level::Warn,
            "INFO" => Level::Info,
            "DBUG" => Level::Debug,
            "TRCE" => Level::Trace,
            _ => Level::Trace,
        }
    }

    pub fn level_to_string(level: Level) -> &'static str {
        match level {
            Level::Error => "EROR|",
            Level::Warn => "WARN|",
            Level::Info => "INFO|",
            Level::Debug => "DBUG|",
            Level::Trace => "TRCE|",
        }
    }

    pub fn make_line(&self, mut enable_stdout: bool, level: Level, data_size: usize) -> Buffer {
        if level == Level::Error {
            self.state.error_count.fetch_add(1, Ordering::Release);
        }

        let self_enable_stdout = self.state.enable_stdout.load(Ordering::Acquire);

        enable_stdout = if self_enable_stdout {
            enable_stdout
        } else {
            self_enable_stdout
        };

        let mut line = Buffer::with_capacity(if data_size > 0 { 64 + data_size } else { 256 });
        line.write(if enable_stdout { 1 } else { 0 });
        line.write(Logger::level_to_u8(level));

        line.write_str(
            CONTEXT
                .now()
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S.%3f ")
                .to_string()
                .as_str(),
        );

        line.write_str(Self::level_to_string(level));

        line
    }

    pub fn write(&self, data: Buffer) {
        let _ = self.sender.send(LogMessage::Line(data));
    }

    pub fn stop(&self) {
        let _ = self.sender.send(LogMessage::Stop);
    }

    pub fn stopped(&self) -> bool {
        self.state.state.load(Ordering::Acquire) == STATE_STOPPED
    }

    pub fn set_log_level(&self, level: Level) {
        self.state
            .level
            .store(Logger::level_to_u8(level), Ordering::Release);
    }

    pub fn get_log_level(&self) -> Level {
        Logger::u8_to_level(self.state.level.load(Ordering::Acquire))
    }

    pub fn setup_logger(
        &'static self,
        enable_stdout: bool,
        log_file: Option<String>,
        log_level: String,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(file) = log_file {
            let path = Path::new(&file);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            if let Ok(file) = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(file.clone())
            {
                let _ = self.sender.send(LogMessage::File(file));
                self.state
                    .enable_stdout
                    .store(enable_stdout, Ordering::Release);
                self.set_log_level(Logger::string_to_level(log_level));
            } else {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("open file failed: {}", file),
                )));
            }
        }

        log::set_logger(self).expect("set logger failed");
        log::set_max_level(Level::Debug.to_level_filter());
        Ok(())
    }
}
