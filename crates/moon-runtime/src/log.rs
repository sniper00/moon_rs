use chrono::Local;
use colored::*;
use log::{Level, Metadata, Record};
use std::{
    error::Error,
    fmt::{self, Write as _},
    fs::{self, File, OpenOptions},
    io::prelude::*,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        mpsc::{self, Sender},
    },
    thread,
};

use crate::{buffer::Buffer, context::CONTEXT};

const STATE_INIT: u8 = 0;
const STATE_RUNNING: u8 = 1;
const STATE_STOPPING: u8 = 2;
const STATE_STOPPED: u8 = 3;

struct LoggerConext {
    enable_stdout: AtomicBool,
    state: Arc<AtomicU8>,
    level: AtomicU8,
}

pub struct Logger {
    sender: Sender<LogMessage>,
    state: Arc<LoggerConext>,
    // Number of log lines enqueued but not yet written by the consumer thread.
    pending: Arc<AtomicUsize>,
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
            // Append the source location (file:line) captured by the `log` macros so
            // every Rust log carries it automatically, matching the manual
            // `({file}:{line})` convention used elsewhere.
            let size_hint = record.args().as_str().map_or(128, str::len);
            let result = self
                .make_line(true, record.level(), size_hint + 64)
                .and_then(|mut line| {
                    write!(
                        line,
                        "00000000| {} ({}:{})",
                        record.args(),
                        record.file().unwrap_or("<unknown>"),
                        record.line().unwrap_or(0)
                    )?;
                    Ok(line)
                });
            match result {
                Ok(line) => self.write(line),
                Err(err) => {
                    // Log::log cannot return errors; do not recurse into this logger.
                    let _ = writeln!(std::io::stderr().lock(), "log formatting failed: {err}");
                }
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
            enable_stdout: AtomicBool::new(true),
            state: Arc::new(AtomicU8::new(STATE_INIT)),
            level: AtomicU8::new(Logger::level_to_u8(Level::Debug)),
        });

        let clone_state = state.clone();

        let pending = Arc::new(AtomicUsize::new(0));
        let clone_pending = pending.clone();

        thread::spawn(move || {
            let mut file: Option<File> = None;

            clone_state.state.store(STATE_RUNNING, Ordering::Release);

            loop {
                match receiver.recv() {
                    Ok(message) => match message {
                        LogMessage::Line(mut line) => {
                            clone_pending.fetch_sub(1, Ordering::Relaxed);
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

                                let _ = writeln!(std::io::stdout().lock(), "{console_str}");
                            }

                            if let Some(target) = file.as_mut() {
                                if let Err(err) = target
                                    .write_all(line.as_slice())
                                    .and_then(|_| target.write_all(b"\n"))
                                {
                                    let _ = writeln!(
                                        std::io::stderr().lock(),
                                        "log file write failed: {err}"
                                    );
                                    file = None;
                                }
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

        Logger {
            sender,
            state,
            pending,
        }
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

    pub fn make_line(
        &self,
        mut enable_stdout: bool,
        level: Level,
        data_size: usize,
    ) -> Result<Buffer, fmt::Error> {
        if level == Level::Error {
            CONTEXT.increment_error_count();
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

        CONTEXT
            .now()
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S.%3f ")
            .write_to(&mut line)?;

        line.write_str(Self::level_to_string(level));

        Ok(line)
    }

    pub fn write(&self, data: Buffer) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        if self.sender.send(LogMessage::Line(data)).is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Number of log lines enqueued but not yet flushed by the consumer thread.
    pub fn pending_count(&self) -> usize {
        self.pending.load(Ordering::Acquire)
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
        // These two settings must apply whether or not a logfile was
        // requested. Previously they only ran inside the `log_file` branch,
        // so a purely console-based run silently ignored the configured level
        // (staying at the Debug default) and could not be told to stop writing
        // to stdout.
        self.state
            .enable_stdout
            .store(enable_stdout, Ordering::Release);
        self.set_log_level(Logger::string_to_level(log_level));

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
            } else {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("open file failed: {}", file),
                )));
            }
        }

        log::set_logger(self).map_err(|err| err.to_string())?;
        log::set_max_level(Level::Debug.to_level_filter());
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/log.rs"]
mod tests;
