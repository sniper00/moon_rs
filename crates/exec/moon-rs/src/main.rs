use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use chrono::Local;
use lib_lua::ffi;

use std::env;
use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use std::{fmt, path::Path};

use lib_core::context::{LuaActorParam, CONTEXT};
use lib_lualib_rs::lua_actor;

fn print_usage() {
    println!("Usage:");
    println!("    moon_rs script.lua [args]\n");
    println!("Examples:");
    println!("    moon_rs main.lua hello\n");
}

#[derive(Debug)]
struct ReturnError(String);

impl fmt::Display for ReturnError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "There is an error: {}", self.0)
    }
}

impl Error for ReturnError {}

fn setup_signal() {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        use tokio::signal::unix::SignalKind;

        tokio::spawn(async {
            const SIGTERM: u32 = 15;
            const SIGINT: u32 = 2;
            const SIGQUIT: u32 = 3;

            loop {
                let mut stream_terminate =
                    tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
                let mut stream_interrupt =
                    tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
                let mut stream_quit = tokio::signal::unix::signal(SignalKind::quit()).unwrap();
                let v = tokio::select! {
                    _= stream_terminate.recv() =>(SIGTERM, "terminate"),
                    _= stream_interrupt.recv() =>(SIGINT, "interrupt"),
                    _= stream_quit.recv() =>(SIGQUIT, "quit")
                };

                log::warn!(
                    "{} signal received, stopping system... ({}:{})",
                    v.1,
                    file!(),
                    line!()
                );
                CONTEXT.shutdown(v.0);
            }
        });
    }

    #[cfg(target_os = "windows")]
    unsafe {
        extern "system" fn console_ctrl_handler(ctrl_type: lib_common::DWORD) -> i32 {
            const CTRL_C_EVENT: lib_common::DWORD = 0;
            const CTRL_CLOSE_EVENT: lib_common::DWORD = 2;
            const CTRL_LOGOFF_EVENT: lib_common::DWORD = 5;
            const CTRL_SHUTDOWN_EVENT: lib_common::DWORD = 6;

            match ctrl_type {
                CTRL_C_EVENT => {
                    log::warn!(
                        "CTRL_C_EVENT received, stopping system... ({}:{})",
                        file!(),
                        line!()
                    );
                    CONTEXT.shutdown(CTRL_C_EVENT);
                    1
                }
                CTRL_CLOSE_EVENT | CTRL_LOGOFF_EVENT | CTRL_SHUTDOWN_EVENT => {
                    CONTEXT.shutdown(ctrl_type);
                    while !CONTEXT.stopped() {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    1
                }
                _ => 0,
            }
        }

        lib_common::set_concole_ctrl_handler(console_ctrl_handler);
        let args: Vec<String> = env::args().collect();
        let mut str = String::new();
        for (i, arg) in args.iter().enumerate() {
            str.push_str(arg.as_str());
            if i == 0 {
                str.push_str("(PID: ");
                str.push_str(&std::process::id().to_string());
                str.push(')');
            }
            str.push(' ');
        }
        let cstr = std::ffi::CString::new(str).expect("CString::new failed");
        lib_common::set_console_title(cstr.as_ptr());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    setup_signal();

    init_logger();

    unsafe {
        ffi::luaL_initcodecache();
    }

    let mut result: Result<(), Box<dyn Error>> = Ok(());

    let args: Vec<String> = env::args().collect();
    let mut argn = 1;
    if args.len() <= argn {
        print_usage();
        result = Err(Box::new(ReturnError("invalid arguments".to_string())));
        return result;
    }

    let mut bootstrap = args[argn].clone();
    let path = Path::new(&bootstrap);
    if !path.is_file() {
        result = Err(Box::new(ReturnError(format!(
            "bootstrap file not found: {}",
            bootstrap
        ))));
        return result;
    }

    if path.extension().map(|ext| ext.to_str().unwrap()) != Some("lua") {
        result = Err(Box::new(ReturnError(format!(
            "bootstrap is not a lua file: {}",
            bootstrap
        ))));
        return result;
    }

    let mut search_path = env::current_dir()?.canonicalize()?;
    if !search_path.join("lualib").is_dir() {
        search_path = env::current_exe()?.canonicalize()?.join("lualib");
    }

    if !search_path.is_dir() {
        result = Err(Box::new(ReturnError(format!(
            "lualib not found: {}",
            search_path.to_str().unwrap_or("")
        ))));
        return result;
    }

    if let Some(path_with_no_prefix) = search_path.to_string_lossy().strip_prefix(r"\\?\") {
        search_path = PathBuf::from(path_with_no_prefix);
    }

    let strpath = search_path.to_string_lossy().replace('\\', "/");
    //Lualib directories are added to the lua search path
    let package_path = format!("package.path='{}/lualib/?.lua;'..package.path;", strpath);

    CONTEXT.set_env("PATH", package_path.as_ref());

    let cwd = path.parent().unwrap_or(Path::new("./"));
    //Change the working directory to the directory where the opened file is located.
    env::set_current_dir(cwd)?;

    bootstrap = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .as_ref()
        .to_string();

    argn += 1;

    let mut arg = package_path;
    arg.push_str("return {");
    for (i, v) in args.iter().enumerate().skip(argn) {
        if i > 1 {
            arg.push(',');
        }
        arg.push_str(&format!("\"{}\"", v));
    }
    arg.push('}');

    log::info!("system start. ({}:{})", file!(), line!());

    lua_actor::new_actor(LuaActorParam {
        unique: true,
        creator: 0,
        session: 0,
        memlimit: 0,
        name: "bootstrap".to_string(),
        source: bootstrap,
        params: arg,
    });

    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if CONTEXT.exit_code() != u32::MAX && CONTEXT.stopped() {
            break;
        }
    }

    CONTEXT.net.clear();

    log::info!("system end. ({}:{})", file!(), line!());

    let error_code = CONTEXT.exit_code();
    if error_code != 0 {
        result = Err(Box::new(ReturnError(error_code.to_string())));
    }

    result?;
    Ok(())
}

fn init_logger() {
    use env_logger::fmt::Color;
    use env_logger::Env;
    use log::LevelFilter;

    let env = Env::default().filter_or("MY_LOG_LEVEL", "debug");

    env_logger::Builder::from_env(env)
        .format(|buf, record| {
            let level_color = match record.level() {
                log::Level::Error => Color::Red,
                log::Level::Warn => Color::Yellow,
                log::Level::Info => Color::Green,
                log::Level::Debug | log::Level::Trace => Color::Cyan,
            };

            let mut level_style = buf.style();
            level_style.set_color(level_color).set_bold(true);

            let mut style = buf.style();
            style.set_color(Color::White).set_dimmed(true);

            writeln!(
                buf,
                "{} | {:<5} | {}",
                Local::now().format("%Y-%m-%d %H:%M:%S.%3f"),
                level_style.value(record.level()),
                record.args()
            )
        })
        .filter(None, LevelFilter::Debug)
        .init();

    log::info!("env_logger initialized. ({}:{})", file!(), line!());
}
