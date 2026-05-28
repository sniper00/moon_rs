use actor::{
    context::{self, LuaActorParam, CONTEXT, LOGGER},
    error::{MoonError, Result},
};
use lualib::lua_actor;
use luars::{Lua, LuaApi, LuaValue, SafeOption, Stdlib, LuaTable};
use mimalloc::MiMalloc;
use std::{
    env,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Marker string searched in the bootstrap script to enable two-phase init.
///
/// When the bootstrap `.lua` file contains `_G["__init__"]` anywhere in its
/// source, the runtime executes the script in a **temporary, isolated VM**
/// before starting the actor system. Inside that VM the global `__init__` is
/// set to `true` so the script can branch on it:
///
/// ```lua
/// if _G["__init__"] then
///     return { logfile = "server.log", loglevel = "info", enable_stdout = false }
/// end
/// -- normal actor code …
/// ```
///
/// The returned table may contain:
/// - `logfile`       – path to the log file
/// - `loglevel`      – log level string (e.g. `"info"`, `"debug"`)
/// - `enable_stdout` – whether to mirror logs to stdout (default `true`)
/// - `path`          – extra `package.path` entries for Lua `require`
const INIT_MARKER: &str = r#"_G["__init__"]"#;

fn print_usage() {
    println!("Usage:");
    println!("    moon_rs script.lua [args]\n");
    println!("Examples:");
    println!("    moon_rs main.lua hello\n");
}

#[cfg(target_os = "windows")]
type Dword = u32;

#[cfg(target_os = "windows")]
type ConsoleHandlerRoutine = extern "system" fn(Dword) -> i32;

#[cfg(target_os = "windows")]
fn set_console_title(title: *const i8) {
    unsafe extern "system" {
        fn SetConsoleTitleA(title: *const i8) -> i32;
    }

    if title.is_null() {
        return;
    }

    unsafe {
        SetConsoleTitleA(title);
    }
}

#[cfg(target_os = "windows")]
fn set_console_ctrl_handler(handler: ConsoleHandlerRoutine) -> i32 {
    unsafe extern "system" {
        fn SetConsoleCtrlHandler(handler: ConsoleHandlerRoutine, add: i32) -> i32;
    }

    unsafe { SetConsoleCtrlHandler(handler, 1) }
}

fn setup_signal() {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        use tokio::signal::unix::SignalKind;

        tokio::spawn(async {
            const SIGTERM: i32 = 15;
            const SIGINT: i32 = 2;
            const SIGQUIT: i32 = 3;

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
                    "'{}' signal received, stopping system... ({}:{})",
                    v.1,
                    file!(),
                    line!()
                );
                CONTEXT.shutdown(v.0);
            }
        });
    }

    #[cfg(target_os = "windows")]
    {
        extern "system" fn console_ctrl_handler(ctrl_type: Dword) -> i32 {
            const CTRL_C_EVENT: Dword = 0;
            const CTRL_CLOSE_EVENT: Dword = 2;
            const CTRL_LOGOFF_EVENT: Dword = 5;
            const CTRL_SHUTDOWN_EVENT: Dword = 6;

            match ctrl_type {
                CTRL_C_EVENT => {
                    log::warn!(
                        "CTRL_C_EVENT received, stopping system... ({}:{})",
                        file!(),
                        line!()
                    );
                    CONTEXT.shutdown(CTRL_C_EVENT as i32);
                    1
                }
                CTRL_CLOSE_EVENT | CTRL_LOGOFF_EVENT | CTRL_SHUTDOWN_EVENT => {
                    CONTEXT.shutdown(ctrl_type as i32);
                    while !CONTEXT.stopped() {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    1
                }
                _ => 0,
            }
        }

        set_console_ctrl_handler(console_ctrl_handler);
        let args: Vec<String> = env::args().collect();
        let mut title = String::new();
        for (i, arg) in args.iter().enumerate() {
            title.push_str(arg.as_str());
            if i == 0 {
                title.push_str("(PID: ");
                title.push_str(&std::process::id().to_string());
                title.push(')');
            }
            title.push(' ');
        }

        let cstr = std::ffi::CString::new(title).expect("CString::new failed");
        set_console_title(cstr.as_ptr());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_signal();

    let mut enable_stdout = true;
    let mut loglevel = String::new();
    let mut logfile: Option<String> = None;

    let args: Vec<String> = env::args().collect();
    let mut argn = 1;
    if args.len() <= argn {
        print_usage();
        return Err(MoonError::Custom("invalid arguments".to_string()));
    }

    let mut bootstrap = args[argn].clone();
    let path = Path::new(&bootstrap);
    if !path.is_file() {
        print_usage();
        return Err(MoonError::Custom(format!(
            "bootstrap file not found: {}",
            bootstrap
        )));
    }

    if path.extension().and_then(std::ffi::OsStr::to_str) != Some("lua") {
        print_usage();
        return Err(MoonError::Custom(format!(
            "bootstrap is not a lua file: {}",
            bootstrap
        )));
    }

    argn += 1;

    let mut arg = String::new();
    arg.push_str("return {");
    for v in args.iter().skip(argn) {
        arg.push_str(&format!("'{}',", v));
    }
    arg.push('}');

    let contents = fs::read_to_string(&bootstrap)?;
    let mut lua_search_path = String::new();

    if contents.contains(INIT_MARKER) {
        let mut vm = Lua::new(SafeOption::default());
        vm.open_stdlib(Stdlib::All)
            .map_err(|e| MoonError::Custom(format!("open stdlib: {}", e)))?;

        let state = vm.global_state_mut();

        state
            .set_global("__init__", LuaValue::boolean(true))
            .map_err(|e| MoonError::Custom(format!("{}", e)))?;

        let _arg_results = vm
            .eval::<LuaValue>(&arg)
            .map_err(|e| MoonError::Custom(format!("execute args: {}", e)))?;

        let result = vm
            .load(&contents).set_name(&bootstrap).call::<_, LuaTable>(_arg_results)
            .map_err(|e| MoonError::Custom(format!("dofile: {}", e)))?;

        logfile = Some(result.get::<String>("logfile").unwrap_or_default());
        enable_stdout = result.get::<bool>("enable_stdout").unwrap_or(true);
        loglevel = result.get::<String>("loglevel").unwrap_or_default();
        lua_search_path = result.get::<String>("path").unwrap_or_default();
    }

    if !lua_search_path.contains("lualib/?.lua") {
        let mut search_path = env::current_dir()?.canonicalize()?;
        if !search_path.join("lualib").is_dir() {
            search_path = env::current_exe()?.canonicalize()?
                .parent().unwrap_or(Path::new(".")).to_path_buf();
        }

        if !search_path.join("lualib").is_dir() {
            return Err(MoonError::Custom(format!(
                "lualib dir not found: {}",
                search_path.to_str().unwrap_or("")
            )));
        }

        if let Some(path_with_no_prefix) = search_path.to_string_lossy().strip_prefix(r"\\?\") {
            search_path = PathBuf::from(path_with_no_prefix);
        }

        let strpath = search_path.to_string_lossy().replace('\\', "/");
        if !lua_search_path.is_empty() && !lua_search_path.ends_with(';') {
            lua_search_path.push(';');
        }
        lua_search_path.push_str(&format!("{}/lualib/?.lua;", strpath));
    }

    CONTEXT.set_env(
        "PATH",
        &format!("package.path='{};'..package.path;", lua_search_path),
    );

    let cwd = path.parent().unwrap_or(Path::new("./"));
    env::set_current_dir(cwd)?;

    bootstrap = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .as_ref()
        .to_string();

    CONTEXT.set_env("ARG", &arg);

    if let Err(err) = LOGGER.setup_logger(enable_stdout, logfile, loglevel) {
        return Err(MoonError::Custom(err.to_string()));
    }

    let package_path = CONTEXT.get_env("PATH").unwrap_or_default();
    let mut package_path = (*package_path).clone();

    package_path.push_str(&arg);

    context::run_monitor();

    context::run_timer();

    log::info!("system start. ({}:{})", file!(), line!());

    lua_actor::new_actor(LuaActorParam {
        id: CONTEXT.next_actor_id(),
        unique: true,
        creator: 0,
        session: 0,
        memlimit: 0,
        name: "bootstrap".to_string(),
        source: bootstrap,
        params: package_path,
        block: true,
    });

    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if CONTEXT.exit_code() != i32::MAX && CONTEXT.stopped() {
            break;
        }
    }

    let error_code = CONTEXT.exit_code();

    log::info!(
        "system end with code {}. ({}:{})",
        error_code,
        file!(),
        line!()
    );

    LOGGER.stop();

    while !LOGGER.stopped() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    if error_code != 0 {
        return Err(error_code.to_string().into());
    }

    Ok(())
}
