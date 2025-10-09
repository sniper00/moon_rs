use lib_core::{
    context::{self, LuaActorParam, CONTEXT, LOGGER},
    error::{Error, Result},
};
use lib_lua::{
    self, cstr, ffi,
    laux::{self, LuaState},
};
use lib_lualib_rs::{lua_actor, not_null_wrapper};
use mimalloc::MiMalloc;
use std::{
    env,
    ffi::CString,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn print_usage() {
    println!("Usage:");
    println!("    moon_rs script.lua [args]\n");
    println!("Examples:");
    println!("    moon_rs main.lua hello\n");
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

        lib_common::set_concole_ctrl_handler(console_ctrl_handler);
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
        lib_common::set_console_title(cstr.as_ptr());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_signal();

    unsafe {
        ffi::luaL_initcodecache();
    }

    let mut enable_stdout = true;
    let mut loglevel = String::new();
    let mut logfile: Option<String> = None;

    let args: Vec<String> = env::args().collect();
    let mut argn = 1;
    if args.len() <= argn {
        print_usage();
        return Err(Error::Custom("invalid arguments".to_string()));
    }

    let mut bootstrap = args[argn].clone();
    let path = Path::new(&bootstrap);
    if !path.is_file() {
        print_usage();
        return Err(Error::Custom(format!(
            "bootstrap file not found: {}",
            bootstrap
        )));
    }

    if path.extension().and_then(std::ffi::OsStr::to_str) != Some("lua") {
        print_usage();
        return Err(Error::Custom(format!(
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
    if contents.starts_with("---__init__---") {
        //has init options
        unsafe {
            let lua = LuaState::new(ffi::luaL_newstate());
            let lua_state = lua.unwrap();
            ffi::luaL_openlibs(lua_state.as_ptr());
            ffi::lua_pushboolean(lua_state.as_ptr(), 1);
            ffi::lua_setglobal(lua_state.as_ptr(), cstr!("__init__"));

            ffi::lua_pushcfunction(lua_state.as_ptr(), not_null_wrapper!(laux::lua_traceback));
            assert_eq!(ffi::lua_gettop(lua_state.as_ptr()), 1);

            if ffi::LUA_OK
                != ffi::luaL_loadfile(
                    lua_state.as_ptr(),
                    CString::new(bootstrap.as_str())?.as_ptr(),
                )
            {
                return Err(Error::Custom(format!(
                    "loadfile {}",
                    laux::lua_opt(lua_state, -1).unwrap_or("unknown error".to_string())
                )));
            }

            if ffi::LUA_OK
                != ffi::luaL_dostring(lua_state.as_ptr(), CString::new(arg.as_str())?.as_ptr())
            {
                return Err(Error::Custom(
                    laux::lua_opt(lua_state, -1).unwrap_or("unknown error".to_string()),
                ));
            }

            if ffi::LUA_OK != ffi::lua_pcall(lua_state.as_ptr(), 1, 1, 1) {
                return Err(Error::Custom(
                    laux::lua_opt(lua_state, -1).unwrap_or("unknown error".to_string()),
                ));
            }

            if ffi::LUA_TTABLE != ffi::lua_type(lua_state.as_ptr(), -1) {
                return Err(Error::Custom("init code must return a table".to_string()));
            }

            logfile = laux::opt_field(lua_state, -1, "logfile");
            enable_stdout = laux::opt_field(lua_state, -1, "enable_stdout").unwrap_or(true);
            loglevel = laux::opt_field(lua_state, -1, "loglevel").unwrap_or_default();
            let mut path: String = laux::opt_field(lua_state, -1, "path").unwrap_or_default();
            if !path.is_empty() {
                path = format!("package.path='{};'..package.path;", path);
                CONTEXT.set_env("PATH", path.as_ref());
            }
        }
    }

    if CONTEXT.get_env("PATH").is_none() {
        let mut search_path = env::current_dir()?.canonicalize()?;
        if !search_path.join("lualib").is_dir() {
            search_path = env::current_exe()?.canonicalize()?.join("lualib");
        }

        if !search_path.is_dir() {
            return Err(Error::Custom(format!(
                "lualib dir not found: {}",
                search_path.to_str().unwrap_or("")
            )));
        }

        if let Some(path_with_no_prefix) = search_path.to_string_lossy().strip_prefix(r"\\?\") {
            search_path = PathBuf::from(path_with_no_prefix);
        }

        let strpath = search_path.to_string_lossy().replace('\\', "/");
        //Lualib directories are added to the lua search path
        let package_path = format!("package.path='{}/lualib/?.lua;'..package.path;", strpath);

        CONTEXT.set_env("PATH", package_path.as_ref());
    }

    let cwd = path.parent().unwrap_or(Path::new("./"));
    //Change the working directory to the directory where the opened file is located.
    env::set_current_dir(cwd)?;

    bootstrap = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .as_ref()
        .to_string();

    CONTEXT.set_env("ARG", &arg);

    if let Err(err) = LOGGER.setup_logger(enable_stdout, logfile, loglevel) {
        return Err(Error::Custom(err.to_string()));
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
