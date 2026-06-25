use mimalloc::MiMalloc;
use moon_base::{
    self, cstr, ffi,
    laux::{self, LuaState},
};
use moon_runtime::{lua_actor, not_null_wrapper};
use moon_runtime::{
    context::{self, CLUSTER_ACTOR_ADDR, CONTEXT, LOGGER, LuaActorParam},
    error::{Error, Result},
};
use tokio::sync::mpsc;
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

        CONTEXT.io_runtime().spawn(async {
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
        type DWORD = u32;
        type ConsoleHandlerRoutine = extern "system" fn(DWORD) -> i32;

        unsafe extern "system" {
            fn SetConsoleTitleA(title: *const i8) -> i32;
            fn SetConsoleCtrlHandler(f: ConsoleHandlerRoutine, add: i32) -> i32;
        }

        extern "system" fn console_ctrl_handler(ctrl_type: DWORD) -> i32 {
            const CTRL_C_EVENT: DWORD = 0;
            const CTRL_CLOSE_EVENT: DWORD = 2;
            const CTRL_LOGOFF_EVENT: DWORD = 5;
            const CTRL_SHUTDOWN_EVENT: DWORD = 6;

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

        unsafe { SetConsoleCtrlHandler(console_ctrl_handler, 1) };
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
        unsafe { SetConsoleTitleA(cstr.as_ptr()) };
    }
}

fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Could not install default TLS provider");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("Failed to create tokio runtime");

    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    CONTEXT.set_main_handle(tokio::runtime::Handle::current());
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

    let bootstrap_path = path.canonicalize()?;

    argn += 1;

    let mut arg = String::new();
    arg.push_str("return {");
    for v in args.iter().skip(argn) {
        arg.push_str(&format!("'{}',", v));
    }
    arg.push('}');

    let contents = fs::read_to_string(&bootstrap_path)?;
    if contents.contains("_G[\"__init__\"]") {
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
                != ffi::luaL_loadstring(
                    lua_state.as_ptr(),
                    CString::new(contents.as_str())?.as_ptr(),
                )
            {
                return Err(Error::Custom(format!(
                    "loadstring {}",
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
                CONTEXT.set_env("PATH", path.as_bytes());
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

        CONTEXT.set_env("PATH", package_path.as_bytes());
    }

    // Lua C dynamic extension libraries are loaded via `require` from the `clib`
    // directory (alongside `lualib`). Append a search template to package.cpath so
    // every actor inherits it (see CONTEXT env "PATH" propagation in lua_actor).
    {
        let mut root = env::current_dir()?.canonicalize()?;
        if !root.join("lualib").is_dir() {
            root = env::current_exe()?.canonicalize()?;
            root.pop();
        }
        if let Some(stripped) = root.to_string_lossy().strip_prefix(r"\\?\") {
            root = PathBuf::from(stripped);
        }
        let root = root.to_string_lossy().replace('\\', "/");

        // Platform-specific shared library extension.
        let ext = if cfg!(target_os = "windows") {
            "dll"
        } else if cfg!(target_os = "macos") {
            "dylib"
        } else {
            "so"
        };

        let cpath = format!(
            "package.cpath='{root}/clib/?.{ext};'..package.cpath;",
            root = root,
            ext = ext
        );

        let package_path = CONTEXT
            .get_env("PATH")
            .map(|p| String::from_utf8_lossy(&p).into_owned())
            .unwrap_or_default();
        CONTEXT.set_env("PATH", format!("{}{}", package_path, cpath).as_bytes());
    }

    let cwd = bootstrap_path.parent().unwrap_or(Path::new("./"));
    //Change the working directory to the directory where the opened file is located.
    env::set_current_dir(cwd)?;

    bootstrap = bootstrap_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .as_ref()
        .to_string();

    CONTEXT.set_env("ARG", arg.as_bytes());

    if let Err(err) = LOGGER.setup_logger(enable_stdout, logfile, loglevel) {
        return Err(Error::Custom(err.to_string()));
    }

    let package_path = CONTEXT.get_env("PATH").unwrap_or_default();
    let mut package_path = String::from_utf8_lossy(&package_path).into_owned();

    package_path.push_str(&arg);

    context::run_monitor();

    context::run_timer();

    // Build the message-decoder dispatch table once, before any actor spawns.
    moon_runtime::init_message_decoders();

    log::info!("system start. ({}:{})", file!(), line!());

    // Pre-register the cluster pseudo-actor so `next_actor_id()` skips its
    // reserved ID (2), preventing a collision if a user actor is spawned
    // before `cluster.init()` runs. The dummy channel is replaced by the real
    // one when cluster.init() calls `register_pseudo_actor`.
    {
        let (dummy_tx, _dummy_rx) = mpsc::unbounded_channel();
        CONTEXT.register_pseudo_actor(CLUSTER_ACTOR_ADDR, dummy_tx);
    }

    lua_actor::new_actor(LuaActorParam {
        id: context::BOOTSTRAP_ACTOR_ADDR,
        unique: true,
        creator: 0,
        session: 0,
        memlimit: 0,
        name: "bootstrap".to_string(),
        source: bootstrap,
        params: package_path,
        block: true,
    });

    let mut last_report = std::time::Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let code = CONTEXT.exit_code();
        if code < 0 || (CONTEXT.exit_code() != i32::MAX && CONTEXT.stopped()) {
            break;
        }

        // Once shutdown has been requested, periodically report which actors
        // are still running so a stuck shutdown can be diagnosed.
        if CONTEXT.exit_code() != i32::MAX && last_report.elapsed() >= Duration::from_secs(3) {
            last_report = std::time::Instant::now();
            let running = CONTEXT.running_actors();
            if !running.is_empty() {
                log::warn!(
                    "waiting for {} actor(s) to stop: [{}].",
                    running.len(),
                    running.join(", ")
                );
            }
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
