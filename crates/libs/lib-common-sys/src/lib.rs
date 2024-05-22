//! Low level bindings to common.

#[cfg(target_os = "windows")]
pub type DWORD = u32;
#[cfg(target_os = "windows")]
pub type ConsoleHandlerRoutine = extern "system" fn(DWORD)->i32;

#[cfg(target_os = "windows")]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn set_console_title(title: *const i8){
    extern "system" {
        fn SetConsoleTitleA(
            title: *const i8
        ) -> i32;
    }

    if title.is_null(){
        return;
    }

    unsafe{
        SetConsoleTitleA(title);
    }
}

#[cfg(target_os = "windows")]
pub fn set_concole_ctrl_handler(f:ConsoleHandlerRoutine) -> i32 {
    extern "system" {
        fn SetConsoleCtrlHandler(
            f: ConsoleHandlerRoutine,
            add: i32
        ) -> i32;
    }

    unsafe{
        SetConsoleCtrlHandler(f, 1)
    }
}
