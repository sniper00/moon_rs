//! Low level bindings to common.

#[cfg(any(target_os = "windows"))]
pub type DWORD = u32;
#[cfg(any(target_os = "windows"))]
pub type ConsoleHandlerRoutine = extern "system" fn(DWORD)->i32;

#[cfg(any(target_os = "windows"))]
extern "C-unwind" {
    pub fn set_console_title(title: *const i8);
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
