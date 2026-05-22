use thiserror::Error;

/// Convenience alias for `Result<T, MoonError>`.
pub type Result<T> = core::result::Result<T, MoonError>;

/// Unified error type for the moon-rs runtime.
///
/// Covers custom string errors, I/O errors, and FFI null-pointer errors.
/// Implements `std::error::Error` and `Display` via `thiserror`.
#[derive(Debug, Error)]
pub enum MoonError {
    #[error("{0}")]
    Custom(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    NulError(#[from] std::ffi::NulError),
}

impl From<&str> for MoonError {
    fn from(val: &str) -> Self {
        Self::Custom(val.to_string())
    }
}

impl From<String> for MoonError {
    fn from(val: String) -> Self {
        Self::Custom(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_from_str() {
        let err: MoonError = "something failed".into();
        assert_eq!(err.to_string(), "something failed");
    }

    #[test]
    fn custom_from_string() {
        let err: MoonError = String::from("bad thing").into();
        assert_eq!(err.to_string(), "bad thing");
    }

    #[test]
    fn io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: MoonError = io_err.into();
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn result_alias_works() {
        fn ok_fn() -> Result<i32> { Ok(42) }
        fn err_fn() -> Result<i32> { Err("fail".into()) }

        assert_eq!(ok_fn().unwrap(), 42);
        assert!(err_fn().is_err());
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<MoonError>();
        assert_sync::<MoonError>();
    }

    #[test]
    fn error_implements_std_error() {
        let err: MoonError = "test".into();
        let _: &dyn std::error::Error = &err;
    }
}
