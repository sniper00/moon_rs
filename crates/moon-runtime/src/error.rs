use derive_more::From;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Custom(String),

    #[from]
    Io(std::io::Error),

    #[from]
    NulError(std::ffi::NulError),

    /// The process should terminate with this code (see `moon.exit` / shutdown
    /// events). It is a dedicated variant — not a `Custom` whose text happens
    /// to parse as a number — so a numeric-looking error message is never
    /// mistaken for an exit code and swallowed without being reported.
    ExitCode(i32),
}

// region:    --- Custom

impl Error {
    pub fn custom_from_err(err: impl std::error::Error) -> Self {
        Self::Custom(err.to_string())
    }

    pub fn custom(val: impl Into<String>) -> Self {
        Self::Custom(val.into())
    }
}

impl From<&str> for Error {
    fn from(val: &str) -> Self {
        Self::Custom(val.to_string())
    }
}

// endregion: --- Custom

// region:    --- Error Boilerplate

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        match self {
            Self::Custom(message) => fmt.write_str(message),
            Self::Io(error) => error.fmt(fmt),
            Self::NulError(error) => error.fmt(fmt),
            Self::ExitCode(code) => write!(fmt, "exit code {code}"),
        }
    }
}

impl std::error::Error for Error {}

// endregion: --- Error Boilerplate

#[cfg(test)]
#[path = "../tests/unit/error.rs"]
mod tests;
