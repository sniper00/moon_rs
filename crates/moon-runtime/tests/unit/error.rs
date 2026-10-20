use super::*;

#[test]
fn display_omits_variant_prefix_for_custom() {
    let error = Error::custom("bootstrap file not found: main.lua");
    assert_eq!(error.to_string(), "bootstrap file not found: main.lua");
}

#[test]
fn display_delegates_to_wrapped_io_error() {
    let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file");
    let error = Error::Io(inner);
    assert_eq!(error.to_string(), "no such file");
    assert!(!error.to_string().contains("Io"));
}

#[test]
fn display_delegates_to_wrapped_nul_error() {
    let error = Error::NulError(std::ffi::CString::new("a\0b").unwrap_err());
    assert_eq!(
        error.to_string(),
        "nul byte found in provided data at position: 1"
    );
    assert!(!error.to_string().contains("NulError"));
}

#[test]
fn display_renders_exit_code() {
    let error = Error::ExitCode(-1);
    assert_eq!(error.to_string(), "exit code -1");
}
