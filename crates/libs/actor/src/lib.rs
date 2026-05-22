/// Actor lifecycle and Lua VM ownership.
pub mod actor;
/// Byte buffer with read-position tracking and front-write support.
pub mod buffer;
/// Global actor registry, message passing, timers, and environment.
pub mod context;
/// Project-level error types (`MoonError`) with `thiserror` derive.
pub mod error;
/// Structured logger with file/stdout output and per-thread formatting.
pub mod log;

/// Escape non-printable bytes as `\xHH` hex sequences, passing ASCII
/// graphic and whitespace characters through unchanged.
pub fn escape_print(input: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut result = String::with_capacity(input.len());

    for byte in input {
        if byte.is_ascii_graphic() || byte.is_ascii_whitespace() {
            result.push(*byte as char);
        } else {
            result.push('\\');
            result.push('x');
            result.push(HEX[(byte >> 4) as usize] as char);
            result.push(HEX[(byte & 0xf) as usize] as char);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_print_ascii_passthrough() {
        assert_eq!(escape_print(b"hello world"), "hello world");
    }

    #[test]
    fn escape_print_escapes_null() {
        assert_eq!(escape_print(b"\x00"), "\\x00");
    }

    #[test]
    fn escape_print_escapes_binary() {
        assert_eq!(escape_print(b"\xff\xfe"), "\\xff\\xfe");
    }

    #[test]
    fn escape_print_mixed() {
        assert_eq!(escape_print(b"a\x01b"), "a\\x01b");
    }

    #[test]
    fn escape_print_empty() {
        assert_eq!(escape_print(b""), "");
    }

    #[test]
    fn escape_print_preserves_whitespace() {
        assert_eq!(escape_print(b"a\tb\nc"), "a\tb\nc");
    }
}
