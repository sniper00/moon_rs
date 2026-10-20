use super::*;

#[test]
fn resp_numbers_preserve_display_format_for_extreme_values() {
    fn check(value: impl fmt::Display) {
        let expected = value.to_string();
        let mut output = Buffer::new();
        write_resp_number(&mut output, value).unwrap();
        assert_eq!(
            output.as_slice(),
            format!("\r\n${}\r\n{}", expected.len(), expected).as_bytes()
        );
    }
    for value in [i64::MIN, -1, 0, 1, i64::MAX] {
        check(value);
    }
    for value in [
        0.0,
        -0.0,
        1.5,
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        -f64::from_bits(1),
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        check(value);
    }
}

#[test]
fn oversized_resp_argument_returns_error_without_publishing_partial_data() {
    let mut output = Buffer::from("prefix");
    let err = write_resp_number(&mut output, "9".repeat(385)).unwrap_err();
    assert!(err.contains("numeric RESP argument formatting failed"));
    assert_eq!(output.as_slice(), b"prefix");
    write_resp_number(&mut output, 42).unwrap();
    assert_eq!(output.as_slice(), b"prefix\r\n$2\r\n42");
}
