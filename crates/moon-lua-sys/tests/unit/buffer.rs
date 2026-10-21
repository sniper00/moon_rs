use super::*;

#[test]
fn test_buffer_1() {
    let mut buf = Buffer::with_capacity(12);
    buf.write_slice(&[0; 4]);
    buf.write_str("1234567");
    buf.seek(4);
    assert!(buf.write_front("bbbb".as_bytes()));

    // advance the read cursor by 8 bytes
    let _ = buf.read(8);

    buf.write_str("abcd");
    assert!(buf.read(4).unwrap() == "567a".as_bytes());
    assert_eq!(buf.len(), 3);
}
#[test]
fn test_buffer_2() {
    let mut buf = Buffer::with_capacity(128);
    assert_eq!(buf.len(), 0);
    let n: i32 = 0;
    buf.write_slice(n.to_le_bytes().as_ref());
    assert_eq!(buf.len(), 4);
    assert!(buf.read(4).unwrap() == n.to_le_bytes().as_ref());
    assert_eq!(buf.len(), 0);
    assert!(buf.read(4).is_none());
}

#[test]
fn test_buffer_3() {
    let mut buf = Buffer::with_capacity(32);
    assert_eq!(buf.len(), 0);

    for _ in 0..100 {
        buf.write_slice(vec![0; 17].as_ref());
        assert!(buf.read(17).is_some());
    }
}

#[test]
fn test_buffer_new() {
    let buffer = Buffer::new();
    assert_eq!(buffer.data.capacity(), DEFAULT_RESERVE);
    assert_eq!(buffer.rpos, 0);
}

#[test]
fn test_buffer_with_capacity() {
    let capacity = 256;
    let buffer = Buffer::with_capacity(capacity);
    assert_eq!(buffer.data.capacity(), capacity);
    assert_eq!(buffer.rpos, 0);
}

#[test]
fn test_buffer_from_slice() {
    let data = [1, 2, 3, 4];
    let buffer = Buffer::from_slice(&data);
    assert_eq!(buffer.data, data);
    assert_eq!(buffer.rpos, 0);
}

#[test]
fn test_write_slice() {
    let mut buffer = Buffer::new();
    let data = [1, 2, 3, 4];
    buffer.write_slice(&data);
    assert_eq!(buffer.data, data);
}

#[test]
fn test_write() {
    let mut buffer = Buffer::new();
    buffer.write(1);
    assert_eq!(buffer.data, vec![1]);
}

#[test]
fn test_unsafe_write() {
    let mut buffer = Buffer::new();
    // SAFETY: Buffer::new reserves capacity.
    unsafe { buffer.unsafe_write(1) };
    assert_eq!(buffer.data, vec![1]);
}

#[test]
fn test_write_front() {
    let mut buffer = Buffer::new();
    buffer.write(0);
    buffer.write_slice(&[1, 2, 3, 4]);
    buffer.seek(1);
    assert!(buffer.write_front(&[0]));
    assert_eq!(buffer.data, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_write_front_byte() {
    let mut buffer = Buffer::new();
    buffer.write(0);
    buffer.write_slice(&[1, 2, 3, 4]);
    buffer.seek(1);
    assert!(buffer.write_front_byte(0));
    assert_eq!(buffer.data, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_write_chars() {
    let mut buffer = Buffer::new();
    buffer.write_chars("hello").unwrap();
    assert_eq!(buffer.data, b"hello".to_vec());
}

#[test]
fn test_write_str() {
    let mut buffer = Buffer::new();
    buffer.write_str("hello");
    assert_eq!(buffer.data, b"hello".to_vec());
}

#[test]
fn test_read() {
    let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    let data = buffer.read(2);
    assert_eq!(data, Some(vec![1, 2]));
    assert_eq!(buffer.rpos, 2);
}

#[test]
fn test_consume() {
    let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    buffer.consume(2);
    assert_eq!(buffer.rpos, 2);
}

#[test]
fn test_seek() {
    let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    assert!(buffer.seek(2));
    assert_eq!(buffer.rpos, 2);
    assert!(buffer.seek(-1));
    assert_eq!(buffer.rpos, 1);
}

#[test]
fn test_clear() {
    let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    buffer.clear();
    assert!(buffer.data.is_empty());
    assert_eq!(buffer.rpos, 0);
}

#[test]
fn test_len() {
    let buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    assert_eq!(buffer.len(), 4);
}

#[test]
fn test_is_empty() {
    let buffer = Buffer::new();
    assert!(buffer.is_empty());
}

#[test]
fn test_prepare() {
    let mut buffer = Buffer::new();
    let _ = buffer.prepare(10);
    assert!(buffer.data.capacity() >= 10);
}

#[test]
fn test_commit() {
    let mut buffer = Buffer::new();
    for byte in buffer.spare_capacity_mut(10) {
        byte.write(0);
    }
    // SAFETY: the reserved region is initialized above.
    assert!(unsafe { buffer.commit(10) });
    assert_eq!(buffer.data.len(), 10);
}

#[test]
fn test_revert() {
    let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    buffer.revert(2);
    assert_eq!(buffer.data.len(), 2);
}

#[test]
fn test_data() {
    let buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    assert_eq!(buffer.data(), &[1, 2, 3, 4]);
}

#[test]
fn test_as_ptr() {
    let buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    unsafe {
        assert_eq!(*buffer.as_ptr(), 1);
    }
}

#[test]
fn test_read_u8() {
    let buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    assert_eq!(buffer.read_u8(1), 2);
}

#[test]
fn test_read_i16() {
    let buffer = Buffer::from_slice(&[0, 1, 0, 2]);
    assert_eq!(buffer.read_i16(0, true), 256);
    assert_eq!(buffer.read_i16(2, true), 512);
}

#[test]
fn test_read_u16() {
    let buffer = Buffer::from_slice(&[0, 1, 0, 2]);
    assert_eq!(buffer.read_u16(0, true), 256);
    assert_eq!(buffer.read_u16(2, true), 512);
}

#[test]
fn test_read_i32() {
    let buffer = Buffer::from_slice(&[1, 0, 0, 0]);
    assert_eq!(buffer.read_i32(0, true), 1);
}

#[test]
fn test_read_u32() {
    let buffer = Buffer::from_slice(&[1, 0, 0, 0]);
    assert_eq!(buffer.read_u32(0, true), 1);
}

#[test]
fn test_as_slice() {
    let buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    assert_eq!(buffer.as_slice(), &[1, 2, 3, 4]);
}

#[test]
fn test_as_mut_slice() {
    let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    buffer.as_mut_slice()[0] = 0;
    assert_eq!(buffer.data, vec![0, 2, 3, 4]);
}

#[test]
fn test_as_mut_vec() {
    let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    // SAFETY: push cannot invalidate the read position.
    unsafe { buffer.as_mut_vec() }.push(5);
    assert_eq!(buffer.data, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_as_vec() {
    let buffer = Buffer::from_slice(&[1, 2, 3, 4]);
    assert_eq!(buffer.as_vec(), &vec![1, 2, 3, 4]);
}

#[test]
fn test_as_pointer() {
    let mut buffer = Buffer::new();
    let ptr = buffer.as_pointer();
    assert!(!ptr.is_null());
}

#[test]
fn test_as_str() {
    let buffer = Buffer::from("hello");
    assert_eq!(buffer.as_str().unwrap(), "hello");
}

#[test]
fn test_from_vec() {
    let v = vec![1, 2, 3, 4];
    let buffer: Buffer = v.into();
    assert_eq!(buffer.data, vec![1, 2, 3, 4]);
}

#[test]
fn test_from_slice() {
    let s = &[1, 2, 3, 4];
    let buffer = Buffer::from_slice(s);
    assert_eq!(buffer.data, vec![1, 2, 3, 4]);
}

#[test]
fn test_from_str() {
    let s = "hello";
    let buffer: Buffer = s.into();
    assert_eq!(buffer.data, b"hello".to_vec());
}

#[test]
fn test_from_string() {
    let s = "hello".to_string();
    let buffer: Buffer = s.into();
    assert_eq!(buffer.data, b"hello".to_vec());
}

#[test]
fn test_default() {
    let buffer: Buffer = Default::default();
    assert_eq!(buffer.data.capacity(), DEFAULT_RESERVE);
    assert_eq!(buffer.rpos, 0);
}

#[test]
fn test_display() {
    let buffer = Buffer::from("hello");
    assert_eq!(format!("{}", buffer), "hello");
}

#[test]
fn test_shift_data_forward() {
    let mut buffer = Buffer::from_slice(b"Hello World!");
    buffer.shift_data(6, 5, 0); // Shift "World" to the beginning
    assert_eq!(&buffer.data[..5], b"World");
    // Original data at 6..11 is now also at 0..5
}

#[test]
fn test_shift_data_backward() {
    let mut buffer = Buffer::from_slice(b"Hello World!");
    buffer.shift_data(0, 5, 7); // Shift "Hello" to position 7
    assert_eq!(&buffer.data[7..12], b"Hello");
}

#[test]
fn test_shift_data_same_position() {
    let mut buffer = Buffer::from_slice(b"Hello");
    buffer.shift_data(0, 5, 0); // No-op
    assert_eq!(&buffer.data, b"Hello");
}

#[test]
fn test_write_pos_and_read_pos() {
    let mut buffer = Buffer::from_slice(b"Hello");
    assert_eq!(buffer.write_pos(), 5);
    assert_eq!(buffer.read_pos(), 0);
    buffer.consume(2);
    assert_eq!(buffer.write_pos(), 5);
    assert_eq!(buffer.read_pos(), 2);
}

#[test]
fn test_data_mut_at() {
    let mut buffer = Buffer::from_slice(b"Hello World");
    buffer.data_mut_at(6, 5).copy_from_slice(b"Rust!");
    assert_eq!(&buffer.data, b"Hello Rust!");
}

#[test]
fn test_prepare_reuses_consumed_space_before_growing() {
    let mut buffer = Buffer::with_capacity(8);
    buffer.write_slice(b"abcdef");
    assert_eq!(buffer.read(4).unwrap(), b"abcd");

    let old_capacity = buffer.data.capacity();
    let ptr = buffer.prepare(4);
    unsafe {
        std::ptr::copy_nonoverlapping(b"ghij".as_ptr(), ptr, 4);
    }
    assert!(unsafe { buffer.commit(4) });

    assert_eq!(buffer.data.capacity(), old_capacity);
    assert_eq!(buffer.read_pos(), 0);
    assert_eq!(buffer.as_slice(), b"efghij");
}

#[test]
fn test_prepare_grows_when_consumed_space_is_not_enough() {
    let mut buffer = Buffer::with_capacity(8);
    buffer.write_slice(b"abcdef");
    assert_eq!(buffer.read(2).unwrap(), b"ab");

    let old_capacity = buffer.data.capacity();
    let ptr = buffer.prepare(5);
    unsafe {
        std::ptr::copy_nonoverlapping(b"ghijk".as_ptr(), ptr, 5);
    }
    assert!(unsafe { buffer.commit(5) });

    assert!(buffer.data.capacity() > old_capacity);
    assert_eq!(buffer.read_pos(), 0);
    assert_eq!(buffer.as_slice(), b"cdefghijk");
}

#[test]
fn test_write_front_failure_keeps_state_unchanged() {
    let mut buffer = Buffer::from_slice(b"payload");
    assert!(!buffer.write_front(b"H"));
    assert_eq!(buffer.read_pos(), 0);
    assert_eq!(buffer.write_pos(), 7);
    assert_eq!(buffer.as_slice(), b"payload");
}

#[test]
fn test_write_front_byte_failure_keeps_state_unchanged() {
    let mut buffer = Buffer::from_slice(b"payload");
    assert!(!buffer.write_front_byte(b'H'));
    assert_eq!(buffer.read_pos(), 0);
    assert_eq!(buffer.write_pos(), 7);
    assert_eq!(buffer.as_slice(), b"payload");
}

#[test]
fn test_commit_over_capacity_keeps_len_unchanged() {
    let mut buffer = Buffer::with_capacity(4);
    buffer.write_slice(b"abcd");

    assert!(!unsafe { buffer.commit(1) });
    assert_eq!(buffer.write_pos(), 4);
    assert_eq!(buffer.as_slice(), b"abcd");
}

#[test]
fn test_consume_past_readable_is_noop() {
    let mut buffer = Buffer::from_slice(b"abc");
    buffer.consume(4);

    assert_eq!(buffer.read_pos(), 0);
    assert_eq!(buffer.as_slice(), b"abc");
}

#[test]
fn oversized_lengths_cannot_wrap_buffer_boundaries() {
    let mut buffer = Buffer::from("abc");
    buffer.consume(1);
    assert!(buffer.read(usize::MAX).is_none());
    buffer.consume(usize::MAX);
    assert_eq!(buffer.as_slice(), b"bc");
    // SAFETY: the size cannot fit, so commit rejects it without reading bytes.
    assert!(!unsafe { buffer.commit(usize::MAX) });
    assert_eq!(buffer.as_slice(), b"bc");
}

#[test]
fn integer_reads_respect_offset_endianness_and_signedness() {
    let mut buffer = Buffer::from_slice(&[0, 0x80, 1, 2, 3]);
    buffer.consume(1);
    assert_eq!(buffer.read_i16(0, false), i16::from_be_bytes([0x80, 1]));
    assert_eq!(buffer.read_u16(0, true), 0x0180);
    assert_eq!(
        buffer.read_i32(0, false),
        i32::from_be_bytes([0x80, 1, 2, 3])
    );
    assert_eq!(buffer.read_u32(0, true), 0x03020180);
    assert!(std::panic::catch_unwind(|| buffer.read_u32(1, false)).is_err());
    assert!(std::panic::catch_unwind(|| buffer.read_u8(usize::MAX)).is_err());
}

#[test]
fn formatting_matches_display_after_consumed_space_is_reused() {
    let mut buffer = Buffer::with_capacity(8);
    buffer.write_str("prefix:");
    buffer.consume(7);
    buffer.write_chars(i64::MIN).unwrap();
    write!(buffer, "/{:08X}/{}", 42, -0.0).unwrap();
    assert_eq!(
        buffer.as_str().unwrap(),
        format!("{}/{:08X}/{}", i64::MIN, 42, -0.0)
    );
}

#[test]
fn formatter_errors_are_returned_and_buffer_remains_usable() {
    struct FailingDisplay;
    impl fmt::Display for FailingDisplay {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("partial")?;
            Err(fmt::Error)
        }
    }
    let mut buffer = Buffer::from("prefix:");
    assert!(buffer.write_chars(FailingDisplay).is_err());
    buffer.write_chars(42).unwrap();
    assert_eq!(buffer.as_slice(), b"prefix:partial42");
}
