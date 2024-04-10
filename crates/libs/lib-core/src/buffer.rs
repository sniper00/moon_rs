use std::fmt;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Buffer {
    rpos: usize,
    data: Vec<u8>,
}

pub const DEFAULT_RESERVE: usize = 128;

#[allow(dead_code)]
impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            data: Vec::<u8>::with_capacity(DEFAULT_RESERVE),
            rpos: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Buffer {
        Buffer {
            data: Vec::<u8>::with_capacity(capacity),
            rpos: 0,
        }
    }

    pub fn from_slice(data: &[u8]) -> Buffer {
        let mut raw = Vec::<u8>::with_capacity(data.len());
        raw.extend_from_slice(data);
        Buffer { data: raw, rpos: 0 }
    }

    pub fn write_slice(&mut self, data: &[u8]) {
        self.prepare(data.len());
        self.data.extend_from_slice(data);
    }

    pub fn write(&mut self, c: u8) {
        self.data.push(c);
    }

    pub fn unsafe_write(&mut self, c: u8) {
        unsafe {
            let len = self.data.len() + 1;
            self.data.set_len(len);
            *self.data.get_unchecked_mut(len - 1) = c;
        }
    }

    pub fn write_front(&mut self, data: &[u8]) -> bool {
        let len: usize = data.len();
        if len > self.rpos {
            return false;
        }
        self.rpos -= len;
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.data.as_mut_ptr().add(self.rpos),
                len,
            );
        }
        true
    }

    pub fn write_front_byte(&mut self, c: u8) -> bool {
        if self.rpos == 0 {
            return false;
        }
        self.rpos -= 1;
        unsafe {
            *self.data.get_unchecked_mut(self.rpos) = c;
        }
        true
    }

    pub fn write_chars<T>(&mut self, data: T)
    where
        T: ToString,
    {
        let s = data.to_string();
        self.write_slice(s.as_bytes());
    }

    pub fn write_str(&mut self, data: &str) {
        self.write_slice(data.as_bytes());
    }

    pub fn read(&mut self, count: usize) -> Option<Vec<u8>> {
        if self.data.len() < self.rpos + count {
            return None;
        }
        let mut v = Vec::with_capacity(count);
        unsafe {
            std::ptr::copy_nonoverlapping(self.data.as_ptr().add(self.rpos), v.as_mut_ptr(), count);
            v.set_len(count);
        }
        self.rpos += count;
        Some(v)
    }

    pub fn consume(&mut self, count: usize) {
        if self.data.len() < self.rpos + count {
            return;
        }
        self.rpos += count;
    }

    pub fn seek(&mut self, pos: isize) -> bool {
        if pos < 0 {
            if self.rpos < pos.unsigned_abs() {
                return false;
            }
            self.rpos -= pos.unsigned_abs();
            return true;
        }

        if self.data.len() < self.rpos + pos as usize {
            return false;
        }
        self.rpos += pos as usize;
        true
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.rpos = 0;
    }

    pub fn len(&self) -> usize {
        self.data.len() - self.rpos
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == self.rpos
    }

    pub fn prepare(&mut self, size: usize) -> &mut [u8] {
        let tail_free_space = self.data.capacity() - self.data.len();
        if tail_free_space < size {
            let count = self.data.len() - self.rpos;
            if tail_free_space + self.rpos >= size {
                unsafe {
                    if count != 0 {
                        let ptr = self.data.as_mut_ptr();
                        std::ptr::copy(ptr.add(self.rpos), ptr, count);
                    }
                    self.rpos = 0;
                    self.data.set_len(self.rpos + count);
                }
            } else {
                let required_size = self.data.len() + size;
                let mut new_vec = Vec::<u8>::with_capacity(required_size);
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.data.as_ptr(),
                        new_vec.as_mut_ptr(),
                        self.data.len(),
                    );
                    new_vec.set_len(self.data.len());
                    self.data = new_vec;
                }
            }
        }

        unsafe { std::slice::from_raw_parts_mut(self.data.as_mut_ptr().add(self.data.len()), size) }
    }

    pub fn commit(&mut self, size: usize) -> bool {
        let len = self.data.len() + size;
        if len > self.data.capacity() {
            return false;
        }

        unsafe {
            self.data.set_len(len);
        }

        true
    }

    pub fn revert(&mut self, size: usize) {
        assert!(
            self.data.len() >= self.rpos + size,
            "revert size is larger than readable size",
        );

        unsafe {
            self.data.set_len(self.data.len() - size);
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data[self.rpos..]
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.rpos) }
    }

    pub fn read_u8(&self, pos: usize) -> u8 {
        self.data[self.rpos + pos]
    }

    pub fn read_i16(&self, pos: usize, le: bool) -> i16 {
        let mut v = 0i16;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut i16 as *mut u8,
                2,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn read_u16(&self, pos: usize, le: bool) -> u16 {
        let mut v = 0u16;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut u16 as *mut u8,
                2,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn read_i32(&self, pos: usize, le: bool) -> i32 {
        let mut v = 0i32;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut i32 as *mut u8,
                4,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn read_u32(&self, pos: usize, le: bool) -> u32 {
        let mut v = 0u32;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr().add(self.rpos + pos),
                &mut v as *mut u32 as *mut u8,
                4,
            );
        }
        if le {
            v.to_le()
        } else {
            v.to_be()
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr() as *mut u8, self.len()) }
    }

    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn as_vec(&mut self) -> &Vec<u8> {
        &self.data
    }

    pub fn as_pointer(&mut self) -> *mut Buffer {
        self as *mut Buffer
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_slice()) }
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(v: Vec<u8>) -> Self {
        Buffer { data: v, rpos: 0 }
    }
}

impl From<&[u8]> for Buffer {
    fn from(v: &[u8]) -> Self {
        Buffer {
            data: v.to_vec(),
            rpos: 0,
        }
    }
}

impl From<&str> for Buffer {
    fn from(v: &str) -> Self {
        Buffer {
            data: v.as_bytes().to_vec(),
            rpos: 0,
        }
    }
}

impl From<String> for Buffer {
    fn from(v: String) -> Self {
        Buffer {
            data: v.into_bytes(),
            rpos: 0,
        }
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Buffer::new()
    }
}

impl fmt::Display for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_1() {
        let mut buf = Buffer::with_capacity(12);
        buf.commit(4);
        buf.write_str("1234567");
        buf.seek(4);
        assert_eq!(buf.write_front("bbbb".as_bytes()), true);

        let r = buf.read(8);
        log::info!("{}", String::from_utf8_lossy(r.unwrap().as_ref()));

        buf.write_str("abcd");
        assert!(buf.read(4).unwrap() == "567a".as_bytes());
        assert!(buf.len() == 3);
    }
    #[test]
    fn test_buffer_2() {
        let mut buf = Buffer::with_capacity(128);
        assert!(buf.len() == 0);
        let n: i32 = 0;
        buf.write_slice(n.to_le_bytes().as_ref());
        assert_eq!(buf.len(), 4);
        assert!(buf.read(4).unwrap() == n.to_le_bytes().as_ref());
        assert!(buf.len() == 0);
        assert!(buf.read(4).is_none());
    }

    #[test]
    fn test_buffer_3() {
        let mut buf = Buffer::with_capacity(32);
        assert!(buf.len() == 0);

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
        buffer.unsafe_write(1);
        assert_eq!(buffer.data, vec![1]);
    }

    #[test]
    fn test_write_front() {
        let mut buffer = Buffer::new();
        buffer.commit(1);
        buffer.write_slice(&[1, 2, 3, 4]);
        buffer.seek(1);
        assert!(buffer.write_front(&[0]));
        assert_eq!(buffer.data, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_write_front_byte() {
        let mut buffer = Buffer::new();
        buffer.commit(1);
        buffer.write_slice(&[1, 2, 3, 4]);
        buffer.seek(1);
        assert!(buffer.write_front_byte(0));
        assert_eq!(buffer.data, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_write_chars() {
        let mut buffer = Buffer::new();
        buffer.write_chars("hello");
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
        let _ = buffer.prepare(10);
        assert!(buffer.commit(10));
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
        buffer.as_mut_vec().push(5);
        assert_eq!(buffer.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_as_vec() {
        let mut buffer = Buffer::from_slice(&[1, 2, 3, 4]);
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
        assert_eq!(buffer.as_str(), "hello");
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
}
